const express = require('express');
const ftp = require('basic-ftp');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const AdmZip = require('adm-zip');
const cron = require('node-cron');
const { Readable, Writable } = require('stream');
require('dotenv').config();

const app = express();
app.use(express.json());

// ============================================
// CONFIGURATION
// ============================================

const config = {
    shopify: { 
        domain: process.env.SHOPIFY_DOMAIN, 
        accessToken: process.env.SHOPIFY_ACCESS_TOKEN, 
        locationId: process.env.SHOPIFY_LOCATION_ID, 
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`,
        graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01/graphql.json`
    },
    ftp: { 
        host: process.env.FTP_HOST, 
        user: process.env.FTP_USERNAME, 
        password: process.env.FTP_PASSWORD, 
        secure: false 
    },
    ralawise: { 
        zipUrl: process.env.RALAWISE_ZIP_URL, 
        maxInventory: parseInt(process.env.MAX_INVENTORY || '20') 
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, 
        chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '12') * 60 * 60 * 1000, // Increased to 12 hours
        maxErrors: parseInt(process.env.MAX_ERRORS || '100')
    },
    rateLimit: {
        requestsPerSecond: parseFloat(process.env.SHOPIFY_RATE_LIMIT || '2'),
        burstSize: parseInt(process.env.SHOPIFY_BURST_SIZE || '40'),
        useGraphQL: process.env.USE_GRAPHQL === 'true' // New option for GraphQL
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { 
    console.error('Missing required environment variables.'); 
    process.exit(1); 
}

// ============================================
// RATE LIMITING SYSTEM
// ============================================

class RateLimiter {
    constructor(requestsPerSecond = 2, burstSize = 40) {
        this.requestsPerSecond = requestsPerSecond;
        this.burstSize = burstSize;
        this.tokens = burstSize;
        this.lastRefill = Date.now();
        this.queue = [];
        this.processing = false;
    }

    async acquire() {
        return new Promise((resolve) => {
            this.queue.push(resolve);
            this.process();
        });
    }

    async process() {
        if (this.processing) return;
        this.processing = true;

        while (this.queue.length > 0) {
            // Refill tokens
            const now = Date.now();
            const timePassed = (now - this.lastRefill) / 1000;
            const tokensToAdd = timePassed * this.requestsPerSecond;
            this.tokens = Math.min(this.burstSize, this.tokens + tokensToAdd);
            this.lastRefill = now;

            if (this.tokens >= 1) {
                this.tokens--;
                const resolve = this.queue.shift();
                resolve();
            } else {
                // Wait until we have at least one token
                const waitTime = (1 / this.requestsPerSecond) * 1000;
                await new Promise(r => setTimeout(r, waitTime));
            }
        }

        this.processing = false;
    }
}

const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);

// ============================================
// STATE MANAGEMENT
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];

// Progress tracking for long-running operations
let syncProgress = {
    inventory: {
        isActive: false,
        current: 0,
        total: 0,
        startTime: null,
        lastUpdate: null,
        estimatedCompletion: null,
        cancelled: false,
        errors: 0,
        updated: 0,
        tagged: 0,
        skipped: 0,
        rateLimitHits: 0
    },
    fullImport: {
        isActive: false,
        current: 0,
        total: 0,
        startTime: null,
        lastUpdate: null,
        estimatedCompletion: null,
        cancelled: false,
        errors: 0,
        created: 0,
        discontinued: 0
    }
};

function loadHistory() { 
    try { 
        if (fs.existsSync(HISTORY_FILE)) { 
            runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); 
            addLog(`Loaded ${runHistory.length} historical run records.`, 'info'); 
        } 
    } catch (e) { 
        addLog(`Could not load history: ${e.message}`, 'warning'); 
    } 
}

function saveHistory() { 
    try { 
        if (runHistory.length > 100) { 
            runHistory = runHistory.slice(0, 100); 
        } 
        fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); 
    } catch (e) { 
        addLog(`Could not save history: ${e.message}`, 'warning'); 
    } 
}

function addToHistory(runData) { 
    runHistory.unshift(runData); 
    if(runHistory.length > 100) runHistory.pop(); 
    saveHistory(); 
}

function checkPauseStateOnStartup() { 
    if (fs.existsSync(PAUSE_LOCK_FILE)) { 
        isSystemPaused = true; 
        addLog('System is PAUSED (found lock file on startup).', 'warning'); 
    } 
    loadHistory(); 
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') { 
    const log = { timestamp: new Date().toISOString(), message, type }; 
    logs.unshift(log); 
    if (logs.length > 500) logs = logs.slice(0, 500); 
    console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); 
}

async function notifyTelegram(message) { 
    if (!config.telegram.botToken || !config.telegram.chatId) return; 
    try { 
        if (message.length > 4096) message = message.substring(0, 4086) + '...'; 
        await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { 
            chat_id: config.telegram.chatId, 
            text: `🏪 Ralawise Sync\n${message}`, 
            parse_mode: 'HTML' 
        }, { timeout: 10000 }); 
    } catch (error) { 
        addLog(`Telegram notification failed: ${error.message}`, 'warning'); 
    } 
}

function delay(ms) { 
    return new Promise(resolve => setTimeout(resolve, ms)); 
}

function applyRalawisePricing(price) { 
    if (typeof price !== 'number' || price < 0) return '0.00'; 
    let p; 
    if (price <= 6) p = price * 2.1; 
    else if (price <= 11) p = price * 1.9; 
    else p = price * 1.75; 
    return p.toFixed(2); 
}

function triggerFailsafe(reason) { 
    if (failsafe.isTriggered) return; 
    failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString(), details: {} }; 
    const msg = `🚨 HARD FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`; 
    addLog(msg, 'error'); 
    notifyTelegram(msg); 
    isRunning.inventory = false; 
    isRunning.fullImport = false; 
}

function requestConfirmation(jobKey, message, details, proceedAction) { 
    confirmation = { 
        isAwaiting: true, 
        message, 
        details, 
        proceedAction, 
        abortAction: () => { 
            addLog(`User aborted '${message}'. System is now paused for safety.`, 'warning'); 
            isSystemPaused = true; 
            fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); 
            notifyTelegram(`🙅‍♂️ User ABORTED operation: ${message}.\n\nSystem has been automatically paused.`); 
        }, 
        jobKey 
    }; 
    const alertMsg = `🤔 CONFIRMATION REQUIRED 🤔\n\n<b>Action Paused:</b> ${message}`; 
    addLog(alertMsg, 'warning'); 
    let debugMsg = ''; 
    if (details.inventoryChange) { 
        debugMsg = `\n\n<b>Details:</b>\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; 
    } 
    notifyTelegram(alertMsg + debugMsg); 
}

function resetConfirmationState() {
    confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
}

function updateProgress(type, current, total) {
    const progress = syncProgress[type];
    progress.current = current;
    progress.total = total;
    progress.lastUpdate = Date.now();
    
    if (progress.startTime && current > 0) {
        const elapsed = Date.now() - progress.startTime;
        const itemsPerMs = current / elapsed;
        const remaining = total - current;
        const estimatedMs = remaining / itemsPerMs;
        progress.estimatedCompletion = new Date(Date.now() + estimatedMs);
    }
}

const shopifyClient = axios.create({ 
    baseURL: config.shopify.baseUrl, 
    headers: { 
        'X-Shopify-Access-Token': config.shopify.accessToken, 
        'Content-Type': 'application/json' 
    }, 
    timeout: 60000 
});

// OPTIMIZED: Better rate limit handling
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) {
    let lastError = null;
    
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            // Wait for rate limiter
            await rateLimiter.acquire();
            
            let response;
            switch (method.toLowerCase()) {
                case 'get': response = await shopifyClient.get(url); break;
                case 'post': response = await shopifyClient.post(url, data); break;
                case 'put': response = await shopifyClient.put(url, data); break;
            }
            
            return response;
            
        } catch (error) {
            lastError = error;
            
            if (error.response && error.response.status === 429) {
                syncProgress.inventory.rateLimitHits++;
                const retryAfter = parseInt(error.response.headers['retry-after'] || '2') * 1000;
                const waitTime = Math.min(retryAfter * Math.pow(1.5, attempt), 30000); // Max 30 seconds
                
                if (attempt === 0) { // Only log first rate limit hit
                    addLog(`Rate limit hit. Waiting ${(waitTime/1000).toFixed(1)}s...`, 'debug');
                }
                
                await delay(waitTime);
            } else if (error.response && error.response.status >= 500) {
                // Server error, retry with exponential backoff
                const waitTime = Math.min(1000 * Math.pow(2, attempt), 10000);
                await delay(waitTime);
            } else {
                // Client error, don't retry
                throw error;
            }
        }
    }
    
    throw lastError || new Error(`Failed after ${retries} attempts`);
}

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

async function fetchInventoryFromFTP() { 
    const client = new ftp.Client(); 
    client.ftp.verbose = false; 
    try { 
        addLog('Connecting to FTP...', 'info'); 
        await client.access(config.ftp); 
        const chunks = []; 
        await client.downloadTo(new Writable({ 
            write(c, e, cb) { chunks.push(c); cb(); } 
        }), '/Stock/Stock_Update.csv'); 
        const buffer = Buffer.concat(chunks); 
        addLog(`FTP download successful, ${buffer.length} bytes`, 'success'); 
        return Readable.from(buffer); 
    } catch (e) { 
        addLog(`FTP error: ${e.message}`, 'error'); 
        throw e; 
    } finally { 
        client.close(); 
    } 
}

async function parseInventoryCSV(stream) { 
    return new Promise((resolve, reject) => { 
        const inventory = new Map(); 
        stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 }))
            .on('data', row => { 
                if (row.SKU && row.SKU !== 'SKU') 
                    inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); 
            })
            .on('end', () => resolve(inventory))
            .on('error', reject); 
    }); 
}

async function getAllShopifyProducts() {
    let allProducts = [];
    let pageCount = 0;
    let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`;
    addLog('Fetching all products from Shopify...', 'info');
    
    while (url) {
        try {
            pageCount++;
            const res = await shopifyRequestWithRetry('get', url);
            const productsOnPage = res.data.products;
            
            if (productsOnPage && productsOnPage.length > 0) {
                allProducts.push(...productsOnPage);
                
                // Log less frequently
                if (pageCount % 5 === 0) {
                    addLog(`Fetched ${pageCount} pages: ${allProducts.length} products total`, 'info');
                }
            } else {
                break;
            }
            
            const linkHeader = res.headers.link;
            const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null;
            
            if (nextLinkMatch && nextLinkMatch[1]) {
                const fullNextUrl = nextLinkMatch[1];
                url = fullNextUrl.replace(config.shopify.baseUrl, '');
            } else {
                url = null;
            }
            
        } catch (error) {
            addLog(`Error fetching page ${pageCount}: ${error.message}`, 'error');
            triggerFailsafe(`Failed to fetch products from Shopify`);
            return [];
        }
    }
    
    addLog(`Completed fetching ${allProducts.length} products from Shopify.`, 'success');
    return allProducts;
}

// OPTIMIZED: Batch inventory updates using GraphQL
async function updateInventoryBatchGraphQL(updates) {
    const BATCH_SIZE = 100; // GraphQL can handle larger batches
    const batches = [];
    
    for (let i = 0; i < updates.length; i += BATCH_SIZE) {
        batches.push(updates.slice(i, i + BATCH_SIZE));
    }
    
    let totalUpdated = 0;
    
    for (const batch of batches) {
        const mutation = `
            mutation inventoryBulkAdjustQuantityAtLocation($inventoryItemAdjustments: [InventoryAdjustItemInput!]!, $locationId: ID!) {
                inventoryBulkAdjustQuantityAtLocation(inventoryItemAdjustments: $inventoryItemAdjustments, locationId: $locationId) {
                    inventoryLevels {
                        id
                        available
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
        `;
        
        const variables = {
            locationId: `gid://shopify/Location/${config.shopify.locationId}`,
            inventoryItemAdjustments: batch.map(u => ({
                inventoryItemId: `gid://shopify/InventoryItem/${u.match.variant.inventory_item_id}`,
                availableDelta: u.newQty - (u.match.variant.inventory_quantity || 0)
            }))
        };
        
        try {
            await rateLimiter.acquire();
            const response = await axios.post(config.shopify.graphqlUrl, {
                query: mutation,
                variables
            }, {
                headers: {
                    'X-Shopify-Access-Token': config.shopify.accessToken,
                    'Content-Type': 'application/json'
                }
            });
            
            if (response.data.data.inventoryBulkAdjustQuantityAtLocation.userErrors.length > 0) {
                addLog(`GraphQL errors: ${JSON.stringify(response.data.data.inventoryBulkAdjustQuantityAtLocation.userErrors)}`, 'warning');
            } else {
                totalUpdated += batch.length;
            }
        } catch (error) {
            addLog(`Failed to update batch: ${error.message}`, 'error');
        }
    }
    
    return totalUpdated;
}

// OPTIMIZED: Smarter inventory update with less API calls
async function updateInventoryBySKU(inventoryMap) { 
    if (isRunning.inventory) { 
        addLog('Inventory update already running.', 'warning'); 
        return; 
    } 
    
    isRunning.inventory = true;
    
    // Reset progress
    syncProgress.inventory = {
        isActive: true,
        current: 0,
        total: 0,
        startTime: Date.now(),
        lastUpdate: Date.now(),
        estimatedCompletion: null,
        cancelled: false,
        errors: 0,
        updated: 0,
        tagged: 0,
        skipped: 0,
        rateLimitHits: 0
    };
    
    let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0, skipped: 0 };
    let timeoutCheck = null;
    
    try { 
        addLog('=== STARTING INVENTORY ANALYSIS ===', 'info');
        addLog(`Using ${config.rateLimit.useGraphQL ? 'GraphQL' : 'REST API'} for updates`, 'info');
        
        // Set up timeout mechanism
        timeoutCheck = setInterval(() => {
            if (Date.now() - syncProgress.inventory.startTime > config.failsafe.maxRuntime) {
                addLog(`Inventory sync timeout reached (${config.failsafe.maxRuntime / 1000 / 60 / 60} hours). Stopping...`, 'error');
                syncProgress.inventory.cancelled = true;
            }
        }, 60000);
        
        const shopifyProducts = await getAllShopifyProducts();
        
        // Build maps for efficient lookups
        const skuToProduct = new Map();
        const productsNeedingTags = new Set();
        const variantsNeedingManagement = [];
        
        shopifyProducts.forEach(p => {
            const hasRalawiseTag = p.tags?.includes('Supplier:Ralawise');
            
            p.variants?.forEach(v => {
                if (v.sku) {
                    const key = v.sku.toUpperCase();
                    skuToProduct.set(key, { product: p, variant: v });
                    
                    if (!hasRalawiseTag) {
                        productsNeedingTags.add(p.id);
                    }
                    
                    if (!v.inventory_management && inventoryMap.has(key)) {
                        variantsNeedingManagement.push(v);
                    }
                }
            });
        });
        
        // Find inventory updates needed
        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match) {
                const currentQty = match.variant.inventory_quantity || 0;
                if (currentQty !== newQty) {
                    updatesToPerform.push({ sku, oldQty: currentQty, newQty, match });
                } else {
                    syncProgress.inventory.skipped++;
                    runResult.skipped++;
                }
            }
        });
        
        syncProgress.inventory.total = updatesToPerform.length + productsNeedingTags.size + variantsNeedingManagement.length;
        
        const totalProducts = skuToProduct.size;
        const updatesNeeded = updatesToPerform.length;
        const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0;
        
        addLog(`Analysis complete:`, 'info');
        addLog(`- ${updatesNeeded} inventory updates needed`, 'info');
        addLog(`- ${productsNeedingTags.size} products need tags`, 'info');
        addLog(`- ${variantsNeedingManagement.length} variants need inventory management`, 'info');
        addLog(`- ${runResult.skipped} SKUs already up-to-date (skipped)`, 'info');
        addLog(`- Change percentage: ${changePercentage.toFixed(2)}%`, 'info');
        
        const executeUpdates = async () => {
            addLog(`Starting optimized update process...`, 'info');
            
            // Step 1: Tag products in bulk (one API call per product that needs it)
            if (productsNeedingTags.size > 0) {
                addLog(`Tagging ${productsNeedingTags.size} products...`, 'info');
                for (const productId of productsNeedingTags) {
                    if (syncProgress.inventory.cancelled) break;
                    
                    try {
                        const product = shopifyProducts.find(p => p.id === productId);
                        await shopifyRequestWithRetry('put', `/products/${productId}.json`, {
                            product: {
                                id: productId,
                                tags: `${product.tags || ''},Supplier:Ralawise`.replace(/^,/, '')
                            }
                        });
                        syncProgress.inventory.tagged++;
                        runResult.tagged++;
                    } catch (e) {
                        addLog(`Failed to tag product ${productId}: ${e.message}`, 'error');
                        syncProgress.inventory.errors++;
                    }
                }
            }
            
            // Step 2: Enable inventory management for variants that need it
            if (variantsNeedingManagement.length > 0) {
                addLog(`Enabling inventory management for ${variantsNeedingManagement.length} variants...`, 'info');
                for (const variant of variantsNeedingManagement) {
                    if (syncProgress.inventory.cancelled) break;
                    
                    try {
                        await shopifyRequestWithRetry('put', `/variants/${variant.id}.json`, {
                            variant: {
                                id: variant.id,
                                inventory_management: 'shopify',
                                inventory_policy: 'deny'
                            }
                        });
                    } catch (e) {
                        addLog(`Failed to enable inventory management for variant ${variant.id}: ${e.message}`, 'error');
                    }
                }
            }
            
            // Step 3: Update inventory levels
            if (config.rateLimit.useGraphQL && updatesToPerform.length > 100) {
                // Use GraphQL for large batches
                addLog(`Using GraphQL batch update for ${updatesToPerform.length} items...`, 'info');
                const updated = await updateInventoryBatchGraphQL(updatesToPerform);
                syncProgress.inventory.updated += updated;
                runResult.updated += updated;
            } else {
                // Use REST API with optimizations
                addLog(`Updating ${updatesToPerform.length} inventory levels...`, 'info');
                
                // Process in larger batches with better rate limiting
                const BATCH_SIZE = 50; // Larger batches since we have rate limiting
                for (let i = 0; i < updatesToPerform.length; i += BATCH_SIZE) {
                    if (syncProgress.inventory.cancelled) break;
                    
                    const batch = updatesToPerform.slice(i, i + BATCH_SIZE);
                    
                    // Process batch items in parallel (with rate limiting)
                    const promises = batch.map(async (u) => {
                        try {
                            // Single API call to set inventory
                            await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                location_id: config.shopify.locationId,
                                inventory_item_id: u.match.variant.inventory_item_id,
                                available: u.newQty
                            });
                            
                            syncProgress.inventory.updated++;
                            runResult.updated++;
                            
                            return { success: true };
                        } catch (e) {
                            // Try connect first if set fails
                            if (e.response && e.response.status === 422) {
                                try {
                                    await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', {
                                        location_id: config.shopify.locationId,
                                        inventory_item_id: u.match.variant.inventory_item_id
                                    });
                                    
                                    await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                        location_id: config.shopify.locationId,
                                        inventory_item_id: u.match.variant.inventory_item_id,
                                        available: u.newQty
                                    });
                                    
                                    syncProgress.inventory.updated++;
                                    runResult.updated++;
                                    return { success: true };
                                } catch (retryError) {
                                    syncProgress.inventory.errors++;
                                    runResult.errors++;
                                    return { success: false, error: retryError.message, sku: u.sku };
                                }
                            } else {
                                syncProgress.inventory.errors++;
                                runResult.errors++;
                                return { success: false, error: e.message, sku: u.sku };
                            }
                        }
                    });
                    
                    const results = await Promise.all(promises);
                    const failures = results.filter(r => !r.success);
                    
                    if (failures.length > 0) {
                        addLog(`Batch had ${failures.length} failures`, 'warning');
                    }
                    
                    // Update progress
                    updateProgress('inventory', syncProgress.inventory.updated, syncProgress.inventory.total);
                    
                    // Log progress every 100 items or 5%
                    const progressPercent = (syncProgress.inventory.updated / updatesToPerform.length) * 100;
                    if (syncProgress.inventory.updated % 100 === 0 || progressPercent % 5 < 0.1) {
                        const eta = syncProgress.inventory.estimatedCompletion ? 
                            syncProgress.inventory.estimatedCompletion.toLocaleTimeString() : 'calculating...';
                        
                        const progressMsg = `Progress: ${syncProgress.inventory.updated}/${updatesToPerform.length} (${progressPercent.toFixed(1)}%) - ETA: ${eta} - Rate limits: ${syncProgress.inventory.rateLimitHits}`;
                        addLog(progressMsg, 'info');
                        
                        // Telegram update every 1000 items
                        if (syncProgress.inventory.updated % 1000 === 0) {
                            notifyTelegram(`📊 Inventory Update Progress:\n${progressMsg}`);
                        }
                    }
                    
                    // Check for too many errors
                    if (syncProgress.inventory.errors > config.failsafe.maxErrors) {
                        addLog(`Too many errors (>${config.failsafe.maxErrors}), stopping`, 'error');
                        syncProgress.inventory.cancelled = true;
                        break;
                    }
                }
            }
            
            const notFound = inventoryMap.size - updatesToPerform.length;
            runResult.status = syncProgress.inventory.cancelled ? 'cancelled' : 'completed';
            
            const runtime = ((Date.now() - syncProgress.inventory.startTime) / 1000 / 60).toFixed(1);
            const finalMsg = `Inventory update ${runResult.status} in ${runtime} minutes:\n✅ ${runResult.updated} updated\n🏷️ ${runResult.tagged} tagged\n⏭️ ${runResult.skipped} skipped\n❌ ${runResult.errors} errors\n❓ ${notFound} not found\n🚦 ${syncProgress.inventory.rateLimitHits} rate limits hit`;
            notifyTelegram(finalMsg);
            addLog(finalMsg, runResult.status === 'completed' ? 'success' : 'warning');
        };
        
        // Check if confirmation needed
        if (changePercentage > config.failsafe.inventoryChangePercentage) {
            requestConfirmation('inventory', 'High inventory change detected', {
                inventoryChange: {
                    threshold: config.failsafe.inventoryChangePercentage,
                    actualPercentage: changePercentage,
                    updatesNeeded,
                    totalProducts,
                    sample: updatesToPerform.slice(0, 10).map(u => ({
                        sku: u.sku,
                        oldQty: u.oldQty,
                        newQty: u.newQty
                    }))
                }
            }, async () => {
                try {
                    await executeUpdates();
                } finally {
                    if (timeoutCheck) clearInterval(timeoutCheck);
                    syncProgress.inventory.isActive = false;
                    isRunning.inventory = false;
                    addToHistory({...runResult, timestamp: new Date().toISOString() });
                }
            });
            return;
        } else {
            await executeUpdates();
        }
        
    } catch (error) {
        addLog(`Inventory update failed critically: ${error.message}`, 'error');
        runResult.errors++;
        triggerFailsafe(`Inventory update failed critically: ${error.message}`);
    } finally {
        if (timeoutCheck) clearInterval(timeoutCheck);
        
        if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') {
            syncProgress.inventory.isActive = false;
            isRunning.inventory = false;
            addToHistory({...runResult, timestamp: new Date().toISOString() });
        }
    }
}

// [Rest of the code remains the same - parseShopifyCSV, processFullImport, syncInventory, syncFullCatalog, web interface, API endpoints, etc.]
// ... Continue with the rest of your existing code from line 500 onwards ...
