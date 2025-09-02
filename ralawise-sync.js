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
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
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
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '12') * 60 * 60 * 1000,
        maxErrors: parseInt(process.env.MAX_ERRORS || '1000')
    },
    rateLimit: {
        requestsPerSecond: parseFloat(process.env.SHOPIFY_RATE_LIMIT || '2'),
        burstSize: parseInt(process.env.SHOPIFY_BURST_SIZE || '40')
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
const ERROR_TRACKING_FILE = path.join(__dirname, '_error_tracking.json');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let errorTracking = {
    errors: [],
    weeklyReport: null,
    lastReportDate: null
};

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
        notFound: 0,
        rateLimitHits: 0,
        actuallyChanged: 0,
        failedUpdates: []
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

// ============================================
// ERROR TRACKING SYSTEM
// ============================================

function loadErrorTracking() {
    try {
        if (fs.existsSync(ERROR_TRACKING_FILE)) {
            errorTracking = JSON.parse(fs.readFileSync(ERROR_TRACKING_FILE, 'utf8'));
            // Clean old errors (older than 30 days)
            const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
            errorTracking.errors = errorTracking.errors.filter(e => new Date(e.timestamp) > thirtyDaysAgo);
            addLog(`Loaded ${errorTracking.errors.length} error records.`, 'info');
        }
    } catch (e) {
        addLog(`Could not load error tracking: ${e.message}`, 'warning');
    }
}

function saveErrorTracking() {
    try {
        // Keep only last 10000 errors
        if (errorTracking.errors.length > 10000) {
            errorTracking.errors = errorTracking.errors.slice(-10000);
        }
        fs.writeFileSync(ERROR_TRACKING_FILE, JSON.stringify(errorTracking, null, 2));
    } catch (e) {
        addLog(`Could not save error tracking: ${e.message}`, 'warning');
    }
}

function trackError(type, sku, errorMessage, details = {}) {
    const error = {
        timestamp: new Date().toISOString(),
        type,
        sku,
        message: errorMessage,
        details
    };
    errorTracking.errors.push(error);
    
    // Save every 100 errors
    if (errorTracking.errors.length % 100 === 0) {
        saveErrorTracking();
    }
}

function generateWeeklyErrorReport() {
    const oneWeekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
    const weeklyErrors = errorTracking.errors.filter(e => new Date(e.timestamp) > oneWeekAgo);
    
    // Group errors by type and message
    const errorGroups = {};
    weeklyErrors.forEach(error => {
        const key = `${error.type}:${error.message}`;
        if (!errorGroups[key]) {
            errorGroups[key] = {
                type: error.type,
                message: error.message,
                count: 0,
                sampleSKUs: [],
                firstSeen: error.timestamp,
                lastSeen: error.timestamp
            };
        }
        errorGroups[key].count++;
        errorGroups[key].lastSeen = error.timestamp;
        if (errorGroups[key].sampleSKUs.length < 5 && error.sku) {
            errorGroups[key].sampleSKUs.push(error.sku);
        }
    });
    
    // Sort by frequency
    const sortedErrors = Object.values(errorGroups).sort((a, b) => b.count - a.count);
    
    const report = {
        generatedAt: new Date().toISOString(),
        periodStart: new Date(oneWeekAgo).toISOString(),
        periodEnd: new Date().toISOString(),
        totalErrors: weeklyErrors.length,
        uniqueErrorTypes: sortedErrors.length,
        topErrors: sortedErrors.slice(0, 10),
        errorsByType: {
            inventory: weeklyErrors.filter(e => e.type === 'inventory').length,
            connection: weeklyErrors.filter(e => e.type === 'connection').length,
            validation: weeklyErrors.filter(e => e.type === 'validation').length,
            api: weeklyErrors.filter(e => e.type === 'api').length,
            other: weeklyErrors.filter(e => !['inventory', 'connection', 'validation', 'api'].includes(e.type)).length
        }
    };
    
    errorTracking.weeklyReport = report;
    errorTracking.lastReportDate = new Date().toISOString();
    saveErrorTracking();
    
    return report;
}

function formatErrorReportForTelegram(report) {
    let message = `📊 <b>Weekly Error Report</b>\n`;
    message += `Period: ${new Date(report.periodStart).toLocaleDateString()} - ${new Date(report.periodEnd).toLocaleDateString()}\n\n`;
    message += `📈 <b>Summary:</b>\n`;
    message += `• Total Errors: ${report.totalErrors}\n`;
    message += `• Unique Error Types: ${report.uniqueErrorTypes}\n\n`;
    message += `📋 <b>Errors by Category:</b>\n`;
    message += `• Inventory: ${report.errorsByType.inventory}\n`;
    message += `• Connection: ${report.errorsByType.connection}\n`;
    message += `• Validation: ${report.errorsByType.validation}\n`;
    message += `• API: ${report.errorsByType.api}\n`;
    message += `• Other: ${report.errorsByType.other}\n\n`;
    
    if (report.topErrors.length > 0) {
        message += `🔝 <b>Top 5 Most Common Errors:</b>\n`;
        report.topErrors.slice(0, 5).forEach((error, index) => {
            message += `\n${index + 1}. <b>${error.type}</b> (${error.count} times)\n`;
            message += `   ${error.message.substring(0, 100)}\n`;
            if (error.sampleSKUs.length > 0) {
                message += `   Sample SKUs: ${error.sampleSKUs.slice(0, 3).join(', ')}\n`;
            }
        });
    }
    
    message += `\n💡 <b>Action Required:</b>\n`;
    message += `Review these errors for your monthly patch update.\n`;
    message += `Full report available on the dashboard.`;
    
    return message;
}

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
    loadErrorTracking();
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

async function shopifyRequestWithRetry(method, url, data = null, retries = 5) {
    let lastError = null;
    
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
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
                const waitTime = Math.min(retryAfter * Math.pow(1.5, attempt), 30000);
                
                if (attempt === 0) {
                    addLog(`Rate limit hit. Waiting ${(waitTime/1000).toFixed(1)}s...`, 'debug');
                }
                
                await delay(waitTime);
            } else if (error.response && error.response.status >= 500) {
                const waitTime = Math.min(1000 * Math.pow(2, attempt), 10000);
                await delay(waitTime);
            } else {
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
        trackError('connection', null, `FTP connection failed: ${e.message}`);
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
            trackError('api', null, `Failed to fetch products page ${pageCount}: ${error.message}`);
            triggerFailsafe(`Failed to fetch products from Shopify`);
            return [];
        }
    }
    
    addLog(`Completed fetching ${allProducts.length} products from Shopify.`, 'success');
    return allProducts;
}

// FIXED: Verify inventory updates actually work
async function verifyAndUpdateInventory(variantId, inventoryItemId, sku, newQty, locationId) {
    try {
        // First, try to get current inventory level
        let currentLevel = null;
        try {
            const levelResponse = await shopifyRequestWithRetry('get', 
                `/inventory_levels.json?inventory_item_ids=${inventoryItemId}&location_ids=${locationId}`);
            if (levelResponse.data.inventory_levels && levelResponse.data.inventory_levels.length > 0) {
                currentLevel = levelResponse.data.inventory_levels[0].available;
            }
        } catch (e) {
            // Inventory level might not exist yet
        }
        
        // If no level exists, connect first
        if (currentLevel === null) {
            try {
                await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', {
                    location_id: locationId,
                    inventory_item_id: inventoryItemId
                });
                addLog(`Connected inventory for ${sku}`, 'debug');
            } catch (e) {
                if (!e.response || e.response.status !== 422) {
                    throw e;
                }
                // Already connected, continue
            }
        }
        
        // Now set the inventory level
        await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
            location_id: locationId,
            inventory_item_id: inventoryItemId,
            available: newQty
        });
        
        // Verify the update worked
        const verifyResponse = await shopifyRequestWithRetry('get', 
            `/inventory_levels.json?inventory_item_ids=${inventoryItemId}&location_ids=${locationId}`);
        
        if (verifyResponse.data.inventory_levels && verifyResponse.data.inventory_levels.length > 0) {
            const actualQty = verifyResponse.data.inventory_levels[0].available;
            if (actualQty === newQty) {
                return { success: true, verified: true };
            } else {
                trackError('inventory', sku, `Verification failed: Set ${newQty} but got ${actualQty}`, {
                    expected: newQty,
                    actual: actualQty,
                    inventoryItemId
                });
                return { success: false, error: `Verification failed: expected ${newQty}, got ${actualQty}` };
            }
        }
        
        return { success: true, verified: false };
        
    } catch (error) {
        const errorMsg = error.response?.data?.errors || error.message;
        trackError('inventory', sku, `Failed to update inventory: ${errorMsg}`, {
            inventoryItemId,
            newQty,
            statusCode: error.response?.status
        });
        return { success: false, error: errorMsg };
    }
}

async function updateInventoryBySKU(inventoryMap) { 
    if (isRunning.inventory) { 
        addLog('Inventory update already running.', 'warning'); 
        return; 
    } 
    
    isRunning.inventory = true;
    
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
        notFound: 0,
        rateLimitHits: 0,
        actuallyChanged: 0,
        failedUpdates: []
    };
    
    let runResult = { 
        type: 'Inventory', 
        status: 'failed', 
        updated: 0, 
        tagged: 0, 
        errors: 0, 
        skipped: 0, 
        notFound: 0,
        actuallyChanged: 0 
    };
    let timeoutCheck = null;
    
    try { 
        addLog('=== STARTING INVENTORY ANALYSIS ===', 'info');
        addLog(`FTP inventory has ${inventoryMap.size} SKUs`, 'info');
        
        timeoutCheck = setInterval(() => {
            if (Date.now() - syncProgress.inventory.startTime > config.failsafe.maxRuntime) {
                addLog(`Inventory sync timeout reached (${config.failsafe.maxRuntime / 1000 / 60 / 60} hours). Stopping...`, 'error');
                syncProgress.inventory.cancelled = true;
            }
        }, 60000);
        
        const shopifyProducts = await getAllShopifyProducts();
        
        const skuToProduct = new Map();
        const productsNeedingTags = new Map();
        const variantsNeedingManagement = [];
        const skusInShopify = new Set();
        
        shopifyProducts.forEach(p => {
            const hasRalawiseTag = p.tags?.includes('Supplier:Ralawise');
            
            p.variants?.forEach(v => {
                if (v.sku) {
                    const key = v.sku.toUpperCase();
                    skusInShopify.add(key);
                    skuToProduct.set(key, { product: p, variant: v });
                    
                    if (!hasRalawiseTag && inventoryMap.has(key)) {
                        productsNeedingTags.set(p.id, p);
                    }
                    
                    if (!v.inventory_management && inventoryMap.has(key)) {
                        variantsNeedingManagement.push(v);
                    }
                }
            });
        });
        
        const updatesToPerform = [];
        let notFoundCount = 0;
        const notFoundSKUs = [];
        
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
            } else {
                notFoundCount++;
                syncProgress.inventory.notFound++;
                runResult.notFound++;
                if (notFoundSKUs.length < 10) {
                    notFoundSKUs.push(sku);
                }
            }
        });
        
        syncProgress.inventory.total = updatesToPerform.length;
        
        const totalProducts = skuToProduct.size;
        const updatesNeeded = updatesToPerform.length;
        const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0;
        
        addLog(`Analysis complete:`, 'info');
        addLog(`- ${updatesNeeded} inventory updates needed`, 'info');
        addLog(`- ${productsNeedingTags.size} products need tags`, 'info');
        addLog(`- ${variantsNeedingManagement.length} variants need inventory management`, 'info');
        addLog(`- ${runResult.skipped} SKUs already up-to-date (skipped)`, 'info');
        addLog(`- ${notFoundCount} SKUs from FTP not found in Shopify`, 'warning');
        addLog(`- Change percentage: ${changePercentage.toFixed(2)}%`, 'info');
        
        if (notFoundSKUs.length > 0) {
            addLog(`Sample of SKUs not found: ${notFoundSKUs.join(', ')}`, 'warning');
            notFoundSKUs.forEach(sku => {
                trackError('validation', sku, 'SKU not found in Shopify', { source: 'FTP' });
            });
        }
        
        const executeUpdates = async () => {
            addLog(`Starting optimized update process...`, 'info');
            
            // Step 1: Tag products
            if (productsNeedingTags.size > 0) {
                addLog(`Tagging ${productsNeedingTags.size} products...`, 'info');
                for (const [productId, product] of productsNeedingTags) {
                    if (syncProgress.inventory.cancelled) break;
                    
                    try {
                        await shopifyRequestWithRetry('put', `/products/${productId}.json`, {
                            product: {
                                id: productId,
                                tags: `${product.tags || ''},Supplier:Ralawise`.replace(/^,/, '')
                            }
                        });
                        syncProgress.inventory.tagged++;
                        runResult.tagged++;
                    } catch (e) {
                        const errorMsg = `Failed to tag product ${productId}: ${e.message}`;
                        addLog(errorMsg, 'error');
                        trackError('api', null, errorMsg, { productId });
                        syncProgress.inventory.errors++;
                        runResult.errors++;
                    }
                }
            }
            
            // Step 2: Enable inventory management
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
                        const errorMsg = `Failed to enable inventory management for variant ${variant.id}: ${e.message}`;
                        addLog(errorMsg, 'error');
                        trackError('api', variant.sku, errorMsg, { variantId: variant.id });
                    }
                }
            }
            
            // Step 3: Update inventory levels with verification
            addLog(`Updating ${updatesToPerform.length} inventory levels with verification...`, 'info');
            
            const BATCH_SIZE = 10; // Smaller batches for better tracking
            for (let i = 0; i < updatesToPerform.length; i += BATCH_SIZE) {
                if (syncProgress.inventory.cancelled) break;
                
                const batch = updatesToPerform.slice(i, i + BATCH_SIZE);
                
                for (const u of batch) {
                    if (syncProgress.inventory.cancelled) break;
                    
                    const result = await verifyAndUpdateInventory(
                        u.match.variant.id,
                        u.match.variant.inventory_item_id,
                        u.sku,
                        u.newQty,
                        config.shopify.locationId
                    );
                    
                    if (result.success) {
                        syncProgress.inventory.updated++;
                        runResult.updated++;
                        if (result.verified) {
                            syncProgress.inventory.actuallyChanged++;
                            runResult.actuallyChanged++;
                        }
                        addLog(`✅ Updated ${u.sku}: ${u.oldQty} → ${u.newQty} ${result.verified ? '(verified)' : ''}`, 'success');
                    } else {
                        syncProgress.inventory.errors++;
                        runResult.errors++;
                        syncProgress.inventory.failedUpdates.push({
                            sku: u.sku,
                            error: result.error
                        });
                        addLog(`❌ Failed to update ${u.sku}: ${result.error}`, 'error');
                    }
                }
                
                // Update progress
                updateProgress('inventory', syncProgress.inventory.updated + syncProgress.inventory.errors, updatesToPerform.length);
                
                // Log progress every 50 items
                if ((syncProgress.inventory.updated + syncProgress.inventory.errors) % 50 === 0) {
                    const processed = syncProgress.inventory.updated + syncProgress.inventory.errors;
                    const progressPercent = (processed / updatesToPerform.length) * 100;
                    const eta = syncProgress.inventory.estimatedCompletion ? 
                        syncProgress.inventory.estimatedCompletion.toLocaleTimeString() : 'calculating...';
                    
                    const progressMsg = `Progress: ${processed}/${updatesToPerform.length} (${progressPercent.toFixed(1)}%)\n` +
                                      `✅ Updated: ${syncProgress.inventory.updated}\n` +
                                      `✓ Verified: ${syncProgress.inventory.actuallyChanged}\n` +
                                      `❌ Failed: ${syncProgress.inventory.errors}\n` +
                                      `ETA: ${eta}`;
                    addLog(progressMsg, 'info');
                    
                    // Telegram update every 500 items
                    if (processed % 500 === 0) {
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
            
            // Save error tracking at the end
            saveErrorTracking();
            
            runResult.status = syncProgress.inventory.cancelled ? 'cancelled' : 'completed';
            
            const runtime = ((Date.now() - syncProgress.inventory.startTime) / 1000 / 60).toFixed(1);
            const finalMsg = `Inventory update ${runResult.status} in ${runtime} minutes:\n` +
                           `✅ ${runResult.updated} processed\n` +
                           `✓ ${runResult.actuallyChanged} verified changes\n` +
                           `🏷️ ${runResult.tagged} tagged\n` +
                           `⏭️ ${runResult.skipped} skipped (already correct)\n` +
                           `❌ ${runResult.errors} errors\n` +
                           `❓ ${runResult.notFound} SKUs not found in Shopify\n` +
                           `🚦 ${syncProgress.inventory.rateLimitHits} rate limits hit`;
            
            // Log failed updates for debugging
            if (syncProgress.inventory.failedUpdates.length > 0) {
                addLog(`Failed updates: ${syncProgress.inventory.failedUpdates.slice(0, 5).map(f => `${f.sku}: ${f.error}`).join(', ')}`, 'error');
            }
            
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
        trackError('system', null, `Critical failure: ${error.message}`);
        runResult.errors++;
        triggerFailsafe(`Inventory update failed critically: ${error.message}`);
    } finally {
        if (timeoutCheck) clearInterval(timeoutCheck);
        
        if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') {
            syncProgress.inventory.isActive = false;
            isRunning.inventory = false;
            addToHistory({...runResult, timestamp: new Date().toISOString() });
            saveErrorTracking();
        }
    }
}

async function downloadAndExtractZip() { 
    const url = `${config.ralawise.zipUrl}?t=${Date.now()}`; 
    addLog(`Downloading zip: ${url}`, 'info'); 
    const res = await axios.get(url, { responseType: 'arraybuffer', timeout: 120000 }); 
    const tempDir = path.join(__dirname, 'temp', `ralawise_${Date.now()}`); 
    fs.mkdirSync(tempDir, { recursive: true }); 
    const zipPath = path.join(tempDir, 'data.zip'); 
    fs.writeFileSync(zipPath, res.data); 
    const zip = new AdmZip(zipPath); 
    zip.extractAllTo(tempDir, true); 
    fs.unlinkSync(zipPath); 
    return { 
        tempDir, 
        csvFiles: fs.readdirSync(tempDir).filter(f => f.endsWith('.csv')).map(f => path.join(tempDir, f)) 
    }; 
}

async function parseShopifyCSV(filePath) { 
    return new Promise((resolve, reject) => { 
        const products = []; 
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', row => { 
                products.push({ 
                    ...row, 
                    price: applyRalawisePricing(parseFloat(row['Variant Price']) || 0), 
                    original_price: parseFloat(row['Variant Price']) || 0 
                }); 
            })
            .on('end', () => resolve(products))
            .on('error', reject); 
    }); 
}

async function processFullImport(csvFiles) { 
    if (isRunning.fullImport) return; 
    isRunning.fullImport = true;
    
    syncProgress.fullImport = {
        isActive: true,
        current: 0,
        total: 0,
        startTime: Date.now(),
        lastUpdate: Date.now(),
        estimatedCompletion: null,
        cancelled: false,
        errors: 0,
        created: 0,
        discontinued: 0
    };
    
    let runResult = { type: 'Full Import', status: 'failed', created: 0, discontinued: 0, errors: 0 };
    
    try { 
        addLog('=== STARTING FULL IMPORT ===', 'info');
        
        const allRows = (await Promise.all(csvFiles.map(parseShopifyCSV))).flat();
        const productsByHandle = new Map();
        
        for (const row of allRows) {
            if (!row.Handle) continue;
            if (!productsByHandle.has(row.Handle)) {
                productsByHandle.set(row.Handle, {
                    ...row,
                    tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''),
                    images: [],
                    variants: [],
                    options: []
                });
            }
            
            const p = productsByHandle.get(row.Handle);
            
            if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) {
                p.images.push({
                    src: row['Image Src'],
                    position: parseInt(row['Image Position']),
                    alt: row['Image Alt Text'] || row.Title
                });
            }
            
            if (row['Variant SKU']) {
                const v = {
                    sku: row['Variant SKU'],
                    price: row.price,
                    option1: row['Option1 Value'],
                    option2: row['Option2 Value'],
                    option3: row['Option3 Value'],
                    inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory)
                };
                p.variants.push(v);
            }
        }
        
        const shopifyProducts = await getAllShopifyProducts();
        const existingHandles = new Set(shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise')).map(p => p.handle));
        const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle));
        
        addLog(`Found ${toCreate.length} new products to create.`, 'info');
        syncProgress.fullImport.total = toCreate.length;
        
        // Create products in smaller batches
        const CREATE_LIMIT = 50; // Increase limit
        for (const p of toCreate.slice(0, CREATE_LIMIT)) {
            if (syncProgress.fullImport.cancelled) break;
            
            try {
                const res = await shopifyRequestWithRetry('post', '/products.json', {
                    product: {
                        title: p.Title,
                        handle: p.Handle,
                        body_html: p['Body (HTML)'],
                        vendor: p.Vendor,
                        product_type: p.Type,
                        tags: p.tags,
                        images: p.images,
                        variants: p.variants.map(v => ({...v, inventory_management: 'shopify', inventory_policy: 'deny'}))
                    }
                });
                
                // Set initial inventory
                for (const v of res.data.product.variants) {
                    const origV = p.variants.find(ov => ov.sku === v.sku);
                    if (origV && v.inventory_item_id && origV.inventory_quantity > 0) {
                        await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', {
                            location_id: config.shopify.locationId,
                            inventory_item_id: v.inventory_item_id
                        }).catch(() => {});
                        
                        await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                            location_id: config.shopify.locationId,
                            inventory_item_id: v.inventory_item_id,
                            available: origV.inventory_quantity
                        });
                    }
                }
                
                syncProgress.fullImport.created++;
                runResult.created++;
                addLog(`✅ Created: ${p.Title}`, 'success');
                
                updateProgress('fullImport', syncProgress.fullImport.created, syncProgress.fullImport.total);
                
            } catch(e) {
                syncProgress.fullImport.errors++;
                runResult.errors++;
                const errorMsg = `Failed to create ${p.Title}: ${e.message}`;
                addLog(errorMsg, 'error');
                trackError('api', p.Handle, errorMsg);
            }
        }
        
        // Handle discontinued products
        const newHandles = new Set(Array.from(productsByHandle.keys()));
        const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle));
        
        addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info');
        
        for (const p of toDiscontinue.slice(0, 100)) { // Increase limit
            if (syncProgress.fullImport.cancelled) break;
            
            try {
                await shopifyRequestWithRetry('put', `/products/${p.id}.json`, {
                    product: { id: p.id, status: 'draft' }
                });
                
                for (const v of p.variants) {
                    if (v.inventory_item_id) {
                        await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                            location_id: config.shopify.locationId,
                            inventory_item_id: v.inventory_item_id,
                            available: 0
                        }).catch(() => {});
                    }
                }
                
                syncProgress.fullImport.discontinued++;
                runResult.discontinued++;
                addLog(`⏸️ Discontinued: ${p.title}`, 'info');
                
            } catch(e) {
                syncProgress.fullImport.errors++;
                runResult.errors++;
                const errorMsg = `Failed to discontinue ${p.title}: ${e.message}`;
                addLog(errorMsg, 'error');
                trackError('api', p.handle, errorMsg);
            }
        }
        
        runResult.status = syncProgress.fullImport.cancelled ? 'cancelled' : 'completed';
        notifyTelegram(`Full import ${runResult.status}:\n✅ ${runResult.created} created\n⏸️ ${runResult.discontinued} discontinued\n❌ ${runResult.errors} errors`);
        
    } catch(e) {
        triggerFailsafe(`Full import failed: ${e.message}`);
        trackError('system', null, `Full import failed: ${e.message}`);
        runResult.errors++;
    } finally {
        syncProgress.fullImport.isActive = false;
        isRunning.fullImport = false;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
        saveErrorTracking();
    }
}

// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() { 
    if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { 
        addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); 
        return; 
    } 
    try { 
        addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); 
        const stream = await fetchInventoryFromFTP(); 
        await updateInventoryBySKU(await parseInventoryCSV(stream)); 
    } catch (error) { 
        triggerFailsafe(`Inventory sync failed: ${error.message}`); 
        addToHistory({ type: 'Inventory', timestamp: new Date().toISOString(), status: 'failed', errors: 1, updated: 0, tagged: 0 }); 
    } 
}

async function syncFullCatalog() { 
    if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { 
        addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); 
        return; 
    } 
    let tempDir; 
    try { 
        addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); 
        const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); 
        tempDir = dir; 
        await processFullImport(csvFiles); 
    } catch (error) { 
        triggerFailsafe(`Full catalog sync failed: ${error.message}`); 
        addToHistory({ type: 'Full Import', timestamp: new Date().toISOString(), status: 'failed', errors: 1, created: 0, discontinued: 0 }); 
    } finally { 
        if (tempDir) { 
            try { 
                fs.rmSync(tempDir, { recursive: true, force: true }); 
                addLog('Cleaned up temp files', 'info'); 
            } catch (cleanupError) { 
                addLog(`Cleanup error: ${cleanupError.message}`, 'warning'); 
            } 
        } 
    } 
}

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const isSystemLockedForActions = isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;
    const canTogglePause = !failsafe.isTriggered && !confirmation.isAwaiting;
    
    const inventoryProgressHTML = syncProgress.inventory.isActive ? 
        `<div class="progress-container">
            <div class="progress-header">
                <span>Inventory Update Progress</span>
                <span>${syncProgress.inventory.current}/${syncProgress.inventory.total}</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%"></div>
            </div>
            <div class="progress-meta">
                <span>ETA: ${syncProgress.inventory.estimatedCompletion ? new Date(syncProgress.inventory.estimatedCompletion).toLocaleTimeString() : 'calculating...'}</span>
                <button onclick="cancelInventory()" class="btn btn-danger btn-sm">Cancel</button>
            </div>
        </div>` : '';
    
    const last24h = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 24*60*60*1000));
    const last7d = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 7*24*60*60*1000));
    
    // Format error report for display
    let errorReportHTML = '';
    if (errorTracking.weeklyReport) {
        const report = errorTracking.weeklyReport;
        errorReportHTML = `
        <div class="card">
            <h2>📊 Weekly Error Report</h2>
            <p style="color: var(--text-secondary);">Generated: ${new Date(report.generatedAt).toLocaleString()}</p>
            <div class="grid grid-3" style="margin: 1rem 0;">
                <div class="stat-card">
                    <div class="label">Total Errors</div>
                    <div class="value">${report.totalErrors}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Unique Types</div>
                    <div class="value">${report.uniqueErrorTypes}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Most Common</div>
                    <div class="value" style="font-size: 1rem;">${report.topErrors[0]?.type || 'None'}</div>
                </div>
            </div>
            <h3>Top Issues:</h3>
            <div class="error-list">
                ${report.topErrors.slice(0, 5).map(error => `
                    <div class="error-item">
                        <strong>${error.type}</strong> (${error.count} times)<br>
                        <span style="color: var(--text-secondary);">${error.message}</span><br>
                        ${error.sampleSKUs.length > 0 ? `<small>SKUs: ${error.sampleSKUs.join(', ')}</small>` : ''}
                    </div>
                `).join('')}
            </div>
            <button onclick="generateReport()" class="btn btn-primary" style="margin-top: 1rem;">Generate New Report</button>
        </div>`;
    }
    
    const html = `<!DOCTYPE html>
<html lang="en">
<head>
    <title>Ralawise Sync • Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        :root { 
            --bg-primary: #0a0a0a; --bg-card: rgba(18, 18, 18, 0.95); --text-primary: #ffffff; 
            --text-secondary: #a1a1a1; --text-muted: #666666; --accent-primary: #6366f1; 
            --accent-success: #10b981; --accent-warning: #f59e0b; --accent-danger: #ef4444; 
            --gradient-primary: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', sans-serif; background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; }
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        .header { text-align: center; margin-bottom: 3rem; }
        .header h1 { font-size: 3rem; font-weight: 800; background: var(--gradient-primary); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .card { background: var(--bg-card); border-radius: 24px; padding: 2rem; margin-bottom: 2rem; }
        .grid { display: grid; gap: 1.5rem; }
        .grid-3 { grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); }
        .grid-4 { grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
        .btn { padding: 0.75rem 1.5rem; border-radius: 12px; border: none; cursor: pointer; font-weight: 600; }
        .btn-primary { background: var(--gradient-primary); color: white; }
        .btn-danger { background: var(--accent-danger); color: white; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .status { padding: 0.5rem 1rem; border-radius: 999px; font-size: 0.875rem; }
        .status.active { background: rgba(16, 185, 129, 0.2); color: #10b981; }
        .status.running { background: rgba(59, 130, 246, 0.2); color: #3b82f6; }
        .progress-container { background: rgba(0, 0, 0, 0.3); border-radius: 12px; padding: 1rem; margin-top: 1rem; }
        .progress-bar { height: 8px; background: rgba(255, 255, 255, 0.1); border-radius: 4px; overflow: hidden; }
        .progress-fill { height: 100%; background: var(--gradient-primary); transition: width 0.3s; }
        .logs { background: rgba(0, 0, 0, 0.5); border-radius: 12px; padding: 1rem; max-height: 400px; overflow-y: auto; font-family: monospace; }
        .log-entry { padding: 0.5rem; margin-bottom: 0.25rem; }
        .log-info { color: #93c5fd; }
        .log-success { color: #86efac; }
        .log-warning { color: #fde047; }
        .log-error { color: #fca5a5; }
        .stat-card { background: rgba(0, 0, 0, 0.3); padding: 1rem; border-radius: 12px; text-align: center; }
        .stat-card .label { font-size: 0.75rem; color: var(--text-muted); margin-bottom: 0.5rem; }
        .stat-card .value { font-size: 2rem; font-weight: 700; }
        .error-item { background: rgba(0, 0, 0, 0.3); padding: 1rem; border-radius: 8px; margin-bottom: 0.5rem; }
    </style>
</head>
<body>
    <div class="container">
        <header class="header">
            <h1>Ralawise Sync</h1>
            <p>Inventory & Product Management System</p>
        </header>
        
        <div class="grid grid-4">
            <div class="card">
                <h2>System</h2>
                <span class="status ${isSystemPaused ? 'paused' : 'active'}">
                    ${isSystemPaused ? '⏸️ Paused' : '✅ Active'}
                </span>
                <br><br>
                <button onclick="togglePause()" class="btn btn-primary" ${!canTogglePause ? 'disabled' : ''}>
                    ${isSystemPaused ? '▶️ Resume' : '⏸️ Pause'}
                </button>
            </div>
            
            <div class="card">
                <h2>Inventory Sync</h2>
                <p><span class="status ${isRunning.inventory ? 'running' : 'active'}">${isRunning.inventory ? '🔄 Running' : '✅ Ready'}</span></p>
                ${inventoryProgressHTML}
                <button onclick="runInventorySync()" class="btn btn-primary" ${isSystemLockedForActions || isRunning.inventory ? 'disabled' : ''}>
                    Run Sync
                </button>
            </div>
            
            <div class="card">
                <h2>Full Import</h2>
                <p><span class="status ${isRunning.fullImport ? 'running' : 'active'}">${isRunning.fullImport ? '🔄 Running' : '✅ Ready'}</span></p>
                <button onclick="runFullImport()" class="btn btn-primary" ${isSystemLockedForActions || isRunning.fullImport ? 'disabled' : ''}>
                    Run Import
                </button>
            </div>
            
            <div class="card">
                <h2>Statistics</h2>
                <p>24h: ${last24h.length} ops</p>
                <p>7d: ${last7d.length} ops</p>
                <p>Total: ${runHistory.length} ops</p>
            </div>
        </div>
        
        ${errorReportHTML}
        
        <div class="card">
            <h2>Activity Log</h2>
            <div class="logs">
                ${logs.map(log => 
                    `<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`
                ).join('')}
            </div>
        </div>
    </div>
    
    <script>
        async function apiPost(endpoint, confirmMsg) {
            if (confirmMsg && !confirm(confirmMsg)) return;
            try {
                const res = await fetch(endpoint, { method: 'POST' });
                const data = await res.json();
                if (!data.success && data.error) alert('Error: ' + data.error);
                window.location.reload();
            } catch (e) {
                alert('Failed: ' + e.message);
            }
        }
        function runInventorySync() { apiPost('/api/sync/inventory', 'Run inventory sync now?'); }
        function runFullImport() { apiPost('/api/sync/full', 'Run full import?'); }
        function togglePause() { apiPost('/api/pause/toggle'); }
        function generateReport() { apiPost('/api/errors/report', 'Generate new weekly error report?'); }
        function cancelInventory() { apiPost('/api/sync/inventory/cancel'); }
        setTimeout(() => window.location.reload(), 30000);
    </script>
</body>
</html>`;
    
    res.send(html);
});

// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', async (req, res) => {
    if (isSystemPaused) return res.status(423).json({ success: false, error: 'System is paused.' });
    if (failsafe.isTriggered) return res.status(423).json({ success: false, error: 'Failsafe is active.' });
    if (confirmation.isAwaiting) return res.status(423).json({ success: false, error: 'Confirmation is pending.' });
    if (isRunning.inventory) return res.status(409).json({ success: false, error: 'Inventory sync already running.' });
    syncInventory();
    res.json({ success: true, message: 'Inventory sync started' });
});

app.post('/api/sync/full', async (req, res) => {
    if (isSystemPaused) return res.status(423).json({ success: false, error: 'System is paused.' });
    if (failsafe.isTriggered) return res.status(423).json({ success: false, error: 'Failsafe is active.' });
    if (confirmation.isAwaiting) return res.status(423).json({ success: false, error: 'Confirmation is pending.' });
    if (isRunning.fullImport) return res.status(409).json({ success: false, error: 'Full import already running.' });
    syncFullCatalog();
    res.json({ success: true, message: 'Full import started' });
});

app.post('/api/sync/inventory/cancel', (req, res) => {
    if (!isRunning.inventory) {
        return res.status(400).json({ success: false, error: 'No inventory sync running' });
    }
    syncProgress.inventory.cancelled = true;
    addLog('Inventory sync cancellation requested', 'warning');
    notifyTelegram('🛑 Inventory sync cancellation requested');
    res.json({ success: true, message: 'Cancellation requested' });
});

app.post('/api/pause/toggle', (req, res) => {
    isSystemPaused = !isSystemPaused;
    if (isSystemPaused) {
        fs.writeFileSync(PAUSE_LOCK_FILE, 'paused');
        addLog('System has been PAUSED.', 'warning');
        notifyTelegram('⏸️ System has been PAUSED.');
    } else {
        try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {}
        addLog('System has been RESUMED.', 'info');
        notifyTelegram('▶️ System has been RESUMED.');
    }
    res.json({ success: true, isPaused: isSystemPaused });
});

app.post('/api/errors/report', (req, res) => {
    const report = generateWeeklyErrorReport();
    const message = formatErrorReportForTelegram(report);
    notifyTelegram(message);
    addLog('Weekly error report generated', 'info');
    res.json({ success: true, report });
});

app.get('/api/errors/report', (req, res) => {
    res.json({ success: true, report: errorTracking.weeklyReport, errors: errorTracking.errors.slice(-100) });
});

// ============================================
// SCHEDULED TASKS
// ============================================

// Daily inventory sync at 2 AM
cron.schedule('0 2 * * *', () => {
    if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.inventory) {
        addLog('⏰ Starting scheduled inventory sync...', 'info');
        syncInventory();
    }
});

// Weekly error report on Mondays at 9 AM
cron.schedule('0 9 * * 1', () => {
    const report = generateWeeklyErrorReport();
    const message = formatErrorReportForTelegram(report);
    notifyTelegram(message);
    addLog('Weekly error report sent', 'info');
});

// Full catalog sync every 2 days at 1 PM
cron.schedule('0 13 */2 * *', () => {
    if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.fullImport) {
        addLog('⏰ Starting scheduled full catalog import...', 'info');
        syncFullCatalog();
    }
}, { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP
// ============================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT}`, 'success');
    addLog(`🛡️ Rate limit: ${config.rateLimit.requestsPerSecond} req/s`, 'info');
    
    setTimeout(() => {
        if (!isSystemPaused && !failsafe.isTriggered && !isRunning.inventory) {
            addLog('🚀 Running initial inventory sync...', 'info');
            syncInventory();
        }
    }, 10000);
});

function shutdown(signal) {
    addLog(`Received ${signal}, shutting down...`, 'info');
    saveHistory();
    saveErrorTracking();
    syncProgress.inventory.cancelled = true;
    syncProgress.fullImport.cancelled = true;
    process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
