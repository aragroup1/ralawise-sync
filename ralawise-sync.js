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
        // NEW: GraphQL endpoint
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
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000,
        maxErrors: parseInt(process.env.MAX_ERRORS || '100')
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { 
    console.error('Missing required environment variables.'); 
    process.exit(1); 
}

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

let syncProgress = {
    inventory: {
        isActive: false, current: 0, total: 0, startTime: null, lastUpdate: null,
        estimatedCompletion: null, cancelled: false, errors: 0, updated: 0, tagged: 0
    },
    fullImport: {
        isActive: false, current: 0, total: 0, startTime: null, lastUpdate: null,
        estimatedCompletion: null, cancelled: false, errors: 0, created: 0, discontinued: 0
    }
};

function loadHistory() { 
    try { 
        if (fs.existsSync(HISTORY_FILE)) { 
            runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); 
            addLog(`Loaded ${runHistory.length} historical run records.`, 'info'); 
        } 
    } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } 
}

function saveHistory() { 
    try { 
        if (runHistory.length > 100) runHistory = runHistory.slice(0, 100); 
        fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); 
    } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } 
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
            chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' 
        }, { timeout: 10000 }); 
    } catch (error) { addLog(`Telegram notification failed: ${error.message}`, 'warning'); } 
}

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
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
        isAwaiting: true, message, details, proceedAction, 
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
function resetConfirmationState() { confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null }; }
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

const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: 60000 });
async function shopifyRequestWithRetry(method, url, data = null, retries = 3) { 
    let attempt = 0; 
    while (attempt < retries) { 
        try { 
            switch (method.toLowerCase()) { 
                case 'get': return await shopifyClient.get(url); 
                case 'post': return await shopifyClient.post(url, data); 
                case 'put': return await shopifyClient.put(url, data); 
            } 
        } catch (error) { 
            if (error.response && error.response.status === 429) { 
                attempt++; 
                const retryAfter = (error.response.headers['retry-after'] || Math.pow(2, attempt)) * 1000 + Math.random() * 1000; 
                addLog(`Shopify REST rate limit hit. Retrying in ${(retryAfter / 1000).toFixed(1)}s...`, 'warning'); 
                await delay(retryAfter); 
            } else { throw error; } 
        } 
    } 
    throw new Error(`Shopify REST API request for ${method.toUpperCase()} ${url} failed after ${retries} retries.`); 
}

// NEW: GraphQL client and request function
const shopifyGraphQLClient = axios.create({
    baseURL: config.shopify.graphqlUrl,
    headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' },
    timeout: 120000
});

async function shopifyGraphQLRequest(query, variables, retries = 3) {
    let attempt = 0;
    while (attempt < retries) {
        try {
            const response = await shopifyGraphQLClient.post('', { query, variables });
            if (response.data.errors) {
                // Handle GraphQL-level errors
                throw new Error(`GraphQL Error: ${JSON.stringify(response.data.errors)}`);
            }
            // Check for rate limit info in extensions
            const cost = response.data.extensions?.cost;
            if (cost?.throttleStatus.currentlyAvailable < cost?.requestedQueryCost) {
                 addLog(`GraphQL rate limit approaching. Pausing for 5s...`, 'warning');
                 await delay(5000);
            }
            return response.data.data;
        } catch (error) {
            if (error.response && error.response.status === 429) {
                attempt++;
                const retryAfter = (error.response.headers['retry-after'] || Math.pow(2, attempt)) * 1000 + Math.random() * 1000;
                addLog(`Shopify GraphQL rate limit hit. Retrying in ${(retryAfter / 1000).toFixed(1)}s...`, 'warning');
                await delay(retryAfter);
            } else {
                // Non-rate-limit errors
                throw error;
            }
        }
    }
    throw new Error(`Shopify GraphQL request failed after ${retries} retries.`);
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
        await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); 
        const buffer = Buffer.concat(chunks); 
        addLog(`FTP download successful, ${buffer.length} bytes`, 'success'); 
        return Readable.from(buffer); 
    } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } 
    finally { client.close(); } 
}
async function parseInventoryCSV(stream) { 
    return new Promise((resolve, reject) => { 
        const inventory = new Map(); 
        stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 }))
            .on('data', row => { if (row.SKU && row.SKU !== 'SKU') inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); })
            .on('end', () => resolve(inventory))
            .on('error', reject); 
    }); 
}
async function getAllShopifyProducts() {
    let allProducts = [];
    let pageCount = 0;
    let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`;
    addLog('Fetching all products from Shopify (using reliable cursor pagination)...', 'info');
    while (url) {
        try {
            pageCount++;
            const res = await shopifyRequestWithRetry('get', url);
            const productsOnPage = res.data.products;
            if (productsOnPage && productsOnPage.length > 0) {
                allProducts.push(...productsOnPage);
                addLog(`Fetched page ${pageCount}: ${productsOnPage.length} products (total: ${allProducts.length})`, 'info');
            } else {
                addLog(`Stopping pagination: Received an empty page of products.`, 'warning');
                break;
            }
            const linkHeader = res.headers.link;
            const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null;
            if (nextLinkMatch && nextLinkMatch[1]) {
                const fullNextUrl = nextLinkMatch[1];
                url = fullNextUrl.replace(config.shopify.baseUrl, '');
            } else { url = null; }
            await delay(500); // Increased delay to be safer
        } catch (error) {
            addLog(`Error fetching page ${pageCount} from Shopify: ${error.message}`, 'error');
            triggerFailsafe(`Failed to fetch all products from Shopify during pagination.`);
            return [];
        }
    }
    addLog(`Completed fetching ${allProducts.length} products from Shopify.`, 'success');
    return allProducts;
}

// ============================================
// REWRITTEN INVENTORY UPDATE FUNCTION (GraphQL)
// ============================================
async function updateInventoryBySKU(inventoryMap) {
    if (isRunning.inventory) {
        addLog('Inventory update already running.', 'warning');
        return;
    }
    
    isRunning.inventory = true;
    syncProgress.inventory = { isActive: true, current: 0, total: 0, startTime: Date.now(), lastUpdate: Date.now(), estimatedCompletion: null, cancelled: false, errors: 0, updated: 0, tagged: 0 };
    let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0 };
    let timeoutCheck = null;

    try {
        addLog('=== STARTING INVENTORY ANALYSIS (GraphQL Method) ===', 'info');
        
        timeoutCheck = setInterval(() => {
            if (Date.now() - syncProgress.inventory.startTime > config.failsafe.maxRuntime) {
                addLog(`Inventory sync timeout reached. Stopping...`, 'error');
                syncProgress.inventory.cancelled = true;
            }
        }, 60000);

        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => {
            if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v });
        }));

        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match && (match.variant.inventory_quantity || 0) !== newQty) {
                updatesToPerform.push({
                    sku,
                    oldQty: match.variant.inventory_quantity || 0,
                    newQty,
                    match
                });
            }
        });
        
        syncProgress.inventory.total = updatesToPerform.length;
        const totalProducts = skuToProduct.size;
        const updatesNeeded = updatesToPerform.length;
        const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0;
        addLog(`Change analysis: ${updatesNeeded} updates for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info');

        const executeUpdates = async () => {
            addLog(`Executing updates for ${updatesNeeded} variants...`, 'info');

            // --- PHASE 1: One-time setup tasks (Tagging, setting inventory management) ---
            // These are less frequent and fine to do with REST API.
            addLog('Phase 1: Performing one-time setup tasks (tagging, etc.)...', 'info');
            for (const u of updatesToPerform) {
                if (syncProgress.inventory.cancelled) break;
                try {
                    if (!u.match.product.tags?.includes('Supplier:Ralawise')) {
                        await shopifyRequestWithRetry('put', `/products/${u.match.product.id}.json`, { product: { id: u.match.product.id, tags: `${u.match.product.tags || ''},Supplier:Ralawise`.replace(/^,/, '') }});
                        runResult.tagged++;
                        syncProgress.inventory.tagged++;
                    }
                    if (!u.match.variant.inventory_management) {
                        await shopifyRequestWithRetry('put', `/variants/${u.match.variant.id}.json`, { variant: { id: u.match.variant.id, inventory_management: 'shopify', inventory_policy: 'deny' }});
                    }
                } catch (e) {
                     runResult.errors++;
                     syncProgress.inventory.errors++;
                     addLog(`Setup failed for ${u.sku}: ${e.message}`, 'error');
                }
                await delay(500); // Be respectful to the REST API
            }
            addLog('Phase 1 complete.', 'success');


            // --- PHASE 2: Bulk Inventory Update with GraphQL ---
            addLog('Phase 2: Starting bulk inventory update with GraphQL...', 'info');
            const inventoryChanges = updatesToPerform.map(u => ({
                inventoryItemId: `gid://shopify/InventoryItem/${u.match.variant.inventory_item_id}`,
                availableDelta: u.newQty - u.oldQty
            }));
            
            const BATCH_SIZE = 100; // Shopify allows up to 100 for this mutation
            const batches = [];
            for (let i = 0; i < inventoryChanges.length; i += BATCH_SIZE) {
                batches.push(inventoryChanges.slice(i, i + BATCH_SIZE));
            }

            const INVENTORY_ADJUST_MUTATION = `
                mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
                    inventoryAdjustQuantities(input: $input) {
                        userErrors {
                            field
                            message
                        }
                    }
                }`;
            
            for (const batch of batches) {
                if (syncProgress.inventory.cancelled) {
                    addLog('Inventory sync cancelled during GraphQL phase', 'warning');
                    break;
                }
                
                try {
                    const variables = {
                        input: {
                            reason: "stock_update",
                            changes: batch,
                            locationId: `gid://shopify/Location/${config.shopify.locationId}`
                        }
                    };
                    
                    const result = await shopifyGraphQLRequest(INVENTORY_ADJUST_MUTATION, variables);
                    
                    if (result.inventoryAdjustQuantities.userErrors.length > 0) {
                        throw new Error(JSON.stringify(result.inventoryAdjustQuantities.userErrors));
                    }
                    
                    runResult.updated += batch.length;
                    syncProgress.inventory.updated += batch.length;

                    updateProgress('inventory', syncProgress.inventory.updated, syncProgress.inventory.total);
                    const percent = ((syncProgress.inventory.updated / syncProgress.inventory.total) * 100).toFixed(1);
                    const eta = syncProgress.inventory.estimatedCompletion ? syncProgress.inventory.estimatedCompletion.toLocaleTimeString() : '...';
                    addLog(`GraphQL Batch successful. Progress: ${syncProgress.inventory.updated}/${syncProgress.inventory.total} (${percent}%) - ETA: ${eta}`, 'success');

                } catch (e) {
                    runResult.errors += batch.length;
                    syncProgress.inventory.errors += batch.length;
                    addLog(`GraphQL batch failed: ${e.message}`, 'error');
                }
                
                await delay(1000); // Delay between batches
            }

            const notFound = inventoryMap.size - updatesToPerform.length;
            runResult.status = syncProgress.inventory.cancelled ? 'cancelled' : 'completed';
            const finalMsg = `Inventory update ${runResult.status}:\n✅ ${runResult.updated} updated via GraphQL\n🏷️ ${runResult.tagged} tagged\n❌ ${runResult.errors} errors\n❓ ${notFound} not found`;
            notifyTelegram(finalMsg);
            addLog(finalMsg, runResult.status === 'completed' ? 'success' : 'warning');
        };

        if (changePercentage > config.failsafe.inventoryChangePercentage) {
            requestConfirmation('inventory', 'High inventory change detected', {
                inventoryChange: {
                    threshold: config.failsafe.inventoryChangePercentage,
                    actualPercentage: changePercentage, updatesNeeded, totalProducts,
                    sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty }))
                }
            }, async () => {
                try { await executeUpdates(); } finally {
                    if (timeoutCheck) clearInterval(timeoutCheck);
                    syncProgress.inventory.isActive = false; isRunning.inventory = false;
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
            syncProgress.inventory.isActive = false; isRunning.inventory = false;
            addToHistory({...runResult, timestamp: new Date().toISOString() });
        }
    }
}
// END OF REWRITTEN FUNCTION

async function downloadAndExtractZip() { /* ... unchanged ... */ }
async function parseShopifyCSV(filePath) { /* ... unchanged ... */ }
async function processFullImport(csvFiles) { /* ... unchanged ... */ }
async function syncInventory() { /* ... unchanged ... */ }
async function syncFullCatalog() { /* ... unchanged ... */ }

// The rest of the script (WEB INTERFACE, API ENDPOINTS, SCHEDULED TASKS, SERVER STARTUP) remains unchanged.
// I have omitted it here for brevity, but you should replace the `updateInventoryBySKU` function and add the new `shopifyGraphQLRequest` helper in your existing full script.
// Make sure to also add the `graphqlUrl` to your config object.
