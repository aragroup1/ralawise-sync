const express = require('express');
const ftp = require('basic-ftp');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const AdmZip = require('adm-zip');
const multer = require('multer');
const cron = require('node-cron');
const { Readable, Writable } = require('stream');
require('dotenv').config();

const app = express();
app.use(express.json());

// Setup file upload
const upload = multer({ 
    dest: path.join(__dirname, 'uploads/'),
    limits: { fileSize: 500 * 1024 * 1024 } // 500MB limit for ZIP files
});

// ============================================
// CONFIGURATION
// ============================================

const config = {
    shopify: { 
        domain: process.env.SHOPIFY_DOMAIN, 
        accessToken: process.env.SHOPIFY_ACCESS_TOKEN, 
        locationId: `gid://shopify/Location/${process.env.SHOPIFY_LOCATION_ID}`,
        locationIdNumber: process.env.SHOPIFY_LOCATION_ID,
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04`,
        graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json` 
    },
    ftp: { 
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        maxInventory: parseInt(process.env.MAX_INVENTORY || '20'),
        baseSKULength: 5
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000,
    },
    rateLimit: {
        requestsPerSecond: 2,
        burstSize: 40
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables (ensure SHOPIFY_LOCATION_ID is set).'); process.exit(1); }

console.log(`Using Shopify Location ID: ${config.shopify.locationIdNumber}`);

// ============================================
// STATE MANAGEMENT & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
const UPLOADED_FILES_DIR = path.join(__dirname, 'uploaded_catalogs');

[UPLOADED_FILES_DIR, path.join(__dirname, 'uploads')].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

class RateLimiter { constructor(rps=2,bs=40){this.rps=rps;this.bs=bs;this.tokens=bs;this.lastRefill=Date.now();this.queue=[];this.processing=false}async acquire(){return new Promise(r=>{this.queue.push(r);this.process()})}async process(){if(this.processing)return;this.processing=true;while(this.queue.length>0){const n=Date.now();const p=(n-this.lastRefill)/1000;this.tokens=Math.min(this.bs,this.tokens+p*this.rps);this.lastRefill=n;if(this.tokens>=1){this.tokens--;const r=this.queue.shift();r()}else{const w=(1/this.rps)*1000;await new Promise(r=>setTimeout(r,w))}}this.processing=false}}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.pop(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }); } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); Object.keys(isRunning).forEach(key => isRunning[key] = false); }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }, timeout: 60000 });
let runHistory = [];
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.pop(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; } loadHistory(); }
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) { let lastError; for (let attempt = 0; attempt < retries; attempt++) { try { await rateLimiter.acquire(); switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); case 'delete': return await shopifyClient.delete(url); } } catch (error) { lastError = error; if (error.response?.status === 429) { const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000); await delay(retryAfter + 500); } else if (error.response?.status >= 500) { await delay(1000 * Math.pow(2, attempt)); } else { throw error; } } } throw lastError; }

async function shopifyGraphQLRequest(query, variables) {
    try {
        await rateLimiter.acquire();
        const response = await axios.post(config.shopify.graphqlUrl, { query, variables }, {
            headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }
        });
        if (response.data.errors) {
            throw new Error(JSON.stringify(response.data.errors));
        }
        return response.data;
    } catch (error) {
        addLog(`GraphQL error: ${error.message}`, 'error');
        throw error;
    }
}

// ============================================
// CORE LOGIC
// ============================================

async function fetchInventoryFromFTP() { const client = new ftp.Client(); try { await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); return Readable.from(Buffer.concat(chunks)); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU) inventory.set(row.SKU.trim().toUpperCase(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
async function getAllShopifyProducts() {
    let allProducts = [];
    let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`;
    addLog('Fetching all Shopify products...', 'info');
    while (url) {
        try {
            const res = await shopifyRequestWithRetry('get', url);
            allProducts.push(...res.data.products);
            const linkHeader = res.headers.link;
            const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null;
            url = nextLinkMatch ? nextLinkMatch[1].replace(config.shopify.baseUrl, '').replace(/.*\/admin/, '') : null;
        } catch (error) {
            addLog(`Error fetching products: ${error.message}`, 'error');
            triggerFailsafe(`Failed to fetch products from Shopify`);
            return [];
        }
    }
    addLog(`Fetched ${allProducts.length} products.`, 'success');
    return allProducts;
}

// ================== FIX START: Reusable Batch Processing Function ==================
async function sendInventoryUpdatesInBatches(adjustments, reason) {
    const BATCH_SIZE = 250; // Shopify's limit for this mutation
    if (!adjustments || adjustments.length === 0) {
        return;
    }

    const totalBatches = Math.ceil(adjustments.length / BATCH_SIZE);
    addLog(`Found ${adjustments.length} inventory changes. Sending in ${totalBatches} batches of up to ${BATCH_SIZE}...`, 'info');

    for (let i = 0; i < adjustments.length; i += BATCH_SIZE) {
        const batch = adjustments.slice(i, i + BATCH_SIZE);
        const currentBatchNum = Math.floor(i / BATCH_SIZE) + 1;
        
        addLog(`   Processing batch ${currentBatchNum} of ${totalBatches}... (${batch.length} items)`, 'info');

        const mutation = `
        mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
          inventoryAdjustQuantities(input: $input) {
            userErrors {
              field
              message
            }
          }
        }`;
        
        try {
            await shopifyGraphQLRequest(mutation, { 
                input: {
                    name: "stock_update",
                    reason: reason,
                    changes: batch
                }
            });
            addLog(`   ✅ Batch ${currentBatchNum} processed successfully.`, 'success');
        } catch (error) {
            addLog(`   ❌ Error processing batch ${currentBatchNum}: ${error.message}`, 'error');
            throw error; // Re-throw to fail the entire sync process
        }
    }
}
// =================== FIX END: Reusable Batch Processing Function ===================


async function processAutomatedDiscontinuations(ftpInventory, allShopifyProducts) {
    addLog('Starting automated discontinuation process...', 'info');
    let discontinuedCount = 0;
    const ftpSkuSet = new Set(ftpInventory.keys());
    const productsToCheck = allShopifyProducts.filter(p => p.tags.includes('Supplier:Ralawise') && p.status === 'active');
    addLog(`Found ${productsToCheck.length} active 'Supplier:Ralawise' products to check.`, 'info');

    for (const product of productsToCheck) {
        const isProductInFtp = product.variants.some(v => ftpSkuSet.has(v.sku?.toUpperCase()));

        if (!isProductInFtp) {
            addLog(`Discontinuing '${product.title}' (ID: ${product.id}) - not found in FTP stock file.`, 'warning');
            try {
                const inventoryAdjustments = product.variants
                    .filter(v => v.inventory_quantity > 0)
                    .map(v => ({
                        inventoryItemId: `gid://shopify/InventoryItem/${v.inventory_item_id}`,
                        locationId: config.shopify.locationId,
                        delta: -v.inventory_quantity
                    }));

                // Use the new batching function for safety, though it's rare for one product to need it
                await sendInventoryUpdatesInBatches(inventoryAdjustments, 'discontinued');
                
                if (inventoryAdjustments.length > 0) {
                     addLog(`   ✅ Set inventory to 0 for ${inventoryAdjustments.length} variants of '${product.title}'.`, 'success');
                } else {
                     addLog(`   ℹ️ No stock to adjust for '${product.title}'.`, 'info');
                }

                await shopifyRequestWithRetry('put', `/products/${product.id}.json`, {
                    product: { id: product.id, status: 'draft' }
                });
                addLog(`   ✅ Set status to 'draft' for '${product.title}'.`, 'success');
                discontinuedCount++;
            } catch (error) {
                addLog(`❌ Failed to discontinue product '${product.title}' (ID: ${product.id}): ${error.message}`, 'error');
            }
        }
    }

    addLog(`Automated discontinuation process finished. Discontinued ${discontinuedCount} products.`, 'info');
    return discontinuedCount;
}

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered;

async function syncInventory() {
    if (isSystemLocked()) {
        addLog('Sync skipped: System is locked.', 'warning');
        return;
    }
    isRunning.inventory = true;
    addLog('🚀 Starting full inventory sync process...', 'info');
    let runResult = { type: 'Inventory Sync', status: 'failed', updated: 0, discontinued: 0, errors: 0 };

    try {
        const startTime = Date.now();
        const ftpStream = await fetchInventoryFromFTP();
        const ftpInventory = await parseInventoryCSV(ftpStream);
        addLog(`Successfully fetched and parsed ${ftpInventory.size} SKUs from FTP.`, 'success');

        const shopifyProducts = await getAllShopifyProducts();
        const discontinuedCount = await processAutomatedDiscontinuations(ftpInventory, shopifyProducts);
        runResult.discontinued = discontinuedCount;

        addLog('Starting inventory level updates for remaining products...', 'info');
        const inventoryAdjustments = [];
        
        for (const product of shopifyProducts) {
            if (product.status !== 'active' || !product.tags.includes('Supplier:Ralawise')) continue;
            for (const variant of product.variants) {
                const upperSku = variant.sku?.toUpperCase();
                if (upperSku && ftpInventory.has(upperSku)) {
                    const ftpQuantity = ftpInventory.get(upperSku);
                    const shopifyQuantity = variant.inventory_quantity;
                    if (ftpQuantity !== shopifyQuantity) {
                        inventoryAdjustments.push({
                            inventoryItemId: `gid://shopify/InventoryItem/${variant.inventory_item_id}`,
                            locationId: config.shopify.locationId,
                            delta: ftpQuantity - shopifyQuantity
                        });
                    }
                }
            }
        }
        
        // Use the new batching function to handle large updates
        await sendInventoryUpdatesInBatches(inventoryAdjustments, 'stock_sync');

        if (inventoryAdjustments.length > 0) {
             addLog(`✅ Successfully sent bulk inventory updates for ${inventoryAdjustments.length} variants.`, 'success');
        } else {
            addLog('ℹ️ No inventory updates were needed.', 'info');
        }
        runResult.updated = inventoryAdjustments.length;

        runResult.status = 'completed';
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        const summary = `Inventory sync ${runResult.status} in ${duration}s:\n🔄 ${runResult.updated} variants updated\n🗑️ ${runResult.discontinued} products discontinued`;
        addLog(summary, 'success');
        notifyTelegram(summary);

    } catch (error) {
        runResult.errors++;
        runResult.status = 'failed';
        triggerFailsafe(`Inventory sync failed: ${error.message}`);
    } finally {
        isRunning.inventory = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    }
}

// ============================================
// API Endpoints
// ============================================
app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });

// ============================================
// WEB INTERFACE
// ============================================
app.get('/', (req, res) => {
    const lastInventorySync = runHistory.find(r => r.type === 'Inventory Sync');
    
    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.stat-card{text-align:center;}.stat-value{font-size:2rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationIdNumber}</div></div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><p>Syncs stock levels and <b>automatically discontinues</b> products not in the FTP file.</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button></div>
    </div>
    <div class="card">
        <h2>Last Inventory Sync</h2>
        <div class="grid" style="grid-template-columns:1fr 1fr;">
            <div class="stat-card"><div class="stat-value">${lastInventorySync?.updated ?? 'N/A'}</div><div class="stat-label">Variants Updated</div></div>
            <div class="stat-card"><div class="stat-value">${lastInventorySync?.discontinued ?? 'N/A'}</div><div class="stat-label">Products Discontinued</div></div>
        </div>
    </div>
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert('Upload error: '+e.message);if(btn)btn.disabled=false;location.reload();}}
    </script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT} (Location: ${config.shopify.locationIdNumber})`, 'success');
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
