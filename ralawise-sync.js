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
        locationId: 'gid://shopify/Location/91260682575', // NOW a GID for GraphQL
        locationIdNumber: '91260682575', // Numeric ID for REST API
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04`,
        graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json` 
    },
    ftp: { 
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        maxInventory: parseInt(process.env.MAX_INVENTORY || '20'),
        preservedSuppliers: ['Supplier:Ralawise', 'Supplier:Apify'],
        discontinuedStockThreshold: 20,
        baseSKULength: 5
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000,
        maxDiscontinuePercentage: 20
    },
    rateLimit: {
        requestsPerSecond: 2,
        burstSize: 40
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables.'); process.exit(1); }

console.log(`Using Shopify Location ID: ${config.shopify.locationIdNumber}`);

// ============================================
// STATE MANAGEMENT & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false, cleanup: false, discontinue: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
const UPLOADED_FILES_DIR = path.join(__dirname, 'uploaded_catalogs');
const DISCONTINUED_FILES_DIR = path.join(__dirname, 'discontinued_files');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let syncProgress = { 
    inventory: { isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null, cancelled: false }, 
    fullImport: { isActive: false, created: 0, discontinued: 0, updated: 0 },
    cleanup: { isActive: false, current: 0, total: 0 },
    discontinue: { isActive: false, current: 0, total: 0 }
};
let inventoryChangeLog = [];
let weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, productsUpdated: 0, errors: [] };

[UPLOADED_FILES_DIR, DISCONTINUED_FILES_DIR, path.join(__dirname, 'uploads')].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

class RateLimiter { constructor(rps=2,bs=40){this.rps=rps;this.bs=bs;this.tokens=bs;this.lastRefill=Date.now();this.queue=[];this.processing=false}async acquire(){return new Promise(r=>{this.queue.push(r);this.process()})}async process(){if(this.processing)return;this.processing=true;while(this.queue.length>0){const n=Date.now();const p=(n-this.lastRefill)/1000;this.tokens=Math.min(this.bs,this.tokens+p*this.rps);this.lastRefill=n;if(this.tokens>=1){this.tokens--;const r=this.queue.shift();r()}else{const w=(1/this.rps)*1000;await new Promise(r=>setTimeout(r,w))}}this.processing=false}}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.pop(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }); } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); Object.keys(isRunning).forEach(key => isRunning[key] = false); }
function resetConfirmationState() { confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null }; }
function updateProgress(type, current, total) { const p = syncProgress[type]; if (!p) return; p.current = current; p.total = total; if (p.startTime && current > 0) { const e = Date.now() - p.startTime; p.estimatedCompletion = new Date(Date.now() + (total - current) / (current / e)); } }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }, timeout: 60000 });
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.pop(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; } loadHistory(); }
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) { let lastError; for (let attempt = 0; attempt < retries; attempt++) { try { await rateLimiter.acquire(); switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); case 'delete': return await shopifyClient.delete(url); } } catch (error) { lastError = error; if (error.response?.status === 429) { const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000); await delay(retryAfter + 500); } else if (error.response?.status >= 500) { await delay(1000 * Math.pow(2, attempt)); } else { throw error; } } } throw lastError; }

// NEW: GraphQL Request Helper
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

function getBaseSKU(variantSKU) { if (!variantSKU) return ''; return variantSKU.substring(0, config.ralawise.baseSKULength).toUpperCase(); }
function generateHandle(baseSKU, title) { const cleanTitle = title ? title.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-') : ''; return `${baseSKU.toLowerCase()}-${cleanTitle}`.substring(0, 100); }
function requestConfirmation(jobKey, message, details, proceedAction) { /* ... */ }

// ============================================
// CORE LOGIC
// ============================================

async function fetchInventoryFromFTP() { const client = new ftp.Client(); try { await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); return Readable.from(Buffer.concat(chunks)); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU) inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
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

async function extractUploadedFiles(uploadedFile, targetDir) { /* ... same as before ... */ }
function getLatestCatalogFiles() { /* ... same as before ... */ }

async function processProductImport() { /* ... same as before, no changes needed here ... */ }

// ============================================
// NEW AUTOMATED DISCONTINUATION LOGIC
// ============================================
async function processAutomatedDiscontinuations(ftpInventory, allShopifyProducts) {
    addLog('Starting automated discontinuation process...', 'info');
    let discontinuedCount = 0;

    // Create a Set of SKUs from the FTP file for fast lookups
    const ftpSkuSet = new Set(ftpInventory.keys());

    const productsToCheck = allShopifyProducts.filter(p => 
        p.tags.includes('Supplier:Ralawise') && p.status === 'active'
    );

    addLog(`Found ${productsToCheck.length} active 'Supplier:Ralawise' products to check.`, 'info');

    for (const product of productsToCheck) {
        // Check if ANY of the product's variants exist in the current FTP stock file
        const isProductInFtp = product.variants.some(v => ftpSkuSet.has(v.sku));

        if (!isProductInFtp) {
            // If no variants are found in the FTP file, this product is discontinued
            addLog(`Discontinuing '${product.title}' (ID: ${product.id}) - not found in FTP stock file.`, 'warning');
            
            try {
                // Step 1: Set all variant inventories to 0 using GraphQL
                const inventoryItemAdjustments = product.variants.map(v => ({
                    inventoryItemId: v.inventory_item_id,
                    availableDelta: -v.inventory_quantity // Adjust by negative current quantity
                }));

                const inventoryMutation = `
                    mutation inventoryBulkAdjustQuantityAtLocation($inventoryItemAdjustments: [InventoryAdjustItemInput!]!, $locationId: ID!) {
                      inventoryBulkAdjustQuantityAtLocation(inventoryItemAdjustments: $inventoryItemAdjustments, locationId: $locationId) {
                        userErrors {
                          field
                          message
                        }
                        inventoryLevels {
                          id
                        }
                      }
                    }`;
                
                await shopifyGraphQLRequest(inventoryMutation, {
                    inventoryItemAdjustments,
                    locationId: config.shopify.locationId
                });
                addLog(`   ✅ Set inventory to 0 for ${product.variants.length} variants of '${product.title}'.`, 'success');

                // Step 2: Set product status to 'draft' using REST API
                await shopifyRequestWithRetry('put', `/products/${product.id}.json`, {
                    product: {
                        id: product.id,
                        status: 'draft'
                    }
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

// ============================================
// SYNC WRAPPERS & API
// ============================================

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// FULLY IMPLEMENTED INVENTORY SYNC
async function syncInventory() {
    if (isSystemLocked()) {
        addLog('Sync skipped: System is locked (another process is running, paused, or in failsafe).', 'warning');
        return;
    }

    isRunning.inventory = true;
    addLog('🚀 Starting full inventory sync process...', 'info');
    let runResult = { type: 'Inventory Sync', status: 'failed', updated: 0, discontinued: 0, errors: 0 };

    try {
        const startTime = Date.now();
        
        // 1. Fetch data from FTP
        const ftpStream = await fetchInventoryFromFTP();
        const ftpInventory = await parseInventoryCSV(ftpStream);
        addLog(`Successfully fetched and parsed ${ftpInventory.size} SKUs from FTP.`, 'success');

        // 2. Fetch all products from Shopify
        const shopifyProducts = await getAllShopifyProducts();
        
        // 3. NEW: Run automated discontinuation process FIRST
        const discontinuedCount = await processAutomatedDiscontinuations(ftpInventory, shopifyProducts);
        runResult.discontinued = discontinuedCount;

        // 4. Update inventory for remaining products
        addLog('Starting inventory level updates for remaining products...', 'info');
        let updates = 0;
        const inventoryAdjustments = [];
        
        for (const product of shopifyProducts) {
            if (product.status !== 'active' || !product.tags.includes('Supplier:Ralawise')) continue;

            for (const variant of product.variants) {
                if (variant.sku && ftpInventory.has(variant.sku)) {
                    const ftpQuantity = ftpInventory.get(variant.sku);
                    const shopifyQuantity = variant.inventory_quantity;

                    if (ftpQuantity !== shopifyQuantity) {
                        inventoryAdjustments.push({
                            inventoryItemId: variant.inventory_item_id,
                            availableDelta: ftpQuantity - shopifyQuantity
                        });
                        updates++;
                    }
                }
            }
        }

        if (inventoryAdjustments.length > 0) {
            addLog(`Found ${updates} variants requiring an inventory update. Sending bulk update...`, 'info');

            const mutation = `
            mutation inventoryBulkAdjustQuantityAtLocation($inventoryItemAdjustments: [InventoryAdjustItemInput!]!, $locationId: ID!) {
              inventoryBulkAdjustQuantityAtLocation(inventoryItemAdjustments: $inventoryItemAdjustments, locationId: $locationId) {
                userErrors { field message }
                inventoryLevels { id }
              }
            }`;
            
            await shopifyGraphQLRequest(mutation, { 
                inventoryItemAdjustments, 
                locationId: config.shopify.locationId 
            });

            addLog(`✅ Successfully sent bulk inventory update for ${updates} variants.`, 'success');
            runResult.updated = updates;
        } else {
            addLog('ℹ️ No inventory updates were needed.', 'info');
        }

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


app.post('/api/upload/products', upload.single('file'), async (req, res) => { /* ... same as before ... */ });
app.get('/api/upload/status', (req, res) => { /* ... same as before ... */ });
app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/import/products', (req, res) => { processProductImport(); res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (confirmation.isAwaiting) { const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); } res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (confirmation.isAwaiting) { confirmation.abortAction(); } res.json({ success: true }); });

// ============================================
// WEB INTERFACE (Updated to remove discontinued section)
// ============================================

app.get('/', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    const lastProductImport = runHistory.find(r => r.type === 'Product Import');
    const lastInventorySync = runHistory.find(r => r.type === 'Inventory Sync');
    
    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn-warning{background:#d29922;color:white;border-color:#f0c674}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem;} .progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden; width: 100%;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}.file-upload{margin-top:1rem;padding:1rem;border:2px dashed #30363d;border-radius:6px;text-align:center;}.file-upload input{display:none;}.file-upload label{cursor:pointer;padding:0.5rem 1rem;background:#21262d;border-radius:6px;display:inline-block;}.upload-status{margin-top:0.5rem;font-size:0.875em;color:#8b949e;}.section-header{font-size:1.1rem;font-weight:600;margin-bottom:0.5rem;color:#58a6ff;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationIdNumber}</div></div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><p>Syncs stock levels and <b>automatically discontinues</b> products not in the FTP file.</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button></div>
    </div>
    
    <div class="card">
        <h2>📦 Product Management</h2>
        <div class="grid">
            <div>
                <div class="section-header">New/Update Products</div>
                <div class="file-upload">
                    <label for="product-file-input" class="btn">📁 Upload Product Catalog</label>
                    <input type="file" id="product-file-input" accept=".csv,.zip" onchange="uploadFile(this, 'products')">
                    <div class="upload-status">
                        ${catalogFiles?.productFiles?.length ? `✅ ${catalogFiles.productFileCount} files (${(catalogFiles.totalProductSize / 1024 / 1024).toFixed(1)} MB)` : '❌ No product files'}
                        ${catalogFiles?.productFiles?.length ? `<br>📅 ${new Date(catalogFiles.productFiles[0].uploadTime).toLocaleString()}` : ''}
                    </div>
                </div>
                <button onclick="apiPost('/api/import/products','Import/update products?')" class="btn btn-primary" ${isSystemLocked() || !catalogFiles?.productFiles?.length ?'disabled':''} style="margin-top:0.5rem;width:100%;">Import Products</button>
            </div>
        </div>
    </div>
    
    <div class="grid">
        <div class="card">
            <h2>Last Product Import</h2>
            <div class="grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card"><div class="stat-value">${lastProductImport?.created ?? 'N/A'}</div><div class="stat-label">Created</div></div>
                <div class="stat-card"><div class="stat-value">${lastProductImport?.updated ?? 'N/A'}</div><div class="stat-label">Updated</div></div>
            </div>
        </div>
        
        <div class="card">
            <h2>Last Inventory Sync</h2>
            <div class="grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card"><div class="stat-value">${lastInventorySync?.updated ?? 'N/A'}</div><div class="stat-label">Variants Updated</div></div>
                <div class="stat-card"><div class="stat-value">${lastInventorySync?.discontinued ?? 'N/A'}</div><div class="stat-label">Products Discontinued</div></div>
            </div>
        </div>
    </div>
    
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert('Upload error: '+e.message);if(btn)btn.disabled=false;location.reload();}}
    async function uploadFile(input,type){const file=input.files[0];if(!file)return;const formData=new FormData();formData.append('file',file);try{const label=input.previousElementSibling;label.textContent='Uploading...';const res=await fetch('/api/upload/'+type,{method:'POST',body:formData});const data=await res.json();if(data.success){alert('File uploaded successfully! '+(data.fileCount?data.fileCount+' files extracted.':''));location.reload();}else{alert('Upload failed: '+data.error);label.textContent='📁 Upload Product Catalog';}}catch(e){alert('Upload error: '+e.message);}}
    </script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());  // Daily at 2 AM

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT} (Location: ${config.shopify.locationIdNumber})`, 'success');
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
