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
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        zipUrl: process.env.RALAWISE_ZIP_URL, maxInventory: parseInt(process.env.MAX_INVENTORY || '20') 
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, 
        chatId: process.env.TELEGRAM_CHAT_ID,
        adminChatId: process.env.TELEGRAM_ADMIN_CHAT_ID || process.env.TELEGRAM_CHAT_ID
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
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables.'); process.exit(1); }

// ============================================
// STATE & ERROR MANAGEMENT
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
const ERROR_LOG_FILE = path.join(__dirname, '_error_log.json');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];

let syncProgress = {
    inventory: {
        isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null,
        cancelled: false, errors: 0, updated: 0, tagged: 0, skipped: 0, notFound: 0,
        rateLimitHits: 0, actuallyChanged: 0, failedUpdates: []
    },
    fullImport: {
        isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null,
        cancelled: false, errors: 0, created: 0, discontinued: 0
    }
};

// ============================================
// HELPER FUNCTIONS (UNCHANGED SECTION)
// ============================================
class RateLimiter { constructor(rps=2,bs=40){this.rps=rps;this.bs=bs;this.tokens=bs;this.lastRefill=Date.now();this.queue=[];this.processing=false}async acquire(){return new Promise(r=>{this.queue.push(r);this.process()})}async process(){if(this.processing)return;this.processing=true;while(this.queue.length>0){const n=Date.now();const p=(n-this.lastRefill)/1000;this.tokens=Math.min(this.bs,this.tokens+p*this.rps);this.lastRefill=n;if(this.tokens>=1){this.tokens--;const r=this.queue.shift();r()}else{const w=(1/this.rps)*1000;await new Promise(r=>setTimeout(r,w))}}this.processing=false}}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.shift(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message, adminOnly = false) { const token = config.telegram.botToken; const chatId = adminOnly ? config.telegram.adminChatId : config.telegram.chatId; if (!token || !chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, { chat_id: chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }); } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); isRunning.inventory = false; isRunning.fullImport = false; }
function resetConfirmationState() { confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null }; }
function updateProgress(type, current, total) { const p = syncProgress[type]; p.current = current; p.total = total; if (p.startTime && current > 0) { const e = Date.now() - p.startTime; p.estimatedCompletion = new Date(Date.now() + (total - current) / (current / e)); } }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }, timeout: 60000 });
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); addLog(`Loaded ${runHistory.length} history records.`, 'info'); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.shift(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; addLog('System PAUSED (lock file found).', 'warning'); } loadHistory(); }
function logError(type, message, details = {}) { addLog(`${type}: ${message}`, 'error'); try { let errors = fs.existsSync(ERROR_LOG_FILE) ? JSON.parse(fs.readFileSync(ERROR_LOG_FILE, 'utf8')) : []; errors.push({ timestamp: new Date().toISOString(), type, message, details }); if (errors.length > 5000) errors = errors.slice(-5000); fs.writeFileSync(ERROR_LOG_FILE, JSON.stringify(errors, null, 2)); } catch (e) { console.error("Failed to write to error log:", e.message); } }
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) { let lastError; for (let attempt = 0; attempt < retries; attempt++) { try { await rateLimiter.acquire(); switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); } } catch (error) { lastError = error; if (error.response?.status === 429) { syncProgress.inventory.rateLimitHits++; const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000) * Math.pow(1.5, attempt); await delay(retryAfter); } else if (error.response?.status >= 500) { await delay(1000 * Math.pow(2, attempt)); } else { throw error; } } } throw lastError; }
// ============================================

function requestConfirmation(jobKey, message, details, proceedAction) { 
    confirmation = { 
        isAwaiting: true, message, details, proceedAction, 
        abortAction: () => { 
            addLog(`User aborted '${message}'. System paused for safety.`, 'warning'); 
            isSystemPaused = true; 
            fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); 
            notifyTelegram(`🙅‍♂️ User ABORTED operation: ${message}.\nSystem automatically paused.`); 
            isRunning[jobKey] = false;
            syncProgress[jobKey].isActive = false;
            resetConfirmationState();
        }, 
        jobKey 
    }; 
    const alertMsg = `🤔 CONFIRMATION REQUIRED\n\n<b>Action Paused:</b> ${message}`; 
    addLog(alertMsg, 'warning'); 
    let debugMsg = ''; 
    if (details.inventoryChange) { 
        debugMsg = `\n\n<b>Details:</b>\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; 
    } 
    notifyTelegram(alertMsg + debugMsg); 
}

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

async function fetchInventoryFromFTP() { /* ... unchanged ... */ }
async function parseInventoryCSV(stream) { /* ... unchanged ... */ }
async function getAllShopifyProducts() { /* ... unchanged ... */ }

async function updateInventoryBySKU(inventoryMap) {
    if (isRunning.inventory) { addLog('Inventory update already running.', 'warning'); return; }
    isRunning.inventory = true;

    syncProgress.inventory = { isActive: true, current: 0, total: 0, startTime: Date.now(), cancelled: false, errors: 0, updated: 0, tagged: 0, skipped: 0, notFound: 0, rateLimitHits: 0, actuallyChanged: 0 };
    let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0, skipped: 0, notFound: 0, actuallyChanged: 0 };
    
    const finalizeRun = () => {
        isRunning.inventory = false;
        syncProgress.inventory.isActive = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    };

    try {
        addLog('=== STARTING INVENTORY ANALYSIS ===', 'info');
        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); }));
        
        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match) {
                if ((match.variant.inventory_quantity || 0) !== newQty) updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match });
                else runResult.skipped++;
            } else { runResult.notFound++; }
        });
        syncProgress.inventory.total = updatesToPerform.length;
        const changePercentage = skuToProduct.size > 0 ? (updatesToPerform.length / skuToProduct.size) * 100 : 0;
        addLog(`Analysis: ${updatesToPerform.length} updates needed. Change: ${changePercentage.toFixed(2)}%`, 'info');

        const executeUpdates = async () => {
            for (let i = 0; i < updatesToPerform.length; i++) {
                if (syncProgress.inventory.cancelled) { runResult.status = 'cancelled'; break; }
                const u = updatesToPerform[i];
                try {
                    await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id, available: u.newQty });
                    runResult.updated++;
                } catch (e) {
                    runResult.errors++; logError('InventoryUpdate', e.message, { sku: u.sku });
                    if (e.response?.status === 422) {
                        try { await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id }); } 
                        catch (connectError) { logError('InventoryConnect', connectError.message, { sku: u.sku }); }
                    }
                }
                updateProgress('inventory', i + 1, updatesToPerform.length);
            }
            if (runResult.status !== 'cancelled') runResult.status = 'completed';
            const finalMsg = `Inventory update ${runResult.status}:\n✅ ${runResult.updated} updated, ⏭️ ${runResult.skipped} skipped, ❌ ${runResult.errors} errors, ❓ ${runResult.notFound} not found`;
            notifyTelegram(finalMsg); addLog(finalMsg, 'success');
        };
        
        if (changePercentage > config.failsafe.inventoryChangePercentage && updatesToPerform.length > 100) {
             requestConfirmation('inventory', 'High inventory change detected', 
                { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded: updatesToPerform.length, sample: updatesToPerform.slice(0, 5).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) } }, 
                async () => { try { await executeUpdates(); } finally { finalizeRun(); } }
             );
             return;
        } else {
            await executeUpdates();
            finalizeRun(); // Finalize here for the direct execution path
        }
    } catch (error) {
        logError('CriticalSyncFail', error.message); runResult.errors++;
        triggerFailsafe(`Inventory sync failed: ${error.message}`);
        finalizeRun(); // Also finalize on critical failure
    }
}

async function downloadAndExtractZip() { /* ... unchanged ... */ }
async function parseShopifyCSV(stream) { /* ... unchanged ... */ }
async function processFullImport(csvFiles) {
    if (isRunning.fullImport) return; isRunning.fullImport = true;
    syncProgress.fullImport = { isActive: true, created: 0, discontinued: 0, errors: 0 };
    let runResult = { type: 'Full Import', status: 'failed', created: 0, discontinued: 0, errors: 0 };
    try {
        addLog('=== STARTING FULL IMPORT ===', 'info');
        const allRows = (await Promise.all(csvFiles.map(parseShopifyCSV))).flat();
        const productsByHandle = new Map();
        for (const row of allRows) {
            if (!row.Handle) continue;
            if (!productsByHandle.has(row.Handle)) productsByHandle.set(row.Handle, { ...row, tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''), images: [], variants: [] });
            const p = productsByHandle.get(row.Handle);
            if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) p.images.push({ src: row['Image Src'] });
            if (row['Variant SKU']) p.variants.push({ sku: row['Variant SKU'], price: row.price, option1: row['Option1 Value'], option2: row['Option2 Value'], inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory) });
        }
        
        const shopifyProducts = await getAllShopifyProducts();
        const existingHandles = new Set(shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise')).map(p => p.handle));
        const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle));
        addLog(`Found ${toCreate.length} new products to create.`, 'info');
        
        for (const p of toCreate.slice(0, 50)) { // Limit new products per run
            try {
                const res = await shopifyRequestWithRetry('post', '/products.json', { product: { title: p.Title, handle: p.Handle, body_html: p['Body (HTML)'], vendor: p.Vendor, tags: p.tags, images: p.images, variants: p.variants.map(v => ({...v, inventory_management: 'shopify' })) } });
                for (const v of res.data.product.variants) {
                    const origV = p.variants.find(ov => ov.sku === v.sku);
                    if (origV?.inventory_quantity > 0) {
                        await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: origV.inventory_quantity }).catch(()=>{});
                    }
                }
                runResult.created++; addLog(`✅ Created: ${p.Title}`, 'success');
            } catch (e) { runResult.errors++; logError('CreateProduct', e.message, { handle: p.Handle }); }
        }
        
        const newHandles = new Set(Array.from(productsByHandle.keys()));
        const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle));
        addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info');
        
        for (const p of toDiscontinue.slice(0, 50)) { // Limit discontinued per run
            try {
                await shopifyRequestWithRetry('put', `/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } });
                for (const v of p.variants) { if (v.inventory_item_id) await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: 0 }).catch(()=>{}); }
                runResult.discontinued++; addLog(`⏸️ Discontinued: ${p.title}`, 'info');
            } catch (e) { runResult.errors++; logError('DiscontinueProduct', e.message, { handle: p.handle }); }
        }
        
        runResult.status = 'completed';
        notifyTelegram(`Full import complete:\n✅ ${runResult.created} created\n⏸️ ${runResult.discontinued} discontinued\n❌ ${runResult.errors} errors`);
    } catch (e) { triggerFailsafe(`Full import failed: ${e.message}`); runResult.errors++; } 
    finally { isRunning.fullImport = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); }
}

async function syncInventory() { if (isRunning.inventory || isSystemPaused || failsafe.isTriggered) return; try { addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); const stream = await fetchInventoryFromFTP(); await updateInventoryBySKU(await parseInventoryCSV(stream)); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); } }
async function syncFullCatalog() { if (isRunning.fullImport || isSystemPaused || failsafe.isTriggered) return; let tempDir; try { addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); } finally { if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true }); } }

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const isSystemLocked = isRunning.inventory || isRunning.fullImport || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;
    const inventoryProgressHTML = syncProgress.inventory.isActive && syncProgress.inventory.total > 0 ? 
        `<div class="progress-container"><div class="progress-bar"><div class="progress-fill" style="width:${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%"></div></div><small>${syncProgress.inventory.current}/${syncProgress.inventory.total}</small></div>` : '';
    
    const lastFullImport = runHistory.find(r => r.type === 'Full Import');

    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.log-info{color:#58a6ff;}.log-success{color:#3fb950;}.log-warning{color:#d29922;}.log-error{color:#f85149;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE':'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}</div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked?'disabled':''}>Run Now</button>${isRunning.inventory?`<button onclick="apiPost('/api/sync/inventory/cancel')" class="btn btn-danger">Cancel</button>`:''}${inventoryProgressHTML}</div>
        <div class="card"><h2>Full Catalog Import</h2><p>Status: ${isRunning.fullImport?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/full','Create/discontinue products?')" class="btn" ${isSystemLocked?'disabled':''}>Run Now</button></div>
        <div class="card"><h2>Last Full Import</h2><div class="grid" style="grid-template-columns:1fr 1fr;"><div class="stat-card"><div class="stat-value">${lastFullImport?.created ?? 'N/A'}</div><div class="stat-label">New Products</div></div><div class="stat-card"><div class="stat-value">${lastFullImport?.discontinued ?? 'N/A'}</div><div class="stat-label">Discontinued</div></div></div></div>
    </div>
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message)}};setTimeout(()=>location.reload(),30000)</script></body></html>`;
    res.send(html);
});

// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', (req, res) => { if (isSystemLockedForActions()) return res.status(400).json(); syncInventory(); res.json({ success: true }); });
app.post('/api/sync/full', (req, res) => { if (isSystemLockedForActions()) return res.status(400).json(); syncFullCatalog(); res.json({ success: true }); });
app.post('/api/sync/inventory/cancel', (req, res) => { if (isRunning.inventory) { syncProgress.inventory.cancelled = true; addLog('User cancelled inventory sync.', 'warning'); } res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared manually.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json(); addLog(`User confirmed: ${confirmation.message}`, 'info'); const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json(); const action = confirmation.abortAction; action(); res.json({ success: true }); });
const isSystemLockedForActions = () => isRunning.inventory || isRunning.fullImport || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// ============================================
// SCHEDULED TASKS
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());
cron.schedule('0 4 * * 0', () => syncFullCatalog(), { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { checkPauseStateOnStartup(); addLog(`✅ Server started on port ${PORT}`, 'success'); setTimeout(() => { if (!isSystemLockedForActions()) { syncInventory(); } }, 5000); });
function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
