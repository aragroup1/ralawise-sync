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
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        zipUrl: process.env.RALAWISE_ZIP_URL, maxInventory: parseInt(process.env.MAX_INVENTORY || '20') 
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000
    },
    rateLimit: {
        requestsPerSecond: parseInt(process.env.RATE_LIMIT_RPS || '2'),
        burstSize: parseInt(process.env.RATE_LIMIT_BURST || '40')
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables.'); process.exit(1); }

// ============================================
// STATE MANAGEMENT & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let syncProgress = { 
    inventory: { isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null, cancelled: false }, 
    fullImport: { isActive: false, created: 0, discontinued: 0 } 
};

class RateLimiter { constructor(rps=2,bs=40){this.rps=rps;this.bs=bs;this.tokens=bs;this.lastRefill=Date.now();this.queue=[];this.processing=false}async acquire(){return new Promise(r=>{this.queue.push(r);this.process()})}async process(){if(this.processing)return;this.processing=true;while(this.queue.length>0){const n=Date.now();const p=(n-this.lastRefill)/1000;this.tokens=Math.min(this.bs,this.tokens+p*this.rps);this.lastRefill=n;if(this.tokens>=1){this.tokens--;const r=this.queue.shift();r()}else{const w=(1/this.rps)*1000;await new Promise(r=>setTimeout(r,w))}}this.processing=false}}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.pop(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }); } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); isRunning.inventory = false; isRunning.fullImport = false; }
function resetConfirmationState() { confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null }; }
function updateProgress(type, current, total) { const p = syncProgress[type]; p.current = current; p.total = total; if (p.startTime && current > 0) { const e = Date.now() - p.startTime; p.estimatedCompletion = new Date(Date.now() + (total - current) / (current / e)); } }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }, timeout: 60000 });
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.pop(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; } loadHistory(); }
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) { let lastError; for (let attempt = 0; attempt < retries; attempt++) { try { await rateLimiter.acquire(); switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); } } catch (error) { lastError = error; if (error.response?.status === 429) { const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000); await delay(retryAfter + 500); } else if (error.response?.status >= 500) { await delay(1000 * Math.pow(2, attempt)); } else { throw error; } } } throw lastError; }

function requestConfirmation(jobKey, message, details, proceedAction) { 
    confirmation = { 
        isAwaiting: true, message, details, proceedAction, 
        abortAction: () => { 
            addLog(`User aborted '${message}'. System paused for safety.`, 'warning'); 
            isSystemPaused = true; 
            fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); 
            notifyTelegram(`🙅‍♂️ User ABORTED operation. System automatically paused.`); 
            isRunning[jobKey] = false; syncProgress[jobKey].isActive = false;
            resetConfirmationState();
        }, 
        jobKey 
    }; 
    const alertMsg = `🤔 CONFIRMATION REQUIRED\n<b>Action Paused:</b> ${message}`; 
    addLog(alertMsg, 'warning'); 
    let debugMsg = `\n\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; 
    notifyTelegram(alertMsg + debugMsg); 
}

// ============================================
// CORE LOGIC
// ============================================

async function fetchInventoryFromFTP() { const client = new ftp.Client(); try { await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); return Readable.from(Buffer.concat(chunks)); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU) inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
async function getAllShopifyProducts() { let allProducts = []; let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`; addLog('Fetching all Shopify products...', 'info'); while (url) { try { const res = await shopifyRequestWithRetry('get', url); allProducts.push(...res.data.products); const linkHeader = res.headers.link; const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null; url = nextLinkMatch ? nextLinkMatch[1].replace(config.shopify.baseUrl, '') : null; } catch (error) { addLog(`Error fetching products: ${error.message}`, 'error'); triggerFailsafe(`Failed to fetch products from Shopify`); return []; } } addLog(`Fetched ${allProducts.length} products.`, 'success'); return allProducts; }
async function downloadAndExtractZip() { const res = await axios.get(`${config.ralawise.zipUrl}?t=${Date.now()}`, { responseType: 'arraybuffer' }); const tempDir = path.join(__dirname, 'temp', `ralawise_${Date.now()}`); fs.mkdirSync(tempDir, { recursive: true }); const zip = new AdmZip(res.data); zip.extractAllTo(tempDir, true); return { tempDir, csvFiles: fs.readdirSync(tempDir).filter(f => f.endsWith('.csv')).map(f => path.join(tempDir, f)) }; }

async function updateInventoryWithGraphQL(updates, runResult) {
    addLog(`Updating ${updates.length} items via GraphQL...`, 'info');
    const BATCH_SIZE = 100; // Max items per GraphQL call
    const batches = [];
    for (let i = 0; i < updates.length; i += BATCH_SIZE) {
        batches.push(updates.slice(i, i + BATCH_SIZE));
    }

    syncProgress.inventory.total = batches.length;
    let processedItems = 0;

    for (let i = 0; i < batches.length; i++) {
        if (syncProgress.inventory.cancelled) {
            runResult.status = 'cancelled';
            addLog('GraphQL update cancelled by user.', 'warning');
            return;
        }

        const batch = batches[i];
        const mutation = `
            mutation inventoryBulkAdjustQuantityAtLocation($inventoryItemAdjustments: [InventoryAdjustItemInput!]!, $locationId: ID!) {
                inventoryBulkAdjustQuantityAtLocation(inventoryItemAdjustments: $inventoryItemAdjustments, locationId: $locationId) {
                    userErrors { field message }
                    inventoryLevels { id available }
                }
            }`;
        
        const variables = {
            locationId: `gid://shopify/Location/${config.shopify.locationId}`,
            inventoryItemAdjustments: batch.map(u => ({
                inventoryItemId: `gid://shopify/InventoryItem/${u.match.variant.inventory_item_id}`,
                availableDelta: u.newQty - u.oldQty
            }))
        };
        
        try {
            const response = await shopifyRequestWithRetry('post', config.shopify.graphqlUrl, { query: mutation, variables });
            
            const topLevelErrors = response.data?.errors;
            const mutationResult = response.data?.data?.inventoryBulkAdjustQuantityAtLocation;
            const userErrors = mutationResult?.userErrors;

            if (topLevelErrors && topLevelErrors.length > 0) {
                addLog(`GraphQL request for batch ${i + 1} failed: ${topLevelErrors[0].message}`, 'error');
                runResult.errors += batch.length;
            } else if (userErrors && userErrors.length > 0) {
                addLog(`GraphQL batch ${i + 1} had ${userErrors.length} user errors. Sample: ${userErrors[0].message}`, 'error');
                runResult.errors += batch.length;
            } else if (!mutationResult) {
                addLog(`GraphQL response for batch ${i + 1} was malformed.`, 'error');
                runResult.errors += batch.length;
            } else {
                runResult.updated += batch.length;
                processedItems += batch.length;
            }

        } catch (e) {
            const errorMsg = e.response?.data?.errors?.[0]?.message || e.message;
            addLog(`GraphQL batch ${i + 1} request failed entirely: ${errorMsg}`, 'error');
            runResult.errors += batch.length;
        }
        updateProgress('inventory', i + 1, batches.length);
        addLog(`Processed batch ${i + 1}/${batches.length}. Total items updated so far: ${processedItems}`, 'info');
    }
}

async function updateInventoryBySKU(inventoryMap) {
    if (isRunning.inventory) { return; }
    isRunning.inventory = true;

    syncProgress.inventory = { isActive: true, current: 0, total: 0, startTime: Date.now(), cancelled: false };
    let runResult = { type: 'Inventory', status: 'failed', updated: 0, skipped: 0, errors: 0, notFound: 0 };
    
    const finalizeRun = (status) => {
        runResult.status = status;
        const runtime = ((Date.now() - syncProgress.inventory.startTime) / 1000 / 60).toFixed(1);
        const finalMsg = `Inventory update ${runResult.status} in ${runtime}m:\n✅ ${runResult.updated} updated\n⏭️ ${runResult.skipped} skipped\n❌ ${runResult.errors} errors\n❓ ${runResult.notFound} not found`;
        notifyTelegram(finalMsg);
        addLog(finalMsg, 'success');
        
        isRunning.inventory = false;
        syncProgress.inventory.isActive = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    };

    try {
        addLog('=== INVENTORY SYNC: ANALYSIS ===', 'info');
        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); }));
        
        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match) {
                const oldQty = match.variant.inventory_quantity || 0;
                if (oldQty !== newQty) updatesToPerform.push({ sku, oldQty, newQty, match });
                else runResult.skipped++;
            } else { runResult.notFound++; }
        });
        
        const changePercentage = skuToProduct.size > 0 ? (updatesToPerform.length / skuToProduct.size) * 100 : 0;
        addLog(`Analysis: ${updatesToPerform.length} updates needed. Change: ${changePercentage.toFixed(2)}%`, 'info');

        const executeUpdates = async () => {
            if (updatesToPerform.length === 0) {
                addLog('No inventory updates required.', 'info');
                finalizeRun('completed');
                return;
            }
            await updateInventoryWithGraphQL(updatesToPerform, runResult);
            finalizeRun(syncProgress.inventory.cancelled ? 'cancelled' : 'completed');
        };
        
        if (changePercentage > config.failsafe.inventoryChangePercentage && updatesToPerform.length > 100) {
             requestConfirmation('inventory', 'High inventory change detected', { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded: updatesToPerform.length, sample: updatesToPerform.slice(0, 5).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) } }, executeUpdates);
        } else {
            await executeUpdates();
        }
    } catch (error) {
        triggerFailsafe(`Inventory sync failed: ${error.message}`);
        finalizeRun('failed');
    }
}

async function processFullImport(csvFiles) {
    if (isRunning.fullImport) return; isRunning.fullImport = true;
    let runResult = { type: 'Full Import', status: 'failed', created: 0, discontinued: 0, errors: 0, createdProducts: [] };
    const finalizeRun = (status) => {
        runResult.status = status;
        let createdProductsMessage = runResult.createdProducts.length > 0 ? `\n\n<b>New Products:</b>\n${runResult.createdProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        notifyTelegram(`Full import ${runResult.status}:\n✅ ${runResult.created} created\n⏸️ ${runResult.discontinued} discontinued\n❌ ${runResult.errors} errors${createdProductsMessage}`);
        isRunning.fullImport = false;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
    };
    try {
        addLog('=== FULL IMPORT: ANALYSIS ===', 'info');
        const allRows = (await Promise.all(csvFiles.map(filePath => new Promise((res, rej) => { const p = []; fs.createReadStream(filePath).pipe(csv()).on('data', r => p.push({ ...r, price: applyRalawisePricing(parseFloat(r['Variant Price'])) })).on('end', () => res(p)).on('error', rej); })))).flat();
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
        
        for (const p of toCreate) {
            try {
                const res = await shopifyRequestWithRetry('post', '/products.json', { product: { title: p.Title, handle: p.Handle, body_html: p['Body (HTML)'], vendor: p.Vendor, tags: p.tags, images: p.images, variants: p.variants.map(v => ({...v, inventory_management: 'shopify' })) } });
                for (const v of res.data.product.variants) {
                    const origV = p.variants.find(ov => ov.sku === v.sku);
                    if (origV?.inventory_quantity > 0) await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: origV.inventory_quantity }).catch(()=>{});
                }
                runResult.created++; runResult.createdProducts.push({ title: p.Title, handle: p.Handle });
                addLog(`✅ Created: ${p.Title}`, 'success');
            } catch (e) { runResult.errors++; addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error'); }
        }
        const newHandles = new Set(Array.from(productsByHandle.keys()));
        const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle));
        addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info');
        
        for (const p of toDiscontinue) {
            try {
                await shopifyRequestWithRetry('put', `/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } });
                for (const v of p.variants) { if (v.inventory_item_id) await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: 0 }).catch(()=>{}); }
                runResult.discontinued++; addLog(`⏸️ Discontinued: ${p.title}`, 'info');
            } catch (e) { runResult.errors++; addLog(`Failed to discontinue ${p.title}: ${e.message}`, 'error'); }
        }
        finalizeRun('completed');
    } catch (e) { triggerFailsafe(`Full import failed: ${e.message}`); runResult.errors++; finalizeRun('failed'); }
}

// ============================================
// SYNC WRAPPERS & API
// ============================================

async function syncInventory() { if (isSystemLocked()) return; try { await updateInventoryBySKU(await parseInventoryCSV(await fetchInventoryFromFTP())); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); } }
async function syncFullCatalog() { if (isSystemLocked()) return; let tempDir; try { const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); } finally { if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true }); } }
app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/sync/full', (req, res) => { syncFullCatalog(); res.json({ success: true }); });
app.post('/api/sync/inventory/cancel', (req, res) => { if (isRunning.inventory) { syncProgress.inventory.cancelled = true; isRunning.inventory = false; syncProgress.inventory.isActive = false; addLog('User cancelled inventory sync.', 'warning'); } res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (confirmation.isAwaiting) { const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); } res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (confirmation.isAwaiting) { confirmation.abortAction(); } res.json({ success: true }); });
const isSystemLocked = () => isRunning.inventory || isRunning.fullImport || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const inventoryProgressHTML = syncProgress.inventory.isActive && syncProgress.inventory.total > 0 ? `<div class="progress-container"><div class="progress-bar"><div class="progress-fill" style="width: ${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%;"></div></div><small>${syncProgress.inventory.current}/${syncProgress.inventory.total} batches</small></div>` : '';
    const lastFullImport = runHistory.find(r => r.type === 'Full Import');
    let newProductsHTML = '<h4>Newly Created Products</h4><p>No new products in last run.</p>';
    if (lastFullImport?.createdProducts?.length > 0) {
        newProductsHTML = `<h4>Newly Created Products (${lastFullImport.createdProducts.length})</h4>
        <ul class="product-list">${lastFullImport.createdProducts.slice(0, 10).map(p => `<li><a href="https://${config.shopify.domain}/admin/products/${p.handle}" target="_blank">${p.title}</a></li>`).join('')}</ul>`;
    }

    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2.5rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem; display: flex; align-items: center; gap: 10px;} .progress-bar{flex-grow: 1; height:8px;background:#30363d;border-radius:4px;overflow:hidden;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': isRunning.inventory || isRunning.fullImport ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}</div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button>${isRunning.inventory?`<button onclick="apiPost('/api/sync/inventory/cancel')" class="btn btn-danger">Cancel</button>`:''}${inventoryProgressHTML}</div>
        <div class="card"><h2>Full Catalog Import</h2><p>Status: ${isRunning.fullImport?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/full','Create/discontinue products?')" class="btn" ${isSystemLocked()?'disabled':''}>Run Now</button></div>
        <div class="card"><h2>Last Full Import Summary</h2><div class="grid" style="grid-template-columns:1fr 1fr;"><div class="stat-card"><div class="stat-value">${lastFullImport?.created ?? 'N/A'}</div><div class="stat-label">New Products</div></div><div class="stat-card"><div class="stat-value">${lastFullImport?.discontinued ?? 'N/A'}</div><div class="stat-label">Discontinued</div></div></div><hr style="border-color:#30363d;margin:1rem 0;">${newProductsHTML}</div>
    </div>
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;const btn=event.target;try{if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message);if(btn)btn.disabled=false;}}</script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());
cron.schedule('0 4 * * 0', () => syncFullCatalog(), { timezone: 'Europe/London' });

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT}`, 'success');
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
