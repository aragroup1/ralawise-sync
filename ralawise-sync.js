const express = require('express');
const ftp = require('basic-ftp');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const multer = require('multer');
const AdmZip = require('adm-zip');
const cron = require('node-cron');
const { Readable, Writable } = require('stream');
require('dotenv').config();

const app = express();
app.use(express.json());

// Setup file upload
const upload = multer({ 
    dest: path.join(__dirname, 'uploads/'),
    limits: { fileSize: 200 * 1024 * 1024 } // 200MB limit
});

// ============================================
// CONFIGURATION
// ============================================

const config = {
    shopify: { 
        domain: process.env.SHOPIFY_DOMAIN, 
        accessToken: process.env.SHOPIFY_ACCESS_TOKEN, 
        locationId: '91260682575', // HARDCODED to correct location ID
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`,
        graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01/graphql.json` 
    },
    ftp: { 
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        maxInventory: parseInt(process.env.MAX_INVENTORY || '20'),
        preservedSuppliers: ['Supplier:Ralawise', 'Supplier:Apify'], // Suppliers to preserve
        discontinuedStockThreshold: 20 // If discontinued item has more than this, keep it active
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000
    },
    rateLimit: {
        requestsPerSecond: 2,
        burstSize: 40
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables.'); process.exit(1); }

console.log(`Using Shopify Location ID: ${config.shopify.locationId}`);

// ============================================
// STATE MANAGEMENT & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false, cleanup: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
const UPLOADED_FILES_DIR = path.join(__dirname, 'uploaded_catalogs');
const UPLOADED_DISCONTINUED_DIR = path.join(__dirname, 'uploaded_discontinued');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let syncProgress = { 
    inventory: { isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null, cancelled: false }, 
    fullImport: { isActive: false, created: 0, discontinued: 0 },
    cleanup: { isActive: false, current: 0, total: 0 }
};
let inventoryChangeLog = []; // Track actual changes made
let weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, errors: [] };

// Create directories if they don't exist
[UPLOADED_FILES_DIR, UPLOADED_DISCONTINUED_DIR, path.join(__dirname, 'uploads')].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

class RateLimiter { constructor(rps=2,bs=40){this.rps=rps;this.bs=bs;this.tokens=bs;this.lastRefill=Date.now();this.queue=[];this.processing=false}async acquire(){return new Promise(r=>{this.queue.push(r);this.process()})}async process(){if(this.processing)return;this.processing=true;while(this.queue.length>0){const n=Date.now();const p=(n-this.lastRefill)/1000;this.tokens=Math.min(this.bs,this.tokens+p*this.rps);this.lastRefill=n;if(this.tokens>=1){this.tokens--;const r=this.queue.shift();r()}else{const w=(1/this.rps)*1000;await new Promise(r=>setTimeout(r,w))}}this.processing=false}}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.pop(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }); } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); isRunning.inventory = false; isRunning.fullImport = false; isRunning.cleanup = false; }
function resetConfirmationState() { confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null }; }
function updateProgress(type, current, total) { const p = syncProgress[type]; if (!p) return; p.current = current; p.total = total; if (p.startTime && current > 0) { const e = Date.now() - p.startTime; p.estimatedCompletion = new Date(Date.now() + (total - current) / (current / e)); } }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }, timeout: 60000 });
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.pop(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; } loadHistory(); }
async function shopifyRequestWithRetry(method, url, data = null, retries = 5) { let lastError; for (let attempt = 0; attempt < retries; attempt++) { try { await rateLimiter.acquire(); switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); case 'delete': return await shopifyClient.delete(url); } } catch (error) { lastError = error; if (error.response?.status === 429) { const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000); await delay(retryAfter + 500); } else if (error.response?.status >= 500) { await delay(1000 * Math.pow(2, attempt)); } else { throw error; } } } throw lastError; }

function requestConfirmation(jobKey, message, details, proceedAction) { 
    confirmation = { 
        isAwaiting: true, message, details, proceedAction, 
        abortAction: () => { 
            addLog(`User aborted '${message}'. System paused for safety.`, 'warning'); 
            isSystemPaused = true; 
            fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); 
            notifyTelegram(`🙅‍♂️ User ABORTED operation. System automatically paused.`); 
            isRunning[jobKey] = false; 
            if (syncProgress[jobKey]) syncProgress[jobKey].isActive = false;
            resetConfirmationState();
        }, 
        jobKey 
    }; 
    const alertMsg = `🤔 CONFIRMATION REQUIRED\n<b>Action Paused:</b> ${message}`; 
    addLog(alertMsg, 'warning'); 
    let debugMsg = details.inventoryChange ? 
        `\n\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.` :
        `\n\nDetails: ${JSON.stringify(details)}\n\nPlease visit the dashboard to review and decide.`;
    notifyTelegram(alertMsg + debugMsg); 
}

// ============================================
// CORE LOGIC
// ============================================

async function fetchInventoryFromFTP() { const client = new ftp.Client(); try { await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); return Readable.from(Buffer.concat(chunks)); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU) inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
async function getAllShopifyProducts() { let allProducts = []; let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`; addLog('Fetching all Shopify products...', 'info'); while (url) { try { const res = await shopifyRequestWithRetry('get', url); allProducts.push(...res.data.products); const linkHeader = res.headers.link; const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null; url = nextLinkMatch ? nextLinkMatch[1].replace(config.shopify.baseUrl, '') : null; } catch (error) { addLog(`Error fetching products: ${error.message}`, 'error'); triggerFailsafe(`Failed to fetch products from Shopify`); return []; } } addLog(`Fetched ${allProducts.length} products.`, 'success'); return allProducts; }

// NEW: Parse discontinued CSV with stock information
async function parseDiscontinuedCSV(filePath) {
    return new Promise((resolve, reject) => {
        const discontinued = new Map();
        let headers = null;
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('headers', (h) => {
                headers = h;
                addLog(`Discontinued CSV headers: ${headers.join(', ')}`, 'info');
            })
            .on('data', row => {
                // Try to find SKU and Stock columns - adjust these based on actual headers
                const sku = row['SKU'] || row['Variant SKU'] || row['Product Code'] || '';
                const stock = parseInt(row['Stock'] || row['Quantity'] || row['Available'] || '0');
                
                if (sku) {
                    discontinued.set(sku.trim().toUpperCase(), {
                        sku: sku.trim(),
                        stock: stock,
                        shouldDiscontinue: stock <= config.ralawise.discontinuedStockThreshold
                    });
                }
            })
            .on('end', () => {
                addLog(`Parsed ${discontinued.size} discontinued items`, 'info');
                addLog(`Items to discontinue (stock ≤ ${config.ralawise.discontinuedStockThreshold}): ${Array.from(discontinued.values()).filter(d => d.shouldDiscontinue).length}`, 'info');
                addLog(`Items to keep (stock > ${config.ralawise.discontinuedStockThreshold}): ${Array.from(discontinued.values()).filter(d => !d.shouldDiscontinue).length}`, 'info');
                resolve(discontinued);
            })
            .on('error', reject);
    });
}

// NEW: Get latest uploaded catalog files
function getLatestCatalogFiles() {
    try {
        const productFiles = fs.existsSync(UPLOADED_FILES_DIR) ? fs.readdirSync(UPLOADED_FILES_DIR).filter(f => f.endsWith('.csv')).map(f => ({
            name: f,
            path: path.join(UPLOADED_FILES_DIR, f),
            uploadTime: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).mtime
        })) : [];
        
        const discontinuedFiles = fs.existsSync(UPLOADED_DISCONTINUED_DIR) ? fs.readdirSync(UPLOADED_DISCONTINUED_DIR).filter(f => f.endsWith('.csv')).map(f => ({
            name: f,
            path: path.join(UPLOADED_DISCONTINUED_DIR, f),
            uploadTime: fs.statSync(path.join(UPLOADED_DISCONTINUED_DIR, f)).mtime
        })) : [];

        productFiles.sort((a, b) => b.uploadTime - a.uploadTime);
        discontinuedFiles.sort((a, b) => b.uploadTime - a.uploadTime);

        return { 
            productFiles, 
            discontinuedFile: discontinuedFiles.length > 0 ? discontinuedFiles[0] : null 
        };
    } catch (e) {
        addLog(`Error reading catalog files: ${e.message}`, 'error');
        return { productFiles: [], discontinuedFile: null };
    }
}

// Updated full import to use uploaded files and handle discontinued items
async function processFullImport() {
    if (isRunning.fullImport) return; 
    isRunning.fullImport = true;
    
    let runResult = { type: 'Full Import', status: 'failed', created: 0, discontinued: 0, errors: 0, createdProducts: [], discontinuedProducts: [] };
    const finalizeRun = (status) => {
        runResult.status = status;
        let createdProductsMessage = runResult.createdProducts.length > 0 ? `\n\n<b>New Products:</b>\n${runResult.createdProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        let discontinuedMessage = runResult.discontinuedProducts.length > 0 ? `\n\n<b>Discontinued:</b>\n${runResult.discontinuedProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        notifyTelegram(`Full import ${runResult.status}:\n✅ ${runResult.created} created\n⏸️ ${runResult.discontinued} discontinued\n❌ ${runResult.errors} errors${createdProductsMessage}${discontinuedMessage}`);
        isRunning.fullImport = false;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== FULL IMPORT: USING UPLOADED FILES ===', 'info');
        
        const catalogFiles = getLatestCatalogFiles();
        if (!catalogFiles?.productFiles || catalogFiles.productFiles.length === 0) {
            throw new Error('No product catalog files found. Please upload a ZIP file first.');
        }
        
        addLog(`Found ${catalogFiles.productFiles.length} product CSV files to process`, 'info');
        
        // Parse discontinued items if file exists
        let discontinuedMap = new Map();
        if (catalogFiles.discontinuedFile) {
            addLog(`Using discontinued file: ${catalogFiles.discontinuedFile.name}`, 'info');
            discontinuedMap = await parseDiscontinuedCSV(catalogFiles.discontinuedFile.path);
        }
        
        // Parse main product catalog from all extracted CSVs
        const allRows = (await Promise.all(catalogFiles.productFiles.map(file => new Promise((res, rej) => { 
            const p = []; 
            fs.createReadStream(file.path)
                .pipe(csv())
                .on('data', r => p.push({ ...r, price: applyRalawisePricing(parseFloat(r['Variant Price'])) }))
                .on('end', () => res(p))
                .on('error', rej); 
        })))).flat();
        
        addLog(`Total rows from all product CSVs: ${allRows.length}`, 'info');
        
        const productsByHandle = new Map();
        
        for (const row of allRows) {
            if (!row.Handle) continue;
            
            const sku = row['Variant SKU'] || '';
            const discontinuedInfo = discontinuedMap.get(sku.toUpperCase());
            
            // Skip if item is discontinued with low stock
            if (discontinuedInfo?.shouldDiscontinue) {
                if (productsByHandle.size % 1000 === 0) addLog(`Skipping discontinued SKU ${sku} (stock: ${discontinuedInfo.stock})`, 'info');
                continue;
            }
            
            if (!productsByHandle.has(row.Handle)) {
                productsByHandle.set(row.Handle, { 
                    ...row, 
                    tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''), 
                    images: [], 
                    variants: [] 
                });
            }
            const p = productsByHandle.get(row.Handle);
            if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) {
                p.images.push({ src: row['Image Src'] });
            }
            if (row['Variant SKU']) {
                p.variants.push({ 
                    sku: row['Variant SKU'], 
                    price: row.price, 
                    option1: row['Option1 Value'], 
                    option2: row['Option2 Value'], 
                    inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory) 
                });
            }
        }
        
        const shopifyProducts = await getAllShopifyProducts();
        const ralawiseProducts = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise'));
        const existingHandles = new Set(ralawiseProducts.map(p => p.handle));
        const newHandles = new Set(Array.from(productsByHandle.keys()));
        
        // Find products to create
        const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle));
        addLog(`Found ${toCreate.length} new products to create.`, 'info');
        
        // Find products to discontinue
        const toDiscontinue = ralawiseProducts.filter(p => {
            if (!newHandles.has(p.handle)) return true; // Not in new catalog
            
            // Check if all variants are discontinued
            const allVariantsDiscontinued = p.variants?.every(v => {
                const discInfo = discontinuedMap.get(v.sku?.toUpperCase());
                return discInfo?.shouldDiscontinue;
            });
            
            return allVariantsDiscontinued;
        });
        
        addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info');
        
        // Create new products (limit to prevent overwhelming)
        for (const p of toCreate.slice(0, 50)) {
            try {
                const res = await shopifyRequestWithRetry('post', '/products.json', { 
                    product: { 
                        title: p.Title, 
                        handle: p.Handle, 
                        body_html: p['Body (HTML)'], 
                        vendor: p.Vendor, 
                        tags: p.tags, 
                        images: p.images, 
                        variants: p.variants.map(v => ({...v, inventory_management: 'shopify' })) 
                    } 
                });
                
                // Set initial inventory
                for (const v of res.data.product.variants) {
                    const origV = p.variants.find(ov => ov.sku === v.sku);
                    if (origV?.inventory_quantity > 0) {
                        try {
                            await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { 
                                location_id: config.shopify.locationId, 
                                inventory_item_id: v.inventory_item_id, 
                                available: origV.inventory_quantity 
                            });
                        } catch (e) {}
                    }
                }
                
                runResult.created++;
                runResult.createdProducts.push({ title: p.Title, handle: p.Handle });
                weeklyReport.productsCreated++;
                addLog(`✅ Created: ${p.Title}`, 'success');
            } catch (e) { 
                runResult.errors++; 
                addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error');
            }
        }
        
        // Discontinue products
        for (const p of toDiscontinue.slice(0, 50)) {
            try {
                await shopifyRequestWithRetry('put', `/products/${p.id}.json`, { 
                    product: { id: p.id, status: 'draft' } 
                });
                
                // Zero out inventory
                for (const v of p.variants || []) { 
                    if (v.inventory_item_id) {
                        try {
                            await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { 
                                location_id: config.shopify.locationId, 
                                inventory_item_id: v.inventory_item_id, 
                                available: 0 
                            });
                        } catch (e) {}
                    }
                }
                
                runResult.discontinued++;
                runResult.discontinuedProducts.push({ title: p.title, handle: p.handle });
                weeklyReport.productsDiscontinued++;
                addLog(`⏸️ Discontinued: ${p.title}`, 'info');
            } catch (e) { 
                runResult.errors++; 
                addLog(`Failed to discontinue ${p.title}: ${e.message}`, 'error'); 
            }
        }
        
        finalizeRun('completed');
    } catch (e) { 
        triggerFailsafe(`Full import failed: ${e.message}`); 
        runResult.errors++; 
        finalizeRun('failed'); 
    }
}

// ... (Other functions like cleanupProductsWithoutSupplierTags, updateInventoryBySKU remain the same) ...

// ============================================
// SYNC WRAPPERS & API
// ============================================

async function syncInventory() { if (isSystemLocked()) return; try { await updateInventoryBySKU(await parseInventoryCSV(await fetchInventoryFromFTP())); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); } }
async function syncFullCatalog() { if (isSystemLocked()) return; try { await processFullImport(); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); } }

// File upload endpoints
app.post('/api/upload/catalog', upload.fields([
    { name: 'product_zip', maxCount: 1 },
    { name: 'discontinued_csv', maxCount: 1 }
]), async (req, res) => {
    try {
        if (!req.files || (!req.files.product_zip && !req.files.discontinued_csv)) {
            return res.status(400).json({ error: 'No files uploaded' });
        }
        
        // Handle product ZIP upload
        if (req.files.product_zip) {
            const file = req.files.product_zip[0];
            
            // Clean old catalog files
            if (fs.existsSync(UPLOADED_FILES_DIR)) {
                fs.rmSync(UPLOADED_FILES_DIR, { recursive: true, force: true });
            }
            fs.mkdirSync(UPLOADED_FILES_DIR, { recursive: true });

            // Extract ZIP
            const zip = new AdmZip(file.path);
            zip.extractAllTo(UPLOADED_FILES_DIR, true);
            
            fs.unlinkSync(file.path); // Clean up temp file
            
            addLog(`Uploaded and extracted ZIP: ${file.originalname}`, 'success');
        }
        
        // Handle discontinued CSV upload
        if (req.files.discontinued_csv) {
            const file = req.files.discontinued_csv[0];

            // Clean old discontinued files
            if (fs.existsSync(UPLOADED_DISCONTINUED_DIR)) {
                 fs.rmSync(UPLOADED_DISCONTINUED_DIR, { recursive: true, force: true });
            }
            fs.mkdirSync(UPLOADED_DISCONTINUED_DIR, { recursive: true });

            const destPath = path.join(UPLOADED_DISCONTINUED_DIR, file.originalname);
            fs.renameSync(file.path, destPath);
            
            addLog(`Uploaded discontinued CSV: ${file.originalname}`, 'success');
        }
        
        res.json({ success: true, message: 'Files processed successfully' });
    } catch (error) {
        addLog(`Upload failed: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/upload/status', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    res.json({
        hasProductFiles: catalogFiles.productFiles.length > 0,
        productFileCount: catalogFiles.productFiles.length,
        discontinuedFile: catalogFiles.discontinuedFile?.name,
        uploadTime: catalogFiles.productFiles.length > 0 ? catalogFiles.productFiles[0].uploadTime : null
    });
});

app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/sync/full', (req, res) => { syncFullCatalog(); res.json({ success: true }); });
app.post('/api/cleanup/once', (req, res) => { cleanupProductsWithoutSupplierTags(); res.json({ success: true }); });
app.post('/api/sync/inventory/cancel', (req, res) => { if (isRunning.inventory) { syncProgress.inventory.cancelled = true; isRunning.inventory = false; syncProgress.inventory.isActive = false; addLog('User cancelled inventory sync.', 'warning'); } res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (confirmation.isAwaiting) { const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); } res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (confirmation.isAwaiting) { confirmation.abortAction(); } res.json({ success: true }); });
const isSystemLocked = () => isRunning.inventory || isRunning.fullImport || isRunning.cleanup || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const inventoryProgressHTML = syncProgress.inventory.isActive && syncProgress.inventory.total > 0 ? `<div class="progress-container"><div class="progress-bar" style="width:${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%"></div><small>${syncProgress.inventory.current}/${syncProgress.inventory.total} items</small></div>` : '';
    const lastFullImport = runHistory.find(r => r.type === 'Full Import');
    const catalogFiles = getLatestCatalogFiles();
    
    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn-warning{background:#d29922;color:white;border-color:#f0c674}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2.5rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem;} .progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden; width: 100%;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}.file-upload{margin-top:1rem;padding:1rem;border:2px dashed #30363d;border-radius:6px;text-align:center;}.file-upload input{display:none;}.file-upload label{cursor:pointer;padding:0.5rem 1rem;background:#21262d;border-radius:6px;display:inline-block;}.upload-status{margin-top:0.5rem;font-size:0.875em;color:#8b949e;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': isRunning.inventory || isRunning.fullImport || isRunning.cleanup ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationId}</div></div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button>${isRunning.inventory?`<button onclick="apiPost('/api/sync/inventory/cancel')" class="btn btn-danger">Cancel</button>`:''}${inventoryProgressHTML}</div>
        <div class="card">
            <h2>Full Catalog Import</h2>
            <form id="upload-form" enctype="multipart/form-data">
                <div class="file-upload">
                    <label for="zip-input" class="btn">📦 Upload Product ZIP</label>
                    <input type="file" id="zip-input" name="product_zip" accept=".zip">
                </div>
                <div class="file-upload" style="margin-top:0.5rem;">
                    <label for="csv-input" class="btn">📉 Upload Discontinued CSV</label>
                    <input type="file" id="csv-input" name="discontinued_csv" accept=".csv">
                </div>
            </form>
            <button onclick="uploadFiles()" class="btn" style="margin-top:0.5rem;">Upload Files</button>
            <div class="upload-status" id="upload-status-display">
                ${catalogFiles.productFiles.length > 0 ? `✅ ${catalogFiles.productFiles.length} product files ready` : '❌ No product files'}
                ${catalogFiles.discontinuedFile ? `<br>✅ Discontinued: ${catalogFiles.discontinuedFile.name}` : '<br>⚠️ No discontinued file'}
                ${catalogFiles.productFiles.length > 0 ? `<br>📅 Last Upload: ${new Date(catalogFiles.productFiles[0].uploadTime).toLocaleString()}` : ''}
            </div>
            <button onclick="apiPost('/api/sync/full','Create/discontinue products?')" class="btn btn-primary" ${isSystemLocked() || catalogFiles.productFiles.length === 0 ?'disabled':''} style="margin-top:0.5rem;">Run Full Import</button>
        </div>
        <div class="card"><h2>One-Off Cleanup</h2>...</div>
    </div>
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message);if(btn)btn.disabled=false;}}
    async function uploadFiles(){
        const zipInput = document.getElementById('zip-input');
        const csvInput = document.getElementById('csv-input');
        if(zipInput.files.length === 0 && csvInput.files.length === 0) {
            alert('Please select at least one file to upload.');
            return;
        }
        const formData = new FormData();
        if(zipInput.files.length > 0) formData.append('product_zip', zipInput.files[0]);
        if(csvInput.files.length > 0) formData.append('discontinued_csv', csvInput.files[0]);
        try {
            document.getElementById('upload-status-display').innerHTML = 'Uploading...';
            const res = await fetch('/api/upload/catalog', { method: 'POST', body: formData });
            const data = await res.json();
            if (res.ok) {
                alert('Files uploaded successfully!');
                location.reload();
            } else {
                alert('Upload failed: ' + data.error);
                location.reload();
            }
        } catch(e) {
            alert('Upload error: ' + e.message);
            location.reload();
        }
    }
    </script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());  // Daily at 2 AM
//cron.schedule('0 4 * * 0', () => syncFullCatalog(), { timezone: 'Europe/London' });  // Weekly Sunday at 4 AM - Disabled for manual import
cron.schedule('0 20 * * *', () => sendDailyReport());  // Daily report at 8 PM
cron.schedule('0 9 * * 1', () => sendWeeklyReport());  // Weekly report Monday at 9 AM

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT} (Location: ${config.shopify.locationId})`, 'success');
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
