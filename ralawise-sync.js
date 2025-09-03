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
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let syncProgress = { 
    inventory: { isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null, cancelled: false }, 
    fullImport: { isActive: false, created: 0, discontinued: 0 },
    cleanup: { isActive: false, current: 0, total: 0 }
};
let inventoryChangeLog = []; // Track actual changes made
let weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, errors: [] };
let discontinuedProducts = new Set(); // Track discontinued SKUs

// Create directories if they don't exist
if (!fs.existsSync(UPLOADED_FILES_DIR)) fs.mkdirSync(UPLOADED_FILES_DIR, { recursive: true });
if (!fs.existsSync(path.join(__dirname, 'uploads'))) fs.mkdirSync(path.join(__dirname, 'uploads'), { recursive: true });

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

// NEW: Extract and process uploaded ZIP files
async function extractUploadedFiles(uploadedFile) {
    try {
        const filePath = uploadedFile.path;
        const originalName = uploadedFile.originalname.toLowerCase();
        
        // Clear existing files
        if (fs.existsSync(UPLOADED_FILES_DIR)) {
            fs.readdirSync(UPLOADED_FILES_DIR).forEach(file => {
                const fullPath = path.join(UPLOADED_FILES_DIR, file);
                if (fs.statSync(fullPath).isDirectory()) {
                    fs.rmSync(fullPath, { recursive: true, force: true });
                } else {
                    fs.unlinkSync(fullPath);
                }
            });
        }
        
        // Check if it's a ZIP file
        if (originalName.endsWith('.zip')) {
            addLog(`Extracting ZIP file: ${originalName}`, 'info');
            
            try {
                const zip = new AdmZip(filePath);
                const tempExtractDir = path.join(UPLOADED_FILES_DIR, 'extracted');
                
                // Extract all files
                zip.extractAllTo(tempExtractDir, true);
                
                // Find all CSV files recursively
                const findCSVFiles = (dir) => {
                    let csvFiles = [];
                    const files = fs.readdirSync(dir);
                    
                    for (const file of files) {
                        const fullPath = path.join(dir, file);
                        const stat = fs.statSync(fullPath);
                        
                        if (stat.isDirectory()) {
                            csvFiles = csvFiles.concat(findCSVFiles(fullPath));
                        } else if (file.toLowerCase().endsWith('.csv')) {
                            csvFiles.push(fullPath);
                        }
                    }
                    
                    return csvFiles;
                };
                
                const csvFiles = findCSVFiles(tempExtractDir);
                addLog(`Found ${csvFiles.length} CSV files in ZIP`, 'info');
                
                // Move CSV files to main directory and rename them
                csvFiles.forEach((csvPath, index) => {
                    const fileName = path.basename(csvPath);
                    let destName = fileName;
                    
                    // Determine file type based on name
                    if (fileName.toLowerCase().includes('discontinue')) {
                        destName = 'discontinued.csv';
                    } else {
                        // For product files, number them if there are multiple
                        destName = csvFiles.length > 2 ? `products_${index + 1}.csv` : 'products.csv';
                    }
                    
                    const destPath = path.join(UPLOADED_FILES_DIR, destName);
                    fs.copyFileSync(csvPath, destPath);
                    addLog(`Extracted: ${fileName} → ${destName}`, 'success');
                });
                
                // Clean up temp directory
                fs.rmSync(tempExtractDir, { recursive: true, force: true });
                
                // Clean up uploaded file
                fs.unlinkSync(filePath);
                
                return { success: true, fileCount: csvFiles.length };
                
            } catch (zipError) {
                addLog(`ZIP extraction failed: ${zipError.message}`, 'error');
                throw zipError;
            }
            
        } else if (originalName.endsWith('.csv')) {
            // Single CSV file - just move it
            const destName = originalName.includes('discontinue') ? 'discontinued.csv' : 'products.csv';
            const destPath = path.join(UPLOADED_FILES_DIR, destName);
            fs.renameSync(filePath, destPath);
            addLog(`Uploaded CSV: ${originalName} → ${destName}`, 'success');
            return { success: true, fileCount: 1 };
            
        } else {
            throw new Error('Invalid file type. Please upload ZIP or CSV files.');
        }
        
    } catch (error) {
        addLog(`File processing error: ${error.message}`, 'error');
        throw error;
    }
}

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
                const stock = parseInt(row['Stock'] || row['Quantity'] || row['Available'] || row['Stock Level'] || '0');
                
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
        if (!fs.existsSync(UPLOADED_FILES_DIR)) return null;
        
        const files = fs.readdirSync(UPLOADED_FILES_DIR)
            .filter(f => f.endsWith('.csv'))
            .map(f => ({
                name: f,
                path: path.join(UPLOADED_FILES_DIR, f),
                uploadTime: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).mtime,
                size: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).size
            }))
            .sort((a, b) => b.uploadTime - a.uploadTime);
        
        // Find product files (all non-discontinued CSV files)
        const productFiles = files.filter(f => !f.name.toLowerCase().includes('discontinue'));
        const discontinuedFile = files.find(f => f.name.toLowerCase().includes('discontinue'));
        
        return { 
            productFiles, 
            discontinuedFile, 
            allFiles: files,
            totalSize: files.reduce((sum, f) => sum + f.size, 0)
        };
    } catch (e) {
        addLog(`Error reading catalog files: ${e.message}`, 'error');
        return null;
    }
}

// Updated inventory sync with better logging
async function updateInventoryWithREST(updates, runResult) {
    addLog(`Updating ${updates.length} items via REST API at location ${config.shopify.locationId}...`, 'info');
    syncProgress.inventory.total = updates.length;
    let processedItems = 0;
    inventoryChangeLog = [];
    
    // Log first 5 items to be updated as examples
    const examples = updates.slice(0, 5);
    addLog(`Sample updates to be performed:`, 'info');
    examples.forEach(u => {
        addLog(`  SKU: ${u.sku}, Product: "${u.match.product.title}", Current: ${u.oldQty}, New: ${u.newQty}, Change: ${u.newQty - u.oldQty > 0 ? '+' : ''}${u.newQty - u.oldQty}`, 'info');
    });

    for (let i = 0; i < updates.length; i++) {
        if (syncProgress.inventory.cancelled) {
            runResult.status = 'cancelled';
            addLog('Inventory update cancelled by user.', 'warning');
            return;
        }

        const update = updates[i];
        
        try {
            // Connect and set inventory
            try {
                await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', {
                    location_id: config.shopify.locationId,
                    inventory_item_id: update.match.variant.inventory_item_id
                });
            } catch (connectError) {
                // Ignore connect errors
            }

            await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                location_id: config.shopify.locationId,
                inventory_item_id: update.match.variant.inventory_item_id,
                available: update.newQty,
                disconnect_if_necessary: true
            });
            
            inventoryChangeLog.push({
                sku: update.sku,
                product: update.match.product.title,
                oldQty: update.oldQty,
                newQty: update.newQty,
                change: update.newQty - update.oldQty,
                status: 'success'
            });
            
            if (processedItems < 3) {
                addLog(`✅ Updated SKU ${update.sku}: ${update.oldQty} → ${update.newQty}`, 'success');
            }
            
            runResult.updated++;
            processedItems++;
            weeklyReport.inventoryUpdates++;
        } catch (e) {
            const errorMsg = e.response?.data?.errors || e.response?.data?.error || e.message;
            if (runResult.errors < 10) {
                addLog(`❌ Failed to update SKU ${update.sku}: ${errorMsg}`, 'error');
            }
            runResult.errors++;
            weeklyReport.errors.push({ sku: update.sku, error: errorMsg, timestamp: new Date().toISOString() });
        }
        
        updateProgress('inventory', i + 1, updates.length);
        
        if ((i + 1) % 100 === 0 || i === updates.length - 1) {
            addLog(`Progress: ${i + 1}/${updates.length} items processed (${processedItems} successful, ${runResult.errors} errors)`, 'info');
        }
    }
    
    // Save change log
    const logFileName = `inventory_changes_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    fs.writeFileSync(path.join(__dirname, logFileName), JSON.stringify(inventoryChangeLog, null, 2));
    addLog(`Change log saved to: ${logFileName}`, 'info');
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
        addToHistory({ ...runResult, timestamp: new Date().toISOString(), changeLog: inventoryChangeLog.slice(0, 100) });
    };

    try {
        addLog(`=== INVENTORY SYNC: ANALYSIS (Location: ${config.shopify.locationId}) ===`, 'info');
        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); }));
        
        addLog(`Total SKUs in Shopify: ${skuToProduct.size}`, 'info');
        addLog(`Total SKUs from Ralawise: ${inventoryMap.size}`, 'info');
        
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
            await updateInventoryWithREST(updatesToPerform, runResult);
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

// Updated full import to handle multiple product CSV files
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
            throw new Error('No product catalog files found. Please upload CSV or ZIP files first.');
        }
        
        addLog(`Using ${catalogFiles.productFiles.length} product file(s), total size: ${(catalogFiles.totalSize / 1024 / 1024).toFixed(2)} MB`, 'info');
        
        // Parse discontinued items if file exists
        let discontinuedMap = new Map();
        if (catalogFiles.discontinuedFile) {
            addLog(`Using discontinued file: ${catalogFiles.discontinuedFile.name}`, 'info');
            discontinuedMap = await parseDiscontinuedCSV(catalogFiles.discontinuedFile.path);
        }
        
        // Parse all product catalog files and combine them
        addLog(`Processing ${catalogFiles.productFiles.length} product files...`, 'info');
        const allRows = [];
        
        for (let i = 0; i < catalogFiles.productFiles.length; i++) {
            const file = catalogFiles.productFiles[i];
            addLog(`Reading file ${i + 1}/${catalogFiles.productFiles.length}: ${file.name}`, 'info');
            
            const rows = await new Promise((res, rej) => { 
                const p = []; 
                fs.createReadStream(file.path)
                    .pipe(csv())
                    .on('data', r => p.push({ ...r, price: applyRalawisePricing(parseFloat(r['Variant Price'])) }))
                    .on('end', () => res(p))
                    .on('error', rej); 
            });
            
            allRows.push(...rows);
            addLog(`  Loaded ${rows.length} rows from ${file.name}`, 'info');
        }
        
        addLog(`Total rows loaded: ${allRows.length}`, 'info');
        
        const productsByHandle = new Map();
        
        for (const row of allRows) {
            if (!row.Handle) continue;
            
            const sku = row['Variant SKU'] || '';
            const discontinuedInfo = discontinuedMap.get(sku.toUpperCase());
            
            // Skip if item is discontinued with low stock
            if (discontinuedInfo?.shouldDiscontinue) {
                continue; // Skip silently to avoid log spam
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
        
        addLog(`Parsed ${productsByHandle.size} unique products from CSV files`, 'info');
        
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
                        } catch (e) {
                            // Ignore inventory errors on creation
                        }
                    }
                }
                
                runResult.created++;
                runResult.createdProducts.push({ title: p.Title, handle: p.Handle });
                weeklyReport.productsCreated++;
                addLog(`✅ Created: ${p.Title}`, 'success');
            } catch (e) { 
                runResult.errors++; 
                addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error');
                weeklyReport.errors.push({ product: p.Title, error: e.message, timestamp: new Date().toISOString() });
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
                        } catch (e) {
                            // Ignore inventory errors
                        }
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

// NEW: One-off cleanup function
async function cleanupProductsWithoutSupplierTags() {
    if (isRunning.cleanup) return;
    isRunning.cleanup = true;
    
    let runResult = { type: 'Cleanup', status: 'failed', deleted: 0, skipped: 0, errors: 0 };
    syncProgress.cleanup = { isActive: true, current: 0, total: 0, startTime: Date.now() };
    
    const finalizeRun = (status) => {
        runResult.status = status;
        const runtime = ((Date.now() - syncProgress.cleanup.startTime) / 1000 / 60).toFixed(1);
        const finalMsg = `Cleanup ${runResult.status} in ${runtime}m:\n🗑️ ${runResult.deleted} deleted\n⏭️ ${runResult.skipped} skipped\n❌ ${runResult.errors} errors`;
        notifyTelegram(finalMsg);
        addLog(finalMsg, 'success');
        isRunning.cleanup = false;
        syncProgress.cleanup.isActive = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== ONE-OFF CLEANUP: FINDING PRODUCTS WITHOUT SUPPLIER TAGS ===', 'warning');
        const allProducts = await getAllShopifyProducts();
        
        // Find products without any preserved supplier tags
        const productsToDelete = allProducts.filter(p => {
            const tags = p.tags || '';
            return !config.ralawise.preservedSuppliers.some(supplier => tags.includes(supplier));
        });
        
        addLog(`Found ${productsToDelete.length} products without supplier tags to delete`, 'warning');
        syncProgress.cleanup.total = productsToDelete.length;
        
        const executeCleanup = async () => {
            for (let i = 0; i < productsToDelete.length; i++) {
                const product = productsToDelete[i];
                try {
                    // Set to draft and zero inventory
                    await shopifyRequestWithRetry('put', `/products/${product.id}.json`, {
                        product: { id: product.id, status: 'draft' }
                    });
                    
                    // Zero out inventory for all variants
                    for (const variant of product.variants || []) {
                        if (variant.inventory_item_id) {
                            try {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                    location_id: config.shopify.locationId,
                                    inventory_item_id: variant.inventory_item_id,
                                    available: 0
                                });
                            } catch (e) {
                                // Ignore inventory errors
                            }
                        }
                    }
                    
                    runResult.deleted++;
                    if (runResult.deleted % 10 === 0) {
                        addLog(`Cleaned up ${runResult.deleted} products so far...`, 'info');
                    }
                } catch (e) {
                    runResult.errors++;
                    addLog(`Failed to cleanup product ${product.title}: ${e.message}`, 'error');
                }
                
                updateProgress('cleanup', i + 1, productsToDelete.length);
            }
            
            finalizeRun('completed');
        };
        
        if (productsToDelete.length > 50) {
            requestConfirmation('cleanup', `Delete ${productsToDelete.length} products without supplier tags?`, 
                { productCount: productsToDelete.length, samples: productsToDelete.slice(0, 5).map(p => p.title) }, 
                executeCleanup);
        } else if (productsToDelete.length > 0) {
            await executeCleanup();
        } else {
            addLog('No products to cleanup', 'info');
            finalizeRun('completed');
        }
    } catch (error) {
        addLog(`Cleanup failed: ${error.message}`, 'error');
        finalizeRun('failed');
    }
}

// ============================================
// REPORTING FUNCTIONS
// ============================================

async function sendDailyReport() {
    const today = runHistory.filter(r => {
        const runDate = new Date(r.timestamp);
        const now = new Date();
        return runDate.toDateString() === now.toDateString() && r.type === 'Inventory';
    });
    
    const totalUpdated = today.reduce((sum, r) => sum + (r.updated || 0), 0);
    const totalErrors = today.reduce((sum, r) => sum + (r.errors || 0), 0);
    
    const message = `📊 <b>Daily Inventory Report</b>\n\n` +
        `📅 Date: ${new Date().toLocaleDateString()}\n` +
        `🔄 Sync Runs: ${today.length}\n` +
        `✅ Items Updated: ${totalUpdated}\n` +
        `❌ Errors: ${totalErrors}`;
    
    notifyTelegram(message);
    addLog('Daily report sent', 'info');
}

async function sendWeeklyReport() {
    const message = `📊 <b>Weekly Summary Report</b>\n\n` +
        `📅 Week Ending: ${new Date().toLocaleDateString()}\n\n` +
        `<b>Inventory:</b>\n` +
        `✅ Total Updates: ${weeklyReport.inventoryUpdates}\n\n` +
        `<b>Products:</b>\n` +
        `➕ Created: ${weeklyReport.productsCreated}\n` +
        `➖ Discontinued: ${weeklyReport.productsDiscontinued}\n\n` +
        `<b>Errors:</b>\n` +
        `❌ Total Errors: ${weeklyReport.errors.length}\n` +
        `${weeklyReport.errors.length > 0 ? `Most Recent: ${weeklyReport.errors[weeklyReport.errors.length - 1]?.error || 'N/A'}` : ''}`;
    
    notifyTelegram(message);
    addLog('Weekly report sent', 'info');
    
    // Reset weekly counters
    weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, errors: [] };
}

// ============================================
// SYNC WRAPPERS & API
// ============================================

async function syncInventory() { if (isSystemLocked()) return; try { await updateInventoryBySKU(await parseInventoryCSV(await fetchInventoryFromFTP())); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); } }
async function syncFullCatalog() { if (isSystemLocked()) return; try { await processFullImport(); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); } }

// File upload endpoints
app.post('/api/upload/catalog', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }
        
        const result = await extractUploadedFiles(req.file);
        
        res.json({ 
            success: true, 
            ...result,
            fileName: req.file.originalname,
            fileSize: req.file.size
        });
    } catch (error) {
        addLog(`Upload failed: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/upload/status', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    res.json({
        hasFiles: !!catalogFiles?.productFiles?.length,
        productFiles: catalogFiles?.productFiles?.map(f => f.name) || [],
        productFileCount: catalogFiles?.productFiles?.length || 0,
        discontinuedFile: catalogFiles?.discontinuedFile?.name,
        uploadTime: catalogFiles?.productFiles?.[0]?.uploadTime,
        totalSize: catalogFiles?.totalSize || 0,
        allFiles: catalogFiles?.allFiles || []
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
app.get('/api/changes/latest', (req, res) => { res.json(inventoryChangeLog); });
const isSystemLocked = () => isRunning.inventory || isRunning.fullImport || isRunning.cleanup || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const inventoryProgressHTML = syncProgress.inventory.isActive && syncProgress.inventory.total > 0 ? `<div class="progress-container"><div class="progress-bar" style="width:${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%"></div><small>${syncProgress.inventory.current}/${syncProgress.inventory.total} items</small></div>` : '';
    const cleanupProgressHTML = syncProgress.cleanup.isActive && syncProgress.cleanup.total > 0 ? `<div class="progress-container"><div class="progress-bar" style="width:${(syncProgress.cleanup.current / syncProgress.cleanup.total * 100).toFixed(1)}%"></div><small>${syncProgress.cleanup.current}/${syncProgress.cleanup.total} products</small></div>` : '';
    const lastFullImport = runHistory.find(r => r.type === 'Full Import');
    const lastInventorySync = runHistory.find(r => r.type === 'Inventory');
    const catalogFiles = getLatestCatalogFiles();
    
    let newProductsHTML = '<h4>Newly Created Products</h4><p>No new products in last run.</p>';
    if (lastFullImport?.createdProducts?.length > 0) {
        newProductsHTML = `<h4>Newly Created Products (${lastFullImport.createdProducts.length})</h4>
        <ul class="product-list">${lastFullImport.createdProducts.slice(0, 10).map(p => `<li><a href="https://${config.shopify.domain}/products/${p.handle}" target="_blank">${p.title}</a></li>`).join('')}</ul>`;
    }
    
    let recentChangesHTML = '';
    if (lastInventorySync?.changeLog?.length > 0) {
        const successful = lastInventorySync.changeLog.filter(c => c.status.includes('success'));
        recentChangesHTML = `<div class="card"><h2>Recent Inventory Changes</h2><p>Last sync: ${new Date(lastInventorySync.timestamp).toLocaleString()}</p><ul class="product-list">${successful.slice(0, 10).map(c => `<li>${c.sku}: ${c.oldQty} → ${c.newQty} (${c.change > 0 ? '+' : ''}${c.change})</li>`).join('')}</ul></div>`;
    }

    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn-warning{background:#d29922;color:white;border-color:#f0c674}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2.5rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem;} .progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden; width: 100%;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}.file-upload{margin-top:1rem;padding:1rem;border:2px dashed #30363d;border-radius:6px;text-align:center;}.file-upload input{display:none;}.file-upload label{cursor:pointer;padding:0.5rem 1rem;background:#21262d;border-radius:6px;display:inline-block;}.upload-status{margin-top:0.5rem;font-size:0.875em;color:#8b949e;}.file-list{text-align:left;margin-top:0.5rem;font-size:0.8em;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': isRunning.inventory || isRunning.fullImport || isRunning.cleanup ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationId}</div></div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button>${isRunning.inventory?`<button onclick="apiPost('/api/sync/inventory/cancel')" class="btn btn-danger">Cancel</button>`:''}${inventoryProgressHTML}</div>
        <div class="card">
            <h2>Full Catalog Import</h2>
            <p>Status: ${isRunning.fullImport?'Running':'Ready'}</p>
            <div class="file-upload">
                <label for="file-input" class="btn">📁 Upload ZIP/CSV</label>
                <input type="file" id="file-input" accept=".csv,.zip" onchange="uploadFile(this)">
                <div class="upload-status">
                    ${catalogFiles?.productFiles?.length ? `✅ ${catalogFiles.productFileCount} product files (${(catalogFiles.totalSize / 1024 / 1024).toFixed(1)} MB)` : '❌ No product files'}
                    ${catalogFiles?.discontinuedFile ? `<br>✅ Discontinued: ${catalogFiles.discontinuedFile.name}` : '<br>⚠️ No discontinued file (optional)'}
                    ${catalogFiles?.productFiles?.length ? `<br>📅 Uploaded: ${new Date(catalogFiles.productFiles[0].uploadTime).toLocaleString()}` : ''}
                    ${catalogFiles?.productFiles?.length > 3 ? `<div class="file-list">Files: ${catalogFiles.productFiles.slice(0,3).map(f => f.name).join(', ')}...</div>` : ''}
                </div>
            </div>
            <button onclick="apiPost('/api/sync/full','Create/discontinue products?')" class="btn" ${isSystemLocked() || !catalogFiles?.productFiles?.length ?'disabled':''} style="margin-top:0.5rem;">Run Import</button>
        </div>
        <div class="card"><h2>One-Off Cleanup</h2><p>Status: ${isRunning.cleanup?'Running':'Ready'}</p><button onclick="apiPost('/api/cleanup/once','⚠️ DELETE all products without Supplier tags?')" class="btn btn-warning" ${isSystemLocked()?'disabled':''}>Clean Non-Supplier Products</button>${cleanupProgressHTML}<p style="font-size:0.8em;color:#8b949e;margin-top:0.5rem;">Preserves: ${config.ralawise.preservedSuppliers.join(', ')}</p></div>
    </div>
    <div class="grid">
        <div class="card"><h2>Last Full Import Summary</h2><div class="grid" style="grid-template-columns:1fr 1fr;"><div class="stat-card"><div class="stat-value">${lastFullImport?.created ?? 'N/A'}</div><div class="stat-label">New Products</div></div><div class="stat-card"><div class="stat-value">${lastFullImport?.discontinued ?? 'N/A'}</div><div class="stat-label">Discontinued</div></div></div><hr style="border-color:#30363d;margin:1rem 0;">${newProductsHTML}</div>
        <div class="card"><h2>Weekly Stats</h2><div class="grid" style="grid-template-columns:1fr 1fr 1fr;"><div class="stat-card"><div class="stat-value">${weeklyReport.inventoryUpdates}</div><div class="stat-label">Inventory Updates</div></div><div class="stat-card"><div class="stat-value">${weeklyReport.productsCreated}</div><div class="stat-label">Products Created</div></div><div class="stat-card"><div class="stat-value">${weeklyReport.productsDiscontinued}</div><div class="stat-label">Products Discontinued</div></div></div></div>
    </div>
    ${recentChangesHTML}
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message);if(btn)btn.disabled=false;}}
    async function uploadFile(input){const file=input.files[0];if(!file)return;const formData=new FormData();formData.append('file',file);try{const label=document.querySelector('label[for="file-input"]');label.textContent='Uploading...';const res=await fetch('/api/upload/catalog',{method:'POST',body:formData});const data=await res.json();if(data.success){alert('File uploaded successfully! '+(data.fileCount?data.fileCount+' CSV files extracted.':''));location.reload();}else{alert('Upload failed: '+data.error);label.textContent='📁 Upload ZIP/CSV';}}catch(e){alert('Upload error: '+e.message);document.querySelector('label[for="file-input"]').textContent='📁 Upload ZIP/CSV';}}
    </script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());  // Daily at 2 AM
cron.schedule('0 4 * * 0', () => syncFullCatalog(), { timezone: 'Europe/London' });  // Weekly Sunday at 4 AM
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
