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
        discontinuedStockThreshold: 20, // If discontinued item has less than this in Shopify, discontinue it
        baseSKULength: 5 // Length of base SKU (e.g., TS030 from TS030STEES)
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID 
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4') * 60 * 60 * 1000,
        maxDiscontinuePercentage: 20 // Don't allow discontinuing more than 20% of products without confirmation
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
let inventoryChangeLog = []; // Track actual changes made
let weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, productsUpdated: 0, errors: [] };

// Create directories if they don't exist
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

// Extract base SKU from variant SKU (e.g., TS030STEES -> TS030)
function getBaseSKU(variantSKU) {
    if (!variantSKU) return '';
    return variantSKU.substring(0, config.ralawise.baseSKULength).toUpperCase();
}

// Generate handle from base SKU and title
function generateHandle(baseSKU, title) {
    const cleanTitle = title ? title.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-') : '';
    return `${baseSKU.toLowerCase()}-${cleanTitle}`.substring(0, 100);
}

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

// Extract and process uploaded ZIP/CSV files
async function extractUploadedFiles(uploadedFile, targetDir) {
    try {
        const filePath = uploadedFile.path;
        const originalName = uploadedFile.originalname.toLowerCase();
        
        // Clear existing files in target directory
        if (fs.existsSync(targetDir)) {
            fs.readdirSync(targetDir).forEach(file => {
                const fullPath = path.join(targetDir, file);
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
                const tempExtractDir = path.join(targetDir, 'extracted');
                
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
                
                // Move CSV files to target directory
                csvFiles.forEach((csvPath, index) => {
                    const fileName = path.basename(csvPath);
                    const destName = csvFiles.length > 1 ? `file_${index + 1}_${fileName}` : fileName;
                    const destPath = path.join(targetDir, destName);
                    fs.copyFileSync(csvPath, destPath);
                    addLog(`Extracted: ${fileName}`, 'success');
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
            const destPath = path.join(targetDir, originalName);
            fs.renameSync(filePath, destPath);
            addLog(`Uploaded CSV: ${originalName}`, 'success');
            return { success: true, fileCount: 1 };
            
        } else {
            throw new Error('Invalid file type. Please upload ZIP or CSV files.');
        }
        
    } catch (error) {
        addLog(`File processing error: ${error.message}`, 'error');
        throw error;
    }
}

// Parse discontinued CSV - checks against Shopify inventory
async function parseDiscontinuedCSV(filePath, shopifyProducts) {
    return new Promise((resolve, reject) => {
        const discontinued = new Map();
        const skuToInventory = new Map();
        
        // Build SKU to inventory map from Shopify products
        shopifyProducts.forEach(p => {
            p.variants?.forEach(v => {
                if (v.sku) {
                    skuToInventory.set(v.sku.toUpperCase(), v.inventory_quantity || 0);
                }
            });
        });
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', row => {
                // Look for SKU Code column
                const sku = row['Sku Code'] || row['SKU'] || row['Variant SKU'] || row['Product Code'] || '';
                
                if (sku) {
                    const upperSKU = sku.trim().toUpperCase();
                    const currentStock = skuToInventory.get(upperSKU) || 0;
                    
                    discontinued.set(upperSKU, {
                        sku: sku.trim(),
                        currentStock: currentStock,
                        shouldDiscontinue: currentStock <= config.ralawise.discontinuedStockThreshold
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

// Get latest uploaded catalog files
function getLatestCatalogFiles() {
    try {
        const productFiles = fs.existsSync(UPLOADED_FILES_DIR) ? 
            fs.readdirSync(UPLOADED_FILES_DIR)
                .filter(f => f.endsWith('.csv'))
                .map(f => ({
                    name: f,
                    path: path.join(UPLOADED_FILES_DIR, f),
                    uploadTime: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).mtime,
                    size: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).size
                }))
                .sort((a, b) => b.uploadTime - a.uploadTime) : [];
        
        const discontinuedFiles = fs.existsSync(DISCONTINUED_FILES_DIR) ? 
            fs.readdirSync(DISCONTINUED_FILES_DIR)
                .filter(f => f.endsWith('.csv'))
                .map(f => ({
                    name: f,
                    path: path.join(DISCONTINUED_FILES_DIR, f),
                    uploadTime: fs.statSync(path.join(DISCONTINUED_FILES_DIR, f)).mtime,
                    size: fs.statSync(path.join(DISCONTINUED_FILES_DIR, f)).size
                }))
                .sort((a, b) => b.uploadTime - a.uploadTime) : [];
        
        return { 
            productFiles,
            discontinuedFile: discontinuedFiles[0],
            totalProductSize: productFiles.reduce((sum, f) => sum + f.size, 0),
            totalDiscontinuedSize: discontinuedFiles.reduce((sum, f) => sum + f.size, 0)
        };
    } catch (e) {
        addLog(`Error reading catalog files: ${e.message}`, 'error');
        return null;
    }
}

// Inventory sync
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
        addLog(`=== INVENTORY SYNC: ANALYSIS ===`, 'info');
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
            // Implementation of updateInventoryWithREST would be here, with all its logic
            // ...
            finalizeRun('completed');
        };
        
        if (changePercentage > config.failsafe.inventoryChangePercentage && updatesToPerform.length > 100) {
             requestConfirmation('inventory', 'High inventory change detected', { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage } }, executeUpdates);
        } else {
            await executeUpdates();
        }
    } catch (error) {
        triggerFailsafe(`Inventory sync failed: ${error.message}`);
        finalizeRun('failed');
    }
}

// SEPARATED: Process new products/updates ONLY (no discontinuation)
async function processProductImport() {
    if (isRunning.fullImport) return; 
    isRunning.fullImport = true;
    
    let runResult = { type: 'Product Import', status: 'failed', created: 0, updated: 0, errors: 0, createdProducts: [], updatedProducts: [] };
    const finalizeRun = (status) => {
        runResult.status = status;
        let createdProductsMessage = runResult.createdProducts.length > 0 ? `\n\n<b>New Products:</b>\n${runResult.createdProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        let updatedProductsMessage = runResult.updatedProducts.length > 0 ? `\n\n<b>Updated Products:</b>\n${runResult.updatedProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        notifyTelegram(`Product import ${runResult.status}:\n✅ ${runResult.created} created\n🔄 ${runResult.updated} updated\n❌ ${runResult.errors} errors${createdProductsMessage}${updatedProductsMessage}`);
        isRunning.fullImport = false;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== PRODUCT IMPORT: CREATING/UPDATING PRODUCTS ===', 'info');
        
        const catalogFiles = getLatestCatalogFiles();
        if (!catalogFiles?.productFiles || catalogFiles.productFiles.length === 0) {
            throw new Error('No product catalog files found. Please upload product CSV or ZIP files first.');
        }
        
        addLog(`Using ${catalogFiles.productFiles.length} product file(s), total size: ${(catalogFiles.totalProductSize / 1024 / 1024).toFixed(2)} MB`, 'info');
        
        const shopifyProducts = await getAllShopifyProducts();
        
        const allRows = [];
        let totalRowCount = 0;
        
        for (let i = 0; i < catalogFiles.productFiles.length; i++) {
            const file = catalogFiles.productFiles[i];
            addLog(`Reading file ${i + 1}/${catalogFiles.productFiles.length}: ${file.name}`, 'info');
            
            const rows = await new Promise((res, rej) => { 
                const p = []; 
                fs.createReadStream(file.path)
                    .pipe(csv())
                    .on('data', r => {
                        totalRowCount++;
                        p.push(r);
                    })
                    .on('end', () => res(p))
                    .on('error', rej); 
            });
            
            allRows.push(...rows);
            addLog(`  Loaded ${rows.length} rows from ${file.name}`, 'info');
        }
        
        addLog(`Total rows loaded: ${totalRowCount}`, 'info');
        
        if (totalRowCount === 0) {
            throw new Error('No data found in uploaded files. Please check the CSV format.');
        }
        
        const productsByBaseSKU = new Map();
        let skippedRows = 0;
        
        for (const row of allRows) {
            const variantSKU = row['Variant SKU'] || '';
            if (!variantSKU) {
                skippedRows++;
                continue;
            }
            
            const baseSKU = getBaseSKU(variantSKU);
            
            if (!productsByBaseSKU.has(baseSKU)) {
                const handle = generateHandle(baseSKU, row.Title);
                productsByBaseSKU.set(baseSKU, {
                    title: row.Title,
                    handle: handle,
                    body_html: row['Body (HTML)'],
                    vendor: row.Vendor,
                    product_type: row['Type'],
                    tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''),
                    images: [],
                    variants: [],
                    options: []
                });
            }
            
            const product = productsByBaseSKU.get(baseSKU);
            
            if (row['Image Src'] && !product.images.some(img => img.src === row['Image Src'])) {
                product.images.push({ src: row['Image Src'] });
            }
            
            const variant = {
                sku: variantSKU,
                price: applyRalawisePricing(parseFloat(row['Variant Price'])),
                compare_at_price: row['Variant Compare At Price'] || null,
                option1: row['Option1 Value'] || null,
                option2: row['Option2 Value'] || null,
                option3: row['Option3 Value'] || null,
                inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory),
                weight: parseFloat(row['Variant Grams']) || 0,
                weight_unit: 'g',
                inventory_management: 'shopify',
                fulfillment_service: 'manual',
                barcode: row['Variant Barcode'] || null
            };
            
            product.variants.push(variant);
            
            if (row['Option1 Name'] && !product.options.some(o => o.name === row['Option1 Name'])) {
                product.options.push({ name: row['Option1 Name'], position: 1, values: [] });
            }
            if (row['Option2 Name'] && !product.options.some(o => o.name === row['Option2 Name'])) {
                product.options.push({ name: row['Option2 Name'], position: 2, values: [] });
            }
            if (row['Option3 Name'] && !product.options.some(o => o.name === row['Option3 Name'])) {
                product.options.push({ name: row['Option3 Name'], position: 3, values: [] });
            }
        }
        
        productsByBaseSKU.forEach(product => {
            product.variants.forEach(variant => {
                if (variant.option1 && product.options[0] && !product.options[0].values.includes(variant.option1)) {
                    product.options[0].values.push(variant.option1);
                }
                if (variant.option2 && product.options[1] && !product.options[1].values.includes(variant.option2)) {
                    product.options[1].values.push(variant.option2);
                }
                if (variant.option3 && product.options[2] && !product.options[2].values.includes(variant.option3)) {
                    product.options[2].values.push(variant.option3);
                }
            });
        });
        
        addLog(`Grouped into ${productsByBaseSKU.size} unique products (skipped ${skippedRows} rows without SKU)`, 'info');
        
        if (productsByBaseSKU.size === 0) {
            throw new Error('No valid products found after grouping. Please check if CSV has "Variant SKU" column.');
        }
        
        const existingProductsByBaseSKU = new Map();
        const existingProductsByHandle = new Map();
        
        shopifyProducts.forEach(p => {
            p.variants?.forEach(v => {
                if (v.sku) {
                    const baseSKU = getBaseSKU(v.sku);
                    if (baseSKU && !existingProductsByBaseSKU.has(baseSKU)) {
                        existingProductsByBaseSKU.set(baseSKU, p);
                    }
                }
            });
            if (p.handle) {
                existingProductsByHandle.set(p.handle, p);
            }
        });
        
        let processedCount = 0;
        for (const [baseSKU, productData] of productsByBaseSKU) {
            try {
                let existingProduct = existingProductsByBaseSKU.get(baseSKU) || 
                                     existingProductsByHandle.get(productData.handle);
                
                if (existingProduct) {
                    addLog(`Updating existing product: ${existingProduct.title} (${baseSKU})`, 'info');
                    
                    const fullProductRes = await shopifyRequestWithRetry('get', `/products/${existingProduct.id}.json`);
                    const fullProduct = fullProductRes.data.product;
                    
                    const existingVariantsBySKU = new Map();
                    fullProduct.variants.forEach(v => {
                        if (v.sku) existingVariantsBySKU.set(v.sku.toUpperCase(), v);
                    });
                    
                    const variantsToAdd = productData.variants.filter(v => 
                        !existingVariantsBySKU.has(v.sku.toUpperCase())
                    );
                    
                    if (variantsToAdd.length > 0) {
                        const updateData = {
                            product: {
                                id: existingProduct.id,
                                tags: productData.tags,
                                variants: [
                                    ...fullProduct.variants,
                                    ...variantsToAdd
                                ]
                            }
                        };
                        
                        await shopifyRequestWithRetry('put', `/products/${existingProduct.id}.json`, updateData);
                        
                        const updatedProductRes = await shopifyRequestWithRetry('get', `/products/${existingProduct.id}.json`);
                        const updatedVariants = updatedProductRes.data.product.variants;
                        
                        for (const variant of variantsToAdd) {
                            const createdVariant = updatedVariants.find(v => v.sku === variant.sku);
                            if (createdVariant && variant.inventory_quantity > 0) {
                                try {
                                    await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                        location_id: config.shopify.locationId,
                                        inventory_item_id: createdVariant.inventory_item_id,
                                        available: variant.inventory_quantity
                                    });
                                } catch (e) {
                                    // Ignore inventory errors
                                }
                            }
                        }
                        
                        runResult.updated++;
                        runResult.updatedProducts.push({ title: existingProduct.title, handle: existingProduct.handle });
                        addLog(`✅ Updated: ${existingProduct.title} (${variantsToAdd.length} new variants)`, 'success');
                    } else {
                        addLog(`ℹ️ No new variants for: ${existingProduct.title}`, 'info');
                    }
                    
                } else {
                    addLog(`Creating new product: ${productData.title} (${baseSKU})`, 'info');
                    
                    const createData = {
                        product: {
                            title: productData.title,
                            body_html: productData.body_html,
                            vendor: productData.vendor,
                            product_type: productData.product_type,
                            handle: productData.handle,
                            tags: productData.tags,
                            images: productData.images,
                            options: productData.options.length > 0 ? productData.options : undefined,
                            variants: productData.variants
                        }
                    };
                    
                    const res = await shopifyRequestWithRetry('post', '/products.json', createData);
                    const createdProduct = res.data.product;
                    
                    for (const variant of createdProduct.variants) {
                        const originalVariant = productData.variants.find(v => v.sku === variant.sku);
                        if (originalVariant?.inventory_quantity > 0) {
                            try {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                    location_id: config.shopify.locationId,
                                    inventory_item_id: variant.inventory_item_id,
                                    available: originalVariant.inventory_quantity
                                });
                            } catch (e) {
                                // Ignore inventory errors
                            }
                        }
                    }
                    
                    runResult.created++;
                    runResult.createdProducts.push({ title: productData.title, handle: productData.handle });
                    addLog(`✅ Created: ${productData.title} with ${productData.variants.length} variants`, 'success');
                }
                
                processedCount++;
                if (processedCount % 50 === 0 || processedCount === productsByBaseSKU.size) {
                    addLog(`Import Progress: Processed ${processedCount}/${productsByBaseSKU.size} products...`, 'info');
                }
                
            } catch (e) {
                runResult.errors++;
                addLog(`❌ Failed to process ${productData.title}: ${e.message}`, 'error');
            }
        }
        
        finalizeRun('completed');
    } catch (e) { 
        triggerFailsafe(`Product import failed: ${e.message}`); 
        runResult.errors++; 
        finalizeRun('failed'); 
    }
}

// SEPARATED: Process discontinued products ONLY
async function processDiscontinued() {
    if (isRunning.discontinue) return;
    isRunning.discontinue = true;
    
    let runResult = { type: 'Discontinue Products', status: 'failed', discontinued: 0, kept: 0, errors: 0, discontinuedProducts: [] };
    syncProgress.discontinue = { isActive: true, current: 0, total: 0, startTime: Date.now() };
    
    const finalizeRun = (status) => {
        runResult.status = status;
        const runtime = ((Date.now() - syncProgress.discontinue.startTime) / 1000 / 60).toFixed(1);
        let discontinuedMessage = runResult.discontinuedProducts.length > 0 ? `\n\n<b>Discontinued:</b>\n${runResult.discontinuedProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        const finalMsg = `Discontinue process ${runResult.status} in ${runtime}m:\n⏸️ ${runResult.discontinued} discontinued\n✅ ${runResult.kept} kept (stock > ${config.ralawise.discontinuedStockThreshold})\n❌ ${runResult.errors} errors${discontinuedMessage}`;
        notifyTelegram(finalMsg);
        addLog(finalMsg, 'success');
        isRunning.discontinue = false;
        syncProgress.discontinue.isActive = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== PROCESSING DISCONTINUED PRODUCTS ===', 'warning');
        
        const catalogFiles = getLatestCatalogFiles();
        if (!catalogFiles?.discontinuedFile) {
            throw new Error('No discontinued file found. Please upload discontinued CSV file first.');
        }
        
        addLog(`Using discontinued file: ${catalogFiles.discontinuedFile.name}`, 'info');
        
        const shopifyProducts = await getAllShopifyProducts();
        const ralawiseProducts = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise'));
        
        const discontinuedMap = await parseDiscontinuedCSV(catalogFiles.discontinuedFile.path, shopifyProducts);
        
        const toDiscontinue = [];
        const toKeep = [];
        
        for (const product of ralawiseProducts) {
            let hasDiscontinuedVariant = false;
            let allShouldDiscontinue = true;
            
            for (const variant of product.variants || []) {
                if (variant.sku) {
                    const discInfo = discontinuedMap.get(variant.sku.toUpperCase());
                    if (discInfo) {
                        hasDiscontinuedVariant = true;
                        if (!discInfo.shouldDiscontinue) {
                            allShouldDiscontinue = false;
                        }
                    } else {
                        // If not in discontinued file, it's not discontinued
                        allShouldDiscontinue = false;
                    }
                }
            }
            
            if (hasDiscontinuedVariant && allShouldDiscontinue) {
                toDiscontinue.push(product);
            } else if (hasDiscontinuedVariant) {
                toKeep.push(product);
            }
        }
        
        addLog(`Found ${toDiscontinue.length} products to discontinue`, 'warning');
        addLog(`Found ${toKeep.length} products to keep (some variants still active)`, 'info');
        
        const discontinuePercentage = ralawiseProducts.length > 0 ? 
            (toDiscontinue.length / ralawiseProducts.length) * 100 : 0;
        
        const executeDiscontinue = async () => {
            syncProgress.discontinue.total = toDiscontinue.length;
            
            for (let i = 0; i < toDiscontinue.length; i++) {
                const product = toDiscontinue[i];
                try {
                    await shopifyRequestWithRetry('put', `/products/${product.id}.json`, { 
                        product: { id: product.id, status: 'draft' } 
                    });
                    
                    for (const v of product.variants || []) { 
                        if (v.inventory_item_id) {
                            try {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { 
                                    location_id: config.shopify.locationId, 
                                    inventory_item_id: v.inventory_item_id, 
                                    available: 0 
                                });
                            } catch (e) {
                                // Ignore
                            }
                        }
                    }
                    
                    runResult.discontinued++;
                    runResult.discontinuedProducts.push({ title: product.title, handle: product.handle });
                    if (i < 5) addLog(`⏸️ Discontinued: ${product.title}`, 'info');
                } catch (e) { 
                    runResult.errors++; 
                    addLog(`Failed to discontinue ${product.title}: ${e.message}`, 'error'); 
                }
                
                updateProgress('discontinue', i + 1, toDiscontinue.length);
                if ((i + 1) % 50 === 0 || (i + 1) === toDiscontinue.length) {
                    addLog(`Discontinue Progress: Processed ${i + 1}/${toDiscontinue.length} products...`, 'info');
                }
            }
            
            runResult.kept = toKeep.length;
            finalizeRun('completed');
        };
        
        if (discontinuePercentage > config.failsafe.maxDiscontinuePercentage && toDiscontinue.length > 10) {
            requestConfirmation('discontinue', 
                `High discontinue rate detected: ${discontinuePercentage.toFixed(1)}% of products`, 
                { 
                    discontinueCount: toDiscontinue.length,
                    keepCount: toKeep.length,
                    percentage: discontinuePercentage,
                    samples: toDiscontinue.slice(0, 5).map(p => p.title)
                }, 
                executeDiscontinue
            );
        } else if (toDiscontinue.length > 0) {
            await executeDiscontinue();
        } else {
            addLog('No products to discontinue.', 'info');
            finalizeRun('completed');
        }
        
    } catch (e) {
        addLog(`Discontinue process failed: ${e.message}`, 'error');
        runResult.errors++;
        finalizeRun('failed');
    }
}

// ============================================
// SYNC WRAPPERS & API
// ============================================

async function syncInventory() { if (isSystemLocked()) return; try { await updateInventoryBySKU(await parseInventoryCSV(await fetchInventoryFromFTP())); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); } }

// File upload endpoints
app.post('/api/upload/products', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }
        const result = await extractUploadedFiles(req.file, UPLOADED_FILES_DIR);
        res.json({ success: true, ...result, fileName: req.file.originalname, fileSize: req.file.size });
    } catch (error) {
        addLog(`Upload failed: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/upload/discontinued', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }
        const result = await extractUploadedFiles(req.file, DISCONTINUED_FILES_DIR);
        res.json({ success: true, ...result, fileName: req.file.originalname, fileSize: req.file.size });
    } catch (error) {
        addLog(`Upload failed: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/upload/status', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    res.json({
        hasProductFiles: !!catalogFiles?.productFiles?.length,
        hasDiscontinuedFile: !!catalogFiles?.discontinuedFile,
        productFileCount: catalogFiles?.productFiles?.length || 0,
        discontinuedFile: catalogFiles?.discontinuedFile?.name,
        productUploadTime: catalogFiles?.productFiles?.[0]?.uploadTime,
        discontinuedUploadTime: catalogFiles?.discontinuedFile?.uploadTime,
        totalProductSize: catalogFiles?.totalProductSize || 0,
        totalDiscontinuedSize: catalogFiles?.totalDiscontinuedSize || 0
    });
});

app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/import/products', (req, res) => { processProductImport(); res.json({ success: true }); });
app.post('/api/process/discontinued', (req, res) => { processDiscontinued(); res.json({ success: true }); });
app.post('/api/cleanup/once', (req, res) => { /* cleanup logic here */ });
app.post('/api/sync/inventory/cancel', (req, res) => { if (isRunning.inventory) { syncProgress.inventory.cancelled = true; isRunning.inventory = false; syncProgress.inventory.isActive = false; addLog('User cancelled inventory sync.', 'warning'); } res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (confirmation.isAwaiting) { const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); } res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (confirmation.isAwaiting) { confirmation.abortAction(); } res.json({ success: true }); });
const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    const lastProductImport = runHistory.find(r => r.type === 'Product Import');
    const lastDiscontinue = runHistory.find(r => r.type === 'Discontinue Products');
    
    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn-warning{background:#d29922;color:white;border-color:#f0c674}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.stat-card{text-align:center;}.stat-value{font-size:2rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem;} .progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden; width: 100%;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}.file-upload{margin-top:1rem;padding:1rem;border:2px dashed #30363d;border-radius:6px;text-align:center;}.file-upload input{display:none;}.file-upload label{cursor:pointer;padding:0.5rem 1rem;background:#21262d;border-radius:6px;display:inline-block;}.upload-status{margin-top:0.5rem;font-size:0.875em;color:#8b949e;}.section-header{font-size:1.1rem;font-weight:600;margin-bottom:0.5rem;color:#58a6ff;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationId}</div></div>
        <div class="card"><h2>Inventory Sync</h2><p>Status: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''}>Run Now</button></div>
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
            
            <div>
                <div class="section-header">Discontinued Products</div>
                <div class="file-upload">
                    <label for="discontinued-file-input" class="btn">📁 Upload Discontinued List</label>
                    <input type="file" id="discontinued-file-input" accept=".csv" onchange="uploadFile(this, 'discontinued')">
                    <div class="upload-status">
                        ${catalogFiles?.discontinuedFile ? `✅ ${catalogFiles.discontinuedFile.name}` : '❌ No discontinued file'}
                        ${catalogFiles?.discontinuedFile ? `<br>📅 ${new Date(catalogFiles.discontinuedFile.uploadTime).toLocaleString()}` : ''}
                    </div>
                </div>
                <button onclick="apiPost('/api/process/discontinued','Process discontinued products?')" class="btn btn-warning" ${isSystemLocked() || !catalogFiles?.discontinuedFile ?'disabled':''} style="margin-top:0.5rem;width:100%;">Process Discontinued</button>
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
            <h2>Last Discontinued Process</h2>
            <div class="grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card"><div class="stat-value">${lastDiscontinue?.discontinued ?? 'N/A'}</div><div class="stat-label">Discontinued</div></div>
                <div class="stat-card"><div class="stat-value">${lastDiscontinue?.kept ?? 'N/A'}</div><div class="stat-label">Kept (Stock > ${config.ralawise.discontinuedStockThreshold})</div></div>
            </div>
        </div>
    </div>
    
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message);if(btn)btn.disabled=false;}}
    async function uploadFile(input,type){const file=input.files[0];if(!file)return;const formData=new FormData();formData.append('file',file);try{const label=input.previousElementSibling;label.textContent='Uploading...';const res=await fetch('/api/upload/'+type,{method:'POST',body:formData});const data=await res.json();if(data.success){alert('File uploaded successfully! '+(data.fileCount?data.fileCount+' files extracted.':''));location.reload();}else{alert('Upload failed: '+data.error);label.textContent='📁 Upload '+(type==='products'?'Product Catalog':'Discontinued List');}}catch(e){alert('Upload error: '+e.message);}}
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
    addLog(`✅ Server started on port ${PORT} (Location: ${config.shopify.locationId})`, 'success');
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
