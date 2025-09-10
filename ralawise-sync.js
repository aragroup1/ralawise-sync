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
        baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
    },
    ftp: { 
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        maxInventory: parseInt(process.env.MAX_INVENTORY || '20'),
        preservedSuppliers: ['Supplier:Ralawise', 'Supplier:Apify'], // Suppliers to preserve
        discontinuedStockThreshold: 0, // Set to 0 for discontinued items
        baseSKULength: 0 // CHANGED: 0 means use full SKU from parentheses
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
        burstSize: 40,
        variantCreationDelay: 2000, // 2 second delay between creating new products to avoid variant limit
        dailyVariantLimit: 1000 // Approximate daily limit
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
const PENDING_PRODUCTS_FILE = path.join(__dirname, '_pending_products.json');
const VARIANT_COUNT_FILE = path.join(__dirname, '_variant_count.json');

let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];
let syncProgress = { 
    inventory: { isActive: false, current: 0, total: 0, startTime: null, estimatedCompletion: null, cancelled: false, discontinued: 0 }, 
    fullImport: { isActive: false, created: 0, discontinued: 0, updated: 0, variantsCreated: 0, hitLimit: false },
    cleanup: { isActive: false, current: 0, total: 0 },
    discontinue: { isActive: false, current: 0, total: 0 }
};
let inventoryChangeLog = []; // Track actual changes made
let weeklyReport = { inventoryUpdates: 0, productsCreated: 0, productsDiscontinued: 0, productsUpdated: 0, errors: [] };

// Create directories if they don't exist
[UPLOADED_FILES_DIR, path.join(__dirname, 'uploads')].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// Track daily variant creation count
function getVariantCount() {
    try {
        if (fs.existsSync(VARIANT_COUNT_FILE)) {
            const data = JSON.parse(fs.readFileSync(VARIANT_COUNT_FILE, 'utf8'));
            // Reset if it's a new day (UTC)
            const today = new Date().toISOString().split('T')[0];
            if (data.date !== today) {
                return { date: today, count: 0 };
            }
            return data;
        }
    } catch (e) {}
    return { date: new Date().toISOString().split('T')[0], count: 0 };
}

function saveVariantCount(count) {
    const data = { date: new Date().toISOString().split('T')[0], count };
    fs.writeFileSync(VARIANT_COUNT_FILE, JSON.stringify(data, null, 2));
}

function incrementVariantCount(amount) {
    const current = getVariantCount();
    current.count += amount;
    saveVariantCount(current.count);
    return current.count;
}

// Load and save pending products
function loadPendingProducts() {
    try {
        if (fs.existsSync(PENDING_PRODUCTS_FILE)) {
            return JSON.parse(fs.readFileSync(PENDING_PRODUCTS_FILE, 'utf8'));
        }
    } catch (e) {}
    return [];
}

function savePendingProducts(products) {
    fs.writeFileSync(PENDING_PRODUCTS_FILE, JSON.stringify(products, null, 2));
}

function clearPendingProducts() {
    try {
        fs.unlinkSync(PENDING_PRODUCTS_FILE);
    } catch (e) {}
}

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

// Extract base SKU from title or variant SKU
function getBaseSKU(title, variantSKU) {
    // First try to extract SKU from parentheses in the title (e.g., "Product Name (B383R)")
    const match = title?.match(/KATEX_INLINE_OPEN([A-Z0-9]+)KATEX_INLINE_CLOSE$/);
    if (match) {
        return match[1].toUpperCase();
    }
    
    // If no parentheses in title, try to extract from variant SKU
    // For variant SKUs like "B383RONES" where "B383R" is the base
    if (variantSKU) {
        // Look for pattern where numbers are followed by letters (common pattern)
        const variantMatch = variantSKU.match(/^([A-Z]+\d+[A-Z]*)/);
        if (variantMatch) {
            return variantMatch[1].toUpperCase();
        }
        
        // Fallback to first 5-6 characters if pattern doesn't match
        return variantSKU.substring(0, 5).toUpperCase();
    }
    
    return '';
}

// Clean product title - remove color prefixes and dashes
function cleanProductTitle(title, option1Value) {
    if (!title) return '';
    
    // Remove SKU in parentheses from the end if present
    title = title.replace(/\s*KATEX_INLINE_OPEN[A-Z0-9]+KATEX_INLINE_CLOSE$/, '');
    
    // Remove the option1 value (usually color) from the beginning of the title
    if (option1Value) {
        // Escape special regex characters in option1Value
        const escapedOption1 = option1Value.replace(/[.*+?^${}()|[```\```/g, '\\$&');
        const prefixPattern = new RegExp(`^${escapedOption1}\\s*[-–—]\\s*`, 'i');
        title = title.replace(prefixPattern, '');
    }
    
    // Remove any remaining leading dashes or special characters
    title = title.replace(/^[\s\-–—]+/, '');
    
    // Replace multiple spaces with single space
    title = title.replace(/\s+/g, ' ');
    
    // Remove dashes (as requested by user)
    title = title.replace(/[-–—]/g, ' ');
    
    // Clean up multiple spaces again
    title = title.replace(/\s+/g, ' ').trim();
    
    return title;
}

// Generate handle from base SKU and title
function generateHandle(baseSKU, title) {
    const cleanTitle = title ? title.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-') : '';
    return `${baseSKU.toLowerCase()}-${cleanTitle}`.substring(0, 100).replace(/-+$/, '');
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

async function fetchInventoryFromFTP() { 
    const client = new ftp.Client(); 
    try { 
        await client.access(config.ftp); 
        const chunks = []; 
        await client.downloadTo(new Writable({ 
            write(c, e, cb) { chunks.push(c); cb(); } 
        }), '/Stock/Stock_Update.csv'); 
        return Readable.from(Buffer.concat(chunks)); 
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
                if (row.SKU) inventory.set(row.SKU.trim().toUpperCase(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); 
            })
            .on('end', () => resolve(inventory))
            .on('error', reject); 
    }); 
}

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
            url = nextLinkMatch ? nextLinkMatch[1].replace(config.shopify.baseUrl, '') : null; 
        } catch (error) { 
            addLog(`Error fetching products: ${error.message}`, 'error'); 
            triggerFailsafe(`Failed to fetch products from Shopify`); 
            return []; 
        } 
    } 
    addLog(`Fetched ${allProducts.length} products.`, 'success'); 
    return allProducts; 
}

// Main inventory sync function with discontinued detection
async function syncInventory() {
    if (isRunning.inventory) return;
    isRunning.inventory = true;
    
    let runResult = { 
        type: 'Inventory Sync', 
        status: 'failed', 
        updated: 0, 
        discontinued: 0, 
        errors: 0, 
        discontinuedProducts: [] 
    };
    
    const finalizeRun = (status) => {
        runResult.status = status;
        runResult.endTime = new Date().toISOString();
        const discontinuedMsg = runResult.discontinuedProducts.length > 0 ? 
            `\n\n<b>Discontinued Products:</b>\n${runResult.discontinuedProducts.slice(0,10).map(p => `- ${p}`).join('\n')}` : '';
        notifyTelegram(`Inventory sync ${runResult.status}:\n📦 ${runResult.updated} updated\n🚫 ${runResult.discontinued} discontinued${discontinuedMsg}`);
        isRunning.inventory = false;
        syncProgress.inventory.isActive = false;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== STARTING INVENTORY SYNC ===', 'info');
        syncProgress.inventory = { isActive: true, current: 0, total: 0, startTime: Date.now(), discontinued: 0 };
        
        // Fetch inventory from FTP
        addLog('Fetching inventory from FTP...', 'info');
        const stream = await fetchInventoryFromFTP();
        const ftpInventory = await parseInventoryCSV(stream);
        addLog(`Loaded ${ftpInventory.size} SKUs from FTP`, 'success');
        
        // Get all Shopify products
        const shopifyProducts = await getAllShopifyProducts();
        
        // Separate Ralawise products
        const ralawiseProducts = shopifyProducts.filter(p => 
            p.tags && p.tags.includes('Supplier:Ralawise')
        );
        
        addLog(`Found ${ralawiseProducts.length} Ralawise products in Shopify`, 'info');
        
        // Build set of all SKUs in FTP inventory
        const ftpSKUs = new Set(Array.from(ftpInventory.keys()));
        
        // Track products to discontinue
        const toDiscontinue = [];
        
        // Process each Ralawise product
        let processedCount = 0;
        syncProgress.inventory.total = ralawiseProducts.length;
        
        for (const product of ralawiseProducts) {
            try {
                let hasAnyVariantInFTP = false;
                let needsUpdate = false;
                const updates = [];
                
                // Check each variant
                for (const variant of product.variants || []) {
                    if (variant.sku) {
                        const upperSKU = variant.sku.toUpperCase();
                        
                        if (ftpSKUs.has(upperSKU)) {
                            hasAnyVariantInFTP = true;
                            const ftpQty = ftpInventory.get(upperSKU);
                            
                            if (ftpQty !== undefined && variant.inventory_quantity !== ftpQty) {
                                updates.push({ variant, newQty: ftpQty });
                                needsUpdate = true;
                            }
                        }
                    }
                }
                
                // If product is active and no variants found in FTP = discontinued
                if (!hasAnyVariantInFTP && product.status === 'active') {
                    const totalStock = product.variants.reduce((sum, v) => sum + (v.inventory_quantity || 0), 0);
                    
                    addLog(`🚫 Product not in FTP: ${product.title} (Status: ${product.status}, Stock: ${totalStock})`, 'warning');
                    toDiscontinue.push(product);
                    
                } else if (needsUpdate) {
                    // Update inventory for products still in FTP
                    for (const { variant, newQty } of updates) {
                        try {
                            // Get inventory item details first
                            const inventoryRes = await shopifyRequestWithRetry('get', `/inventory_items/${variant.inventory_item_id}.json`);
                            const inventoryItem = inventoryRes.data.inventory_item;
                            
                            if (inventoryItem.tracked) {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                    location_id: config.shopify.locationId,
                                    inventory_item_id: variant.inventory_item_id,
                                    available: newQty
                                });
                                
                                addLog(`Updated ${variant.sku}: ${variant.inventory_quantity} → ${newQty}`, 'success');
                            }
                        } catch (e) {
                            addLog(`Failed to update ${variant.sku}: ${e.message}`, 'error');
                            runResult.errors++;
                        }
                    }
                    
                    runResult.updated++;
                }
                
                processedCount++;
                updateProgress('inventory', processedCount, ralawiseProducts.length);
                
                if (processedCount % 50 === 0 || processedCount === ralawiseProducts.length) {
                    addLog(`Progress: ${processedCount}/${ralawiseProducts.length} products checked...`, 'info');
                }
                
            } catch (e) {
                addLog(`Error processing ${product.title}: ${e.message}`, 'error');
                runResult.errors++;
            }
        }
        
        // Process discontinued products
        addLog(`Found ${toDiscontinue.length} products to discontinue`, 'info');
        
        for (const product of toDiscontinue) {
            try {
                addLog(`Discontinuing: ${product.title}`, 'warning');
                
                // First, set all variant inventory to 0
                for (const variant of product.variants || []) {
                    if (variant.inventory_quantity > 0) {
                        try {
                            // Get inventory item details
                            const inventoryRes = await shopifyRequestWithRetry('get', `/inventory_items/${variant.inventory_item_id}.json`);
                            const inventoryItem = inventoryRes.data.inventory_item;
                            
                            if (inventoryItem.tracked) {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', {
                                    location_id: config.shopify.locationId,
                                    inventory_item_id: variant.inventory_item_id,
                                    available: 0
                                });
                                addLog(`Set ${variant.sku} inventory to 0`, 'success');
                            }
                        } catch (e) {
                            addLog(`Failed to zero inventory for ${variant.sku}: ${e.message}`, 'error');
                        }
                    }
                }
                
                // Then set product to draft status
                try {
                    await shopifyRequestWithRetry('put', `/products/${product.id}.json`, {
                        product: { 
                            id: product.id, 
                            status: 'draft' 
                        }
                    });
                    
                    runResult.discontinued++;
                    runResult.discontinuedProducts.push(product.title);
                    syncProgress.inventory.discontinued++;
                    
                    addLog(`✅ Discontinued: ${product.title} (set to draft)`, 'success');
                } catch (e) {
                    addLog(`Failed to set ${product.title} to draft: ${e.message}`, 'error');
                    runResult.errors++;
                }
                
            } catch (e) {
                addLog(`Failed to discontinue ${product.title}: ${e.message}`, 'error');
                runResult.errors++;
            }
        }
        
        addLog(`=== INVENTORY SYNC COMPLETED ===`, 'success');
        addLog(`Updated: ${runResult.updated} | Discontinued: ${runResult.discontinued} | Errors: ${runResult.errors}`, 'info');
        
        finalizeRun('completed');
    } catch (e) {
        addLog(`Inventory sync failed: ${e.message}`, 'error');
        triggerFailsafe(`Inventory sync failed: ${e.message}`);
        finalizeRun('failed');
    }
}

// Extract and process uploaded ZIP/CSV files
async function extractUploadedFiles(uploadedFile, targetDir) {
    try {
        const filePath = uploadedFile.path;
        const originalName = uploadedFile.originalname.toLowerCase();
        
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
        
        if (originalName.endsWith('.zip')) {
            addLog(`Extracting ZIP file: ${originalName}`, 'info');
            const zip = new AdmZip(filePath);
            const tempExtractDir = path.join(targetDir, 'extracted');
            zip.extractAllTo(tempExtractDir, true);
            
            const findCSVFiles = (dir) => {
                let csvFiles = [];
                fs.readdirSync(dir).forEach(file => {
                    const fullPath = path.join(dir, file);
                    if (fs.statSync(fullPath).isDirectory()) {
                        csvFiles = csvFiles.concat(findCSVFiles(fullPath));
                    } else if (file.toLowerCase().endsWith('.csv')) {
                        csvFiles.push(fullPath);
                    }
                });
                return csvFiles;
            };
            
            const csvFiles = findCSVFiles(tempExtractDir);
            addLog(`Found ${csvFiles.length} CSV files in ZIP`, 'info');
            
            csvFiles.forEach((csvPath, index) => {
                const fileName = path.basename(csvPath);
                const destName = csvFiles.length > 1 ? `file_${index + 1}_${fileName}` : fileName;
                const destPath = path.join(targetDir, destName);
                fs.copyFileSync(csvPath, destPath);
                addLog(`Extracted: ${fileName}`, 'success');
            });
            
            fs.rmSync(tempExtractDir, { recursive: true, force: true });
            fs.unlinkSync(filePath);
            return { success: true, fileCount: csvFiles.length };
        } else if (originalName.endsWith('.csv')) {
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

// Get latest uploaded catalog files
function getLatestCatalogFiles() {
    try {
        const productFiles = fs.existsSync(UPLOADED_FILES_DIR) ? 
            fs.readdirSync(UPLOADED_FILES_DIR).filter(f => f.endsWith('.csv')).map(f => ({
                name: f,
                path: path.join(UPLOADED_FILES_DIR, f),
                uploadTime: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).mtime,
                size: fs.statSync(path.join(UPLOADED_FILES_DIR, f)).size
            })).sort((a, b) => b.uploadTime - a.uploadTime) : [];
        
        return { 
            productFiles,
            totalProductSize: productFiles.reduce((sum, f) => sum + f.size, 0)
        };
    } catch (e) {
        addLog(`Error reading catalog files: ${e.message}`, 'error');
        return null;
    }
}

// SEPARATED: Process new products/updates ONLY
async function processProductImport() {
    if (isRunning.fullImport) return; 
    isRunning.fullImport = true;
    
    let runResult = { type: 'Product Import', status: 'failed', created: 0, updated: 0, errors: 0, createdProducts: [], updatedProducts: [], pendingSaved: 0 };
    let productCreationCount = 0;
    let hitVariantLimit = false;
    
    const finalizeRun = (status) => {
        runResult.status = status;
        let createdProductsMessage = runResult.createdProducts.length > 0 ? `\n\n<b>New Products:</b>\n${runResult.createdProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        let updatedProductsMessage = runResult.updatedProducts.length > 0 ? `\n\n<b>Updated Products:</b>\n${runResult.updatedProducts.slice(0,10).map(p => `- ${p.title}`).join('\n')}` : '';
        let pendingMessage = runResult.pendingSaved > 0 ? `\n\n⏳ <b>${runResult.pendingSaved} products saved for tomorrow</b> (hit daily variant limit)` : '';
        notifyTelegram(`Product import ${runResult.status}:\n✅ ${runResult.created} created\n🔄 ${runResult.updated} updated\n❌ ${runResult.errors} errors${pendingMessage}${createdProductsMessage}${updatedProductsMessage}`);
        isRunning.fullImport = false;
        syncProgress.fullImport.hitLimit = hitVariantLimit;
        addToHistory({...runResult, timestamp: new Date().toISOString() });
    };
    
    try {
        addLog('=== PRODUCT IMPORT: CREATING/UPDATING PRODUCTS ===', 'info');
        
        // Check current variant count
        const variantCount = getVariantCount();
        addLog(`Today's variant count: ${variantCount.count}/${config.rateLimit.dailyVariantLimit}`, 'info');
        
        // Check for pending products from previous run
        let pendingProducts = loadPendingProducts();
        let productsToProcess = [];
        
        if (pendingProducts.length > 0) {
            addLog(`Found ${pendingProducts.length} pending products from previous run`, 'info');
            productsToProcess = pendingProducts;
        } else {
            const catalogFiles = getLatestCatalogFiles();
            if (!catalogFiles?.productFiles || catalogFiles.productFiles.length === 0) {
                throw new Error('No product catalog files found. Please upload product CSV or ZIP files first.');
            }
            
            addLog(`Using ${catalogFiles.productFiles.length} product file(s)`, 'info');
            
            const allRows = [];
            let totalRowCount = 0;
            
            for (const file of catalogFiles.productFiles) {
                const rows = await new Promise((res, rej) => { 
                    const p = []; 
                    fs.createReadStream(file.path).pipe(csv()).on('data', r => {
                        totalRowCount++;
                        p.push(r);
                    }).on('end', () => res(p)).on('error', rej); 
                });
                allRows.push(...rows);
            }
            
            addLog(`Total rows loaded: ${totalRowCount}`, 'info');
            
            if (totalRowCount === 0) throw new Error('No data found in uploaded files.');
            
            const productsByBaseSKU = new Map();
            let skippedRows = 0;
            
            for (const row of allRows) {
                const variantSKU = row['Variant SKU'] || '';
                if (!variantSKU) {
                    skippedRows++;
                    continue;
                }
                
                const baseSKU = getBaseSKU(row.Title, variantSKU);
                if (!baseSKU) {
                    skippedRows++;
                    continue;
                }
                
                if (!productsByBaseSKU.has(baseSKU)) {
                    const cleanTitle = cleanProductTitle(row.Title, row['Option1 Value']);
                    const handle = generateHandle(baseSKU, cleanTitle);
                    
                    productsByBaseSKU.set(baseSKU, {
                        baseSKU,
                        title: cleanTitle, 
                        handle, 
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
                
                const variantExists = product.variants.some(v => v.sku === variantSKU);
                if (!variantExists) {
                    product.variants.push({
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
                    });
                }
                
                if (row['Option1 Name'] && !product.options.some(o => o.name === row['Option1 Name'])) 
                    product.options.push({ name: row['Option1 Name'], position: 1, values: [] });
                if (row['Option2 Name'] && !product.options.some(o => o.name === row['Option2 Name'])) 
                    product.options.push({ name: row['Option2 Name'], position: 2, values: [] });
                if (row['Option3 Name'] && !product.options.some(o => o.name === row['Option3 Name'])) 
                    product.options.push({ name: row['Option3 Name'], position: 3, values: [] });
            }
            
            productsByBaseSKU.forEach(product => {
                product.variants.forEach(variant => {
                    if (variant.option1 && product.options[0] && !product.options[0].values.includes(variant.option1)) 
                        product.options[0].values.push(variant.option1);
                    if (variant.option2 && product.options[1] && !product.options[1].values.includes(variant.option2)) 
                        product.options[1].values.push(variant.option2);
                    if (variant.option3 && product.options[2] && !product.options[2].values.includes(variant.option3)) 
                        product.options[2].values.push(variant.option3);
                });
            });
            
            productsToProcess = Array.from(productsByBaseSKU.values());
            addLog(`Grouped into ${productsToProcess.length} unique products (skipped ${skippedRows} rows without SKU)`, 'info');
        }
        
        if (productsToProcess.length === 0) throw new Error('No valid products found to process.');
        
        const shopifyProducts = await getAllShopifyProducts();
        
        const existingProductsByBaseSKU = new Map();
        const existingProductsByHandle = new Map();
        
        shopifyProducts.forEach(p => {
            const titleMatch = p.title?.match(/KATEX_INLINE_OPEN([A-Z0-9]+)KATEX_INLINE_CLOSE$/);
            if (titleMatch) {
                existingProductsByBaseSKU.set(titleMatch[1].toUpperCase(), p);
            }
            
            p.variants?.forEach(v => {
                if (v.sku) {
                    const baseSKU = getBaseSKU(p.title, v.sku);
                    if (baseSKU && !existingProductsByBaseSKU.has(baseSKU)) 
                        existingProductsByBaseSKU.set(baseSKU, p);
                }
            });
            
            if (p.handle) existingProductsByHandle.set(p.handle, p);
        });
        
        let processedCount = 0;
        const pendingForNextRun = [];
        
        for (const productData of productsToProcess) {
            try {
                const currentVariantCount = getVariantCount();
                
                // Check if we're approaching the limit
                if (currentVariantCount.count >= config.rateLimit.dailyVariantLimit - 10) {
                    hitVariantLimit = true;
                    pendingForNextRun.push(productData);
                    addLog(`⏳ Saving ${productData.title} for tomorrow (approaching daily limit)`, 'warning');
                    continue;
                }
                
                const baseSKU = productData.baseSKU;
                let existingProduct = existingProductsByBaseSKU.get(baseSKU) || existingProductsByHandle.get(productData.handle);
                
                if (existingProduct) {
                    addLog(`Updating existing product: ${existingProduct.title} (${baseSKU})`, 'info');
                    
                    const fullProductRes = await shopifyRequestWithRetry('get', `/products/${existingProduct.id}.json`);
                    const fullProduct = fullProductRes.data.product;
                    
                    const existingVariantsBySKU = new Map();
                    fullProduct.variants.forEach(v => {
                        if (v.sku) existingVariantsBySKU.set(v.sku.toUpperCase(), v);
                    });
                    
                    const variantsToAdd = productData.variants.filter(v => !existingVariantsBySKU.has(v.sku.toUpperCase()));
                    
                    if (variantsToAdd.length > 0) {
                        const allOptions = fullProduct.options || [];
                        const optionValues = {};
                        
                        fullProduct.variants.forEach(v => {
                            if (v.option1 && allOptions[0]) {
                                if (!optionValues[allOptions[0].name]) optionValues[allOptions[0].name] = new Set();
                                optionValues[allOptions[0].name].add(v.option1);
                            }
                            if (v.option2 && allOptions[1]) {
                                if (!optionValues[allOptions[1].name]) optionValues[allOptions[1].name] = new Set();
                                optionValues[allOptions[1].name].add(v.option2);
                            }
                            if (v.option3 && allOptions[2]) {
                                if (!optionValues[allOptions[2].name]) optionValues[allOptions[2].name] = new Set();
                                optionValues[allOptions[2].name].add(v.option3);
                            }
                        });
                        
                        variantsToAdd.forEach(v => {
                            if (v.option1 && allOptions[0]) {
                                if (!optionValues[allOptions[0].name]) optionValues[allOptions[0].name] = new Set();
                                optionValues[allOptions[0].name].add(v.option1);
                            }
                            if (v.option2 && allOptions[1]) {
                                if (!optionValues[allOptions[1].name]) optionValues[allOptions[1].name] = new Set();
                                optionValues[allOptions[1].name].add(v.option2);
                            }
                            if (v.option3 && allOptions[2]) {
                                if (!optionValues[allOptions[2].name]) optionValues[allOptions[2].name] = new Set();
                                optionValues[allOptions[2].name].add(v.option3);
                            }
                        });
                        
                        const updatedOptions = allOptions.map(opt => ({
                            ...opt,
                            values: optionValues[opt.name] ? Array.from(optionValues[opt.name]) : opt.values
                        }));
                        
                        await shopifyRequestWithRetry('put', `/products/${existingProduct.id}.json`, { 
                            product: { 
                                id: existingProduct.id, 
                                tags: productData.tags, 
                                options: updatedOptions.length > 0 ? updatedOptions : undefined,
                                variants: [...fullProduct.variants, ...variantsToAdd] 
                            } 
                        });
                        
                        incrementVariantCount(variantsToAdd.length);
                        syncProgress.fullImport.variantsCreated += variantsToAdd.length;
                        
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
                                } catch (e) { /* Ignore */ }
                            }
                        }
                        
                        runResult.updated++;
                        runResult.updatedProducts.push({ title: existingProduct.title, handle: existingProduct.handle });
                        addLog(`✅ Updated: ${existingProduct.title} (${variantsToAdd.length} new variants)`, 'success');
                    } else {
                        addLog(`ℹ️ No new variants for: ${existingProduct.title}`, 'info');
                    }
                } else {
                    if (productData.variants.length > 100) {
                        addLog(`⚠️ Skipping ${productData.title}: Too many variants (${productData.variants.length} > 100)`, 'warning');
                        runResult.errors++;
                        continue;
                    }
                    
                    addLog(`Creating new product: ${productData.title} (${baseSKU})`, 'info');
                    
                    if (productCreationCount > 0) {
                        await delay(config.rateLimit.variantCreationDelay);
                    }
                    
                    const createData = { 
                        product: { 
                            ...productData, 
                            baseSKU: undefined, // Remove our custom field
                            options: productData.options.length > 0 ? productData.options : undefined 
                        } 
                    };
                    
                    const res = await shopifyRequestWithRetry('post', '/products.json', createData);
                    const createdProduct = res.data.product;
                    productCreationCount++;
                    
                    incrementVariantCount(productData.variants.length);
                    syncProgress.fullImport.variantsCreated += productData.variants.length;
                    
                    for (const variant of createdProduct.variants) {
                        const originalVariant = productData.variants.find(v => v.sku === variant.sku);
                        if (originalVariant?.inventory_quantity > 0) {
                            try {
                                await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { 
                                    location_id: config.shopify.locationId, 
                                    inventory_item_id: variant.inventory_item_id, 
                                    available: originalVariant.inventory_quantity 
                                });
                            } catch (e) { /* Ignore */ }
                        }
                    }
                    
                    runResult.created++;
                    runResult.createdProducts.push({ title: productData.title, handle: productData.handle });
                    addLog(`✅ Created: ${productData.title} with ${productData.variants.length} variants`, 'success');
                }
                
                processedCount++;
                if (processedCount % 50 === 0 || processedCount === productsToProcess.length) {
                    const variantStatus = getVariantCount();
                    addLog(`Progress: ${processedCount}/${productsToProcess.length} | Variants today: ${variantStatus.count}/${config.rateLimit.dailyVariantLimit}`, 'info');
                }
            } catch (e) {
                let errorDetails = e.message;
                if (e.response?.data?.errors) errorDetails = JSON.stringify(e.response.data.errors);
                if (e.response?.data?.product) errorDetails = JSON.stringify(e.response.data.product);
                if (e.response?.data?.base) errorDetails = JSON.stringify(e.response.data.base);
                
                if (errorDetails.includes('Daily variant creation limit')) {
                    hitVariantLimit = true;
                    pendingForNextRun.push(productData);
                    addLog(`⏳ Hit daily limit, saving ${productData.title} for tomorrow`, 'warning');
                } else {
                    runResult.errors++;
                    addLog(`❌ Failed to process ${productData.title}: ${errorDetails}`, 'error');
                }
            }
        }
        
        // Save pending products for next run
        if (pendingForNextRun.length > 0) {
            savePendingProducts(pendingForNextRun);
            runResult.pendingSaved = pendingForNextRun.length;
            addLog(`💾 Saved ${pendingForNextRun.length} products to process tomorrow`, 'info');
            const hoursUntilReset = Math.ceil((24 - new Date().getUTCHours()) % 24);
            addLog(`⏰ Variant limit resets in ~${hoursUntilReset} hours (midnight UTC)`, 'info');
        } else {
            clearPendingProducts();
        }
        
        finalizeRun('completed');
    } catch (e) { 
        triggerFailsafe(`Product import failed: ${e.message}`); 
        runResult.errors++; 
        finalizeRun('failed'); 
    }
}

// Resume pending products (can be called manually or by schedule)
async function resumePendingProducts() {
    const pending = loadPendingProducts();
    if (pending.length > 0) {
        addLog(`Found ${pending.length} pending products to process`, 'info');
        await processProductImport();
    } else {
        addLog('No pending products to process', 'info');
    }
}

// ============================================
// SYNC WRAPPERS & API
// ============================================

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;

app.post('/api/upload/products', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const result = await extractUploadedFiles(req.file, UPLOADED_FILES_DIR);
        res.json({ success: true, ...result, fileName: req.file.originalname, fileSize: req.file.size });
    } catch (error) {
        addLog(`Upload failed: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/upload/status', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    const pendingProducts = loadPendingProducts();
    const variantCount = getVariantCount();
    res.json({
        hasProductFiles: !!catalogFiles?.productFiles?.length,
        productFileCount: catalogFiles?.productFiles?.length || 0,
        productUploadTime: catalogFiles?.productFiles?.[0]?.uploadTime,
        totalProductSize: catalogFiles?.totalProductSize || 0,
        pendingProductsCount: pendingProducts.length,
        variantCountToday: variantCount.count,
        variantLimitRemaining: config.rateLimit.dailyVariantLimit - variantCount.count
    });
});

app.post('/api/sync/inventory', (req, res) => { syncInventory(); res.json({ success: true }); });
app.post('/api/import/products', (req, res) => { processProductImport(); res.json({ success: true }); });
app.post('/api/resume/pending', (req, res) => { resumePendingProducts(); res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch(e){} addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafe = { isTriggered: false }; addLog('Failsafe cleared.', 'warning'); res.json({ success: true }); });
app.post('/api/confirmation/proceed', (req, res) => { if (confirmation.isAwaiting) { const action = confirmation.proceedAction; resetConfirmationState(); if (action) action(); } res.json({ success: true }); });
app.post('/api/confirmation/abort', (req, res) => { if (confirmation.isAwaiting) { confirmation.abortAction(); } res.json({ success: true }); });

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const catalogFiles = getLatestCatalogFiles();
    const lastProductImport = runHistory.find(r => r.type === 'Product Import');
    const lastInventorySync = runHistory.find(r => r.type === 'Inventory Sync');
    const pendingProducts = loadPendingProducts();
    const variantCount = getVariantCount();
    const hoursUntilReset = Math.ceil((24 - new Date().getUTCHours()) % 24);
    
    const html = `<!DOCTYPE html><html lang="en"><head><title>Ralawise Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}.container{max-width:1400px;margin:auto;padding:1rem;}.card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}.btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn-warning{background:#d29922;color:white;border-color:#f0c674}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}.alert{padding:1rem;border-radius:6px;margin-bottom:1rem;border:1px solid;}.alert-warning{background-color:rgba(210,149,34,0.1);border-color:#d29922;}.alert-info{background-color:rgba(31,111,235,0.1);border-color:#1f6feb;}.stat-card{text-align:center;}.stat-value{font-size:2rem;font-weight:600;}.stat-label{font-size:0.8rem;color:#8b949e;}.product-list{list-style:none;padding:0;font-size:0.9em;max-height:150px;overflow-y:auto;} .product-list a{color:#58a6ff;text-decoration:none;} .product-list a:hover{text-decoration:underline;} .progress-container{margin-top:0.5rem;} .progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden; width: 100%;} .progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);transition:width 0.5s;}.location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}.file-upload{margin-top:1rem;padding:1rem;border:2px dashed #30363d;border-radius:6px;text-align:center;}.file-upload input{display:none;}.file-upload label{cursor:pointer;padding:0.5rem 1rem;background:#21262d;border-radius:6px;display:inline-block;}.upload-status{margin-top:0.5rem;font-size:0.875em;color:#8b949e;}.section-header{font-size:1.1rem;font-weight:600;margin-bottom:0.5rem;color:#58a6ff;}.variant-limit-bar{margin-top:0.5rem;}.variant-limit-text{font-size:0.875em;color:#8b949e;margin-bottom:0.25rem;}.discontinued-note{font-size:0.8rem;color:#d29922;margin-top:0.5rem;padding:0.5rem;background:rgba(210,149,34,0.1);border-radius:4px;}</style></head><body><div class="container"><h1>Ralawise Sync</h1>
    ${confirmation.isAwaiting ? `<div class="alert alert-warning"><h3>🤔 Confirmation Required</h3><p>${confirmation.message}</p><div><button onclick="apiPost('/api/confirmation/proceed')" class="btn btn-primary">Proceed</button> <button onclick="apiPost('/api/confirmation/abort')" class="btn">Abort & Pause</button></div></div>` : ''}
    ${pendingProducts.length > 0 ? `<div class="alert alert-info"><h3>⏳ Pending Products</h3><p>${pendingProducts.length} products are waiting to be processed (hit daily variant limit).</p><button onclick="apiPost('/api/resume/pending','Resume pending products?')" class="btn btn-primary">Resume Now</button> <span style="margin-left:1rem;font-size:0.875em;">Limit resets in ~${hoursUntilReset} hours</span></div>` : ''}
    
    <div class="grid">
        <div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered||confirmation.isAwaiting?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}<div class="location-info">Location ID: ${config.shopify.locationId}</div></div>
        <div class="card"><h2>Daily Variant Limit</h2><div class="variant-limit-text">Today: ${variantCount.count} / ${config.rateLimit.dailyVariantLimit} variants created</div><div class="progress-bar"><div class="progress-fill" style="width:${(variantCount.count/config.rateLimit.dailyVariantLimit*100).toFixed(1)}%"></div></div><div class="variant-limit-text" style="margin-top:0.5rem;">Resets in ~${hoursUntilReset} hours (midnight UTC)</div></div>
    </div>
    
    <div class="card">
        <h2>📦 Product Management</h2>
        <div class="grid">
            <div>
                <div class="section-header">Inventory Sync</div>
                <p style="font-size:0.875em;color:#8b949e;margin:0.5rem 0;">Updates inventory and automatically detects discontinued products</p>
                <button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${isSystemLocked()?'disabled':''} style="width:100%;">Run Inventory Sync</button>
                <div class="discontinued-note">
                    📌 Products with "Supplier:Ralawise" tag that are no longer in FTP will be:
                    <br>• Set to 0 inventory
                    <br>• Changed to draft status
                </div>
            </div>
            
            <div>
                <div class="section-header">Product Import</div>
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
            ${lastProductImport?.pendingSaved ? `<div style="margin-top:1rem;text-align:center;color:#d29922;">⏳ ${lastProductImport.pendingSaved} products saved for later</div>` : ''}
        </div>
        
        <div class="card">
            <h2>Last Inventory Sync</h2>
            <div class="grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card"><div class="stat-value">${lastInventorySync?.updated ?? 'N/A'}</div><div class="stat-label">Updated</div></div>
                <div class="stat-card"><div class="stat-value">${lastInventorySync?.discontinued ?? 'N/A'}</div><div class="stat-label">Discontinued</div></div>
            </div>
        </div>
    </div>
    
    <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
    </div><script>
    async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{const btn=event.target;if(btn)btn.disabled=true;await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message);if(btn)btn.disabled=false;}}
    async function uploadFile(input,type){const file=input.files[0];if(!file)return;const formData=new FormData();formData.append('file',file);try{const label=input.previousElementSibling;label.textContent='Uploading...';const res=await fetch('/api/upload/'+type,{method:'POST',body:formData});const data=await res.json();if(data.success){alert('File uploaded successfully! '+(data.fileCount?data.fileCount+' files extracted.':''));location.reload();}else{alert('Upload failed: '+data.error);label.textContent='📁 Upload Product Catalog';}}catch(e){alert('Upload error: '+e.message);}}
    </script></body></html>`;
    res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());  // Daily at 2 AM for inventory sync
cron.schedule('5 0 * * *', () => resumePendingProducts()); // Daily at 12:05 AM UTC (after limit reset)

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT} (Location: ${config.shopify.locationId})`, 'success');
    
    // Check for pending products on startup
    const pending = loadPendingProducts();
    if (pending.length > 0) {
        addLog(`📋 ${pending.length} products pending from previous run`, 'warning');
    }
    
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
