const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const sanitizeHtml = require('sanitize-html'); // <-- NEW: Import the sanitizer library

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0 };
let lastRun = { inventory: {}, products: { createdItems: [] }, discontinued: { discontinuedItems: [] }, deduplicate: {}, mapSkus: {}, details: {} };
let logs = [];
let systemPaused = false;
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
let errorSummary = new Map();

const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false, deduplicate: false, 'failsafe-confirm': false, details: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const SUPPLIER_TAG = process.env.SUPPLIER_TAG || 'Supplier:Apify';
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions ---
// NEW: Enhanced error logging to show Shopify's detailed response
function getDetailedError(error) {
    if (error.response && error.response.data) {
        // Shopify often returns errors in { errors: { field: ["message"] } }
        const errorData = JSON.stringify(error.response.data.errors || error.response.data);
        return `${error.message} - Data: ${errorData}`;
    }
    return error.message;
}

function addLog(message, type = 'info', error = null) {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs.length = 200;
  
  const displayMessage = error ? `${message}: ${getDetailedError(error)}` : message;
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${displayMessage}`);

  if (type === 'error') {
    stats.errors++;
    let normalizedError = (error?.message || message).replace(/"[^"]+"/g, '"{VAR}"').replace(/\b\d{5,}\b/g, '{ID}');
    errorSummary.set(normalizedError, (errorSummary.get(normalizedError) || 0) + 1);
  }
}
// ... other helper functions like notifyTelegram, startBackgroundJob, triggerFailsafe, etc. remain the same ...
async function notifyTelegram(text) { if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed`, 'warning', e); } }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}`, 'error', e); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
async function triggerFailsafe(reason, continuationFn) { failsafeTriggered = true; failsafeReason = reason; pendingFailsafeAction = continuationFn; addLog(`FAILSAFE TRIGGERED: ${reason}. System paused pending user confirmation.`, 'error'); await notifyTelegram(`<b>FAILSAFE TRIGGERED</b>\n\n<b>Reason:</b> ${reason}\n\nPlease go to the dashboard to confirm or abort this action.`); return true; }

// --- Data Fetching & Processing ---
// ... getApifyProducts, getShopifyProducts, getShopifyInventoryLevels, etc. remain the same ...
async function getApifyProducts() { let allItems = []; let offset = 0; addLog('Starting Apify product fetch...', 'info'); try { while (true) { const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); allItems.push(...data); if (data.length < 1000) break; offset += 1000; } } catch (error) { addLog(`Apify fetch error`, 'error', error); throw error; } addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info'); return allItems; }
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at,body_html' } = {}) { let allProducts = []; addLog(`Starting Shopify fetch...`, 'info'); try { let url = `/products.json?limit=250&fields=${fields}`; while (url) { const response = await shopifyClient.get(url); allProducts.push(...response.data.products); const linkHeader = response.headers.link; url = null; if (linkHeader) { const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); if (nextLink) { const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; } } await new Promise(r => setTimeout(r, 500)); } const activeCount = allProducts.filter(p => p.status === 'active').length; const draftCount = allProducts.filter(p => p.status === 'draft').length; const archivedCount = allProducts.filter(p => p.status === 'archived').length; addLog(`Shopify fetch complete: ${allProducts.length} total products (${activeCount} active, ${draftCount} draft, ${archivedCount} archived).`, 'info'); } catch (error) { addLog(`Shopify fetch error`, 'error', error); throw error; } return allProducts; }
function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = (item.handle || item.title).toLowerCase().replace(/[^a-z0-9\s-]/g, '').replace(/\s+/g, '-'); let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20; if (String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out')) inventory = 0; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images }; }).filter(p => p && p.sku); }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---
// ... deduplicateProductsJob, improvedMapSkusJob, updateInventoryJob, handleDiscontinuedProductsJob remain the same logically ...

// UPDATED: createNewProductsJob now sanitizes HTML
async function createNewProductsJob(token) {
    if (failsafeTriggered) { addLog('New product sync skipped: Failsafe is active.', 'warning'); return; }
    addLog('Starting new product creation (SKU-based check)...', 'info');
    let created = 0, errors = 0;

    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;
        
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: true });
        const skuMap = new Map();
        for (const product of shopifyData) {
            const sku = product.variants?.[0]?.sku;
            if (sku) skuMap.set(sku.toLowerCase(), product);
        }

        const toCreate = apifyProcessed.filter(p => !matchShopifyProductBySku(p, skuMap).product);
        addLog(`Found ${toCreate.length} new products to create.`, 'info');

        const performCreation = async (jobToken) => {
            const createdItems = [];
            const maxToCreate = Math.min(toCreate.length, FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE);
            
            for (let i = 0; i < maxToCreate; i++) {
                if (shouldAbort(jobToken)) break;
                const apifyProd = toCreate[i];
                try {
                    // <-- UPDATED: Sanitize the HTML before sending
                    const cleanHtml = sanitizeHtml(apifyProd.body_html || '', {
                        allowedTags: sanitizeHtml.defaults.allowedTags.concat(['h1', 'h2', 'u', 'img']),
                        allowedAttributes: { ...sanitizeHtml.defaults.allowedAttributes, '*': ['style'] }
                    });

                    const newProduct = { product: { 
                        title: apifyProd.title, 
                        body_html: cleanHtml,
                        vendor: 'Imported', 
                        product_type: '', 
                        tags: SUPPLIER_TAG, 
                        status: apifyProd.inventory > 0 ? 'active' : 'draft', 
                        variants: [{ price: apifyProd.price, sku: apifyProd.sku, inventory_management: 'shopify' }], 
                        images: apifyProd.images 
                    }};

                    const { data } = await shopifyClient.post('/products.json', newProduct);
                    const inventoryItemId = data.product.variants[0].inventory_item_id;
                    await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: inventoryItemId, location_id: config.shopify.locationId, available: apifyProd.inventory });
                    
                    created++;
                    createdItems.push({ id: data.product.id, title: apifyProd.title, sku: apifyProd.sku });
                    addLog(`Created product: "${apifyProd.title}" with SKU: ${apifyProd.sku}`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error creating product "${apifyProd.title}"`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 1000));
            }
            stats.newProducts += created;
            lastRun.products = { at: new Date().toISOString(), created, errors, skipped: toCreate.length - created, createdItems };
            addLog(`Product creation complete: ${created} created, ${errors} errors.`, 'success');
        };

        if (toCreate.length > 0) {
            if (toCreate.length > FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE) {
                const reason = `Attempting to create ${toCreate.length} products, which exceeds failsafe limit of ${FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE}.`;
                if (await triggerFailsafe(reason, performCreation)) return;
            }
            await performCreation(token);
        } else {
             addLog('No new products to create.', 'success');
             lastRun.products = { at: new Date().toISOString(), created: 0, errors: 0, skipped: 0, createdItems: [] };
        }
    } catch (e) {
        addLog(`Critical error in product creation job`, 'error', e);
        errors++;
        lastRun.products = { at: new Date().toISOString(), created, errors, skipped: toCreate.length - created, createdItems: [] };
    }
}

// NEW JOB: To sync titles and descriptions for existing products
async function syncProductDetailsJob(token) {
    if (failsafeTriggered) { addLog('Product detail sync skipped: Failsafe is active.', 'warning'); return; }
    addLog('Starting product detail sync (Title, Description)...', 'info');
    let updated = 0, errors = 0, inSync = 0;

    try {
        const [apifyData, shopifyData] = await Promise.all([
            getApifyProducts(),
            getShopifyProducts({ fields: 'id,title,variants,tags,body_html' }) // Ensure body_html is fetched
        ]);

        if (shouldAbort(token)) return;

        const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        
        const shopifySkuMap = new Map();
        for (const product of supplierProducts) {
            const sku = product.variants?.[0]?.sku;
            if (sku) shopifySkuMap.set(sku.toLowerCase(), product);
        }

        const updatesToPerform = [];
        for (const apifyProd of apifyProcessed) {
            const shopifyProd = shopifySkuMap.get(apifyProd.sku.toLowerCase());
            if (shopifyProd) {
                const cleanHtml = sanitizeHtml(apifyProd.body_html || '', {
                    allowedTags: sanitizeHtml.defaults.allowedTags.concat(['h1', 'h2', 'u', 'img']),
                    allowedAttributes: { ...sanitizeHtml.defaults.allowedAttributes, '*': ['style'] }
                });

                const titleChanged = apifyProd.title.trim() !== shopifyProd.title.trim();
                const bodyChanged = cleanHtml.trim() !== (shopifyProd.body_html || '').trim();

                if (titleChanged || bodyChanged) {
                    updatesToPerform.push({
                        id: shopifyProd.id,
                        title: apifyProd.title,
                        body_html: cleanHtml,
                        reason: `${titleChanged ? 'Title ' : ''}${bodyChanged ? 'Description' : ''}`.trim()
                    });
                } else {
                    inSync++;
                }
            }
        }
        
        addLog(`Detail Sync Summary: ${updatesToPerform.length} updates needed, ${inSync} products already in sync.`, 'info');
        
        if (updatesToPerform.length > 0) {
            for (const update of updatesToPerform) {
                if (shouldAbort(token)) break;
                try {
                    await shopifyClient.put(`/products/${update.id}.json`, {
                        product: { id: update.id, title: update.title, body_html: update.body_html }
                    });
                    updated++;
                    addLog(`Updated details for "${update.title}" (Reason: ${update.reason})`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error updating details for "${update.title}"`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 600)); // Rate limit
            }
        }
        
    } catch (e) {
        addLog(`Critical error in product detail sync job`, 'error', e);
        errors++;
    }

    lastRun.details = { at: new Date().toISOString(), updated, errors, inSync };
    addLog(`Product detail sync complete: ${updated} updated, ${errors} errors.`, 'success');
}


// --- UI AND API ---
// ... UI remains largely the same, but we add a new button ...
app.get('/', (req, res) => {
  const status = systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING');
  res.send(`
    <!-- ... head, style, nav ... -->
    <body>
        <!-- ... navbar ... -->
        <div class="container">
            <!-- ... status card ... -->
            <div class="col-md-6">
                <div class="card">
                <div class="card-header"><h5 class="mb-0"><i class="fas fa-tools me-2"></i>Manual Actions</h5></div>
                <div class="card-body">
                    <div class="d-flex flex-wrap justify-content-center gap-2">
                        <button class="btn btn-primary" onclick="runSync('inventory')"><i class="fas fa-boxes"></i> Sync Inventory</button>
                        <button class="btn btn-primary" onclick="runSync('products')"><i class="fas fa-plus-circle"></i> Sync New Products</button>
                        <button class="btn btn-primary" onclick="runSync('details')"><i class="fas fa-pencil-alt"></i> Sync Product Details</button> <!-- NEW BUTTON -->
                        <button class="btn btn-primary" onclick="runSync('discontinued')"><i class="fas fa-archive"></i> Check Discontinued</button>
                        <button class="btn btn-warning" onclick="runSync('improved-map-skus')"><i class="fas fa-map-signs"></i> Map SKUs</button>
                        <button class="btn btn-secondary" onclick="runSync('deduplicate')"><i class="fas fa-clone"></i> Delete Duplicates</button>
                    </div>
                </div>
                </div>
            </div>
            <!-- ... logs card ... -->
        </div>
        <!-- ... script tag ... -->
    </body>
    </html>
  `);
});

// ... Other API endpoints like /api/status, /api/pause, /api/failsafe/* remain the same ...
app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason }));
app.post('/api/pause', (req, res) => { /* ... */ });
app.post('/api/failsafe/confirm', (req, res) => { /* ... */ });
app.post('/api/failsafe/abort', (req, res) => { /* ... */ });
app.post('/api/sync/deduplicate', (req, res) => { /* ... */ });
app.post('/api/sync/improved-map-skus', (req, res) => { /* ... */ });


// UPDATED: Added new 'details' job to the sync endpoint
app.post('/api/sync/:type', (req, res) => {
  if (failsafeTriggered) return res.status(409).json({s: 0, msg: 'System is in failsafe mode.'});
  const jobs = { 
      inventory: updateInventoryJob, 
      products: createNewProductsJob, 
      discontinued: handleDiscontinuedProductsJob, 
      'improved-map-skus': improvedMapSkusJob,
      details: syncProductDetailsJob // <-- NEW
  };
  const { type } = req.params;
  if (!jobs[type]) return res.status(400).json({s: 0, msg: 'Invalid job type'});
  
  const jobNameMap = {
      inventory: 'Manual inventory sync',
      products: 'Manual product creation',
      discontinued: 'Manual discontinued check',
      'improved-map-skus': 'Comprehensive SKU Mapping',
      details: 'Manual product detail sync'
  };

  return startBackgroundJob(type, jobNameMap[type] || `Manual ${type} sync`, t => jobs[type](t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0, msg: 'Job already running.'});
});

// Scheduled jobs & Server Start
// ... unchanged ...
cron.schedule('0 4 * * *', () => { // Example schedule for the new job, e.g., 4 AM daily
    if (!systemPaused && !failsafeTriggered) {
        startBackgroundJob('details', 'Scheduled Product Detail Sync', t => syncProductDetailsJob(t));
    }
});

const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  // ... startup checks ...
});
// ... process signal handlers ...
