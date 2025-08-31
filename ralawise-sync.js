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
  shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` },
  ftp: { host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false },
  ralawise: { zipUrl: process.env.RALAWISE_ZIP_URL, maxInventory: parseInt(process.env.MAX_INVENTORY || '20') },
  telegram: { botToken: process.env.TELEGRAM_BOT_TOKEN, chatId: process.env.TELEGRAM_CHAT_ID },
  failsafe: { inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10') }
};
const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error(`Missing required environment variables.`); process.exit(1); }

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

function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) { runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); addLog(`Loaded ${runHistory.length} historical run records.`, 'info'); } } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) { runHistory = runHistory.slice(0, 100); } fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); if(runHistory.length > 100) runHistory.pop(); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; addLog('System is PAUSED (found lock file on startup).', 'warning'); } loadHistory(); }

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs = logs.slice(0, 500); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }, { timeout: 10000 }); } catch (error) { addLog(`Telegram notification failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString(), details: {} }; const msg = `🚨 HARD FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); isRunning.inventory = false; isRunning.fullImport = false; }
function requestConfirmation(jobKey, message, details, proceedAction) { confirmation = { isAwaiting: true, message, details, proceedAction, abortAction: () => { addLog(`User aborted '${message}'. System is now paused for safety.`, 'warning'); isSystemPaused = true; fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); notifyTelegram(`🙅‍♂️ User ABORTED operation: ${message}.\n\nSystem has been automatically paused.`); }, jobKey }; const alertMsg = `🤔 CONFIRMATION REQUIRED 🤔\n\n<b>Action Paused:</b> ${message}`; addLog(alertMsg, 'warning'); let debugMsg = ''; if (details.inventoryChange) { debugMsg = `\n\n<b>Details:</b>\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; } notifyTelegram(alertMsg + debugMsg); }

const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: 60000 });
async function shopifyRequestWithRetry(method, url, data = null, retries = 3) { let attempt = 0; while (attempt < retries) { try { switch (method.toLowerCase()) { case 'get': return await shopifyClient.get(url); case 'post': return await shopifyClient.post(url, data); case 'put': return await shopifyClient.put(url, data); } } catch (error) { if (error.response && error.response.status === 429) { attempt++; const retryAfter = (error.response.headers['retry-after'] || Math.pow(2, attempt)) * 1000 + Math.random() * 1000; addLog(`Shopify rate limit hit. Retrying in ${(retryAfter / 1000).toFixed(1)}s... (Attempt ${attempt}/${retries})`, 'warning'); await delay(retryAfter); } else { throw error; } } } throw new Error(`Shopify API request for ${method.toUpperCase()} ${url} failed after ${retries} retries.`); }

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

async function fetchInventoryFromFTP() { const client = new ftp.Client(); client.ftp.verbose = false; try { addLog('Connecting to FTP...', 'info'); await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); const buffer = Buffer.concat(chunks); addLog(`FTP download successful, ${buffer.length} bytes`, 'success'); return Readable.from(buffer); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU && row.SKU !== 'SKU') inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }

// ====================================================================================
// FIX #1: Corrected the infinite loop pagination issue.
// ====================================================================================
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
            } else {
                url = null;
            }

            await delay(250);
        } catch (error) {
            addLog(`Error fetching page ${pageCount} from Shopify: ${error.message}`, 'error');
            triggerFailsafe(`Failed to fetch all products from Shopify during pagination.`);
            return [];
        }
    }

    addLog(`Completed fetching ${allProducts.length} products from Shopify.`, 'success');
    return allProducts;
}

async function updateInventoryBySKU(inventoryMap) { if (isRunning.inventory) { addLog('Inventory update already running.', 'warning'); return; } isRunning.inventory = true; let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0 }; try { addLog('=== STARTING INVENTORY ANALYSIS ===', 'info'); const shopifyProducts = await getAllShopifyProducts(); const skuToProduct = new Map(); shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); })); const updatesToPerform = []; inventoryMap.forEach((newQty, sku) => { const match = skuToProduct.get(sku.toUpperCase()); if (match && (match.variant.inventory_quantity || 0) !== newQty) updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match }); }); const totalProducts = skuToProduct.size; const updatesNeeded = updatesToPerform.length; const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0; addLog(`Change analysis: ${updatesNeeded} updates for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info'); const executeUpdates = async () => { addLog(`Executing updates for ${updatesNeeded} products...`, 'info'); for (const u of updatesToPerform) { try { if (!u.match.product.tags?.includes('Supplier:Ralawise')) { await shopifyRequestWithRetry('put', `/products/${u.match.product.id}.json`, { product: { id: u.match.product.id, tags: `${u.match.product.tags || ''},Supplier:Ralawise`.replace(/^,/, '') } }); runResult.tagged++; await delay(200); } if (!u.match.variant.inventory_management) { await shopifyRequestWithRetry('put', `/variants/${u.match.variant.id}.json`, { variant: { id: u.match.variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } }); await delay(200); } await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id }).catch(() => {}); await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id, available: u.newQty }); runResult.updated++; addLog(`Updated ${u.match.product.title} (${u.sku}): ${u.oldQty} → ${u.newQty}`, 'success'); await delay(250); } catch (e) { runResult.errors++; addLog(`Failed to update ${u.sku}: ${e.message}`, 'error'); } } const notFound = inventoryMap.size - updatesToPerform.length; runResult.status = 'completed'; notifyTelegram(`Inventory update complete:\n✅ ${runResult.updated} updated, 🏷️ ${runResult.tagged} tagged, ❌ ${runResult.errors} errors, ❓ ${notFound} not found`); }; if (changePercentage > config.failsafe.inventoryChangePercentage) { requestConfirmation('inventory', `High inventory change detected`, { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded, totalProducts, sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) }}, async () => { try { await executeUpdates(); } finally { isRunning.inventory = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); } }); return; } else { await executeUpdates(); } } catch (error) { addLog(`Inventory update failed critically: ${error.message}`, 'error'); runResult.errors++; triggerFailsafe(`Inventory update failed critically: ${error.message}`); } finally { if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') { isRunning.inventory = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); } } }
async function downloadAndExtractZip() { const url = `${config.ralawise.zipUrl}?t=${Date.now()}`; addLog(`Downloading zip: ${url}`, 'info'); const res = await axios.get(url, { responseType: 'arraybuffer', timeout: 120000 }); const tempDir = path.join(__dirname, 'temp', `ralawise_${Date.now()}`); fs.mkdirSync(tempDir, { recursive: true }); const zipPath = path.join(tempDir, 'data.zip'); fs.writeFileSync(zipPath, res.data); const zip = new AdmZip(zipPath); zip.extractAllTo(tempDir, true); fs.unlinkSync(zipPath); return { tempDir, csvFiles: fs.readdirSync(tempDir).filter(f => f.endsWith('.csv')).map(f => path.join(tempDir, f)) }; }
async function parseShopifyCSV(filePath) { return new Promise((resolve, reject) => { const products = []; fs.createReadStream(filePath).pipe(csv()).on('data', row => { products.push({ ...row, price: applyRalawisePricing(parseFloat(row['Variant Price']) || 0), original_price: parseFloat(row['Variant Price']) || 0 }); }).on('end', () => resolve(products)).on('error', reject); }); }
async function processFullImport(csvFiles) { if (isRunning.fullImport) return; isRunning.fullImport = true; let runResult = { type: 'Full Import', status: 'failed', created: 0, discontinued: 0, errors: 0 }; try { addLog('=== STARTING FULL IMPORT ===', 'info'); const allRows = (await Promise.all(csvFiles.map(parseShopifyCSV))).flat(); const productsByHandle = new Map(); for (const row of allRows) { if (!row.Handle) continue; if (!productsByHandle.has(row.Handle)) productsByHandle.set(row.Handle, { ...row, tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''), images: [], variants: [], options: [] }); const p = productsByHandle.get(row.Handle); if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) p.images.push({ src: row['Image Src'], position: parseInt(row['Image Position']), alt: row['Image Alt Text'] || row.Title }); if (row['Variant SKU']) { const v = { sku: row['Variant SKU'], price: row.price, option1: row['Option1 Value'], option2: row['Option2 Value'], option3: row['Option3 Value'], inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory) }; p.variants.push(v); } } const shopifyProducts = await getAllShopifyProducts(); const existingHandles = new Set(shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise')).map(p => p.handle)); const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle)); addLog(`Found ${toCreate.length} new products to create.`, 'info'); for (const p of toCreate.slice(0, 30)) { try { const res = await shopifyRequestWithRetry('post', '/products.json', { product: { title: p.Title, handle: p.Handle, body_html: p['Body (HTML)'], vendor: p.Vendor, product_type: p.Type, tags: p.tags, images: p.images, variants: p.variants.map(v => ({...v, inventory_management: 'shopify', inventory_policy: 'deny'})) } }); for (const v of res.data.product.variants) { const origV = p.variants.find(ov => ov.sku === v.sku); if (origV && v.inventory_item_id && origV.inventory_quantity > 0) { await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id }).catch(()=>{}); await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: origV.inventory_quantity }); } } runResult.created++; addLog(`✅ Created: ${p.Title}`, 'success'); await delay(1000); } catch(e) { runResult.errors++; addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error'); } } const newHandles = new Set(Array.from(productsByHandle.keys())); const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle)); addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info'); for (const p of toDiscontinue.slice(0, 50)) { try { await shopifyRequestWithRetry('put', `/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } }); for (const v of p.variants) { if (v.inventory_item_id) await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: 0 }).catch(()=>{}); } runResult.discontinued++; addLog(`⏸️ Discontinued: ${p.title}`, 'info'); await delay(500); } catch(e) { runResult.errors++; addLog(`Failed to discontinue ${p.title}: ${e.message}`, 'error'); } } runResult.status = 'completed'; notifyTelegram(`Full import complete:\n✅ ${runResult.created} created\n⏸️ ${runResult.discontinued} discontinued\n❌ ${runResult.errors} errors`); } catch(e) { triggerFailsafe(`Full import failed: ${e.message}`); runResult.errors++; } finally { isRunning.fullImport = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); } }

// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } try { addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); const stream = await fetchInventoryFromFTP(); await updateInventoryBySKU(await parseInventoryCSV(stream)); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); addToHistory({ type: 'Inventory', timestamp: new Date().toISOString(), status: 'failed', errors: 1, updated:0, tagged:0 }); } }
async function syncFullCatalog() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } let tempDir; try { addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); addToHistory({ type: 'Full Import', timestamp: new Date().toISOString(), status: 'failed', errors: 1, created:0, discontinued:0 }); } finally { if (tempDir) { try { fs.rmSync(tempDir, { recursive: true, force: true }); addLog('Cleaned up temp files', 'info'); } catch (cleanupError) { addLog(`Cleanup error: ${cleanupError.message}`, 'warning'); } } } }

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    // ====================================================================================
    // FIX #2: Corrected the pause lockout bug.
    // The "Pause/Resume" button should not be disabled just because the system is paused.
    // It should only be disabled in more critical states (failsafe/confirmation).
    // ====================================================================================
    const isSystemLockedForActions = isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;
    const canTogglePause = !failsafe.isTriggered && !confirmation.isAwaiting;

    const confirmationDetailsHTML = confirmation.details.inventoryChange ? `<div class="confirmation-details"><div class="detail-grid"><div class="detail-card"><span class="detail-label">Threshold</span><span class="detail-value">${confirmation.details.inventoryChange.threshold}%</span></div><div class="detail-card"><span class="detail-label">Detected Change</span><span class="detail-value danger">${confirmation.details.inventoryChange.actualPercentage.toFixed(2)}%</span></div><div class="detail-card"><span class="detail-label">Updates Pending</span><span class="detail-value">${confirmation.details.inventoryChange.updatesNeeded}</span></div></div><div class="sample-changes"><h4>Sample Changes</h4><div class="changes-list">${confirmation.details.inventoryChange.sample.map(item => `<div class="change-item"><span class="sku">${item.sku}</span><span class="arrow">→</span><span class="qty-change">${item.oldQty} → ${item.newQty}</span></div>`).join('')}</div></div></div>` : '';
    const lastInventoryRun = runHistory.find(r => r.type === 'Inventory') || {};
    const lastFullImportRun = runHistory.find(r => r.type === 'Full Import') || {};
    const last24h = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 24*60*60*1000));
    const last7d = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 7*24*60*60*1000));

    const html = `<!DOCTYPE html>
    <html lang="en">
    <head>
        <title>Ralawise Sync • Premium Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
        <style>
            :root { --bg-primary: #0a0a0a; --bg-secondary: #111111; --bg-card: rgba(18, 18, 18, 0.95); --bg-hover: rgba(255, 255, 255, 0.03); --text-primary: #ffffff; --text-secondary: #a1a1a1; --text-muted: #666666; --accent-primary: #6366f1; --accent-success: #10b981; --accent-warning: #f59e0b; --accent-danger: #ef4444; --accent-info: #3b82f6; --gradient-primary: linear-gradient(135deg, #667eea 0%, #764ba2 100%); --gradient-success: linear-gradient(135deg, #34d399 0%, #10b981 100%); --gradient-warning: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%); --gradient-danger: linear-gradient(135deg, #f87171 0%, #ef4444 100%); --gradient-dark: linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%); --shadow-lg: 0 8px 24px rgba(0,0,0,0.4); --shadow-xl: 0 12px 48px rgba(0,0,0,0.5); --radius-xl: 24px; }
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Inter', sans-serif; background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; line-height: 1.6; position: relative; overflow-x: hidden; }
            body::before { content: ''; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: radial-gradient(circle at 20% 50%, rgba(99, 102, 241, 0.1) 0%, transparent 50%), radial-gradient(circle at 80% 80%, rgba(168, 85, 247, 0.1) 0%, transparent 50%), radial-gradient(circle at 40% 20%, rgba(236, 72, 153, 0.1) 0%, transparent 50%); z-index: -1; animation: backgroundShift 30s ease infinite; }
            @keyframes backgroundShift { 0%, 100% { transform: scale(1) rotate(0deg); } 25% { transform: scale(1.1) rotate(1deg); } 50% { transform: scale(0.95) rotate(-1deg); } 75% { transform: scale(1.05) rotate(0.5deg); } }
            .container { max-width: 1400px; margin: 0 auto; padding: 2rem; animation: fadeIn 0.6s ease; }
            .header { text-align: center; margin-bottom: 3rem; position: relative; }
            .header h1 { font-size: clamp(2.5rem, 5vw, 3.5rem); font-weight: 800; background: var(--gradient-primary); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; letter-spacing: -1px; margin-bottom: 0.5rem; animation: slideDown 0.8s ease; }
            .header p { color: var(--text-secondary); font-size: 1.125rem; font-weight: 300; letter-spacing: 0.5px; animation: slideUp 0.8s ease; }
            .card { background: var(--bg-card); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: var(--radius-xl); padding: 2rem; backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); box-shadow: var(--shadow-lg); position: relative; overflow: hidden; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); animation: fadeInUp 0.6s ease backwards; }
            .card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px; background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.5), transparent); animation: shimmer 3s infinite; }
            @keyframes shimmer { 0% { transform: translateX(-100%); } 100% { transform: translateX(100%); } }
            .card:hover { transform: translateY(-2px); box-shadow: var(--shadow-xl); border-color: rgba(99, 102, 241, 0.2); }
            .card h2 { font-size: 1.25rem; font-weight: 600; margin-bottom: 1.5rem; display: flex; align-items: center; gap: 0.75rem; color: var(--text-primary); }
            .grid { display: grid; gap: 1.5rem; margin-bottom: 2rem; } .grid-3 { grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); } .grid-4 { grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); }
            .stat-card { background: var(--gradient-dark); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 1.5rem; position: relative; overflow: hidden; transition: all 0.3s ease; text-align: center; }
            .stat-card .label { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; color: var(--text-muted); font-weight: 600; margin-bottom: 0.5rem; }
            .stat-card .value { font-size: 2rem; font-weight: 700; color: var(--text-primary); margin-bottom: 0.25rem; }
            .stat-card .meta { font-size: 0.875rem; color: var(--text-secondary); }
            .status { display: inline-flex; align-items: center; gap: 0.5rem; padding: 0.5rem 1rem; border-radius: 9999px; font-size: 0.875rem; font-weight: 600; transition: all 0.3s ease; }
            .status.active { background: rgba(16, 185, 129, 0.1); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); } .status.paused { background: rgba(245, 158, 11, 0.1); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); } .status.running { background: rgba(59, 130, 246, 0.1); color: #3b82f6; border: 1px solid rgba(59, 130, 246, 0.3); animation: pulse 2s infinite; }
            @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
            .btn { padding: 0.75rem 1.5rem; border-radius: 12px; font-weight: 600; font-size: 0.875rem; border: none; cursor: pointer; transition: all 0.2s ease; text-transform: uppercase; letter-spacing: 0.5px; display: inline-flex; align-items: center; gap: 0.5rem; position: relative; overflow: hidden; }
            .btn:hover { transform: translateY(-2px); box-shadow: var(--shadow-lg); } .btn:active { transform: translateY(0); } .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none !important; }
            .btn-primary { background: var(--gradient-primary); color: white; } .btn-success { background: var(--gradient-success); color: white; } .btn-warning { background: var(--gradient-warning); color: white; } .btn-danger { background: var(--gradient-danger); color: white; } .btn-secondary { background: var(--gradient-dark); color: var(--text-primary); border: 1px solid rgba(255, 255, 255, 0.1); }
            .btn-group { display: flex; flex-wrap: wrap; gap: 1rem; }
            .alert { padding: 1.5rem; border-radius: 16px; margin-bottom: 2rem; border: 1px solid; backdrop-filter: blur(10px); animation: slideIn 0.5s ease; position: relative; overflow: hidden; }
            .alert::before { content: ''; position: absolute; top: 0; left: 0; bottom: 0; width: 4px; }
            .alert.failsafe { background: rgba(239, 68, 68, 0.1); border-color: rgba(239, 68, 68, 0.3); } .alert.failsafe::before { background: var(--gradient-danger); }
            .alert.confirmation { background: rgba(59, 130, 246, 0.1); border-color: rgba(59, 130, 246, 0.3); } .alert.confirmation::before { background: var(--gradient-primary); }
            .alert h3 { font-size: 1.25rem; font-weight: 600; margin-bottom: 0.75rem; display: flex; align-items: center; gap: 0.75rem; } .alert p { color: var(--text-secondary); margin-bottom: 1rem; }
            .confirmation-details { margin-top: 1.5rem; } .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }
            .detail-card { background: rgba(0, 0, 0, 0.3); padding: 1rem; border-radius: 12px; text-align: center; }
            .detail-label { display: block; font-size: 0.75rem; text-transform: uppercase; color: var(--text-muted); margin-bottom: 0.5rem; font-weight: 600; }
            .detail-value { display: block; font-size: 1.5rem; font-weight: 700; color: var(--text-primary); } .detail-value.danger { color: var(--accent-danger); }
            .sample-changes { background: rgba(0, 0, 0, 0.3); padding: 1rem; border-radius: 12px; } .sample-changes h4 { font-size: 0.875rem; font-weight: 600; margin-bottom: 1rem; color: var(--text-secondary); }
            .changes-list { display: grid; gap: 0.5rem; } .change-item { display: flex; align-items: center; gap: 1rem; padding: 0.5rem; background: rgba(255, 255, 255, 0.02); border-radius: 8px; font-family: monospace; font-size: 0.875rem; }
            .change-item .sku { color: var(--accent-primary); font-weight: 600; min-width: 100px; } .change-item .arrow { color: var(--text-muted); } .change-item .qty-change { color: var(--text-primary); }
            .history-table { width: 100%; border-collapse: collapse; } .history-table th, .history-table td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid rgba(255, 255, 255, 0.05); }
            .history-table thead { background: rgba(255, 255, 255, 0.02); } .history-table th { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; color: var(--text-muted); font-weight: 600; }
            .history-table tr:hover { background: var(--bg-hover); }
            .logs { background: rgba(0, 0, 0, 0.5); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 1rem; max-height: 400px; overflow-y: auto; font-family: 'SF Mono', 'Monaco', monospace; font-size: 0.8125rem; line-height: 1.6; }
            .log-entry { padding: 0.5rem; margin-bottom: 0.25rem; border-left: 2px solid transparent; transition: all 0.2s ease; } .log-entry:hover { background: rgba(255, 255, 255, 0.02); padding-left: 1rem; }
            .log-info { border-left-color: var(--accent-info); color: #93c5fd; } .log-success { border-left-color: var(--accent-success); color: #86efac; } .log-warning { border-left-color: var(--accent-warning); color: #fde047; } .log-error { border-left-color: var(--accent-danger); color: #fca5a5; }
            @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } } @keyframes fadeInUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } } @keyframes slideDown { from { opacity: 0; transform: translateY(-20px); } to { opacity: 1; transform: translateY(0); } } @keyframes slideUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } } @keyframes slideIn { from { opacity: 0; transform: translateX(-100px); } to { opacity: 1; transform: translateX(0); } }
            @media (max-width: 768px) { .container { padding: 1rem; } .header h1 { font-size: 2rem; } .grid-3, .grid-4 { grid-template-columns: 1fr; } .btn-group { flex-direction: column; } .btn { width: 100%; justify-content: center; } }
        </style>
    </head>
    <body>
        <div class="container">
            <header class="header"><h1>Ralawise Sync</h1><p>Premium Inventory & Product Management System</p></header>
            
            ${failsafe.isTriggered ? `<div class="alert failsafe"><h3>🚨 Failsafe Activated</h3><p><strong>Reason:</strong> ${failsafe.reason}</p><div class="btn-group"><button onclick="clearFailsafe()" class="btn btn-success">✅ Clear Failsafe & Resume</button></div></div>` : ''}
            
            ${confirmation.isAwaiting ? `<div class="alert confirmation"><h3>🤔 Confirmation Required</h3><p><strong>Action:</strong> ${confirmation.message}</p>${confirmationDetailsHTML}<div class="btn-group"><button onclick="proceed()" class="btn btn-success">✅ Proceed Anyway</button><button onclick="abort()" class="btn btn-danger">❌ Abort & Pause</button></div></div>` : ''}
            
            <div class="grid grid-3">
                <div class="card"><h2>System Status</h2><div class="stat-card ${isSystemPaused ? 'warning' : 'success'}"><div class="label">Overall Status</div><div class="value"><span class="status ${isSystemPaused ? 'paused' : 'active'}">${isSystemPaused ? '⏸️ Paused' : '✅ Active'}</span></div></div><div style="margin-top: 1rem;"><button onclick="togglePause()" class="btn ${isSystemPaused ? 'btn-success' : 'btn-warning'}" ${!canTogglePause ? 'disabled' : ''}>${isSystemPaused ? '▶️ Resume System' : '⏸️ Pause System'}</button></div></div>
                <div class="card"><h2>Job Status</h2><div style="display: grid; gap: 1rem;"><div class="stat-card ${isRunning.inventory ? 'info' : ''}"><div class="label">Inventory Sync</div><div class="value"><span class="status ${isRunning.inventory ? 'running' : 'active'}">${isRunning.inventory ? '🔄 Running' : '✅ Ready'}</span></div></div><div class="stat-card ${isRunning.fullImport ? 'info' : ''}"><div class="label">Full Import</div><div class="value"><span class="status ${isRunning.fullImport ? 'running' : 'active'}">${isRunning.fullImport ? '🔄 Running' : '✅ Ready'}</span></div></div></div></div>
                <div class="card"><h2>Quick Actions</h2><div class="btn-group" style="flex-direction: column;"><button onclick="runInventorySync()" class="btn btn-primary" ${isSystemLockedForActions ? 'disabled' : ''}>🔄 Run Inventory Sync</button><button onclick="runFullImport()" class="btn btn-primary" ${isSystemLockedForActions ? 'disabled' : ''}>📦 Run Full Import</button><button onclick="clearLogs()" class="btn btn-secondary" ${isSystemLockedForActions ? 'disabled' : ''}>🗑️ Clear Logs</button></div></div>
            </div>
            
             <div class="card" style="animation-delay: 0.2s;">
                <h2>Historical Data</h2>
                <div class="grid grid-3" style="margin-bottom: 1.5rem;">
                    <div class="stat-card"><div class="label">Last 24 Hours</div><div class="value">${last24h.length}</div><div class="meta">Total operations</div></div>
                    <div class="stat-card"><div class="label">Last 7 Days</div><div class="value">${last7d.length}</div><div class="meta">Total operations</div></div>
                    <div class="stat-card"><div class="label">All Time</div><div class="value">${runHistory.length}</div><div class="meta">Total operations</div></div>
                </div>
                ${runHistory.length > 0 ? `<div style="overflow-x: auto;"><table class="history-table"><thead><tr><th>Time</th><th>Type</th><th>Updated</th><th>Created</th><th>Tagged</th><th>Discontinued</th><th>Errors</th></tr></thead><tbody>${runHistory.slice(0, 10).map(run => `<tr><td>${new Date(run.timestamp).toLocaleString()}</td><td><span class="status ${run.type.toLowerCase().includes('inventory') ? 'info' : 'active'}">${run.type}</span></td><td>${run.updated ?? '-'}</td><td>${run.created ?? '-'}</td><td>${run.tagged ?? '-'}</td><td>${run.discontinued ?? '-'}</td><td style="color:${(run.errors || 0) > 0 ? 'var(--accent-danger)' : 'inherit'}">${run.errors || 0}</td></tr>`).join('')}</tbody></table></div>` : '<p style="text-align: center; color: var(--text-muted);">No historical data yet</p>'}
            </div>
            
            <div class="card" style="animation-delay: 0.3s;">
                <h2>Activity Log</h2>
                <div class="logs" id="logs">${logs.length > 0 ? logs.map(log => `<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('') : '<div class="log-entry log-info">System initialized. Waiting for operations...</div>'}</div>
            </div>
        </div>
        <script>
            async function apiPost(endpoint, confirmMsg) { if (confirmMsg && !confirm(confirmMsg)) return; const btn = event.target; btn.disabled = true; try { const res = await fetch(endpoint, { method: 'POST' }); const data = await res.json(); if (!data.success && data.error) { alert('Error: ' + data.error); btn.disabled = false; return; } window.location.reload(); } catch (e) { alert('Failed: ' + e.message); btn.disabled = false; } }
            function runInventorySync() { apiPost('/api/sync/inventory', 'Run inventory sync now?'); }
            function runFullImport() { apiPost('/api/sync/full', 'Create new products and mark discontinued ones?'); }
            function togglePause() { apiPost('/api/pause/toggle'); }
            function clearFailsafe() { apiPost('/api/failsafe/clear', 'Clear failsafe and resume operations?'); }
            function clearLogs() { apiPost('/api/logs/clear', 'Clear all logs?'); }
            function proceed() { apiPost('/api/confirmation/proceed', 'Proceed with the pending action despite the warning?'); }
            function abort() { apiPost('/api/confirmation/abort', 'Abort the action and pause the system?'); }
            setTimeout(() => window.location.reload(), 60000);
        </script>
    </body>
    </html>`;
    res.send(html);
});

// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', async (req, res) => { if (isSystemPaused) return res.status(423).json({ success: false, error: 'System is paused.' }); if (failsafe.isTriggered) return res.status(423).json({ success: false, error: 'Failsafe is active.' }); if (confirmation.isAwaiting) return res.status(423).json({ success: false, error: 'Confirmation is pending.' }); if (isRunning.inventory) return res.status(409).json({ success: false, error: 'Inventory sync already running.' }); syncInventory(); res.json({ success: true, message: 'Inventory sync started' }); });
app.post('/api/sync/full', async (req, res) => { if (isSystemPaused) return res.status(423).json({ success: false, error: 'System is paused.' }); if (failsafe.isTriggered) return res.status(423).json({ success: false, error: 'Failsafe is active.' }); if (confirmation.isAwaiting) return res.status(423).json({ success: false, error: 'Confirmation is pending.' }); if (isRunning.fullImport) return res.status(409).json({ success: false, error: 'Full import already running.' }); syncFullCatalog(); res.json({ success: true, message: 'Full import started' }); });
app.post('/api/logs/clear', (req, res) => { logs = []; addLog('Logs cleared manually', 'info'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { addLog('Failsafe manually cleared.', 'warning'); failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} }; isRunning.inventory = false; isRunning.fullImport = false; notifyTelegram('✅ Failsafe has been manually cleared. Operations are resuming.'); res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) { fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); addLog('System has been MANUALLY PAUSED.', 'warning'); notifyTelegram('⏸️ System has been manually PAUSED.'); } else { try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {} addLog('System has been RESUMED.', 'info'); notifyTelegram('▶️ System has been manually RESUMED.'); } res.json({ success: true, isPaused: isSystemPaused }); });
app.post('/api/confirmation/proceed', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json({ success: false, error: 'No confirmation pending.' }); addLog(`User confirmed to PROCEED with: ${confirmation.message}`, 'info'); notifyTelegram(`👍 User confirmed to PROCEED with: ${confirmation.message}`); if (confirmation.proceedAction) setTimeout(confirmation.proceedAction, 0); confirmation = { isAwaiting: false }; res.json({ success: true, message: 'Action proceeding.' }); });
app.post('/api/confirmation/abort', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json({ success: false, error: 'No confirmation pending.' }); if (confirmation.abortAction) confirmation.abortAction(); const jobKey = confirmation.jobKey; if (jobKey) isRunning[jobKey] = false; confirmation = { isAwaiting: false }; res.json({ success: true, message: 'Action aborted and system paused.' }); });

// ============================================
// SCHEDULED TASKS
// ============================================

cron.schedule('0 * * * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.inventory) { addLog('⏰ Starting scheduled inventory sync...', 'info'); syncInventory(); } else { addLog('⏰ Skipped scheduled inventory sync: System is busy or paused.', 'warning'); } });
cron.schedule('0 13 */2 * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.fullImport) { addLog('⏰ Starting scheduled full catalog import...', 'info'); syncFullCatalog(); } else { addLog('⏰ Skipped scheduled full import: System is busy or paused.', 'warning'); } }, { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { checkPauseStateOnStartup(); addLog(`✅ Ralawise Sync Server started on port ${PORT}`, 'success'); addLog(`🛡️ Confirmation Threshold: >${config.failsafe.inventoryChangePercentage}% inventory change`, 'info'); setTimeout(() => { if (!isSystemPaused && !failsafe.isTriggered && !isRunning.inventory) { addLog('🚀 Running initial inventory sync...', 'info'); syncInventory(); } }, 10000); });
function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (error) => { console.error('Uncaught Exception:', error); addLog(`FATAL Uncaught Exception: ${error.message}`, 'error'); });
process.on('unhandledRejection', (reason, promise) => { console.error('Unhandled Rejection at:', promise, 'reason:', reason); addLog(`FATAL Unhandled Rejection: ${reason}`, 'error'); });
