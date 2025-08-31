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

let lastRun = { inventory: {}, fullImport: {} };
let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };

function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; addLog('System is PAUSED (found lock file on startup).', 'warning'); } }

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs = logs.slice(0, 500); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }, { timeout: 10000 }); } catch (error) { addLog(`Telegram notification failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString(), details: {} }; const msg = `🚨 HARD FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); }
function requestConfirmation(jobKey, message, details, proceedAction) { confirmation = { isAwaiting: true, message, details, proceedAction, abortAction: () => { addLog(`User aborted '${message}'. System is now paused for safety.`, 'warning'); isSystemPaused = true; fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); notifyTelegram(`🙅‍♂️ User ABORTED the operation for: ${message}.\n\nSystem has been automatically paused.`); }, jobKey }; const alertMsg = `🤔 CONFIRMATION REQUIRED 🤔\n\n<b>Action Paused:</b> ${message}`; addLog(alertMsg, 'warning'); let debugMsg = ''; if (details.inventoryChange) { debugMsg = `\n\n<b>Details:</b>\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; } notifyTelegram(alertMsg + debugMsg); }

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: 30000 });
async function fetchInventoryFromFTP() { const client = new ftp.Client(); client.ftp.verbose = false; try { addLog('Connecting to FTP...', 'info'); await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); const buffer = Buffer.concat(chunks); addLog(`FTP download successful, ${buffer.length} bytes`, 'success'); return Readable.from(buffer); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU && row.SKU !== 'SKU') inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
async function getAllShopifyProducts() { let all = []; let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`; while (url) { const res = await shopifyClient.get(url); all.push(...res.data.products); const link = res.headers.link; url = link && link.includes('rel="next"') ? link.match(/<([^>]+)>/)[1].replace(config.shopify.baseUrl, '') : null; await delay(250); } addLog(`Fetched ${all.length} products from Shopify`, 'success'); return all; }
async function updateInventoryBySKU(inventoryMap) { if (isRunning.inventory) { addLog('Inventory update already running.', 'warning'); return; } isRunning.inventory = true; try { addLog('=== STARTING INVENTORY ANALYSIS ===', 'info'); const shopifyProducts = await getAllShopifyProducts(); const skuToProduct = new Map(); shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); })); const updatesToPerform = []; inventoryMap.forEach((newQty, sku) => { const match = skuToProduct.get(sku.toUpperCase()); if (match && (match.variant.inventory_quantity || 0) !== newQty) updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match }); }); const totalProducts = skuToProduct.size; const updatesNeeded = updatesToPerform.length; const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0; addLog(`Change analysis: ${updatesNeeded} updates for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info'); const executeUpdates = async () => { let updated = 0, errors = 0, tagged = 0; addLog(`Executing updates for ${updatesNeeded} products...`, 'info'); for (const u of updatesToPerform) { try { if (!u.match.product.tags?.includes('Supplier:Ralawise')) { await shopifyClient.put(`/products/${u.match.product.id}.json`, { product: { id: u.match.product.id, tags: `${u.match.product.tags || ''},Supplier:Ralawise`.replace(/^,/, '') } }); tagged++; await delay(200); } if (!u.match.variant.inventory_management) { await shopifyClient.put(`/variants/${u.match.variant.id}.json`, { variant: { id: u.match.variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } }); await delay(200); } await shopifyClient.post('/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id }).catch(() => {}); await shopifyClient.post('/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id, available: u.newQty }); updated++; addLog(`Updated ${u.match.product.title} (${u.sku}): ${u.oldQty} → ${u.newQty}`, 'success'); await delay(250); } catch (e) { errors++; addLog(`Failed to update ${u.sku}: ${e.response ? JSON.stringify(e.response.data) : e.message}`, 'error'); if (e.response?.status === 429) await delay(5000); } } const notFound = inventoryMap.size - updatesToPerform.length; lastRun.inventory = { updated, errors, tagged, timestamp: new Date().toISOString() }; notifyTelegram(`Inventory update complete:\n✅ ${updated} updated, 🏷️ ${tagged} tagged, ❌ ${errors} errors, ❓ ${notFound} not found`); }; if (changePercentage > config.failsafe.inventoryChangePercentage) { requestConfirmation('inventory', `High inventory change detected`, { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded, totalProducts, sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) }}, async () => { try { await executeUpdates(); } finally { isRunning.inventory = false; } }); return; } else { await executeUpdates(); } } catch (error) { triggerFailsafe(`Inventory update failed critically: ${error.message}`); } finally { if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') isRunning.inventory = false; } }
async function downloadAndExtractZip() { const url = `${config.ralawise.zipUrl}?t=${Date.now()}`; addLog(`Downloading zip: ${url}`, 'info'); const res = await axios.get(url, { responseType: 'arraybuffer', timeout: 120000 }); const tempDir = path.join(__dirname, 'temp', `ralawise_${Date.now()}`); fs.mkdirSync(tempDir, { recursive: true }); const zipPath = path.join(tempDir, 'data.zip'); fs.writeFileSync(zipPath, res.data); const zip = new AdmZip(zipPath); zip.extractAllTo(tempDir, true); fs.unlinkSync(zipPath); return { tempDir, csvFiles: fs.readdirSync(tempDir).filter(f => f.endsWith('.csv')).map(f => path.join(tempDir, f)) }; }
async function parseShopifyCSV(filePath) { return new Promise((resolve, reject) => { const products = []; fs.createReadStream(filePath).pipe(csv()).on('data', row => { products.push({ ...row, price: applyRalawisePricing(parseFloat(row['Variant Price']) || 0), original_price: parseFloat(row['Variant Price']) || 0 }); }).on('end', () => resolve(products)).on('error', reject); }); }
async function processFullImport(csvFiles) { if (isRunning.fullImport) return; isRunning.fullImport = true; let created = 0, discontinued = 0, errors = 0; try { addLog('=== STARTING FULL IMPORT ===', 'info'); const allRows = (await Promise.all(csvFiles.map(parseShopifyCSV))).flat(); const productsByHandle = new Map(); for (const row of allRows) { if (!row.Handle) continue; if (!productsByHandle.has(row.Handle)) productsByHandle.set(row.Handle, { ...row, tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''), images: [], variants: [], options: [] }); const p = productsByHandle.get(row.Handle); if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) p.images.push({ src: row['Image Src'], position: parseInt(row['Image Position']), alt: row['Image Alt Text'] || row.Title }); if (row['Variant SKU']) { const v = { sku: row['Variant SKU'], price: row.price, option1: row['Option1 Value'], option2: row['Option2 Value'], option3: row['Option3 Value'], inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory) }; p.variants.push(v); } } const shopifyProducts = await getAllShopifyProducts(); const existingHandles = new Set(shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise')).map(p => p.handle)); const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle)); addLog(`Found ${toCreate.length} new products to create.`, 'info'); for (const p of toCreate.slice(0, 30)) { try { const res = await shopifyClient.post('/products.json', { product: { title: p.Title, handle: p.Handle, body_html: p['Body (HTML)'], vendor: p.Vendor, product_type: p.Type, tags: p.tags, images: p.images, variants: p.variants.map(v => ({...v, inventory_management: 'shopify', inventory_policy: 'deny'})) } }); for (const v of res.data.product.variants) { const origV = p.variants.find(ov => ov.sku === v.sku); if (origV && v.inventory_item_id && origV.inventory_quantity > 0) { await shopifyClient.post('/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id }).catch(()=>{}); await shopifyClient.post('/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: origV.inventory_quantity }); } } created++; addLog(`✅ Created: ${p.Title}`, 'success'); await delay(1000); } catch(e) { errors++; addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error'); } } const newHandles = new Set(Array.from(productsByHandle.keys())); const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle)); addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info'); for (const p of toDiscontinue.slice(0, 50)) { try { await shopifyClient.put(`/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } }); for (const v of p.variants) { if (v.inventory_item_id) await shopifyClient.post('/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: v.inventory_item_id, available: 0 }).catch(()=>{}); } discontinued++; addLog(`⏸️ Discontinued: ${p.title}`, 'info'); await delay(500); } catch(e) { errors++; addLog(`Failed to discontinue ${p.title}: ${e.message}`, 'error'); } } lastRun.fullImport = { created, discontinued, errors, timestamp: new Date().toISOString() }; notifyTelegram(`Full import complete:\n✅ ${created} created\n⏸️ ${discontinued} discontinued\n❌ ${errors} errors`); } catch(e) { triggerFailsafe(`Full import failed: ${e.message}`); } finally { isRunning.fullImport = false; } }

// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } try { addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); const stream = await fetchInventoryFromFTP(); await updateInventoryBySKU(await parseInventoryCSV(stream)); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); lastRun.inventory.errors++; } }
async function syncFullCatalog() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } let tempDir; try { addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); lastRun.fullImport.errors++; } finally { if (tempDir) { try { fs.rmSync(tempDir, { recursive: true, force: true }); addLog('Cleaned up temp files', 'info'); } catch (cleanupError) { addLog(`Cleanup error: ${cleanupError.message}`, 'warning'); } } } }

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    const isSystemLocked = isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;
    const confirmationDetailsHTML = confirmation.details.inventoryChange ? `<div class="stats"><div class="stat"><h3>Threshold</h3><p>${confirmation.details.inventoryChange.threshold}%</p></div><div class="stat"><h3>Detected Change</h3><p>${confirmation.details.inventoryChange.actualPercentage.toFixed(2)}%</p></div><div class="stat"><h3>Updates Pending</h3><p>${confirmation.details.inventoryChange.updatesNeeded}</p></div><div class="stat full-width"><h3>Sample Changes (SKU: Old -> New)</h3><pre>${confirmation.details.inventoryChange.sample.map(item => `${item.sku}: ${item.oldQty} -> ${item.newQty}`).join('\n')}</pre></div></div>` : '';
    const html = `<!DOCTYPE html>
    <html>
    <head>
        <title>Ralawise Sync Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
            :root { --bg-color: #121212; --card-bg: rgba(38, 38, 38, 0.7); --text-color: #EAEAEA; --text-muted: #A0A0A0; --border-color: rgba(255, 255, 255, 0.1); --primary-gradient: linear-gradient(90deg, #8E2DE2 0%, #4A00E0 100%); --green-gradient: linear-gradient(90deg, #11998e 0%, #38ef7d 100%); --amber-gradient: linear-gradient(90deg, #FFC107 0%, #FF8F00 100%); --red-gradient: linear-gradient(90deg, #cb2d3e 0%, #ef473a 100%); }
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Inter', sans-serif; background-color: var(--bg-color); color: var(--text-color); min-height: 100vh; padding: 2rem; background-image: radial-gradient(circle at top right, rgba(74, 0, 224, 0.2) 0%, transparent 40%), radial-gradient(circle at bottom left, rgba(142, 45, 226, 0.2) 0%, transparent 50%); }
            .container { max-width: 1200px; margin: 0 auto; display: grid; gap: 2rem; }
            .header { text-align: center; margin-bottom: 1rem; } .header h1 { font-size: 2.5rem; font-weight: 700; letter-spacing: -1px; } .header p { color: var(--text-muted); font-size: 1.1rem; }
            .card { background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 16px; padding: 2rem; backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); box-shadow: 0 8px 32px rgba(0,0,0,0.2); }
            .card h2 { margin-bottom: 1.5rem; border-bottom: 1px solid var(--border-color); padding-bottom: 1rem; font-weight: 600; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; }
            .stat { background: rgba(0,0,0,0.2); border-radius: 12px; padding: 1.5rem; text-align: center; }
            .stat h3 { font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; } .stat p { font-size: 2.5rem; font-weight: 700; line-height: 1; } .stat small { display: block; margin-top: 0.75rem; color: var(--text-muted); font-size: 0.8rem; }
            .stat.full-width { grid-column: 1 / -1; } .stat pre { text-align: left; background: var(--bg-color); padding: 1rem; border-radius: 8px; color: var(--text-muted); max-height: 150px; overflow-y: auto; font-family: 'SF Mono', monospace; font-size: 0.8rem; }
            button { color: #fff; border: none; padding: 0.8rem 1.5rem; border-radius: 8px; cursor: pointer; font-size: 1rem; font-weight: 600; transition: all 0.2s ease; background-size: 200% auto; }
            button:hover { transform: translateY(-2px); box-shadow: 0 4px 15px rgba(0,0,0,0.2); } button:disabled { background-image: none !important; background-color: #444 !important; color: #888; cursor: not-allowed; transform: none; box-shadow: none; }
            .btn-group { display: flex; flex-wrap: wrap; gap: 1rem; } .btn-action { background-image: var(--primary-gradient); } .btn-pause { background-image: var(--amber-gradient); } .btn-resume { background-image: var(--green-gradient); } .btn-proceed { background-image: var(--green-gradient); } .btn-abort { background-image: var(--red-gradient); }
            .logs { background: rgba(0,0,0,0.3); padding: 1.5rem; border-radius: 12px; max-height: 400px; overflow-y: auto; font-family: 'SF Mono', monospace; font-size: 0.85rem; line-height: 1.6; }
            .log-entry { margin-bottom: 0.5rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border-color); } .log-info { color: #58a6ff; } .log-success { color: #56d364; } .log-warning { color: #f0ad4e; } .log-error { color: #f85149; }
            .status-badge { display: inline-block; padding: 0.5rem 1rem; border-radius: 20px; font-size: 0.9rem; font-weight: 600; text-transform: uppercase; }
            .status-idle { background-color: rgba(86, 211, 100, 0.2); color: #56d364; } .status-running { background-color: rgba(240, 173, 78, 0.2); color: #f0ad4e; animation: pulse 2s infinite; } @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.7; } 100% { opacity: 1; } }
            .banner { padding: 2rem; border-radius: 16px; margin-bottom: 2rem; border: 1px solid; } .banner h2 { border-bottom: 1px solid; padding-bottom: 1rem; margin-bottom: 1rem; }
            .failsafe-banner { background: rgba(231, 76, 60, 0.1); border-color: #e74c3c; } .confirmation-banner { background: rgba(52, 152, 219, 0.1); border-color: #3498db; }
        </style>
    </head>
    <body>
        <div class="container">
            <header class="header"><h1>Ralawise Sync Dashboard</h1><p>Automated Inventory & Product Management</p></header>
            
            ${failsafe.isTriggered ? `<div class="banner failsafe-banner"><h2>🚨 Failsafe Active</h2><p style="margin-bottom:1.5rem"><strong>Reason:</strong> ${failsafe.reason}</p><div class="btn-group"><button onclick="clearFailsafe()" class="btn-resume">✅ Clear Failsafe</button></div></div>` : ''}
            
            ${confirmation.isAwaiting ? `<div class="banner confirmation-banner"><h2>🤔 Confirmation Required</h2><p style="margin-bottom:1.5rem"><strong>Action Paused:</strong> ${confirmation.message}</p>${confirmationDetailsHTML}<div class="btn-group" style="margin-top:1.5rem"><button onclick="proceed()" class="btn-proceed">👍 Proceed Anyway</button><button onclick="abort()" class="btn-abort">🚫 Abort & Pause System</button></div></div>` : ''}
            
            <div class="card">
              <h2>System Status</h2>
              <div class="stats">
                <div class="stat"><h3>Overall Status</h3><p class="status-badge" style="background:${isSystemPaused ? 'rgba(230, 126, 34, 0.2)' : 'rgba(46, 204, 113, 0.2)'};color:${isSystemPaused ? '#e67e22' : '#2ecc71'}">${isSystemPaused ? 'Paused' : 'Active'}</p></div>
                <div class="stat"><h3>Inventory Sync</h3><p class="status-badge ${isRunning.inventory ? 'status-running' : 'status-idle'}">${isRunning.inventory ? 'Running' : 'Idle'}</p></div>
                <div class="stat"><h3>Full Import</h3><p class="status-badge ${isRunning.fullImport ? 'status-running' : 'status-idle'}">${isRunning.fullImport ? 'Running' : 'Idle'}</p></div>
              </div>
            </div>

            <div class="card">
              <h2>Manual Controls</h2>
              <div class="btn-group">
                <button onclick="togglePause()" class="${isSystemPaused ? 'btn-resume' : 'btn-pause'}" ${isSystemLocked ? 'disabled' : ''}>${isSystemPaused ? '▶️ Resume System' : '⏸️ Pause System'}</button>
                <button onclick="runInventorySync()" class="btn-action" ${isSystemLocked ? 'disabled' : ''}>🔄 Run Inventory Sync</button>
                <button onclick="runFullImport()" class="btn-action" ${isSystemLocked ? 'disabled' : ''}>📦 Run Full Import</button>
                <button onclick="clearLogs()" class="btn-action" ${isSystemLocked ? 'disabled' : ''}>🗑️ Clear Logs</button>
              </div>
            </div>

            <div class="card">
              <h2>Last Run Statistics</h2>
              <div class="stats">
                <div class="stat"><h3>Inventory Updated</h3><p>${lastRun.inventory.updated || 0}</p><small>${lastRun.inventory.timestamp ? new Date(lastRun.inventory.timestamp).toLocaleString() : 'Never'}</small></div>
                <div class="stat"><h3>Products Tagged</h3><p>${lastRun.inventory.tagged || 0}</p><small>With Supplier:Ralawise</small></div>
                <div class="stat"><h3>Products Created</h3><p>${lastRun.fullImport.created || 0}</p><small>${lastRun.fullImport.timestamp ? new Date(lastRun.fullImport.timestamp).toLocaleString() : 'Never'}</small></div>
                <div class="stat"><h3>Discontinued</h3><p>${lastRun.fullImport.discontinued || 0}</p><small>Marked as draft</small></div>
              </div>
            </div>

            <div class="card">
                <h2>Activity Log</h2>
                <div class="logs" id="logs">${logs.map(log => `<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div>
            </div>
        </div>
        <script>
            async function apiPost(endpoint, confirmMsg) { if (confirmMsg && !confirm(confirmMsg)) return; const btn = event.target; btn.disabled = true; try { await fetch(endpoint, { method: 'POST' }); window.location.reload(); } catch (e) { alert('Failed: ' + e.message); btn.disabled = false; } }
            function runInventorySync() { apiPost('/api/sync/inventory', 'Run inventory sync now?'); }
            function runFullImport() { apiPost('/api/sync/full', 'Create new products and mark discontinued ones?'); }
            function togglePause() { apiPost('/api/pause/toggle'); }
            function clearFailsafe() { apiPost('/api/failsafe/clear', 'Clear failsafe and resume operations?'); }
            function clearLogs() { apiPost('/api/logs/clear', 'Clear all logs?'); }
            function proceed() { apiPost('/api/confirmation/proceed', 'Proceed with the pending action?'); }
            function abort() { apiPost('/api/confirmation/abort', 'Abort the action and pause the system?'); }
            setTimeout(() => window.location.reload(), 30000);
        </script>
    </body>
    </html>`;
    res.send(html);
});

// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', async (req, res) => { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) return res.status(423).json({ error: `Syncs are paused.` }); if (isRunning.inventory) return res.status(409).json({ error: 'Inventory sync already running' }); syncInventory(); res.json({ success: true, message: 'Inventory sync started' }); });
app.post('/api/sync/full', async (req, res) => { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) return res.status(423).json({ error: `Syncs are paused.` }); if (isRunning.fullImport) return res.status(409).json({ error: 'Full import already running' }); syncFullCatalog(); res.json({ success: true, message: 'Full import started' }); });
app.post('/api/logs/clear', (req, res) => { logs = []; addLog('Logs cleared manually', 'info'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { addLog('Failsafe manually cleared.', 'warning'); failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} }; notifyTelegram('✅ Failsafe has been manually cleared. Operations are resuming.'); res.json({ success: true }); });
app.post('/api/pause/toggle', (req, res) => { isSystemPaused = !isSystemPaused; if (isSystemPaused) { fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); addLog('System has been MANUALLY PAUSED.', 'warning'); notifyTelegram('⏸️ System has been manually PAUSED.'); } else { try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {} addLog('System has been RESUMED.', 'info'); notifyTelegram('▶️ System has been manually RESUMED.'); } res.json({ success: true, isPaused: isSystemPaused }); });
app.post('/api/confirmation/proceed', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json({ error: 'No confirmation pending.' }); addLog(`User confirmed to PROCEED with: ${confirmation.message}`, 'info'); notifyTelegram(`👍 User confirmed to PROCEED with: ${confirmation.message}`); if (confirmation.proceedAction) setTimeout(confirmation.proceedAction, 0); confirmation = { isAwaiting: false }; res.json({ success: true, message: 'Action proceeding.' }); });
app.post('/api/confirmation/abort', (req, res) => { if (!confirmation.isAwaiting) return res.status(400).json({ error: 'No confirmation pending.' }); if (confirmation.abortAction) confirmation.abortAction(); const jobKey = confirmation.jobKey; if (jobKey) isRunning[jobKey] = false; confirmation = { isAwaiting: false }; res.json({ success: true, message: 'Action aborted and system paused.' }); });

// ============================================
// SCHEDULED TASKS
// ============================================

cron.schedule('0 * * * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.inventory) { addLog('⏰ Starting scheduled inventory sync...', 'info'); syncInventory(); } else { addLog('⏰ Skipped scheduled inventory sync: System is busy or paused.', 'warning'); } });
cron.schedule('0 13 */2 * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.fullImport) { addLog('⏰ Starting scheduled full catalog import...', 'info'); syncFullCatalog(); } else { addLog('⏰ Skipped scheduled full import: System is busy or paused.', 'warning'); } }, { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { checkPauseStateOnStartup(); addLog(`✅ Ralawise Sync Server started on port ${PORT}`, 'success'); addLog(`🛡️ Confirmation Threshold: >${config.failsafe.inventoryChangePercentage}% inventory change`, 'info'); setTimeout(() => { if (!isRunning.inventory) { addLog('🚀 Running initial inventory sync...', 'info'); syncInventory(); } }, 10000); });
function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (error) => { console.error('Uncaught Exception:', error); addLog(`FATAL Uncaught Exception: ${error.message}`, 'error'); });
process.on('unhandledRejection', (reason, promise) => { console.error('Unhandled Rejection at:', promise, 'reason:', reason); addLog(`FATAL Unhandled Rejection: ${reason}`, 'error'); });
