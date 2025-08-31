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
  const pendingReviewsList = Array.from(pendingReviews.values())
    .filter(r => r.status === 'pending')
    .map(r => ({
      ...r,
      ageMinutes: Math.round((Date.now() - new Date(r.timestamp).getTime()) / 1000 / 60)
    }));
  
  const html = `
    <!DOCTYPE html>
    <html>
    <head>
      <title>Ralawise Sync | Luxury Dashboard</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
      <style>
        * { 
          margin: 0; 
          padding: 0; 
          box-sizing: border-box; 
        }
        
        :root {
          --primary-gold: linear-gradient(135deg, #FFD700, #FFA500);
          --primary-dark: #0a0a0a;
          --secondary-dark: #151515;
          --card-dark: rgba(30, 30, 30, 0.6);
          --text-primary: #ffffff;
          --text-secondary: rgba(255, 255, 255, 0.7);
          --text-muted: rgba(255, 255, 255, 0.5);
          --accent-gold: #FFD700;
          --accent-orange: #FFA500;
          --success: #00D4AA;
          --danger: #FF3366;
          --warning: #FFB800;
          --info: #00B4D8;
        }
        
        body { 
          font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
          background: #000000;
          min-height: 100vh;
          color: var(--text-primary);
          position: relative;
          overflow-x: hidden;
        }
        
        /* Animated gradient background */
        body::before {
          content: '';
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 25%, #16213e 50%, #0f3460 75%, #0a0a0a 100%);
          background-size: 400% 400%;
          animation: gradientShift 20s ease infinite;
          z-index: -2;
        }
        
        /* Floating orbs animation */
        body::after {
          content: '';
          position: fixed;
          width: 600px;
          height: 600px;
          background: radial-gradient(circle, rgba(255, 215, 0, 0.1) 0%, transparent 70%);
          border-radius: 50%;
          top: -300px;
          right: -300px;
          animation: float 20s infinite ease-in-out;
          z-index: -1;
        }
        
        @keyframes gradientShift {
          0%, 100% { background-position: 0% 50%; }
          50% { background-position: 100% 50%; }
        }
        
        @keyframes float {
          0%, 100% { transform: translate(0, 0) scale(1); }
          33% { transform: translate(-100px, 100px) scale(1.1); }
          66% { transform: translate(100px, -100px) scale(0.9); }
        }
        
        /* Container */
        .container { 
          max-width: 1400px; 
          margin: 0 auto; 
          padding: 2rem;
          position: relative;
        }
        
        /* Header */
        .header {
          text-align: center;
          margin-bottom: 3rem;
          position: relative;
          animation: fadeInDown 0.8s ease;
        }
        
        h1 {
          font-family: 'Playfair Display', serif;
          font-size: 4rem;
          font-weight: 700;
          background: linear-gradient(135deg, #FFD700, #FFA500, #FFD700);
          background-size: 200% 200%;
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          text-fill-color: transparent;
          animation: goldShimmer 3s ease infinite;
          margin-bottom: 0.5rem;
          letter-spacing: -1px;
        }
        
        .header-subtitle {
          font-size: 1.1rem;
          color: var(--text-secondary);
          font-weight: 300;
          letter-spacing: 2px;
          text-transform: uppercase;
        }
        
        @keyframes goldShimmer {
          0%, 100% { background-position: 0% 50%; }
          50% { background-position: 100% 50%; }
        }
        
        /* Luxury Cards */
        .card {
          background: linear-gradient(135deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.02));
          backdrop-filter: blur(20px);
          -webkit-backdrop-filter: blur(20px);
          border: 1px solid rgba(255, 215, 0, 0.2);
          border-radius: 24px;
          padding: 2rem;
          margin-bottom: 2rem;
          position: relative;
          overflow: hidden;
          box-shadow: 
            0 20px 40px rgba(0, 0, 0, 0.5),
            inset 0 1px 0 rgba(255, 255, 255, 0.1);
          transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
          animation: fadeIn 0.6s ease;
        }
        
        .card::before {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          height: 1px;
          background: linear-gradient(90deg, transparent, var(--accent-gold), transparent);
          animation: shimmer 3s infinite;
        }
        
        @keyframes shimmer {
          0% { transform: translateX(-100%); }
          100% { transform: translateX(100%); }
        }
        
        .card:hover {
          transform: translateY(-5px);
          border-color: rgba(255, 215, 0, 0.4);
          box-shadow: 
            0 30px 60px rgba(0, 0, 0, 0.6),
            inset 0 1px 0 rgba(255, 255, 255, 0.2),
            0 0 30px rgba(255, 215, 0, 0.1);
        }
        
        .card h2 {
          font-family: 'Playfair Display', serif;
          font-size: 1.8rem;
          font-weight: 700;
          margin-bottom: 1.5rem;
          color: var(--text-primary);
          display: flex;
          align-items: center;
          gap: 1rem;
        }
        
        .card h2::after {
          content: '';
          flex: 1;
          height: 1px;
          background: linear-gradient(90deg, rgba(255, 215, 0, 0.3), transparent);
        }
        
        /* Stats Grid */
        .stats {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
          gap: 1.5rem;
        }
        
        .stat {
          background: linear-gradient(135deg, rgba(255, 215, 0, 0.1), rgba(255, 165, 0, 0.05));
          backdrop-filter: blur(10px);
          border: 1px solid rgba(255, 215, 0, 0.2);
          border-radius: 16px;
          padding: 1.5rem;
          text-align: center;
          position: relative;
          overflow: hidden;
          transition: all 0.3s ease;
        }
        
        .stat::before {
          content: '';
          position: absolute;
          top: -50%;
          left: -50%;
          width: 200%;
          height: 200%;
          background: radial-gradient(circle, rgba(255, 215, 0, 0.1) 0%, transparent 70%);
          opacity: 0;
          transition: opacity 0.3s ease;
        }
        
        .stat:hover {
          transform: translateY(-5px) scale(1.02);
          border-color: var(--accent-gold);
          box-shadow: 0 10px 30px rgba(255, 215, 0, 0.2);
        }
        
        .stat:hover::before {
          opacity: 1;
        }
        
        .stat.error {
          background: linear-gradient(135deg, rgba(255, 51, 102, 0.2), rgba(255, 51, 102, 0.1));
          border-color: rgba(255, 51, 102, 0.3);
        }
        
        .stat.warning {
          background: linear-gradient(135deg, rgba(255, 184, 0, 0.2), rgba(255, 184, 0, 0.1));
          border-color: rgba(255, 184, 0, 0.3);
        }
        
        .stat.success {
          background: linear-gradient(135deg, rgba(0, 212, 170, 0.2), rgba(0, 212, 170, 0.1));
          border-color: rgba(0, 212, 170, 0.3);
        }
        
        .stat.review {
          background: linear-gradient(135deg, rgba(0, 180, 216, 0.2), rgba(0, 180, 216, 0.1));
          border-color: rgba(0, 180, 216, 0.3);
        }
        
        .stat h3 {
          font-size: 0.75rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 1.5px;
          color: var(--text-secondary);
          margin-bottom: 1rem;
        }
        
        .stat p {
          font-size: 2.5rem;
          font-weight: 700;
          margin: 0.5rem 0;
          background: linear-gradient(135deg, #FFD700, #FFA500);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
        }
        
        .stat small {
          display: block;
          font-size: 0.85rem;
          color: var(--text-muted);
          margin-top: 0.5rem;
        }
        
        /* Luxury Buttons */
        .btn {
          background: linear-gradient(135deg, #FFD700, #FFA500);
          color: var(--primary-dark);
          border: none;
          padding: 0.875rem 2rem;
          border-radius: 12px;
          font-size: 0.95rem;
          font-weight: 600;
          cursor: pointer;
          position: relative;
          overflow: hidden;
          transition: all 0.3s ease;
          text-transform: uppercase;
          letter-spacing: 1px;
          margin: 0.5rem;
          display: inline-flex;
          align-items: center;
          gap: 0.5rem;
        }
        
        .btn::before {
          content: '';
          position: absolute;
          top: 50%;
          left: 50%;
          width: 0;
          height: 0;
          background: rgba(255, 255, 255, 0.3);
          border-radius: 50%;
          transform: translate(-50%, -50%);
          transition: width 0.6s, height 0.6s;
        }
        
        .btn:hover {
          transform: translateY(-2px);
          box-shadow: 
            0 10px 30px rgba(255, 215, 0, 0.3),
            0 0 20px rgba(255, 215, 0, 0.2);
        }
        
        .btn:hover::before {
          width: 300px;
          height: 300px;
        }
        
        .btn:active {
          transform: translateY(0);
        }
        
        .btn:disabled {
          background: linear-gradient(135deg, #444, #333);
          cursor: not-allowed;
          transform: none;
          opacity: 0.5;
        }
        
        .btn.btn-secondary {
          background: linear-gradient(135deg, rgba(255, 255, 255, 0.1), rgba(255, 255, 255, 0.05));
          color: var(--text-primary);
          border: 1px solid rgba(255, 215, 0, 0.3);
        }
        
        .btn.btn-danger {
          background: linear-gradient(135deg, #FF3366, #FF1744);
          color: white;
        }
        
        .btn.btn-success {
          background: linear-gradient(135deg, #00D4AA, #00B894);
          color: white;
        }
        
        .btn.btn-warning {
          background: linear-gradient(135deg, #FFB800, #FF9800);
          color: var(--primary-dark);
        }
        
        .btn.btn-info {
          background: linear-gradient(135deg, #00B4D8, #0077B6);
          color: white;
        }
        
        /* Alert Boxes */
        .alert {
          background: linear-gradient(135deg, rgba(255, 51, 102, 0.2), rgba(255, 51, 102, 0.1));
          backdrop-filter: blur(10px);
          border: 1px solid rgba(255, 51, 102, 0.3);
          border-radius: 16px;
          padding: 1.5rem;
          margin-bottom: 2rem;
          display: flex;
          align-items: center;
          justify-content: space-between;
          animation: slideInLeft 0.5s ease;
          position: relative;
          overflow: hidden;
        }
        
        .alert::before {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          height: 100%;
          width: 4px;
          background: var(--danger);
        }
        
        .alert.warning {
          background: linear-gradient(135deg, rgba(255, 184, 0, 0.2), rgba(255, 184, 0, 0.1));
          border-color: rgba(255, 184, 0, 0.3);
        }
        
        .alert.warning::before {
          background: var(--warning);
        }
        
        .alert.review {
          background: linear-gradient(135deg, rgba(0, 180, 216, 0.2), rgba(0, 180, 216, 0.1));
          border-color: rgba(0, 180, 216, 0.3);
        }
        
        .alert.review::before {
          background: var(--info);
        }
        
        .alert-content {
          flex: 1;
        }
        
        .alert-title {
          font-weight: 600;
          font-size: 1.1rem;
          margin-bottom: 0.25rem;
        }
        
        .alert-message {
          color: var(--text-secondary);
          font-size: 0.95rem;
        }
        
        .alert-actions {
          display: flex;
          gap: 0.5rem;
        }
        
        /* Logs Container */
        .logs {
          background: linear-gradient(135deg, rgba(10, 10, 10, 0.9), rgba(20, 20, 20, 0.9));
          backdrop-filter: blur(10px);
          border: 1px solid rgba(255, 215, 0, 0.1);
          border-radius: 16px;
          padding: 1.5rem;
          max-height: 500px;
          overflow-y: auto;
          font-family: 'Courier New', monospace;
          font-size: 0.875rem;
          line-height: 1.8;
        }
        
        .logs::-webkit-scrollbar {
          width: 8px;
        }
        
        .logs::-webkit-scrollbar-track {
          background: rgba(255, 255, 255, 0.05);
          border-radius: 4px;
        }
        
        .logs::-webkit-scrollbar-thumb {
          background: linear-gradient(135deg, #FFD700, #FFA500);
          border-radius: 4px;
        }
        
        .log-entry {
          padding: 0.5rem;
          margin-bottom: 0.25rem;
          border-left: 2px solid transparent;
          transition: all 0.2s ease;
        }
        
        .log-entry:hover {
          background: rgba(255, 215, 0, 0.05);
          border-left-color: var(--accent-gold);
          padding-left: 1rem;
        }
        
        .log-info { color: #58a6ff; }
        .log-success { color: var(--success); }
        .log-warning { color: var(--warning); }
        .log-error { color: var(--danger); }
        .log-debug { color: #999; }
        
        /* Review Cards */
        .review-card {
          background: linear-gradient(135deg, rgba(0, 180, 216, 0.1), rgba(0, 180, 216, 0.05));
          border: 1px solid rgba(0, 180, 216, 0.3);
          border-radius: 16px;
          padding: 1.5rem;
          margin-bottom: 1rem;
          animation: slideIn 0.5s ease;
        }
        
        .review-card h4 {
          color: var(--info);
          margin-bottom: 1rem;
          font-size: 1.2rem;
        }
        
        .review-card pre {
          background: rgba(0, 0, 0, 0.3);
          padding: 1rem;
          border-radius: 8px;
          overflow-x: auto;
          font-size: 0.85rem;
          border: 1px solid rgba(255, 215, 0, 0.1);
        }
        
        /* Loading Spinner */
        .spinner {
          width: 40px;
          height: 40px;
          border: 3px solid rgba(255, 215, 0, 0.2);
          border-top-color: var(--accent-gold);
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        
        /* Pulse Animation */
        .pulse {
          animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.7; }
        }
        
        /* Running State */
        .running {
          position: relative;
          overflow: hidden;
        }
        
        .running::after {
          content: '';
          position: absolute;
          top: 0;
          left: -100%;
          width: 100%;
          height: 100%;
          background: linear-gradient(90deg, transparent, rgba(255, 215, 0, 0.3), transparent);
          animation: slide 2s infinite;
        }
        
        @keyframes slide {
          to { left: 100%; }
        }
        
        /* Animations */
        @keyframes fadeIn {
          from { 
            opacity: 0; 
            transform: translateY(20px);
          }
          to { 
            opacity: 1; 
            transform: translateY(0);
          }
        }
        
        @keyframes fadeInDown {
          from { 
            opacity: 0; 
            transform: translateY(-20px);
          }
          to { 
            opacity: 1; 
            transform: translateY(0);
          }
        }
        
        @keyframes slideInLeft {
          from {
            opacity: 0;
            transform: translateX(-100px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }
        
        @keyframes slideIn {
          from {
            opacity: 0;
            transform: scale(0.9);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
        
        /* Responsive Design */
        @media (max-width: 768px) {
          h1 { font-size: 2.5rem; }
          .container { padding: 1rem; }
          .stats { grid-template-columns: 1fr; }
          .btn { padding: 0.75rem 1.5rem; font-size: 0.875rem; }
        }
        
        /* Detail Sections */
        .detail-section {
          background: rgba(255, 255, 255, 0.02);
          border-radius: 12px;
          padding: 1rem;
          margin-top: 1rem;
          border: 1px solid rgba(255, 215, 0, 0.1);
        }
        
        .detail-section h4 {
          color: var(--accent-gold);
          margin-bottom: 0.5rem;
          font-size: 0.9rem;
          text-transform: uppercase;
          letter-spacing: 1px;
        }
        
        /* Icon animations */
        .icon-spin {
          animation: spin 2s linear infinite;
        }
        
        .icon-pulse {
          animation: pulse 2s ease-in-out infinite;
        }
        
        /* Tooltip */
        .tooltip {
          position: relative;
          display: inline-block;
        }
        
        .tooltip .tooltiptext {
          visibility: hidden;
          width: 200px;
          background: linear-gradient(135deg, rgba(20, 20, 20, 0.95), rgba(30, 30, 30, 0.95));
          color: var(--text-primary);
          text-align: center;
          border-radius: 8px;
          padding: 0.5rem;
          position: absolute;
          z-index: 1;
          bottom: 125%;
          left: 50%;
          margin-left: -100px;
          opacity: 0;
          transition: opacity 0.3s;
          border: 1px solid rgba(255, 215, 0, 0.2);
          font-size: 0.85rem;
        }
        
        .tooltip:hover .tooltiptext {
          visibility: visible;
          opacity: 1;
        }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1><i class="fas fa-crown"></i> Ralawise Sync</h1>
          <p class="header-subtitle">Luxury E-Commerce Management System</p>
        </div>
        
        ${reviewState.pending ? `
        <div class="alert review">
          <div class="alert-content">
            <div class="alert-title">📋 Review Pending</div>
            <div class="alert-message">Operation waiting for your approval</div>
          </div>
          <div class="alert-actions">
            <button class="btn btn-info" onclick="window.location.reload()">
              <i class="fas fa-sync-alt"></i> Refresh
            </button>
          </div>
        </div>
        ` : ''}
        
        ${failsafeState.triggered ? `
        <div class="alert">
          <div class="alert-content">
            <div class="alert-title">⚠️ Failsafe Active</div>
            <div class="alert-message">${failsafeState.canOverride ? 'Override available - Review required' : 'All operations blocked for safety'}</div>
          </div>
          <div class="alert-actions">
            ${failsafeState.canOverride ? `
              <button class="btn btn-warning" onclick="overrideFailsafe()">
                <i class="fas fa-exclamation-triangle"></i> Override
              </button>
            ` : ''}
            <button class="btn btn-secondary" onclick="clearFailsafe()">
              <i class="fas fa-times"></i> Clear
            </button>
          </div>
        </div>
        ` : ''}
        
        ${pauseState.paused && !failsafeState.triggered ? `
        <div class="alert warning">
          <div class="alert-content">
            <div class="alert-title">⏸️ System Paused</div>
            <div class="alert-message">${pauseState.reason} (by ${pauseState.pausedBy})</div>
          </div>
          <div class="alert-actions">
            <button class="btn btn-success" onclick="resumeSystem()">
              <i class="fas fa-play"></i> Resume
            </button>
          </div>
        </div>
        ` : ''}
        
        ${pendingReviewsList.length > 0 ? `
        <div class="card">
          <h2><i class="fas fa-clipboard-check"></i> Pending Reviews</h2>
          ${pendingReviewsList.map(review => `
            <div class="review-card">
              <h4>${review.operation}</h4>
              <div class="detail-section">
                <p><strong>Request ID:</strong> ${review.id}</p>
                <p><strong>Age:</strong> ${review.ageMinutes} minutes</p>
                <pre>${JSON.stringify(review.details, null, 2)}</pre>
              </div>
              <div style="margin-top: 1rem;">
                <button class="btn btn-success" onclick="approveReview('${review.id}')">
                  <i class="fas fa-check"></i> Approve
                </button>
                <button class="btn btn-danger" onclick="rejectReview('${review.id}')">
                  <i class="fas fa-times"></i> Reject
                </button>
              </div>
            </div>
          `).join('')}
        </div>
        ` : ''}
        
        <div class="card">
          <h2><i class="fas fa-chart-line"></i> System Status</h2>
          <div class="stats">
            <div class="stat ${isRunning.inventory ? 'running' : ''} ${failsafeState.triggered ? 'error' : ''} ${pauseState.paused && !failsafeState.triggered ? 'warning' : ''} ${reviewState.pending ? 'review' : ''}">
              <h3>Inventory Sync</h3>
              <p>${failsafeState.triggered ? '⛔' : (pauseState.paused ? '⏸️' : (reviewState.pending ? '📋' : (isRunning.inventory ? '🔄' : '✅')))}</p>
              <small>${failsafeState.triggered ? 'Blocked' : (pauseState.paused ? 'Paused' : (reviewState.pending ? 'Awaiting Review' : (isRunning.inventory ? 'Running...' : 'Ready')))}</small>
            </div>
            <div class="stat ${isRunning.fullImport ? 'running' : ''} ${failsafeState.triggered ? 'error' : ''} ${pauseState.paused && !failsafeState.triggered ? 'warning' : ''} ${reviewState.pending ? 'review' : ''}">
              <h3>Full Import</h3>
              <p>${failsafeState.triggered ? '⛔' : (pauseState.paused ? '⏸️' : (reviewState.pending ? '📋' : (isRunning.fullImport ? '🔄' : '✅')))}</p>
              <small>${failsafeState.triggered ? 'Blocked' : (pauseState.paused ? 'Paused' : (reviewState.pending ? 'Awaiting Review' : (isRunning.fullImport ? 'Running...' : 'Ready')))}</small>
            </div>
            <div class="stat ${failsafeState.triggered ? 'error' : (pauseState.paused ? 'warning' : (reviewState.pending ? 'review' : 'success'))}">
              <h3>System Health</h3>
              <p>${failsafeState.triggered ? '🚨' : (pauseState.paused ? '⏸️' : (reviewState.pending ? '📋' : '💎'))}</p>
              <small>${failsafeState.triggered ? 'Failsafe' : (pauseState.paused ? 'Paused' : (reviewState.pending ? 'Review' : 'Optimal'))}</small>
            </div>
          </div>
        </div>
        
        <div class="card">
          <h2><i class="fas fa-trophy"></i> Performance Metrics</h2>
          <div class="stats">
            <div class="stat">
              <h3>Inventory Updated</h3>
              <p>${lastRun.inventory.updated}</p>
              <small>${lastRun.inventory.timestamp ? new Date(lastRun.inventory.timestamp).toLocaleString() : 'Never'}</small>
            </div>
            <div class="stat">
              <h3>Products Tagged</h3>
              <p>${lastRun.inventory.tagged || 0}</p>
              <small>Supplier:Ralawise</small>
            </div>
            <div class="stat">
              <h3>Products Created</h3>
              <p>${lastRun.fullImport.created}</p>
              <small>${lastRun.fullImport.timestamp ? new Date(lastRun.fullImport.timestamp).toLocaleString() : 'Never'}</small>
            </div>
            <div class="stat">
              <h3>Discontinued</h3>
              <p>${lastRun.fullImport.discontinued}</p>
              <small>Marked as draft</small>
            </div>
          </div>
        </div>
        
        <div class="card">
          <h2><i class="fas fa-cogs"></i> Control Center</h2>
          
          <div style="margin-bottom: 2rem;">
            ${!pauseState.paused && !failsafeState.triggered ? `
              <button class="btn btn-warning" onclick="pauseSystem()">
                <i class="fas fa-pause"></i> Pause System
              </button>
            ` : ''}
            ${pauseState.paused && !failsafeState.triggered ? `
              <button class="btn btn-success" onclick="resumeSystem()">
                <i class="fas fa-play"></i> Resume System
              </button>
            ` : ''}
            ${failsafeState.triggered ? `
              <button class="btn btn-danger" onclick="clearFailsafe()">
                <i class="fas fa-shield-alt"></i> Clear Failsafe
              </button>
            ` : ''}
          </div>
          
          <h3 style="margin: 2rem 0 1rem 0; color: var(--accent-gold); font-size: 1.1rem;">
            <i class="fas fa-play-circle"></i> Manual Operations
          </h3>
          <div>
            <button class="btn" onclick="runInventorySync()" id="invBtn" ${isSystemBlocked() ? 'disabled' : ''}>
              <i class="fas fa-sync-alt"></i> Sync Inventory
            </button>
            <button class="btn" onclick="runFullImport()" id="fullBtn" ${isSystemBlocked() ? 'disabled' : ''}>
              <i class="fas fa-download"></i> Full Import
            </button>
            <button class="btn btn-secondary" onclick="clearLogs()">
              <i class="fas fa-trash"></i> Clear Logs
            </button>
            <button class="btn btn-secondary" onclick="window.location.reload()">
              <i class="fas fa-redo"></i> Refresh
            </button>
          </div>
          
          <div class="detail-section" style="margin-top: 2rem;">
            <h4><i class="fas fa-info-circle"></i> System Configuration</h4>
            <p style="color: var(--text-secondary); line-height: 2;">
              <strong>Schedule:</strong> Inventory every 60min • Full import bi-daily at 13:00 UK<br>
              <strong>Review Thresholds:</strong> Inventory >${config.review.inventoryChangeReview}% • Products >${config.review.newProductsReview}<br>
              <strong>Failsafe Limits:</strong> Max ${config.failsafe.maxChangePercentage}% change • ${config.failsafe.maxProductsToCreate} creates • ${config.failsafe.maxProductsToDiscontinue} discontinues<br>
              <strong>Pricing Formula:</strong> £0-6 ×2.1 • £7-11 ×1.9 • £11+ ×1.75
            </p>
          </div>
        </div>
        
        <div class="card">
          <h2><i class="fas fa-terminal"></i> Activity Log</h2>
          <div class="logs" id="logs">
            ${logs.map(log => `
              <div class="log-entry log-${log.type}">
                <i class="fas fa-angle-right"></i> [${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}
              </div>
            `).join('')}
            ${logs.length === 0 ? '<div class="log-entry log-info"><i class="fas fa-info-circle"></i> System initialized. Waiting for operations...</div>' : ''}
          </div>
        </div>
      </div>
      
      <script>
        // All your existing JavaScript functions here
        function isSystemBlocked() {
          return ${failsafeState.triggered || pauseState.paused || reviewState.pending};
        }
        
        async function approveReview(requestId) {
          if (!confirm('Approve this operation?')) return;
          try {
            const res = await fetch('/api/review/' + requestId + '/approve', { method: 'POST' });
            const data = await res.json();
            if (data.success) {
              alert('✅ Review approved successfully!');
              window.location.reload();
            } else {
              alert('❌ Failed: ' + data.message);
            }
          } catch (e) {
            alert('Failed to approve review: ' + e.message);
          }
        }
        
        async function rejectReview(requestId) {
          const reason = prompt('Reason for rejection:');
          if (!reason) return;
          try {
            const res = await fetch('/api/review/' + requestId + '/reject', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ reason })
            });
            const data = await res.json();
            if (data.success) {
              alert('Review rejected!');
              window.location.reload();
            } else {
              alert('Failed: ' + data.message);
            }
          } catch (e) {
            alert('Failed to reject review: ' + e.message);
          }
        }
        
        async function pauseSystem() {
          const reason = prompt('Enter reason for pausing the system:', 'Maintenance');
          if (!reason) return;
          
          try {
            const res = await fetch('/api/pause', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ reason })
            });
            const data = await res.json();
            if (data.success) {
              window.location.reload();
            } else {
              alert('Failed to pause: ' + data.message);
            }
          } catch (e) {
            alert('Failed to pause system: ' + e.message);
          }
        }
        
        async function resumeSystem() {
          if (!confirm('Resume system operations?')) return;
          
          try {
            const res = await fetch('/api/resume', { method: 'POST' });
            const data = await res.json();
            if (data.success) {
              window.location.reload();
            } else {
              alert('Failed to resume: ' + data.message);
            }
          } catch (e) {
            alert('Failed to resume system: ' + e.message);
          }
        }
        
        async function runInventorySync() {
          if (!confirm('Run inventory sync now?')) return;
          const btn = document.getElementById('invBtn');
          btn.disabled = true;
          btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Starting...';
          try {
            const res = await fetch('/api/sync/inventory', { method: 'POST' });
            const data = await res.json();
            if (data.error) {
              alert('Error: ' + data.error);
              btn.disabled = false;
              btn.innerHTML = '<i class="fas fa-sync-alt"></i> Sync Inventory';
            } else {
              btn.innerHTML = '<i class="fas fa-check"></i> Started!';
              setTimeout(() => window.location.reload(), 2000);
            }
          } catch (e) {
            alert('Failed to start sync: ' + e.message);
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-sync-alt"></i> Sync Inventory';
          }
        }
        
        async function runFullImport() {
          if (!confirm('This will create new products and mark discontinued ones as draft. Continue?')) return;
          const btn = document.getElementById('fullBtn');
          btn.disabled = true;
          btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Starting...';
          try {
            const res = await fetch('/api/sync/full', { method: 'POST' });
            const data = await res.json();
            if (data.error) {
              alert('Error: ' + data.error);
              btn.disabled = false;
              btn.innerHTML = '<i class="fas fa-download"></i> Full Import';
            } else {
              btn.innerHTML = '<i class="fas fa-check"></i> Started!';
              setTimeout(() => window.location.reload(), 2000);
            }
          } catch (e) {
            alert('Failed to start import: ' + e.message);
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-download"></i> Full Import';
          }
        }
        
        async function clearLogs() {
          if (!confirm('Clear all logs?')) return;
          await fetch('/api/logs/clear', { method: 'POST' });
          window.location.reload();
        }
        
        async function clearFailsafe() {
          if (!confirm('Clear failsafe and allow operations to continue?')) return;
          try {
            const res = await fetch('/api/failsafe/clear', { method: 'POST' });
            const data = await res.json();
            if (data.success) {
              window.location.reload();
            }
          } catch (e) {
            alert('Failed to clear failsafe: ' + e.message);
          }
        }
        
        async function overrideFailsafe() {
          const reason = prompt('⚠️ WARNING: Override safety failsafe?\\n\\nProvide reason:', 'Legitimate bulk update');
          if (!reason) return;
          
          if (!confirm('⚠️ FINAL WARNING\\n\\nProceed with override?')) return;
          
          try {
            const res = await fetch('/api/failsafe/override', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ reason })
            });
            const data = await res.json();
            if (data.success) {
              window.location.reload();
            } else {
              alert('Failed: ' + data.message);
            }
          } catch (e) {
            alert('Failed to override: ' + e.message);
          }
        }
        
        // Auto-refresh
        const checkRefresh = () => {
          const shouldRefresh = ${isRunning.inventory || isRunning.fullImport || reviewState.pending};
          setTimeout(() => window.location.reload(), shouldRefresh ? 30000 : 60000);
        };
        checkRefresh();
      </script>
    </body>
    </html>
  `;
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
