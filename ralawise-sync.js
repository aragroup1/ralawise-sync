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
const HISTORY_FILE = path.join(__dirname, '_history.json');
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };

// Historical tracking
let runHistory = [];

function loadHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
      addLog('Historical data loaded', 'info');
    }
  } catch (e) {
    addLog('Could not load history: ' + e.message, 'warning');
  }
}

function saveHistory() {
  try {
    // Keep only last 100 runs
    if (runHistory.length > 100) {
      runHistory = runHistory.slice(-100);
    }
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2));
  } catch (e) {
    addLog('Could not save history: ' + e.message, 'warning');
  }
}

function addToHistory(type, stats) {
  runHistory.push({
    type,
    timestamp: new Date().toISOString(),
    ...stats
  });
  saveHistory();
}

function checkPauseStateOnStartup() { 
  if (fs.existsSync(PAUSE_LOCK_FILE)) { 
    isSystemPaused = true; 
    addLog('System is PAUSED (found lock file on startup).', 'warning'); 
  } 
  loadHistory();
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs = logs.slice(0, 500); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { if (message.length > 4096) message = message.substring(0, 4086) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }, { timeout: 10000 }); } catch (error) { addLog(`Telegram notification failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { 
  if (failsafe.isTriggered) return; 
  failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString(), details: {} }; 
  const msg = `🚨 HARD FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`; 
  addLog(msg, 'error'); 
  notifyTelegram(msg); 
  // Clear running states when failsafe triggers
  isRunning.inventory = false;
  isRunning.fullImport = false;
}
function requestConfirmation(jobKey, message, details, proceedAction) { confirmation = { isAwaiting: true, message, details, proceedAction, abortAction: () => { addLog(`User aborted '${message}'. System is now paused for safety.`, 'warning'); isSystemPaused = true; fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); notifyTelegram(`🙅‍♂️ User ABORTED the operation for: ${message}.\n\nSystem has been automatically paused.`); }, jobKey }; const alertMsg = `🤔 CONFIRMATION REQUIRED 🤔\n\n<b>Action Paused:</b> ${message}`; addLog(alertMsg, 'warning'); let debugMsg = ''; if (details.inventoryChange) { debugMsg = `\n\n<b>Details:</b>\nDetected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\nPlease visit the dashboard to review and decide.`; } notifyTelegram(alertMsg + debugMsg); }

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: 30000 });
async function fetchInventoryFromFTP() { const client = new ftp.Client(); client.ftp.verbose = false; try { addLog('Connecting to FTP...', 'info'); await client.access(config.ftp); const chunks = []; await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), '/Stock/Stock_Update.csv'); const buffer = Buffer.concat(chunks); addLog(`FTP download successful, ${buffer.length} bytes`, 'success'); return Readable.from(buffer); } catch (e) { addLog(`FTP error: ${e.message}`, 'error'); throw e; } finally { client.close(); } }
async function parseInventoryCSV(stream) { return new Promise((resolve, reject) => { const inventory = new Map(); stream.pipe(csv({ headers: ['SKU', 'Quantity'], skipLines: 1 })).on('data', row => { if (row.SKU && row.SKU !== 'SKU') inventory.set(row.SKU.trim(), Math.min(parseInt(row.Quantity) || 0, config.ralawise.maxInventory)); }).on('end', () => resolve(inventory)).on('error', reject); }); }
async function getAllShopifyProducts() { let all = []; let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`; while (url) { const res = await shopifyClient.get(url); all.push(...res.data.products); const link = res.headers.link; url = link && link.includes('rel="next"') ? link.match(/<([^>]+)>/)[1].replace(config.shopify.baseUrl, '') : null; await delay(250); } addLog(`Fetched ${all.length} products from Shopify`, 'success'); return all; }
async function updateInventoryBySKU(inventoryMap) { 
  if (isRunning.inventory) { 
    addLog('Inventory update already running.', 'warning'); 
    return; 
  } 
  isRunning.inventory = true; 
  try { 
    addLog('=== STARTING INVENTORY ANALYSIS ===', 'info'); 
    const shopifyProducts = await getAllShopifyProducts(); 
    const skuToProduct = new Map(); 
    shopifyProducts.forEach(p => p.variants?.forEach(v => { 
      if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); 
    })); 
    const updatesToPerform = []; 
    inventoryMap.forEach((newQty, sku) => { 
      const match = skuToProduct.get(sku.toUpperCase()); 
      if (match && (match.variant.inventory_quantity || 0) !== newQty) 
        updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match }); 
    }); 
    const totalProducts = skuToProduct.size; 
    const updatesNeeded = updatesToPerform.length; 
    const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0; 
    addLog(`Change analysis: ${updatesNeeded} updates for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info'); 
    
    const executeUpdates = async () => { 
      let updated = 0, errors = 0, tagged = 0; 
      addLog(`Executing updates for ${updatesNeeded} products...`, 'info'); 
      for (const u of updatesToPerform) { 
        try { 
          if (!u.match.product.tags?.includes('Supplier:Ralawise')) { 
            await shopifyClient.put(`/products/${u.match.product.id}.json`, { 
              product: { id: u.match.product.id, tags: `${u.match.product.tags || ''},Supplier:Ralawise`.replace(/^,/, '') } 
            }); 
            tagged++; 
            await delay(200); 
          } 
          if (!u.match.variant.inventory_management) { 
            await shopifyClient.put(`/variants/${u.match.variant.id}.json`, { 
              variant: { id: u.match.variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } 
            }); 
            await delay(200); 
          } 
          await shopifyClient.post('/inventory_levels/connect.json', { 
            location_id: config.shopify.locationId, 
            inventory_item_id: u.match.variant.inventory_item_id 
          }).catch(() => {}); 
          await shopifyClient.post('/inventory_levels/set.json', { 
            location_id: config.shopify.locationId, 
            inventory_item_id: u.match.variant.inventory_item_id, 
            available: u.newQty 
          }); 
          updated++; 
          addLog(`Updated ${u.match.product.title} (${u.sku}): ${u.oldQty} → ${u.newQty}`, 'success'); 
          await delay(250); 
        } catch (e) { 
          errors++; 
          addLog(`Failed to update ${u.sku}: ${e.response ? JSON.stringify(e.response.data) : e.message}`, 'error'); 
          if (e.response?.status === 429) {
            addLog('Rate limit hit, waiting 10 seconds...', 'warning');
            await delay(10000);
          }
        } 
      } 
      const notFound = inventoryMap.size - updatesToPerform.length; 
      lastRun.inventory = { updated, errors, tagged, timestamp: new Date().toISOString() }; 
      addToHistory('inventory', { updated, errors, tagged, notFound, totalProcessed: updatesNeeded });
      notifyTelegram(`Inventory update complete:\n✅ ${updated} updated, 🏷️ ${tagged} tagged, ❌ ${errors} errors, ❓ ${notFound} not found`); 
    }; 
    
    if (changePercentage > config.failsafe.inventoryChangePercentage) { 
      requestConfirmation('inventory', `High inventory change detected`, { 
        inventoryChange: { 
          threshold: config.failsafe.inventoryChangePercentage, 
          actualPercentage: changePercentage, 
          updatesNeeded, 
          totalProducts, 
          sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) 
        }
      }, async () => { 
        try { 
          await executeUpdates(); 
        } finally { 
          isRunning.inventory = false; 
        } 
      }); 
      return; 
    } else { 
      await executeUpdates(); 
    } 
  } catch (error) { 
    triggerFailsafe(`Inventory update failed critically: ${error.message}`); 
  } finally { 
    if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') 
      isRunning.inventory = false; 
  } 
}

async function downloadAndExtractZip() { const url = `${config.ralawise.zipUrl}?t=${Date.now()}`; addLog(`Downloading zip: ${url}`, 'info'); const res = await axios.get(url, { responseType: 'arraybuffer', timeout: 120000 }); const tempDir = path.join(__dirname, 'temp', `ralawise_${Date.now()}`); fs.mkdirSync(tempDir, { recursive: true }); const zipPath = path.join(tempDir, 'data.zip'); fs.writeFileSync(zipPath, res.data); const zip = new AdmZip(zipPath); zip.extractAllTo(tempDir, true); fs.unlinkSync(zipPath); return { tempDir, csvFiles: fs.readdirSync(tempDir).filter(f => f.endsWith('.csv')).map(f => path.join(tempDir, f)) }; }
async function parseShopifyCSV(filePath) { return new Promise((resolve, reject) => { const products = []; fs.createReadStream(filePath).pipe(csv()).on('data', row => { products.push({ ...row, price: applyRalawisePricing(parseFloat(row['Variant Price']) || 0), original_price: parseFloat(row['Variant Price']) || 0 }); }).on('end', () => resolve(products)).on('error', reject); }); }
async function processFullImport(csvFiles) { 
  if (isRunning.fullImport) return; 
  isRunning.fullImport = true; 
  let created = 0, discontinued = 0, errors = 0; 
  try { 
    addLog('=== STARTING FULL IMPORT ===', 'info'); 
    const allRows = (await Promise.all(csvFiles.map(parseShopifyCSV))).flat(); 
    const productsByHandle = new Map(); 
    for (const row of allRows) { 
      if (!row.Handle) continue; 
      if (!productsByHandle.has(row.Handle)) 
        productsByHandle.set(row.Handle, { 
          ...row, 
          tags: `${row.Tags || ''},Supplier:Ralawise`.replace(/^,/, ''), 
          images: [], 
          variants: [], 
          options: [] 
        }); 
      const p = productsByHandle.get(row.Handle); 
      if (row['Image Src'] && !p.images.some(img => img.src === row['Image Src'])) 
        p.images.push({ src: row['Image Src'], position: parseInt(row['Image Position']), alt: row['Image Alt Text'] || row.Title }); 
      if (row['Variant SKU']) { 
        const v = { 
          sku: row['Variant SKU'], 
          price: row.price, 
          option1: row['Option1 Value'], 
          option2: row['Option2 Value'], 
          option3: row['Option3 Value'], 
          inventory_quantity: Math.min(parseInt(row['Variant Inventory Qty']) || 0, config.ralawise.maxInventory) 
        }; 
        p.variants.push(v); 
      } 
    } 
    const shopifyProducts = await getAllShopifyProducts(); 
    const existingHandles = new Set(shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise')).map(p => p.handle)); 
    const toCreate = Array.from(productsByHandle.values()).filter(p => !existingHandles.has(p.Handle)); 
    addLog(`Found ${toCreate.length} new products to create.`, 'info'); 
    for (const p of toCreate.slice(0, 30)) { 
      try { 
        const res = await shopifyClient.post('/products.json', { 
          product: { 
            title: p.Title, 
            handle: p.Handle, 
            body_html: p['Body (HTML)'], 
            vendor: p.Vendor, 
            product_type: p.Type, 
            tags: p.tags, 
            images: p.images, 
            variants: p.variants.map(v => ({...v, inventory_management: 'shopify', inventory_policy: 'deny'})) 
          } 
        }); 
        for (const v of res.data.product.variants) { 
          const origV = p.variants.find(ov => ov.sku === v.sku); 
          if (origV && v.inventory_item_id && origV.inventory_quantity > 0) { 
            await shopifyClient.post('/inventory_levels/connect.json', { 
              location_id: config.shopify.locationId, 
              inventory_item_id: v.inventory_item_id 
            }).catch(()=>{}); 
            await shopifyClient.post('/inventory_levels/set.json', { 
              location_id: config.shopify.locationId, 
              inventory_item_id: v.inventory_item_id, 
              available: origV.inventory_quantity 
            }); 
          } 
        } 
        created++; 
        addLog(`✅ Created: ${p.Title}`, 'success'); 
        await delay(1000); 
      } catch(e) { 
        errors++; 
        addLog(`❌ Failed to create ${p.Title}: ${e.message}`, 'error'); 
      } 
    } 
    const newHandles = new Set(Array.from(productsByHandle.keys())); 
    const toDiscontinue = shopifyProducts.filter(p => p.tags?.includes('Supplier:Ralawise') && !newHandles.has(p.handle)); 
    addLog(`Found ${toDiscontinue.length} products to discontinue.`, 'info'); 
    for (const p of toDiscontinue.slice(0, 50)) { 
      try { 
        await shopifyClient.put(`/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } }); 
        for (const v of p.variants) { 
          if (v.inventory_item_id) 
            await shopifyClient.post('/inventory_levels/set.json', { 
              location_id: config.shopify.locationId, 
              inventory_item_id: v.inventory_item_id, 
              available: 0 
            }).catch(()=>{}); 
        } 
        discontinued++; 
        addLog(`⏸️ Discontinued: ${p.title}`, 'info'); 
        await delay(500); 
      } catch(e) { 
        errors++; 
        addLog(`Failed to discontinue ${p.title}: ${e.message}`, 'error'); 
      } 
    } 
    lastRun.fullImport = { created, discontinued, errors, timestamp: new Date().toISOString() }; 
    addToHistory('fullImport', { created, discontinued, errors });
    notifyTelegram(`Full import complete:\n✅ ${created} created\n⏸️ ${discontinued} discontinued\n❌ ${errors} errors`); 
  } catch(e) { 
    triggerFailsafe(`Full import failed: ${e.message}`); 
  } finally { 
    isRunning.fullImport = false; 
  } 
}

// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } try { addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); const stream = await fetchInventoryFromFTP(); await updateInventoryBySKU(await parseInventoryCSV(stream)); } catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); lastRun.inventory.errors++; } }
async function syncFullCatalog() { if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; } let tempDir; try { addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); lastRun.fullImport.errors++; } finally { if (tempDir) { try { fs.rmSync(tempDir, { recursive: true, force: true }); addLog('Cleaned up temp files', 'info'); } catch (cleanupError) { addLog(`Cleanup error: ${cleanupError.message}`, 'warning'); } } } }

// ============================================
// WEB INTERFACE - ULTRA MODERN DESIGN
// ============================================

app.get('/', (req, res) => {
    const isSystemLocked = (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting);
    
    // Calculate stats for history
    const last24h = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 24*60*60*1000));
    const last7d = runHistory.filter(r => new Date(r.timestamp) > new Date(Date.now() - 7*24*60*60*1000));
    
    const confirmationDetailsHTML = confirmation.details.inventoryChange ? `
      <div class="confirmation-details">
        <div class="detail-grid">
          <div class="detail-card">
            <span class="detail-label">Threshold</span>
            <span class="detail-value">${confirmation.details.inventoryChange.threshold}%</span>
          </div>
          <div class="detail-card">
            <span class="detail-label">Detected Change</span>
            <span class="detail-value danger">${confirmation.details.inventoryChange.actualPercentage.toFixed(2)}%</span>
          </div>
          <div class="detail-card">
            <span class="detail-label">Updates Pending</span>
            <span class="detail-value">${confirmation.details.inventoryChange.updatesNeeded}</span>
          </div>
        </div>
        <div class="sample-changes">
          <h4>Sample Changes</h4>
          <div class="changes-list">
            ${confirmation.details.inventoryChange.sample.map(item => `
              <div class="change-item">
                <span class="sku">${item.sku}</span>
                <span class="arrow">→</span>
                <span class="qty-change">${item.oldQty} → ${item.newQty}</span>
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    ` : '';
    
    const html = `<!DOCTYPE html>
    <html lang="en">
    <head>
        <title>Ralawise Sync • Premium Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            :root {
                --bg-primary: #0a0a0a;
                --bg-secondary: #111111;
                --bg-card: rgba(18, 18, 18, 0.95);
                --bg-hover: rgba(255, 255, 255, 0.03);
                
                --text-primary: #ffffff;
                --text-secondary: #a1a1a1;
                --text-muted: #666666;
                
                --accent-primary: #6366f1;
                --accent-success: #10b981;
                --accent-warning: #f59e0b;
                --accent-danger: #ef4444;
                --accent-info: #3b82f6;
                
                --gradient-primary: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                --gradient-success: linear-gradient(135deg, #34d399 0%, #10b981 100%);
                --gradient-warning: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%);
                --gradient-danger: linear-gradient(135deg, #f87171 0%, #ef4444 100%);
                --gradient-dark: linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%);
                
                --shadow-sm: 0 2px 4px rgba(0,0,0,0.2);
                --shadow-md: 0 4px 12px rgba(0,0,0,0.3);
                --shadow-lg: 0 8px 24px rgba(0,0,0,0.4);
                --shadow-xl: 0 12px 48px rgba(0,0,0,0.5);
                
                --radius-sm: 8px;
                --radius-md: 12px;
                --radius-lg: 16px;
                --radius-xl: 24px;
            }
            
            body {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                background: var(--bg-primary);
                color: var(--text-primary);
                min-height: 100vh;
                line-height: 1.6;
                position: relative;
                overflow-x: hidden;
            }
            
            /* Animated background */
            body::before {
                content: '';
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: 
                    radial-gradient(circle at 20% 50%, rgba(99, 102, 241, 0.1) 0%, transparent 50%),
                    radial-gradient(circle at 80% 80%, rgba(168, 85, 247, 0.1) 0%, transparent 50%),
                    radial-gradient(circle at 40% 20%, rgba(236, 72, 153, 0.1) 0%, transparent 50%);
                z-index: -1;
                animation: backgroundShift 30s ease infinite;
            }
            
            @keyframes backgroundShift {
                0%, 100% { transform: scale(1) rotate(0deg); }
                25% { transform: scale(1.1) rotate(1deg); }
                50% { transform: scale(0.95) rotate(-1deg); }
                75% { transform: scale(1.05) rotate(0.5deg); }
            }
            
            .container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 2rem;
                animation: fadeIn 0.6s ease;
            }
            
            /* Header */
            .header {
                text-align: center;
                margin-bottom: 3rem;
                position: relative;
            }
            
            .header h1 {
                font-size: clamp(2.5rem, 5vw, 3.5rem);
                font-weight: 800;
                background: var(--gradient-primary);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                letter-spacing: -1px;
                margin-bottom: 0.5rem;
                animation: slideDown 0.8s ease;
            }
            
            .header p {
                color: var(--text-secondary);
                font-size: 1.125rem;
                font-weight: 300;
                letter-spacing: 0.5px;
                animation: slideUp 0.8s ease;
            }
            
            /* Cards */
            .card {
                background: var(--bg-card);
                border: 1px solid rgba(255, 255, 255, 0.05);
                border-radius: var(--radius-xl);
                padding: 2rem;
                backdrop-filter: blur(20px);
                -webkit-backdrop-filter: blur(20px);
                box-shadow: var(--shadow-lg);
                position: relative;
                overflow: hidden;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                animation: fadeInUp 0.6s ease backwards;
            }
            
            .card::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 1px;
                background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.5), transparent);
                animation: shimmer 3s infinite;
            }
            
            @keyframes shimmer {
                0% { transform: translateX(-100%); }
                100% { transform: translateX(100%); }
            }
            
            .card:hover {
                transform: translateY(-2px);
                box-shadow: var(--shadow-xl);
                border-color: rgba(99, 102, 241, 0.2);
            }
            
            .card h2 {
                font-size: 1.25rem;
                font-weight: 600;
                margin-bottom: 1.5rem;
                display: flex;
                align-items: center;
                gap: 0.75rem;
                color: var(--text-primary);
            }
            
            .card h2 .icon {
                width: 24px;
                height: 24px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: var(--gradient-primary);
                border-radius: var(--radius-sm);
                font-size: 14px;
            }
            
            /* Grid layouts */
            .grid {
                display: grid;
                gap: 1.5rem;
                margin-bottom: 2rem;
            }
            
            .grid-3 {
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            }
            
            .grid-4 {
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            }
            
            /* Stats */
            .stat-card {
                background: var(--gradient-dark);
                border: 1px solid rgba(255, 255, 255, 0.05);
                border-radius: var(--radius-lg);
                padding: 1.5rem;
                position: relative;
                overflow: hidden;
                transition: all 0.3s ease;
            }
            
            .stat-card:hover {
                transform: translateY(-4px);
                box-shadow: var(--shadow-lg);
            }
            
            .stat-card .label {
                font-size: 0.75rem;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: var(--text-muted);
                font-weight: 600;
                margin-bottom: 0.5rem;
            }
            
            .stat-card .value {
                font-size: 2rem;
                font-weight: 700;
                color: var(--text-primary);
                margin-bottom: 0.25rem;
            }
            
            .stat-card .meta {
                font-size: 0.875rem;
                color: var(--text-secondary);
            }
            
            .stat-card.success { border-color: rgba(16, 185, 129, 0.3); }
            .stat-card.warning { border-color: rgba(245, 158, 11, 0.3); }
            .stat-card.danger { border-color: rgba(239, 68, 68, 0.3); }
            .stat-card.info { border-color: rgba(59, 130, 246, 0.3); }
            
            /* Status badges */
            .status {
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                padding: 0.5rem 1rem;
                border-radius: 9999px;
                font-size: 0.875rem;
                font-weight: 600;
                transition: all 0.3s ease;
            }
            
            .status.active {
                background: rgba(16, 185, 129, 0.1);
                color: #10b981;
                border: 1px solid rgba(16, 185, 129, 0.3);
            }
            
            .status.paused {
                background: rgba(245, 158, 11, 0.1);
                color: #f59e0b;
                border: 1px solid rgba(245, 158, 11, 0.3);
            }
            
            .status.running {
                background: rgba(59, 130, 246, 0.1);
                color: #3b82f6;
                border: 1px solid rgba(59, 130, 246, 0.3);
                animation: pulse 2s infinite;
            }
            
            .status.error {
                background: rgba(239, 68, 68, 0.1);
                color: #ef4444;
                border: 1px solid rgba(239, 68, 68, 0.3);
            }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.6; }
            }
            
            /* Buttons */
            .btn {
                padding: 0.75rem 1.5rem;
                border-radius: var(--radius-md);
                font-weight: 600;
                font-size: 0.875rem;
                border: none;
                cursor: pointer;
                transition: all 0.2s ease;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                position: relative;
                overflow: hidden;
            }
            
            .btn::before {
                content: '';
                position: absolute;
                top: 50%;
                left: 50%;
                width: 0;
                height: 0;
                border-radius: 50%;
                background: rgba(255, 255, 255, 0.1);
                transform: translate(-50%, -50%);
                transition: width 0.6s, height 0.6s;
            }
            
            .btn:hover::before {
                width: 300px;
                height: 300px;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: var(--shadow-lg);
            }
            
            .btn:active {
                transform: translateY(0);
            }
            
            .btn:disabled {
                opacity: 0.5;
                cursor: not-allowed;
                transform: none !important;
            }
            
            .btn-primary {
                background: var(--gradient-primary);
                color: white;
            }
            
            .btn-success {
                background: var(--gradient-success);
                color: white;
            }
            
            .btn-warning {
                background: var(--gradient-warning);
                color: white;
            }
            
            .btn-danger {
                background: var(--gradient-danger);
                color: white;
            }
            
            .btn-secondary {
                background: var(--gradient-dark);
                color: var(--text-primary);
                border: 1px solid rgba(255, 255, 255, 0.1);
            }
            
            .btn-group {
                display: flex;
                flex-wrap: wrap;
                gap: 1rem;
            }
            
            /* Alerts */
            .alert {
                padding: 1.5rem;
                border-radius: var(--radius-lg);
                margin-bottom: 2rem;
                border: 1px solid;
                backdrop-filter: blur(10px);
                animation: slideIn 0.5s ease;
                position: relative;
                overflow: hidden;
            }
            
            .alert::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                bottom: 0;
                width: 4px;
            }
            
            .alert.failsafe {
                background: rgba(239, 68, 68, 0.1);
                border-color: rgba(239, 68, 68, 0.3);
            }
            
            .alert.failsafe::before {
                background: var(--gradient-danger);
            }
            
            .alert.confirmation {
                background: rgba(59, 130, 246, 0.1);
                border-color: rgba(59, 130, 246, 0.3);
            }
            
            .alert.confirmation::before {
                background: var(--gradient-primary);
            }
            
            .alert h3 {
                font-size: 1.25rem;
                font-weight: 600;
                margin-bottom: 0.75rem;
                display: flex;
                align-items: center;
                gap: 0.75rem;
            }
            
            .alert p {
                color: var(--text-secondary);
                margin-bottom: 1rem;
            }
            
            /* Confirmation details */
            .confirmation-details {
                margin-top: 1.5rem;
            }
            
            .detail-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 1rem;
                margin-bottom: 1.5rem;
            }
            
            .detail-card {
                background: rgba(0, 0, 0, 0.3);
                padding: 1rem;
                border-radius: var(--radius-md);
                text-align: center;
            }
            
            .detail-label {
                display: block;
                font-size: 0.75rem;
                text-transform: uppercase;
                color: var(--text-muted);
                margin-bottom: 0.5rem;
                font-weight: 600;
            }
            
            .detail-value {
                display: block;
                font-size: 1.5rem;
                font-weight: 700;
                color: var(--text-primary);
            }
            
            .detail-value.danger {
                color: var(--accent-danger);
            }
            
            .sample-changes {
                background: rgba(0, 0, 0, 0.3);
                padding: 1rem;
                border-radius: var(--radius-md);
            }
            
            .sample-changes h4 {
                font-size: 0.875rem;
                font-weight: 600;
                margin-bottom: 1rem;
                color: var(--text-secondary);
            }
            
            .changes-list {
                display: grid;
                gap: 0.5rem;
            }
            
            .change-item {
                display: flex;
                align-items: center;
                gap: 1rem;
                padding: 0.5rem;
                background: rgba(255, 255, 255, 0.02);
                border-radius: var(--radius-sm);
                font-family: monospace;
                font-size: 0.875rem;
            }
            
            .change-item .sku {
                color: var(--accent-primary);
                font-weight: 600;
                min-width: 100px;
            }
            
            .change-item .arrow {
                color: var(--text-muted);
            }
            
            .change-item .qty-change {
                color: var(--text-primary);
            }
            
            /* History table */
            .history-table {
                width: 100%;
                border-collapse: collapse;
            }
            
            .history-table th {
                text-align: left;
                padding: 0.75rem;
                font-size: 0.75rem;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: var(--text-muted);
                border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                font-weight: 600;
            }
            
            .history-table td {
                padding: 0.75rem;
                font-size: 0.875rem;
                color: var(--text-secondary);
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            
            .history-table tr:hover {
                background: var(--bg-hover);
            }
            
            /* Logs */
            .logs {
                background: rgba(0, 0, 0, 0.5);
                border: 1px solid rgba(255, 255, 255, 0.05);
                border-radius: var(--radius-md);
                padding: 1rem;
                max-height: 400px;
                overflow-y: auto;
                font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
                font-size: 0.8125rem;
                line-height: 1.6;
            }
            
            .logs::-webkit-scrollbar {
                width: 6px;
            }
            
            .logs::-webkit-scrollbar-track {
                background: rgba(255, 255, 255, 0.02);
            }
            
            .logs::-webkit-scrollbar-thumb {
                background: rgba(255, 255, 255, 0.1);
                border-radius: 3px;
            }
            
            .log-entry {
                padding: 0.5rem;
                margin-bottom: 0.25rem;
                border-left: 2px solid transparent;
                transition: all 0.2s ease;
            }
            
            .log-entry:hover {
                background: rgba(255, 255, 255, 0.02);
                padding-left: 1rem;
            }
            
            .log-info { border-left-color: var(--accent-info); color: #93c5fd; }
            .log-success { border-left-color: var(--accent-success); color: #86efac; }
            .log-warning { border-left-color: var(--accent-warning); color: #fde047; }
            .log-error { border-left-color: var(--accent-danger); color: #fca5a5; }
            
            /* Animations */
            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }
            
            @keyframes fadeInUp {
                from { opacity: 0; transform: translateY(20px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            @keyframes slideDown {
                from { opacity: 0; transform: translateY(-20px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            @keyframes slideUp {
                from { opacity: 0; transform: translateY(20px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            @keyframes slideIn {
                from { opacity: 0; transform: translateX(-100px); }
                to { opacity: 1; transform: translateX(0); }
            }
            
            /* Responsive */
            @media (max-width: 768px) {
                .container { padding: 1rem; }
                .header h1 { font-size: 2rem; }
                .grid-3, .grid-4 { grid-template-columns: 1fr; }
                .btn-group { flex-direction: column; }
                .btn { width: 100%; justify-content: center; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header class="header">
                <h1>Ralawise Sync</h1>
                <p>Premium Inventory & Product Management System</p>
            </header>
            
            ${failsafe.isTriggered ? `
            <div class="alert failsafe">
                <h3>🚨 Failsafe Activated</h3>
                <p><strong>Reason:</strong> ${failsafe.reason}</p>
                <p><strong>Time:</strong> ${failsafe.timestamp ? new Date(failsafe.timestamp).toLocaleString() : 'Unknown'}</p>
                <div class="btn-group">
                    <button onclick="clearFailsafe()" class="btn btn-success">✅ Clear Failsafe & Resume</button>
                </div>
            </div>
            ` : ''}
            
            ${confirmation.isAwaiting ? `
            <div class="alert confirmation">
                <h3>🤔 Confirmation Required</h3>
                <p><strong>Action:</strong> ${confirmation.message}</p>
                ${confirmationDetailsHTML}
                <div class="btn-group">
                    <button onclick="proceed()" class="btn btn-success">✅ Proceed Anyway</button>
                    <button onclick="abort()" class="btn btn-danger">❌ Abort & Pause</button>
                </div>
            </div>
            ` : ''}
            
            <div class="grid grid-3">
                <div class="card">
                    <h2><span class="icon">📊</span> System Status</h2>
                    <div class="stat-card ${isSystemPaused ? 'warning' : 'success'}">
                        <div class="label">Overall Status</div>
                        <div class="value">
                            <span class="status ${isSystemPaused ? 'paused' : 'active'}">
                                ${isSystemPaused ? '⏸️ Paused' : '✅ Active'}
                            </span>
                        </div>
                    </div>
                    <div style="margin-top: 1rem;">
                        <button onclick="togglePause()" class="btn ${isSystemPaused ? 'btn-success' : 'btn-warning'}" ${failsafe.isTriggered || confirmation.isAwaiting ? 'disabled' : ''}>
                            ${isSystemPaused ? '▶️ Resume System' : '⏸️ Pause System'}
                        </button>
                    </div>
                </div>
                
                <div class="card">
                    <h2><span class="icon">🔄</span> Job Status</h2>
                    <div style="display: grid; gap: 1rem;">
                        <div class="stat-card ${isRunning.inventory ? 'info' : ''}">
                            <div class="label">Inventory Sync</div>
                            <div class="value">
                                <span class="status ${isRunning.inventory ? 'running' : 'active'}">
                                    ${isRunning.inventory ? '🔄 Running' : '✅ Ready'}
                                </span>
                            </div>
                        </div>
                        <div class="stat-card ${isRunning.fullImport ? 'info' : ''}">
                            <div class="label">Full Import</div>
                            <div class="value">
                                <span class="status ${isRunning.fullImport ? 'running' : 'active'}">
                                    ${isRunning.fullImport ? '🔄 Running' : '✅ Ready'}
                                </span>
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h2><span class="icon">🎮</span> Quick Actions</h2>
                    <div class="btn-group" style="flex-direction: column;">
                        <button onclick="runInventorySync()" class="btn btn-primary" ${isSystemLocked ? 'disabled' : ''}>
                            🔄 Run Inventory Sync
                        </button>
                        <button onclick="runFullImport()" class="btn btn-primary" ${isSystemLocked ? 'disabled' : ''}>
                            📦 Run Full Import
                        </button>
                        <button onclick="clearLogs()" class="btn btn-secondary">
                            🗑️ Clear Logs
                        </button>
                    </div>
                </div>
            </div>
            
            <div class="card" style="animation-delay: 0.1s;">
                <h2><span class="icon">📈</span> Performance Metrics</h2>
                <div class="grid grid-4">
                    <div class="stat-card">
                        <div class="label">Last Inventory Update</div>
                        <div class="value">${lastRun.inventory.updated || 0}</div>
                        <div class="meta">${lastRun.inventory.timestamp ? new Date(lastRun.inventory.timestamp).toLocaleString() : 'Never'}</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Products Tagged</div>
                        <div class="value">${lastRun.inventory.tagged || 0}</div>
                        <div class="meta">Supplier:Ralawise</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Products Created</div>
                        <div class="value">${lastRun.fullImport.created || 0}</div>
                        <div class="meta">${lastRun.fullImport.timestamp ? new Date(lastRun.fullImport.timestamp).toLocaleString() : 'Never'}</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Discontinued</div>
                        <div class="value">${lastRun.fullImport.discontinued || 0}</div>
                        <div class="meta">Marked as draft</div>
                    </div>
                </div>
            </div>
            
            <div class="card" style="animation-delay: 0.2s;">
                <h2><span class="icon">📜</span> Historical Data</h2>
                <div class="grid grid-3" style="margin-bottom: 1.5rem;">
                    <div class="stat-card">
                        <div class="label">Last 24 Hours</div>
                        <div class="value">${last24h.length}</div>
                        <div class="meta">Total operations</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Last 7 Days</div>
                        <div class="value">${last7d.length}</div>
                        <div class="meta">Total operations</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">All Time</div>
                        <div class="value">${runHistory.length}</div>
                        <div class="meta">Total operations</div>
                    </div>
                </div>
                ${runHistory.length > 0 ? `
                <div style="overflow-x: auto;">
                    <table class="history-table">
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Type</th>
                                <th>Updated</th>
                                <th>Created</th>
                                <th>Tagged</th>
                                <th>Discontinued</th>
                                <th>Errors</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${runHistory.slice(0, 10).map(run => `
                                <tr>
                                    <td>${new Date(run.timestamp).toLocaleString()}</td>
                                    <td><span class="status ${run.type === 'inventory' ? 'info' : 'active'}">${run.type}</span></td>
                                    <td>${run.updated || '-'}</td>
                                    <td>${run.created || '-'}</td>
                                    <td>${run.tagged || '-'}</td>
                                    <td>${run.discontinued || '-'}</td>
                                    <td>${run.errors || 0}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
                ` : '<p style="text-align: center; color: var(--text-muted);">No historical data yet</p>'}
            </div>
            
            <div class="card" style="animation-delay: 0.3s;">
                <h2><span class="icon">📝</span> Activity Log</h2>
                <div class="logs">
                    ${logs.length > 0 ? logs.map(log => `
                        <div class="log-entry log-${log.type}">
                            [${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}
                        </div>
                    `).join('') : '<div class="log-entry log-info">System initialized. Waiting for operations...</div>'}
                </div>
            </div>
        </div>
        
        <script>
            async function apiPost(endpoint, confirmMsg) { 
                if (confirmMsg && !confirm(confirmMsg)) return; 
                const btn = event.target; 
                btn.disabled = true; 
                try { 
                    const res = await fetch(endpoint, { method: 'POST' }); 
                    const data = await res.json();
                    if (!data.success && data.error) {
                        alert('Error: ' + data.error);
                        btn.disabled = false;
                        return;
                    }
                    window.location.reload(); 
                } catch (e) { 
                    alert('Failed: ' + e.message); 
                    btn.disabled = false; 
                } 
            }
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
// API ENDPOINTS - Fixed
// ============================================

app.post('/api/sync/inventory', async (req, res) => { 
  if (isSystemPaused) return res.json({ error: 'System is paused', success: false }); 
  if (failsafe.isTriggered) return res.json({ error: 'Failsafe is active', success: false }); 
  if (confirmation.isAwaiting) return res.json({ error: 'Confirmation is pending', success: false }); 
  if (isRunning.inventory) return res.json({ error: 'Inventory sync already running', success: false }); 
  syncInventory(); 
  res.json({ success: true, message: 'Inventory sync started' }); 
});

app.post('/api/sync/full', async (req, res) => { 
  if (isSystemPaused) return res.json({ error: 'System is paused', success: false }); 
  if (failsafe.isTriggered) return res.json({ error: 'Failsafe is active', success: false }); 
  if (confirmation.isAwaiting) return res.json({ error: 'Confirmation is pending', success: false }); 
  if (isRunning.fullImport) return res.json({ error: 'Full import already running', success: false }); 
  syncFullCatalog(); 
  res.json({ success: true, message: 'Full import started' }); 
});

app.post('/api/logs/clear', (req, res) => { 
  logs = []; 
  addLog('Logs cleared manually', 'info'); 
  res.json({ success: true }); 
});

app.post('/api/failsafe/clear', (req, res) => { 
  addLog('Failsafe manually cleared.', 'warning'); 
  failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} }; 
  // Clear running states when clearing failsafe
  isRunning.inventory = false;
  isRunning.fullImport = false;
  notifyTelegram('✅ Failsafe has been manually cleared. Operations are resuming.'); 
  res.json({ success: true }); 
});

app.post('/api/pause/toggle', (req, res) => { 
  isSystemPaused = !isSystemPaused; 
  if (isSystemPaused) { 
    fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); 
    addLog('System has been MANUALLY PAUSED.', 'warning'); 
    notifyTelegram('⏸️ System has been manually PAUSED.'); 
  } else { 
    try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {} 
    addLog('System has been RESUMED.', 'info'); 
    notifyTelegram('▶️ System has been manually RESUMED.'); 
  } 
  res.json({ success: true, isPaused: isSystemPaused }); 
});

app.post('/api/confirmation/proceed', (req, res) => { 
  if (!confirmation.isAwaiting) return res.json({ error: 'No confirmation pending.', success: false }); 
  addLog(`User confirmed to PROCEED with: ${confirmation.message}`, 'info'); 
  notifyTelegram(`👍 User confirmed to PROCEED with: ${confirmation.message}`); 
  if (confirmation.proceedAction) setTimeout(confirmation.proceedAction, 0); 
  confirmation = { isAwaiting: false }; 
  res.json({ success: true, message: 'Action proceeding.' }); 
});

app.post('/api/confirmation/abort', (req, res) => { 
  if (!confirmation.isAwaiting) return res.json({ error: 'No confirmation pending.', success: false }); 
  if (confirmation.abortAction) confirmation.abortAction(); 
  const jobKey = confirmation.jobKey; 
  if (jobKey) isRunning[jobKey] = false; 
  confirmation = { isAwaiting: false }; 
  res.json({ success: true, message: 'Action aborted and system paused.' }); 
});

// ============================================
// SCHEDULED TASKS
// ============================================

cron.schedule('0 * * * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.inventory) { addLog('⏰ Starting scheduled inventory sync...', 'info'); syncInventory(); } else { addLog('⏰ Skipped scheduled inventory sync: System is busy or paused.', 'warning'); } });
cron.schedule('0 13 */2 * *', () => { if (!isSystemPaused && !failsafe.isTriggered && !confirmation.isAwaiting && !isRunning.fullImport) { addLog('⏰ Starting scheduled full catalog import...', 'info'); syncFullCatalog(); } else { addLog('⏰ Skipped scheduled full import: System is busy or paused.', 'warning'); } }, { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { 
  checkPauseStateOnStartup(); 
  addLog(`✅ Ralawise Sync Server started on port ${PORT}`, 'success'); 
  addLog(`🛡️ Confirmation Threshold: >${config.failsafe.inventoryChangePercentage}% inventory change`, 'info'); 
  setTimeout(() => { 
    if (!isSystemPaused && !isRunning.inventory) { 
      addLog('🚀 Running initial inventory sync...', 'info'); 
      syncInventory(); 
    } 
  }, 10000); 
});

function shutdown(signal) { 
  addLog(`Received ${signal}, shutting down...`, 'info'); 
  saveHistory(); // Save history before shutdown
  process.exit(0); 
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (error) => { console.error('Uncaught Exception:', error); addLog(`FATAL Uncaught Exception: ${error.message}`, 'error'); });
process.on('unhandledRejection', (reason, promise) => { console.error('Unhandled Rejection at:', promise, 'reason:', reason); addLog(`FATAL Unhandled Rejection: ${reason}`, 'error'); });
