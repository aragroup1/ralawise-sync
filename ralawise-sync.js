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

// FIXED: Better pagination handling with safety limits
async function getAllShopifyProducts() { 
    let all = []; 
    let pageCount = 0;
    let url = `/products.json?limit=250&fields=id,handle,title,variants,tags,status`; 
    addLog('Fetching all products from Shopify...', 'info');
    
    while (url && pageCount < 100) { // Safety limit: max 100 pages (25,000 products)
        pageCount++;
        try {
            const res = await shopifyRequestWithRetry('get', url); 
            
            if (!res.data.products || res.data.products.length === 0) {
                addLog('No more products found, stopping pagination', 'info');
                break;
            }
            
            all.push(...res.data.products); 
            addLog(`Fetched page ${pageCount}: ${res.data.products.length} products (total: ${all.length})`, 'info');
            
            // Parse Link header more carefully
            const linkHeader = res.headers.link;
            if (linkHeader && linkHeader.includes('rel="next"')) {
                const matches = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
                if (matches && matches[1]) {
                    url = matches[1].replace(config.shopify.baseUrl, '');
                } else {
                    url = null;
                }
            } else {
                url = null;
            }
            
            await delay(250);
        } catch (error) {
            addLog(`Error fetching page ${pageCount}: ${error.message}`, 'error');
            break;
        }
    } 
    
    if (pageCount >= 100) {
        addLog('WARNING: Reached page limit of 100 pages. There may be more products.', 'warning');
    }
    
    addLog(`Completed fetching ${all.length} products from Shopify in ${pageCount} pages`, 'success'); 
    return all; 
}

async function updateInventoryBySKU(inventoryMap) { if (isRunning.inventory) { addLog('Inventory update already running.', 'warning'); return; } isRunning.inventory = true; let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0 }; try { addLog('=== STARTING INVENTORY ANALYSIS ===', 'info'); const shopifyProducts = await getAllShopifyProducts(); const skuToProduct = new Map(); shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); })); const updatesToPerform = []; inventoryMap.forEach((newQty, sku) => { const match = skuToProduct.get(sku.toUpperCase()); if (match && (match.variant.inventory_quantity || 0) !== newQty) updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match }); }); const totalProducts = skuToProduct.size; const updatesNeeded = updatesToPerform.length; const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0; addLog(`Change analysis: ${updatesNeeded} updates for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info'); const executeUpdates = async () => { addLog(`Executing updates for ${updatesNeeded} products...`, 'info'); for (const u of updatesToPerform) { try { if (!u.match.product.tags?.includes('Supplier:Ralawise')) { await shopifyRequestWithRetry('put', `/products/${u.match.product.id}.json`, { product: { id: u.match.product.id, tags: `${u.match.product.tags || ''},Supplier:Ralawise`.replace(/^,/, '') } }); runResult.tagged++; await delay(200); } if (!u.match.variant.inventory_management) { await shopifyRequestWithRetry('put', `/variants/${u.match.variant.id}.json`, { variant: { id: u.match.variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } }); await delay(200); } await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id }).catch(() => {}); await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id, available: u.newQty }); runResult.updated++; addLog(`Updated ${u.match.product.title} (${u.sku}): ${u.oldQty} → ${u.newQty}`, 'success'); await delay(250); } catch (e) { runResult.errors++; addLog(`Failed to update ${u.sku}: ${e.message}`, 'error'); } } const notFound = inventoryMap.size - updatesToPerform.length; runResult.status = 'completed'; notifyTelegram(`Inventory update complete:\n✅ ${runResult.updated} updated, 🏷️ ${runResult.tagged} tagged, ❌ ${runResult.errors} errors, ❓ ${notFound} not found`); }; if (changePercentage > config.failsafe.inventoryChangePercentage) { requestConfirmation('inventory', `High inventory change detected`, { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded, totalProducts, sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) }}, async () => { try { await executeUpdates(); } finally { isRunning.inventory = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); } }); return; } else { await executeUpdates(); } } catch (error) { addLog(`Inventory update failed critically: ${error.message}`, 'error'); runResult.errors++; triggerFailsafe(`Inventory update failed critically: ${error.message}`); } finally { if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') { isRunning.inventory = false; addToHistory({...runResult, timestamp: new Date().toISOString() }); } } }
async function downloadAndExtractZip() { const url = `${config.ralawise.zipUrl}?t=${Date.now()}`;
