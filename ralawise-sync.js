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
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
  },
  ftp: {
    host: process.env.FTP_HOST,
    user: process.env.FTP_USERNAME,
    password: process.env.FTP_PASSWORD,
    secure: false
  },
  ralawise: {
    zipUrl: process.env.RALAWISE_ZIP_URL,
    maxInventory: parseInt(process.env.MAX_INVENTORY || '20')
  },
  telegram: {
    botToken: process.env.TELEGRAM_BOT_TOKEN,
    chatId: process.env.TELEGRAM_CHAT_ID
  },
  failsafe: {
    inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10')
  }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) {
  console.error(`Missing required environment variables. Please check your config.`);
  process.exit(1);
}

// ============================================
// STATE MANAGEMENT
// ============================================

let lastRun = { inventory: {}, fullImport: {} };
let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');

// --- NEW: Confirmation State ---
let confirmation = {
    isAwaiting: false,
    message: '',
    details: {},
    proceedAction: null, // Function to call on "Proceed"
    abortAction: null,   // Function to call on "Abort"
    jobKey: null         // e.g., 'inventory'
};

function checkPauseStateOnStartup() {
    if (fs.existsSync(PAUSE_LOCK_FILE)) {
        isSystemPaused = true;
        addLog('System is PAUSED (found lock file on startup).', 'warning');
    }
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs = logs.slice(0, 500); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message) { if (!config.telegram.botToken || !config.telegram.chatId) return; try { const maxLength = 4096; if (message.length > maxLength) message = message.substring(0, maxLength - 10) + '...'; await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, { chat_id: config.telegram.chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' }, { timeout: 10000 }); } catch (error) { addLog(`Telegram notification failed: ${error.message}`, 'warning'); } }
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(originalPrice) { if (typeof originalPrice !== 'number' || originalPrice < 0) return '0.00'; let finalPrice; if (originalPrice <= 6) finalPrice = originalPrice * 2.1; else if (originalPrice <= 11) finalPrice = originalPrice * 1.9; else finalPrice = originalPrice * 1.75; return finalPrice.toFixed(2); }

// --- MODIFIED: This is now a hard failsafe for critical errors ---
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString(), details: {} }; const errorMessage = `🚨 HARD FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`; addLog(errorMessage, 'error'); notifyTelegram(errorMessage); }

// --- NEW: Confirmation Request Function ---
function requestConfirmation(jobKey, message, details, proceedAction) {
    confirmation = {
        isAwaiting: true,
        message,
        details,
        proceedAction,
        abortAction: () => {
            addLog(`User aborted '${message}'. System is now paused for safety.`, 'warning');
            isSystemPaused = true;
            fs.writeFileSync(PAUSE_LOCK_FILE, 'paused');
            notifyTelegram(`🙅‍♂️ User ABORTED the operation for: ${message}.\n\nSystem has been automatically paused.`);
        },
        jobKey
    };

    const alertMessage = `🤔 CONFIRMATION REQUIRED 🤔\n\n<b>Action Paused:</b> ${message}`;
    addLog(alertMessage, 'warning');

    let debugMessage = '';
    if (details.inventoryChange) {
        debugMessage = `\n\n<b>Details:</b>\n` +
            `Detected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code> (Threshold: ${details.inventoryChange.threshold}%)\n\n`+
            `Please visit the dashboard to review and decide whether to proceed or abort.`;
    }
    notifyTelegram(alertMessage + debugMessage);
}


// ============================================
// FTP, SHOPIFY, AND IMPORT FUNCTIONS
// ============================================

// [PREVIOUS FUNCTIONS ARE UNCHANGED AND OMITTED FOR BREVITY - FULL SCRIPT IS BELOW]


// --- MODIFIED: updateInventoryBySKU is now split into analysis and execution ---
async function updateInventoryBySKU(inventoryMap) {
    if (isRunning.inventory) { addLog('Inventory update already running, skipping...', 'warning'); return { skipped: true }; }
    isRunning.inventory = true;
    
    try {
        addLog('=== STARTING INVENTORY ANALYSIS ===', 'info');
        
        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); }));
        
        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match) {
                const currentQty = match.variant.inventory_quantity || 0;
                if (currentQty !== newQty) updatesToPerform.push({ sku, oldQty: currentQty, newQty, match });
            }
        });

        const totalProducts = skuToProduct.size;
        const updatesNeeded = updatesToPerform.length;
        const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0;
        addLog(`Inventory change analysis: ${updatesNeeded} updates needed for ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info');

        // This is the function that will perform the updates
        const executeUpdates = async () => {
            let updated = 0, errors = 0, tagged = 0;
            addLog(`Executing inventory updates for ${updatesNeeded} products...`, 'info');
            
            for (const update of updatesToPerform) {
                const { sku, newQty, oldQty, match } = update;
                const { product, variant } = match;
                try {
                    if (!product.tags?.includes('Supplier:Ralawise')) {
                        const newTags = product.tags ? `${product.tags},Supplier:Ralawise` : 'Supplier:Ralawise';
                        await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, tags: newTags } });
                        tagged++; await delay(200);
                    }
                    if (!variant.inventory_management) {
                        await shopifyClient.put(`/variants/${variant.id}.json`, { variant: { id: variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } });
                        await delay(200);
                    }
                    await shopifyClient.post('/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: variant.inventory_item_id }).catch(() => {});
                    await shopifyClient.post('/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: variant.inventory_item_id, available: newQty });
                    addLog(`Updated ${product.title} (${sku}): ${oldQty} → ${newQty}`, 'success');
                    updated++; await delay(250);
                } catch (error) {
                    errors++; addLog(`Failed to update ${sku}: ${error.response ? JSON.stringify(error.response.data) : error.message}`, 'error');
                    if (error.response?.status === 429) await delay(5000);
                }
            }
            const notFound = inventoryMap.size - skuToProduct.size;
            lastRun.inventory = { updated, errors, tagged, timestamp: new Date().toISOString() };
            const message = `Inventory update complete:\n✅ ${updated} updated\n🏷️ ${tagged} tagged\n❌ ${errors} errors\n❓ ${notFound} SKUs not found`;
            addLog(message, 'success');
            if (updated > 0 || tagged > 0) await notifyTelegram(message);
        };

        // Decision point
        if (changePercentage > config.failsafe.inventoryChangePercentage) {
            requestConfirmation(
                'inventory',
                `High inventory change detected`,
                { inventoryChange: { threshold: config.failsafe.inventoryChangePercentage, actualPercentage: changePercentage, updatesNeeded, totalProducts, sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty })) }},
                async () => {
                    try { await executeUpdates(); } 
                    finally { isRunning.inventory = false; }
                }
            );
            // Don't set isRunning to false here; it stays true until user action
            return;
        } else {
            // Proceed automatically
            await executeUpdates();
        }
    } catch (error) {
        triggerFailsafe(`Inventory update process failed critically: ${error.message}`);
    } finally {
        // Only set to false if not awaiting confirmation
        if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') {
            isRunning.inventory = false;
        }
    }
}


// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() {
    if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Inventory sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; }
    try { addLog('=== INVENTORY SYNC TRIGGERED ===', 'info'); const stream = await fetchInventoryFromFTP(); await updateInventoryBySKU(await parseInventoryCSV(stream)); } 
    catch (error) { triggerFailsafe(`Inventory sync failed: ${error.message}`); lastRun.inventory.errors++; }
}
async function syncFullCatalog() {
    if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) { addLog(`Full catalog sync skipped: System is ${isSystemPaused ? 'PAUSED' : failsafe.isTriggered ? 'in FAILSAFE' : 'awaiting confirmation'}.`, 'warning'); return; }
    let tempDir;
    try { addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info'); const { tempDir: dir, csvFiles } = await downloadAndExtractZip(); tempDir = dir; await processFullImport(csvFiles); } 
    catch (error) { triggerFailsafe(`Full catalog sync failed: ${error.message}`); lastRun.fullImport.errors++; } 
    finally { if (tempDir) { try { fs.rmSync(tempDir, { recursive: true, force: true }); addLog('Cleaned up temporary files', 'info'); } catch (cleanupError) { addLog(`Cleanup error: ${cleanupError.message}`, 'warning'); } } }
}

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
    // [UI CODE OMITTED FOR BREVITY - FULL VERSION BELOW]
});

// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', async (req, res) => {
  if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) return res.status(423).json({ error: `Syncs are paused.` });
  if (isRunning.inventory) return res.status(409).json({ error: 'Inventory sync already running' });
  syncInventory();
  res.json({ success: true, message: 'Inventory sync started' });
});

app.post('/api/sync/full', async (req, res) => {
  if (isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting) return res.status(423).json({ error: `Syncs are paused.` });
  if (isRunning.fullImport) return res.status(409).json({ error: 'Full import already running' });
  syncFullCatalog();
  res.json({ success: true, message: 'Full import started' });
});

app.post('/api/logs/clear', (req, res) => { logs = []; addLog('Logs cleared manually', 'info'); res.json({ success: true }); });

app.post('/api/failsafe/clear', (req, res) => {
    addLog('Failsafe manually cleared.', 'warning');
    failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
    notifyTelegram('✅ Failsafe has been manually cleared. Operations are resuming.');
    res.json({ success: true });
});

app.post('/api/pause/toggle', (req, res) => {
    isSystemPaused = !isSystemPaused;
    if (isSystemPaused) { fs.writeFileSync(PAUSE_LOCK_FILE, 'paused'); addLog('System has been MANUALLY PAUSED.', 'warning'); notifyTelegram('⏸️ System has been manually PAUSED.'); } 
    else { try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {} addLog('System has been RESUMED.', 'info'); notifyTelegram('▶️ System has been manually RESUMED.'); }
    res.json({ success: true, isPaused: isSystemPaused });
});

// --- NEW: Confirmation Endpoints ---
app.post('/api/confirmation/proceed', (req, res) => {
    if (!confirmation.isAwaiting) return res.status(400).json({ error: 'No confirmation is currently pending.' });

    addLog(`User confirmed to PROCEED with: ${confirmation.message}`, 'info');
    notifyTelegram(`👍 User confirmed to PROCEED with: ${confirmation.message}`);
    
    // Execute the stored action in the background
    if (confirmation.proceedAction) {
        setTimeout(confirmation.proceedAction, 0); 
    }
    
    // Reset state
    confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
    res.json({ success: true, message: 'Action is proceeding.' });
});

app.post('/api/confirmation/abort', (req, res) => {
    if (!confirmation.isAwaiting) return res.status(400).json({ error: 'No confirmation is currently pending.' });
    
    if (confirmation.abortAction) {
        confirmation.abortAction(); // This pauses the system
    }

    // Reset state and the job lock
    const jobKey = confirmation.jobKey;
    if (jobKey) isRunning[jobKey] = false;

    confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
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
  setTimeout(() => { if (!isRunning.inventory) { addLog('🚀 Running initial inventory sync...', 'info'); syncInventory(); } }, 10000);
});

// [Graceful shutdown functions here]
