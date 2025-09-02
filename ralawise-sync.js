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
        host: process.env.FTP_HOST, user: process.env.FTP_USERNAME, password: process.env.FTP_PASSWORD, secure: false 
    },
    ralawise: { 
        zipUrl: process.env.RALAWISE_ZIP_URL, maxInventory: parseInt(process.env.MAX_INVENTORY || '20') 
    },
    telegram: { 
        botToken: process.env.TELEGRAM_BOT_TOKEN, 
        chatId: process.env.TELEGRAM_CHAT_ID,
        adminChatId: process.env.TELEGRAM_ADMIN_CHAT_ID || process.env.TELEGRAM_CHAT_ID // Admin for reports
    },
    failsafe: { 
        inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10'),
        maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '12') * 60 * 60 * 1000,
        maxErrors: parseInt(process.env.MAX_ERRORS || '1000')
    },
    rateLimit: {
        requestsPerSecond: parseFloat(process.env.SHOPIFY_RATE_LIMIT || '2'),
        burstSize: parseInt(process.env.SHOPIFY_BURST_SIZE || '40')
    }
};

const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
if (requiredConfig.some(key => !process.env[key])) { console.error('Missing required environment variables.'); process.exit(1); }

// ============================================
// STATE & ERROR MANAGEMENT
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
let isSystemPaused = false;
const PAUSE_LOCK_FILE = path.join(__dirname, '_paused.lock');
const HISTORY_FILE = path.join(__dirname, '_history.json');
const ERROR_LOG_FILE = path.join(__dirname, '_error_log.json'); // NEW: For structured errors
let confirmation = { isAwaiting: false, message: '', details: {}, proceedAction: null, abortAction: null, jobKey: null };
let runHistory = [];

// [The rest of the state management, helper functions, RateLimiter class, etc. from the previous version remains here]
// ...
// --- Start of unchanged section ---

class RateLimiter { /* ... same as before ... */ }
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);
let syncProgress = { /* ... same as before ... */ };
function loadHistory() { /* ... same as before ... */ }
function saveHistory() { /* ... same as before ... */ }
function addToHistory(runData) { runHistory.unshift(runData); if(runHistory.length > 100) runHistory.pop(); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) { isSystemPaused = true; addLog('System PAUSED (lock file found).', 'warning'); } loadHistory(); }
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 500) logs.shift(); console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`); }
async function notifyTelegram(message, adminOnly = false) {
    const token = config.telegram.botToken;
    const chatId = adminOnly ? config.telegram.adminChatId : config.telegram.chatId;
    if (!token || !chatId) return;
    try {
        if (message.length > 4096) message = message.substring(0, 4086) + '...';
        await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, { chat_id: chatId, text: `🏪 Ralawise Sync\n${message}`, parse_mode: 'HTML' });
    } catch (error) { addLog(`Telegram failed: ${error.message}`, 'warning'); }
}
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function applyRalawisePricing(price) { if (typeof price !== 'number' || price < 0) return '0.00'; let p; if (price <= 6) p = price * 2.1; else if (price <= 11) p = price * 1.9; else p = price * 1.75; return p.toFixed(2); }
function triggerFailsafe(reason) { if (failsafe.isTriggered) return; failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() }; const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`; addLog(msg, 'error'); notifyTelegram(msg); isRunning.inventory = false; isRunning.fullImport = false; }
function requestConfirmation(jobKey, message, details, proceedAction) { /* ... same as before ... */ }
function resetConfirmationState() { /* ... same as before ... */ }
function updateProgress(type, current, total) { /* ... same as before ... */ }
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: 60000 });
// --- End of unchanged section ---

// NEW: Structured Error Logging
function logError(type, message, details = {}) {
    addLog(`${type}: ${message}`, 'error');
    const errorEntry = {
        timestamp: new Date().toISOString(),
        type,
        message,
        details
    };
    try {
        let errors = [];
        if (fs.existsSync(ERROR_LOG_FILE)) {
            errors = JSON.parse(fs.readFileSync(ERROR_LOG_FILE, 'utf8'));
        }
        errors.push(errorEntry);
        // Keep the log file from growing indefinitely (e.g., last 5000 errors)
        if (errors.length > 5000) {
            errors = errors.slice(errors.length - 5000);
        }
        fs.writeFileSync(ERROR_LOG_FILE, JSON.stringify(errors, null, 2));
    } catch (e) {
        console.error("Failed to write to error log file:", e.message);
    }
}


async function shopifyRequestWithRetry(method, url, data = null, retries = 5) {
    let lastError = null;
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            await rateLimiter.acquire();
            let response;
            switch (method.toLowerCase()) {
                case 'get': response = await shopifyClient.get(url); break;
                case 'post': response = await shopifyClient.post(url, data); break;
                case 'put': response = await shopifyClient.put(url, data); break;
            }
            return response;
        } catch (error) {
            lastError = error;
            if (error.response && error.response.status === 429) {
                // ... same rate limit logic as before
            } else { throw error; }
        }
    }
    throw lastError;
}

// ============================================
// CORE LOGIC FUNCTIONS
// ============================================

async function fetchInventoryFromFTP() { /* ... same as before ... */ }
async function parseInventoryCSV(stream) { /* ... same as before ... */ }
async function getAllShopifyProducts() { /* ... same as before ... */ }

// MODIFIED: updateInventoryBySKU with better logging and error handling
async function updateInventoryBySKU(inventoryMap) {
    if (isRunning.inventory) { addLog('Inventory update already running.', 'warning'); return; }
    isRunning.inventory = true;

    syncProgress.inventory = { isActive: true, current: 0, total: 0, startTime: Date.now(), cancelled: false, errors: 0, updated: 0, tagged: 0, skipped: 0, notFound: 0, rateLimitHits: 0 };
    let runResult = { type: 'Inventory', status: 'failed', updated: 0, tagged: 0, errors: 0, skipped: 0, notFound: 0 };
    
    // This wrapper ensures that isRunning is set to false even if confirmation is pending
    const finalizeRun = () => {
        isRunning.inventory = false;
        syncProgress.inventory.isActive = false;
        addToHistory({ ...runResult, timestamp: new Date().toISOString() });
    };

    try {
        addLog('=== STARTING INVENTORY ANALYSIS ===', 'info');
        const shopifyProducts = await getAllShopifyProducts();
        const skuToProduct = new Map();
        shopifyProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) skuToProduct.set(v.sku.toUpperCase(), { product: p, variant: v }); }));
        
        const updatesToPerform = [];
        inventoryMap.forEach((newQty, sku) => {
            const match = skuToProduct.get(sku.toUpperCase());
            if (match) {
                if ((match.variant.inventory_quantity || 0) !== newQty) updatesToPerform.push({ sku, oldQty: match.variant.inventory_quantity || 0, newQty, match });
                else runResult.skipped++;
            } else { runResult.notFound++; }
        });

        const changePercentage = skuToProduct.size > 0 ? (updatesToPerform.length / skuToProduct.size) * 100 : 0;
        addLog(`Analysis: ${updatesToPerform.length} updates needed, ${runResult.skipped} skipped, ${runResult.notFound} not found. Change: ${changePercentage.toFixed(2)}%`, 'info');

        const executeUpdates = async () => {
            for (let i = 0; i < updatesToPerform.length; i++) {
                if (syncProgress.inventory.cancelled) { runResult.status = 'cancelled'; break; }
                const u = updatesToPerform[i];
                try {
                    await shopifyRequestWithRetry('post', '/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id, available: u.newQty });
                    runResult.updated++;
                    // MODIFIED: Add success log to confirm the update
                    if (i % 25 === 0) { // Log success less frequently to avoid clutter
                        addLog(`SUCCESS: Updated ${u.sku} -> ${u.newQty}`, 'success');
                    }
                } catch (e) {
                    runResult.errors++;
                    const errorMessage = e.response?.data?.errors?.inventory_item_id || e.message;
                    // MODIFIED: Use new structured error logging
                    logError('InventoryUpdate', errorMessage, { sku: u.sku });
                    // Try to connect if that was the issue
                    if (e.response && e.response.status === 422) {
                        try {
                           await shopifyRequestWithRetry('post', '/inventory_levels/connect.json', { location_id: config.shopify.locationId, inventory_item_id: u.match.variant.inventory_item_id });
                           addLog(`Connected new inventory item for SKU: ${u.sku}`, 'info');
                        } catch(connectError) {
                            logError('InventoryConnect', connectError.message, { sku: u.sku });
                        }
                    }
                }
                updateProgress('inventory', i + 1, updatesToPerform.length);
            }
            if (runResult.status !== 'cancelled') runResult.status = 'completed';
            const runtime = ((Date.now() - syncProgress.inventory.startTime) / 1000 / 60).toFixed(1);
            const finalMsg = `Inventory update ${runResult.status} in ${runtime}m:\n✅ ${runResult.updated} updated\n⏭️ ${runResult.skipped} skipped\n❌ ${runResult.errors} errors\n❓ ${runResult.notFound} not found`;
            notifyTelegram(finalMsg); addLog(finalMsg, 'success');
        };
        
        if (changePercentage > config.failsafe.inventoryChangePercentage && updatesToPerform.length > 100) {
             requestConfirmation('inventory', 'High inventory change detected', { inventoryChange: { /* ... */ } }, 
                async () => { 
                    try { await executeUpdates(); } finally { finalizeRun(); } 
                }
             );
             // Return here to wait for user confirmation. The confirmation action will handle finalization.
             return;
        } else {
            await executeUpdates();
        }

    } catch (error) {
        logError('CriticalSyncFail', error.message);
        runResult.errors++;
        triggerFailsafe(`Inventory sync failed critically: ${error.message}`);
    } finally {
        // This block will now run only for non-confirmation paths or if confirmation fails to proceed
        if (!confirmation.isAwaiting || confirmation.jobKey !== 'inventory') {
             finalizeRun();
        }
    }
}


async function downloadAndExtractZip() { /* ... same as before ... */ }
async function parseShopifyCSV(stream) { /* ... same as before ... */ }
async function processFullImport(csvFiles) { /* ... same as before ... */ }
async function syncInventory() { /* ... same as before ... */ }
async function syncFullCatalog() { /* ... same as before ... */ }

// NEW: Function to generate and send error reports
async function generateAndSendErrorReport() {
    addLog('Generating weekly error report...', 'info');
    if (!fs.existsSync(ERROR_LOG_FILE)) {
        addLog('No error log file found. Skipping report.', 'info');
        return;
    }

    try {
        const errors = JSON.parse(fs.readFileSync(ERROR_LOG_FILE, 'utf8'));
        const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        
        const recentErrors = errors.filter(e => new Date(e.timestamp) > oneWeekAgo);

        if (recentErrors.length === 0) {
            await notifyTelegram('🎉 **Weekly Report**: No errors recorded in the last 7 days!', true);
            return;
        }

        const errorCounts = recentErrors.reduce((acc, error) => {
            const key = `${error.type}: ${error.message}`;
            acc[key] = (acc[key] || 0) + 1;
            return acc;
        }, {});

        const sortedErrors = Object.entries(errorCounts)
            .sort(([, a], [, b]) => b - a)
            .slice(0, 10); // Top 10 errors

        let reportMessage = `📊 **Weekly Error Report**\n\nTotal errors this week: ${recentErrors.length}\n\n`;
        reportMessage += '<b>Top Errors:</b>\n';
        sortedErrors.forEach(([error, count], index) => {
            reportMessage += `${index + 1}. (x${count}) <code>${error}</code>\n`;
        });

        await notifyTelegram(reportMessage, true);
        addLog('Weekly error report sent to admin.', 'success');

    } catch (e) {
        logError('ReportGeneration', `Failed to generate error report: ${e.message}`);
    }
}


// ============================================
// WEB INTERFACE (with new Reports section)
// ============================================
app.get('/', (req, res) => { /* ... UI code ... with added Reports card */ });
// ... [Full UI code will be at the end]


// ============================================
// API ENDPOINTS (with new Reports endpoint)
// ============================================
app.post('/api/sync/inventory', (req, res) => { /* ... */ });
app.post('/api/sync/full', (req, res) => { /* ... */ });
app.post('/api/sync/inventory/cancel', (req, res) => { /* ... */ });
app.post('/api/pause/toggle', (req, res) => { /* ... */ });
app.post('/api/failsafe/clear', (req, res) => { /* ... */ });
app.post('/api/confirmation/proceed', (req, res) => { /* ... */ });
app.post('/api/confirmation/abort', (req, res) => { /* ... */ });

// NEW: API Endpoint to trigger error report on-demand
app.post('/api/reports/errors', async (req, res) => {
    generateAndSendErrorReport();
    res.json({ success: true, message: 'Error report generation started.' });
});

// ============================================
// SCHEDULED TASKS (with new weekly report task)
// ============================================
cron.schedule('0 2 * * *', () => syncInventory());
cron.schedule('0 4 * * 0', () => syncFullCatalog(), { timezone: 'Europe/London' });

// NEW: Cron job for the weekly error report (e.g., Monday at 9 AM)
cron.schedule('0 9 * * 1', () => generateAndSendErrorReport(), { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    checkPauseStateOnStartup();
    addLog(`✅ Server started on port ${PORT}`, 'success');
    // ...
});
function shutdown(signal) { /* ... */ }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// --- Full UI Code for app.get('/') ---
app.get('/', (req, res) => {
    const isSystemLocked = isRunning.inventory || isRunning.fullImport || isSystemPaused || failsafe.isTriggered || confirmation.isAwaiting;
    const inventoryProgressHTML = syncProgress.inventory.isActive && syncProgress.inventory.total > 0 ? 
        `<div class="progress-container"><span>Progress: ${syncProgress.inventory.current}/${syncProgress.inventory.total}</span><div class="progress-bar"><div class="progress-fill" style="width:${(syncProgress.inventory.current / syncProgress.inventory.total * 100).toFixed(1)}%"></div></div></div>` : '';

    const html = `<!DOCTYPE html><html><head><title>Ralawise Sync</title><style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;}.container{max-width:1400px;margin:auto;padding:2rem;}.card{background:#161b22;border:1px solid #30363d;padding:2rem;border-radius:0.5rem;margin-bottom:1rem;}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:1rem;}.btn{padding:0.75rem 1.5rem;border:1px solid #30363d;border-radius:0.5rem;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:bold;}.btn-primary{background:#238636;color:white;border-color:#2ea043;}.btn-danger{background:#da3633;color:white;border-color:#f85149}.btn:disabled{opacity:0.5;cursor:not-allowed;}.logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:0.5rem;font-family:monospace;white-space:pre-wrap;}.log-info{color:#58a6ff;}.log-success{color:#3fb950;}.log-warning{color:#d29922;}.log-error{color:#f85149;}.progress-container{margin-top:1rem;}.progress-bar{height:8px;background:#30363d;border-radius:4px;overflow:hidden;margin-top:0.5rem;}.progress-fill{height:100%;background:linear-gradient(90deg, #1f6feb, #2ea043);}</style></head><body><div class="container"><h1>Ralawise Sync</h1><div class="grid"><div class="card"><h2>System</h2><p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE':'Active'}</p><button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}</div><div class="card"><h2>Jobs</h2><p>Inventory Sync: ${isRunning.inventory?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/inventory','Run inventory sync now?')" class="btn btn-primary" ${isSystemLocked?'disabled':''}>Run Inventory</button>${isRunning.inventory?`<button onclick="apiPost('/api/sync/inventory/cancel')" class="btn btn-danger">Cancel</button>`:''}${inventoryProgressHTML}<hr style="margin:1rem 0;border-color:#30363d;"><p>Full Import: ${isRunning.fullImport?'Running':'Ready'}</p><button onclick="apiPost('/api/sync/full','This will create/discontinue products. Continue?')" class="btn" ${isSystemLocked?'disabled':''}>Run Full Import</button></div><div class="card"><h2>Reports</h2><p>Generate and send a report of the most common errors from the last 7 days.</p><button onclick="apiPost('/api/reports/errors')" class="btn" ${isSystemLocked?'disabled':''}>Generate Error Report</button></div></div><div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div></div><script>async function apiPost(url,confirmMsg){if(confirmMsg&&!confirm(confirmMsg))return;try{await fetch(url,{method:'POST'});setTimeout(()=>location.reload(),500)}catch(e){alert(e.message)}};setTimeout(()=>location.reload(),30000)</script></body></html>`;
    res.send(html);
});
