const express = require('express');
const ftp = require('basic-ftp');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const AdmZip = require('adm-zip');
const cron = require('node-cron');
const { Readable, PassThrough } = require('stream');
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
    maxChangePercentage: parseInt(process.env.MAX_CHANGE_PERCENTAGE || '10'),
    maxProductsToCreate: parseInt(process.env.MAX_PRODUCTS_CREATE || '50'),
    maxProductsToDiscontinue: parseInt(process.env.MAX_PRODUCTS_DISCONTINUE || '50'),
    minExpectedProducts: parseInt(process.env.MIN_EXPECTED_PRODUCTS || '100')
  }
};

// Validate required config
const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
const missingConfig = requiredConfig.filter(key => !process.env[key]);
if (missingConfig.length > 0) {
  console.error(`Missing required environment variables: ${missingConfig.join(', ')}`);
  console.error('Please check your .env file or Railway environment variables');
  process.exit(1);
}

// ============================================
// SHOPIFY CLIENT
// ============================================

const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 30000
});

// ============================================
// STATE MANAGEMENT
// ============================================

let lastRun = {
  inventory: { updated: 0, errors: 0, tagged: 0, timestamp: null },
  fullImport: { created: 0, discontinued: 0, errors: 0, timestamp: null }
};

let logs = [];
let isRunning = {
  inventory: false,
  fullImport: false
};

// Failsafe state
let failsafeState = {
  triggered: false,
  reason: '',
  timestamp: null,
  details: {}
};

let lastKnownGoodState = {
  totalProducts: 0,
  inventoryLevels: new Map(),
  timestamp: null
};

// ============================================
// HELPER FUNCTIONS
// ============================================

function addLog(message, type = 'info') {
  const log = {
    timestamp: new Date().toISOString(),
    message,
    type
  };
  logs.unshift(log);
  if (logs.length > 500) logs = logs.slice(0, 500);
  console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`);
}

async function notifyTelegram(message, isUrgent = false) {
  if (!config.telegram.botToken || !config.telegram.chatId) return;
  
  // Add emoji based on urgency
  const prefix = isUrgent ? '🚨 URGENT: ' : '🏪 ';
  
  try {
    await axios.post(
      `https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`,
      {
        chat_id: config.telegram.chatId,
        text: `${prefix}Ralawise Sync\n${message}`,
        parse_mode: 'HTML'
      },
      { timeout: 5000 }
    );
  } catch (error) {
    addLog(`Telegram notification failed: ${error.message}`, 'warning');
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================
// FAILSAFE FUNCTIONS
// ============================================

async function triggerFailsafe(reason, details = {}) {
  if (failsafeState.triggered) return;
  
  failsafeState = {
    triggered: true,
    reason,
    timestamp: new Date().toISOString(),
    details
  };
  
  addLog(`⚠️ FAILSAFE TRIGGERED: ${reason}`, 'error');
  
  // Build detailed Telegram message
  let telegramMessage = `<b>⚠️ FAILSAFE ACTIVATED</b>\n\n`;
  telegramMessage += `<b>Reason:</b> ${reason}\n`;
  telegramMessage += `<b>Time:</b> ${new Date().toLocaleString()}\n\n`;
  
  if (details.inventoryChanges) {
    telegramMessage += `<b>Inventory Changes Detected:</b>\n`;
    telegramMessage += `• Products to update: ${details.inventoryChanges.toUpdate}\n`;
    telegramMessage += `• Total products: ${details.inventoryChanges.total}\n`;
    telegramMessage += `• Change percentage: ${details.inventoryChanges.percentage}%\n\n`;
  }
  
  if (details.productChanges) {
    telegramMessage += `<b>Product Changes Detected:</b>\n`;
    telegramMessage += `• To create: ${details.productChanges.toCreate}\n`;
    telegramMessage += `• To discontinue: ${details.productChanges.toDiscontinue}\n\n`;
  }
  
  if (details.samples) {
    telegramMessage += `<b>Sample of Changes:</b>\n`;
    details.samples.slice(0, 5).forEach(sample => {
      telegramMessage += `• ${sample}\n`;
    });
    telegramMessage += '\n';
  }
  
  telegramMessage += `<b>Action Required:</b>\n`;
  telegramMessage += `System has been paused. Please review the changes and manually clear the failsafe to continue.\n\n`;
  telegramMessage += `Dashboard: ${process.env.RAILWAY_STATIC_URL || 'http://localhost:3000'}`;
  
  // Send urgent Telegram notification
  await notifyTelegram(telegramMessage, true);
  
  // Stop all running jobs
  isRunning.inventory = false;
  isRunning.fullImport = false;
}

function clearFailsafe() {
  if (!failsafeState.triggered) return false;
  
  const previousReason = failsafeState.reason;
  failsafeState = {
    triggered: false,
    reason: '',
    timestamp: null,
    details: {}
  };
  
  addLog('Failsafe cleared manually', 'success');
  notifyTelegram(`✅ Failsafe cleared\nPrevious reason: ${previousReason}\nSystem resumed.`);
  
  return true;
}

function checkInventoryFailsafe(inventoryUpdates, totalProducts) {
  const updateCount = inventoryUpdates.length;
  const changePercentage = (updateCount / totalProducts) * 100;
  
  if (changePercentage > config.failsafe.maxChangePercentage) {
    const samples = inventoryUpdates.slice(0, 10).map(update => 
      `${update.title}: ${update.currentInventory} → ${update.newInventory}`
    );
    
    triggerFailsafe(
      `Inventory change threshold exceeded: ${changePercentage.toFixed(1)}% > ${config.failsafe.maxChangePercentage}%`,
      {
        inventoryChanges: {
          toUpdate: updateCount,
          total: totalProducts,
          percentage: changePercentage.toFixed(1)
        },
        samples
      }
    );
    return false;
  }
  
  return true;
}

function checkProductFailsafe(toCreate, toDiscontinue, existingCount) {
  const issues = [];
  const details = { productChanges: { toCreate: toCreate.length, toDiscontinue: toDiscontinue.length } };
  
  if (toCreate.length > config.failsafe.maxProductsToCreate) {
    issues.push(`Too many products to create: ${toCreate.length} > ${config.failsafe.maxProductsToCreate}`);
    details.samples = toCreate.slice(0, 5).map(p => `Create: ${p.title}`);
  }
  
  if (toDiscontinue.length > config.failsafe.maxProductsToDiscontinue) {
    issues.push(`Too many products to discontinue: ${toDiscontinue.length} > ${config.failsafe.maxProductsToDiscontinue}`);
    if (!details.samples) details.samples = [];
    details.samples.push(...toDiscontinue.slice(0, 5).map(p => `Discontinue: ${p.title}`));
  }
  
  const createPercentage = (toCreate.length / Math.max(existingCount, 1)) * 100;
  const discontinuePercentage = (toDiscontinue.length / Math.max(existingCount, 1)) * 100;
  
  if (createPercentage > config.failsafe.maxChangePercentage) {
    issues.push(`Product creation exceeds threshold: ${createPercentage.toFixed(1)}% > ${config.failsafe.maxChangePercentage}%`);
  }
  
  if (discontinuePercentage > config.failsafe.maxChangePercentage) {
    issues.push(`Product discontinuation exceeds threshold: ${discontinuePercentage.toFixed(1)}% > ${config.failsafe.maxChangePercentage}%`);
  }
  
  if (issues.length > 0) {
    triggerFailsafe(issues.join('; '), details);
    return false;
  }
  
  return true;
}

function saveGoodState(products) {
  lastKnownGoodState.totalProducts = products.length;
  lastKnownGoodState.inventoryLevels.clear();
  
  products.forEach(product => {
    product.variants?.forEach(variant => {
      if (variant.sku) {
        lastKnownGoodState.inventoryLevels.set(
          variant.sku.toUpperCase(),
          variant.inventory_quantity || 0
        );
      }
    });
  });
  
  lastKnownGoodState.timestamp = new Date().toISOString();
  addLog(`Saved good state: ${products.length} products, ${lastKnownGoodState.inventoryLevels.size} SKUs`, 'info');
}

// ============================================
// PRICING FUNCTIONS
// ============================================

function calculateRalawisePrice(basePrice) {
  // New pricing structure:
  // £0-6: multiply by 2.1
  // £7-11: multiply by 1.9
  // £11.01+: multiply by 1.75
  
  const price = parseFloat(basePrice) || 0;
  let finalPrice = 0;
  let multiplier = 1;
  
  if (price <= 6) {
    finalPrice = price * 2.1;
    multiplier = 2.1;
  } else if (price <= 11) {
    finalPrice = price * 1.9;
    multiplier = 1.9;
  } else {
    finalPrice = price * 1.75;
    multiplier = 1.75;
  }
  
  // Round to 2 decimal places
  finalPrice = Math.round(finalPrice * 100) / 100;
  
  // Ensure minimum price of £5
  if (finalPrice < 5) {
    finalPrice = 5.00;
  }
  
  // Calculate compare at price (20% markup from final price)
  const compareAtPrice = Math.round(finalPrice * 1.2 * 100) / 100;
  
  addLog(`Price calculation: £${price.toFixed(2)} × ${multiplier} = £${finalPrice.toFixed(2)} (compare at: £${compareAtPrice.toFixed(2)})`, 'debug');
  
  return {
    originalPrice: price,
    finalPrice,
    compareAtPrice,
    multiplier
  };
}

// ============================================
// FTP FUNCTIONS - FIXED VERSION
// ============================================

async function fetchInventoryFromFTP() {
  const client = new ftp.Client();
  client.ftp.verbose = false;
  
  try {
    addLog('Connecting to Ralawise FTP server...', 'info');
    await client.access(config.ftp);
    
    const csvPath = '/Stock/Stock_Update.csv';
    addLog(`Downloading ${csvPath}...`, 'info');
    
    // Create a PassThrough stream to collect the data
    const passThrough = new PassThrough();
    const chunks = [];
    
    // Collect chunks as they come
    passThrough.on('data', (chunk) => {
      chunks.push(chunk);
    });
    
    // Download to the PassThrough stream
    await client.downloadTo(passThrough, csvPath);
    
    // Convert collected chunks to a readable stream
    const buffer = Buffer.concat(chunks);
    const readableStream = Readable.from(buffer);
    
    addLog(`FTP download successful - received ${buffer.length} bytes`, 'success');
    client.close();
    
    return readableStream;
  } catch (error) {
    client.close();
    
    // Alternative method: download to temp file
    addLog('Trying alternative FTP download method...', 'info');
    return await fetchInventoryFromFTPAlternative();
  }
}

// Alternative FTP download method using temp file
async function fetchInventoryFromFTPAlternative() {
  const client = new ftp.Client();
  client.ftp.verbose = false;
  
  try {
    addLog('Connecting to FTP (alternative method)...', 'info');
    await client.access(config.ftp);
    
    // Create temp directory if it doesn't exist
    const tempDir = path.join(__dirname, 'temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    // Download to temp file
    const tempFile = path.join(tempDir, `inventory_${Date.now()}.csv`);
    const csvPath = '/Stock/Stock_Update.csv';
    
    addLog(`Downloading to temp file: ${tempFile}`, 'info');
    await client.downloadTo(tempFile, csvPath);
    
    client.close();
    
    // Read file and create stream
    const fileContent = fs.readFileSync(tempFile);
    const stream = Readable.from(fileContent);
    
    // Clean up temp file
    fs.unlinkSync(tempFile);
    
    addLog('FTP download successful (via temp file)', 'success');
    return stream;
  } catch (error) {
    client.close();
    addLog(`FTP error: ${error.message}`, 'error');
    
    // Send Telegram alert for FTP failure
    await notifyTelegram(`❌ FTP Connection Failed\n\nError: ${error.message}\n\nPlease check FTP credentials and server status.`, true);
    
    throw error;
  }
}

async function parseInventoryCSV(stream) {
  return new Promise((resolve, reject) => {
    const inventory = new Map();
    let rowCount = 0;
    let isFirstRow = true;
    
    stream
      .pipe(csv({
        separator: ',',
        headers: false // We'll handle headers manually
      }))
      .on('data', (row) => {
        rowCount++;
        
        // Skip the header row
        if (isFirstRow) {
          isFirstRow = false;
          // Check if this looks like a header
          if (row[0] === 'SKU' || row[0] === 'sku') {
            return;
          }
        }
        
        // Parse data rows
        const sku = row[0]?.trim();
        const qty = parseInt(row[1]) || 0;
        
        if (sku && sku !== '' && sku !== 'SKU') {
          // Apply max inventory cap
          const cappedQty = Math.min(qty, config.ralawise.maxInventory);
          inventory.set(sku, cappedQty);
        }
      })
      .on('end', () => {
        addLog(`Parsed ${inventory.size} SKUs from ${rowCount} rows`, 'info');
        
        // Check if we have minimum expected products
        if (inventory.size < config.failsafe.minExpectedProducts) {
          triggerFailsafe(
            `Too few products in FTP file: ${inventory.size} < ${config.failsafe.minExpectedProducts}`,
            {
              inventoryChanges: {
                found: inventory.size,
                expected: config.failsafe.minExpectedProducts
              }
            }
          );
        }
        
        resolve(inventory);
      })
      .on('error', (error) => {
        addLog(`CSV parse error: ${error.message}`, 'error');
        reject(error);
      });
  });
}

// ============================================
// SHOPIFY FUNCTIONS
// ============================================

async function getAllShopifyProducts() {
  let allProducts = [];
  let pageInfo = null;
  let pageCount = 0;
  const limit = 250;
  
  try {
    addLog('Fetching all products from Shopify...', 'info');
    
    // First request without page_info
    let url = `/products.json?limit=${limit}&fields=id,handle,title,variants,tags,status`;
    let response = await shopifyClient.get(url);
    let products = response.data.products;
    allProducts.push(...products);
    pageCount++;
    addLog(`Fetched page ${pageCount}: ${products.length} products (total: ${allProducts.length})`, 'info');
    
    // Check for next page in Link header
    let linkHeader = response.headers.link;
    
    while (linkHeader && linkHeader.includes('rel="next"')) {
      // Extract page_info from link header
      const matches = linkHeader.match(/<[^>]*[?&]page_info=([^>&]*)[^>]*>; rel="next"/);
      if (!matches) break;
      
      pageInfo = matches[1];
      url = `/products.json?limit=${limit}&fields=id,handle,title,variants,tags,status&page_info=${pageInfo}`;
      
      await delay(250); // Rate limiting
      
      response = await shopifyClient.get(url);
      products = response.data.products;
      allProducts.push(...products);
      pageCount++;
      
      addLog(`Fetched page ${pageCount}: ${products.length} products (total: ${allProducts.length})`, 'info');
      
      linkHeader = response.headers.link;
      
      // Safety check to prevent infinite loops
      if (products.length === 0) break;
    }
    
    addLog(`Completed fetching ${allProducts.length} products from Shopify`, 'success');
    return allProducts;
  } catch (error) {
    addLog(`Shopify fetch error: ${error.message}`, 'error');
    if (error.response) {
      addLog(`Response status: ${error.response.status}`, 'error');
      addLog(`Response data: ${JSON.stringify(error.response.data)}`, 'error');
    }
    
    // Send Telegram alert for Shopify API failure
    await notifyTelegram(`❌ Shopify API Error\n\nError: ${error.message}\n\nPlease check API credentials and rate limits.`, true);
    
    throw error;
  }
}

async function updateInventoryBySKU(inventoryMap) {
  if (failsafeState.triggered) {
    addLog('Inventory update blocked - failsafe is active', 'warning');
    return { blocked: true, reason: 'failsafe' };
  }
  
  if (isRunning.inventory) {
    addLog('Inventory update already running, skipping...', 'warning');
    return { skipped: true };
  }
  
  isRunning.inventory = true;
  let updated = 0, errors = 0, tagged = 0, notFound = 0;
  const inventoryUpdates = [];
  
  try {
    addLog('=== STARTING INVENTORY UPDATE ===', 'info');
    
    // Get all Shopify products
    const shopifyProducts = await getAllShopifyProducts();
    
    // Save as good state if no issues
    if (!failsafeState.triggered && shopifyProducts.length > config.failsafe.minExpectedProducts) {
      saveGoodState(shopifyProducts);
    }
    
    // Build SKU to product/variant map
    const skuToProduct = new Map();
    for (const product of shopifyProducts) {
      for (const variant of (product.variants || [])) {
        if (variant.sku) {
          // Store both uppercase and original case
          skuToProduct.set(variant.sku.toUpperCase(), {
            product,
            variant
          });
          // Also store with original case
          skuToProduct.set(variant.sku, {
            product,
            variant
          });
        }
      }
    }
    
    addLog(`Found ${skuToProduct.size} SKU entries in Shopify (including case variations)`, 'info');
    addLog(`Have ${inventoryMap.size} SKUs from Ralawise to process`, 'info');
    
    // Prepare inventory updates
    for (const [sku, newQty] of inventoryMap) {
      // Try exact match first, then uppercase
      let match = skuToProduct.get(sku) || skuToProduct.get(sku.toUpperCase());
      
      if (!match) {
        notFound++;
        continue;
      }
      
      const { product, variant } = match;
      const currentQty = variant.inventory_quantity || 0;
      
      if (currentQty !== newQty) {
        inventoryUpdates.push({
          product,
          variant,
          sku,
          title: product.title,
          currentInventory: currentQty,
          newInventory: newQty
        });
      }
    }
    
    // Check failsafe before proceeding
    const totalRalawiseProducts = skuToProduct.size / 2; // Divided by 2 because we store both cases
    if (!checkInventoryFailsafe(inventoryUpdates, totalRalawiseProducts)) {
      isRunning.inventory = false;
      return { blocked: true, reason: 'failsafe triggered' };
    }
    
    // Process updates
    for (const update of inventoryUpdates) {
      const { product, variant, sku, newInventory } = update;
      
      try {
        // Add Supplier:Ralawise tag if not present
        if (!product.tags?.includes('Supplier:Ralawise')) {
          const currentTags = product.tags || '';
          const newTags = currentTags ? `${currentTags},Supplier:Ralawise` : 'Supplier:Ralawise';
          
          await shopifyClient.put(`/products/${product.id}.json`, {
            product: { id: product.id, tags: newTags }
          });
          tagged++;
          addLog(`Tagged product: ${product.title}`, 'success');
          await delay(200);
        }
        
        // Ensure inventory tracking is enabled
        if (!variant.inventory_management) {
          await shopifyClient.put(`/variants/${variant.id}.json`, {
            variant: {
              id: variant.id,
              inventory_management: 'shopify',
              inventory_policy: 'deny'
            }
          });
          addLog(`Enabled inventory tracking for: ${product.title}`, 'info');
          await delay(200);
        }
        
        // Connect inventory item to location if needed
        try {
          await shopifyClient.post('/inventory_levels/connect.json', {
            location_id: parseInt(config.shopify.locationId),
            inventory_item_id: variant.inventory_item_id
          });
        } catch (connectError) {
          // Already connected, ignore error
        }
        
        // Set inventory level
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: parseInt(config.shopify.locationId),
          inventory_item_id: variant.inventory_item_id,
          available: newInventory
        });
        
        addLog(`Updated ${product.title} (${sku}): ${update.currentInventory} → ${newInventory}`, 'success');
        updated++;
        
        await delay(250); // Rate limiting
      } catch (error) {
        errors++;
        addLog(`Failed to update ${sku}: ${error.message}`, 'error');
        
        if (error.response?.status === 429) {
          addLog('Rate limit hit, waiting 5 seconds...', 'warning');
          await delay(5000);
        }
      }
    }
    
    lastRun.inventory = {
      updated,
      errors,
      tagged,
      timestamp: new Date().toISOString()
    };
    
    const message = `Inventory update complete:\n✅ ${updated} updated\n🏷️ ${tagged} tagged\n❌ ${errors} errors\n❓ ${notFound} SKUs not found`;
    addLog(message, 'success');
    
    // Send success notification only if significant changes
    if (updated > 0 || tagged > 0) {
      await notifyTelegram(message);
    }
    
    return { updated, errors, tagged, notFound };
  } catch (error) {
    addLog(`Inventory update failed: ${error.message}`, 'error');
    throw error;
  } finally {
    isRunning.inventory = false;
  }
}

// ============================================
// FULL IMPORT FUNCTIONS
// ============================================

async function downloadAndExtractZip() {
  try {
    // Add timestamp to URL to bypass cache
    const timestamp = Date.now();
    const url = `${config.ralawise.zipUrl}?t=${timestamp}`;
    addLog(`Downloading zip from: ${url}`, 'info');
    
    const response = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 60000,
      maxContentLength: 100 * 1024 * 1024 // 100MB max
    });
    
    addLog(`Downloaded ${(response.data.length / 1024 / 1024).toFixed(2)} MB`, 'info');
    
    // Create temp directory
    const tempDir = path.join(__dirname, 'temp', `ralawise_${timestamp}`);
    fs.mkdirSync(tempDir, { recursive: true });
    
    // Save and extract zip
    const zipPath = path.join(tempDir, 'data.zip');
    fs.writeFileSync(zipPath, response.data);
    
    addLog('Extracting zip file...', 'info');
    const zip = new AdmZip(zipPath);
    zip.extractAllTo(tempDir, true);
    
    // Find all CSV files
    const csvFiles = fs.readdirSync(tempDir)
      .filter(file => file.endsWith('.csv'))
      .map(file => path.join(tempDir, file));
    
    addLog(`Found ${csvFiles.length} CSV files in zip`, 'info');
    
    // Delete the zip file to save space
    fs.unlinkSync(zipPath);
    
    return { tempDir, csvFiles };
  } catch (error) {
    addLog(`Zip download/extract error: ${error.message}`, 'error');
    
    // Send Telegram alert for download failure
    await notifyTelegram(`❌ Failed to download Ralawise catalog\n\nError: ${error.message}\n\nURL: ${config.ralawise.zipUrl}`, true);
    
    throw error;
  }
}

async function parseShopifyCSV(filePath) {
  return new Promise((resolve, reject) => {
    const products = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // Apply new pricing structure when parsing
        const basePrice = parseFloat(row['Variant Price']) || 0;
        const pricing = calculateRalawisePrice(basePrice);
        
        products.push({
          handle: row['Handle'],
          title: row['Title'],
          body_html: row['Body (HTML)'],
          vendor: row['Vendor'] || 'Ralawise',
          product_type: row['Type'],
          tags: row['Tags'],
          published: row['Published'],
          option1_name: row['Option1 Name'],
          option1_value: row['Option1 Value'],
          option2_name: row['Option2 Name'],
          option2_value: row['Option2 Value'],
          option3_name: row['Option3 Name'],
          option3_value: row['Option3 Value'],
          sku: row['Variant SKU'],
          weight: parseInt(row['Variant Grams']) || 0,
          inventory_tracker: row['Variant Inventory Tracker'],
          inventory_qty: parseInt(row['Variant Inventory Qty']) || 0,
          inventory_policy: row['Variant Inventory Policy'],
          fulfillment_service: row['Variant Fulfillment Service'],
          price: pricing.finalPrice,  // Use calculated price
          compare_at_price: pricing.compareAtPrice,  // Use calculated compare price
          original_price: pricing.originalPrice,  // Store original for reference
          requires_shipping: row['Variant Requires Shipping'] === 'TRUE',
          barcode: row['Variant Barcode'],
          image_src: row['Image Src'],
          image_position: row['Image Position'],
          image_alt: row['Image Alt Text'],
          status: row['Status'] || 'active'
        });
      })
      .on('end', () => resolve(products))
      .on('error', reject);
  });
}

async function processFullImport(csvFiles) {
  if (failsafeState.triggered) {
    addLog('Full import blocked - failsafe is active', 'warning');
    return { blocked: true, reason: 'failsafe' };
  }
  
  if (isRunning.fullImport) {
    addLog('Full import already running, skipping...', 'warning');
    return { skipped: true };
  }
  
  isRunning.fullImport = true;
  let created = 0, discontinued = 0, errors = 0;
  
  try {
    addLog('=== STARTING FULL PRODUCT IMPORT ===', 'info');
    
    // Parse all CSV files
    const allRalawiseProducts = [];
    for (const file of csvFiles) {
      const products = await parseShopifyCSV(file);
      allRalawiseProducts.push(...products);
      addLog(`Parsed ${products.length} rows from ${path.basename(file)}`, 'info');
    }
    
    addLog(`Total rows parsed: ${allRalawiseProducts.length}`, 'info');
    
    // Group variants by handle
    const productsByHandle = new Map();
    
    for (const row of allRalawiseProducts) {
      if (!row.handle) continue;
      
      if (!productsByHandle.has(row.handle)) {
        productsByHandle.set(row.handle, {
          handle: row.handle,
          title: row.title,
          body_html: row.body_html,
          vendor: row.vendor,
          product_type: row.product_type,
          tags: `${row.tags || ''},Supplier:Ralawise`.replace(/^,/, ''),
          status: row.status,
          images: [],
          variants: [],
          options: []
        });
      }
      
      const product = productsByHandle.get(row.handle);
      
      // Add image if present and unique
      if (row.image_src && !product.images.some(img => img.src === row.image_src)) {
        product.images.push({
          src: row.image_src,
          position: parseInt(row.image_position) || product.images.length + 1,
          alt: row.image_alt || row.title
        });
      }
      
      // Add variant if it has SKU or price
      if (row.sku || row.price) {
        const variant = {
          sku: row.sku,
          price: row.price,  // Already calculated in parseShopifyCSV
          compare_at_price: row.compare_at_price,  // Already calculated
          inventory_quantity: Math.min(row.inventory_qty, config.ralawise.maxInventory),
          inventory_management: 'shopify',
          inventory_policy: 'deny',
          weight: row.weight,
          requires_shipping: row.requires_shipping,
          barcode: row.barcode
        };
        
        // Add option values
        if (row.option1_value) variant.option1 = row.option1_value;
        if (row.option2_value) variant.option2 = row.option2_value;
        if (row.option3_value) variant.option3 = row.option3_value;
        
        product.variants.push(variant);
        
        // Track option names
        if (row.option1_name && !product.options.some(o => o.name === row.option1_name)) {
          product.options.push({ name: row.option1_name, values: new Set() });
        }
        if (row.option2_name && !product.options.some(o => o.name === row.option2_name)) {
          product.options.push({ name: row.option2_name, values: new Set() });
        }
        if (row.option3_name && !product.options.some(o => o.name === row.option3_name)) {
          product.options.push({ name: row.option3_name, values: new Set() });
        }
      }
    }
    
    // Collect unique option values
    for (const product of productsByHandle.values()) {
      for (const variant of product.variants) {
        if (variant.option1 && product.options[0]) {
          product.options[0].values.add(variant.option1);
        }
        if (variant.option2 && product.options[1]) {
          product.options[1].values.add(variant.option2);
        }
        if (variant.option3 && product.options[2]) {
          product.options[2].values.add(variant.option3);
        }
      }
      
      // Convert Sets to Arrays
      product.options = product.options.map(o => ({
        name: o.name,
        values: Array.from(o.values)
      }));
    }
    
    addLog(`Consolidated to ${productsByHandle.size} unique products`, 'info');
    
    // Get existing Shopify products
    const shopifyProducts = await getAllShopifyProducts();
    const ralawiseProducts = shopifyProducts.filter(p => 
      p.tags && p.tags.includes('Supplier:Ralawise')
    );
    
    addLog(`Found ${ralawiseProducts.length} existing Ralawise products in Shopify`, 'info');
    
    // Build comparison sets
    const existingHandles = new Set(ralawiseProducts.map(p => p.handle));
    const existingSKUs = new Set();
    ralawiseProducts.forEach(p => {
      p.variants?.forEach(v => {
        if (v.sku) existingSKUs.add(v.sku.toUpperCase());
      });
    });
    
    const newHandles = new Set(productsByHandle.keys());
    
    // Find products to create
    const toCreate = [];
    for (const [handle, product] of productsByHandle) {
      if (!existingHandles.has(handle)) {
        // Check if any SKU already exists
        const skuExists = product.variants.some(v => 
          v.sku && existingSKUs.has(v.sku.toUpperCase())
        );
        
        if (!skuExists) {
          toCreate.push(product);
        }
      }
    }
    
    // Find products to discontinue
    const toDiscontinue = ralawiseProducts.filter(p => !newHandles.has(p.handle));
    
    addLog(`Found ${toCreate.length} new products to create`, 'info');
    addLog(`Found ${toDiscontinue.length} products to discontinue`, 'info');
    
    // Check failsafe before proceeding
    if (!checkProductFailsafe(toCreate, toDiscontinue, ralawiseProducts.length)) {
      isRunning.fullImport = false;
      return { blocked: true, reason: 'failsafe triggered' };
    }
    
    // Create new products (limit to avoid timeouts)
    const createLimit = Math.min(30, config.failsafe.maxProductsToCreate);
    const productsToCreate = toCreate.slice(0, createLimit);
    
    if (toCreate.length > createLimit) {
      addLog(`Creating first ${createLimit} of ${toCreate.length} products`, 'warning');
    }
    
    for (const product of productsToCreate) {
      try {
        // Prepare Shopify product
        const shopifyProduct = {
          product: {
            title: product.title,
            body_html: product.body_html || '',
            handle: product.handle,
            vendor: product.vendor,
            product_type: product.product_type,
            tags: product.tags,
            status: 'active',
            images: product.images.slice(0, 10), // Shopify limit
            variants: product.variants
          }
        };
        
        // Add options if present
        if (product.options.length > 0) {
          shopifyProduct.product.options = product.options;
        }
        
        // Create product
        const response = await shopifyClient.post('/products.json', shopifyProduct);
        const createdProduct = response.data.product;
        
        // Set inventory for each variant
        for (let i = 0; i < createdProduct.variants.length; i++) {
          const variant = createdProduct.variants[i];
          const originalVariant = product.variants[i];
          
          if (variant.inventory_item_id && originalVariant.inventory_quantity > 0) {
            try {
              // Connect to location
              await shopifyClient.post('/inventory_levels/connect.json', {
                location_id: parseInt(config.shopify.locationId),
                inventory_item_id: variant.inventory_item_id
              }).catch(() => {}); // Ignore if already connected
              
              // Set inventory
              await shopifyClient.post('/inventory_levels/set.json', {
                location_id: parseInt(config.shopify.locationId),
                inventory_item_id: variant.inventory_item_id,
                available: originalVariant.inventory_quantity
              });
            } catch (invError) {
              addLog(`Inventory set failed for ${variant.sku}: ${invError.message}`, 'warning');
            }
          }
        }
        
        addLog(`✅ Created: ${product.title} (${product.variants.length} variants)`, 'success');
        created++;
        
        await delay(1000); // Rate limiting
      } catch (error) {
        errors++;
        addLog(`❌ Failed to create ${product.title}: ${error.message}`, 'error');
        
        if (error.response?.status === 429) {
          addLog('Rate limit hit, waiting 10 seconds...', 'warning');
          await delay(10000);
        }
      }
    }
    
    // Discontinue products (limit to avoid issues)
    const discontinueLimit = Math.min(50, config.failsafe.maxProductsToDiscontinue);
    const productsToDiscontinue = toDiscontinue.slice(0, discontinueLimit);
    
    if (toDiscontinue.length > discontinueLimit) {
      addLog(`Discontinuing first ${discontinueLimit} of ${toDiscontinue.length} products`, 'warning');
    }
    
    for (const product of productsToDiscontinue) {
      try {
        // Set status to draft
        await shopifyClient.put(`/products/${product.id}.json`, {
          product: {
            id: product.id,
            status: 'draft'
          }
        });
        
        // Set all variant inventory to 0
        for (const variant of (product.variants || [])) {
          if (variant.inventory_item_id) {
            try {
              await shopifyClient.post('/inventory_levels/set.json', {
                location_id: parseInt(config.shopify.locationId),
                inventory_item_id: variant.inventory_item_id,
                available: 0
              });
            } catch (invError) {
              // Ignore inventory errors for discontinued products
            }
          }
        }
        
        addLog(`⏸️ Discontinued: ${product.title}`, 'info');
        discontinued++;
        
        await delay(500);
      } catch (error) {
        errors++;
        addLog(`Failed to discontinue ${product.title}: ${error.message}`, 'error');
      }
    }
    
    lastRun.fullImport = {
      created,
      discontinued,
      errors,
      timestamp: new Date().toISOString()
    };
    
    const message = `Full import complete:\n✅ ${created} created\n⏸️ ${discontinued} discontinued\n❌ ${errors} errors`;
    addLog(message, 'success');
    await notifyTelegram(message);
    
    return { created, discontinued, errors };
  } catch (error) {
    addLog(`Full import failed: ${error.message}`, 'error');
    throw error;
  } finally {
    isRunning.fullImport = false;
  }
}

// ============================================
// MAIN SYNC FUNCTIONS
// ============================================

async function syncInventory() {
  try {
    if (failsafeState.triggered) {
      addLog('Sync blocked by failsafe', 'warning');
      return { blocked: true };
    }
    
    addLog('=== INVENTORY SYNC STARTED ===', 'info');
    
    // Fetch CSV from FTP
    const stream = await fetchInventoryFromFTP();
    
    // Parse inventory data
    const inventoryMap = await parseInventoryCSV(stream);
    
    // Update Shopify
    const result = await updateInventoryBySKU(inventoryMap);
    
    if (!result.blocked) {
      addLog('=== INVENTORY SYNC COMPLETE ===', 'success');
    }
    
    return result;
  } catch (error) {
    addLog(`Inventory sync error: ${error.message}`, 'error');
    lastRun.inventory.errors++;
    
    // Clean up temp directory on error
    const tempDir = path.join(__dirname, 'temp');
    if (fs.existsSync(tempDir)) {
      try {
        const files = fs.readdirSync(tempDir);
        files.forEach(file => {
          const filePath = path.join(tempDir, file);
          if (fs.statSync(filePath).isFile()) {
            fs.unlinkSync(filePath);
          }
        });
      } catch (cleanupError) {
        // Ignore cleanup errors
      }
    }
    
    throw error;
  }
}

async function syncFullCatalog() {
  let tempDir;
  
  try {
    if (failsafeState.triggered) {
      addLog('Sync blocked by failsafe', 'warning');
      return { blocked: true };
    }
    
    addLog('=== FULL CATALOG SYNC STARTED ===', 'info');
    
    // Download and extract zip
    const { tempDir: dir, csvFiles } = await downloadAndExtractZip();
    tempDir = dir;
    
    // Process import
    const result = await processFullImport(csvFiles);
    
    // Cleanup temp files
    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
      addLog('Cleaned up temporary files', 'info');
    }
    
    if (!result.blocked) {
      addLog('=== FULL CATALOG SYNC COMPLETE ===', 'success');
    }
    
    return result;
  } catch (error) {
    addLog(`Full catalog sync error: ${error.message}`, 'error');
    lastRun.fullImport.errors++;
    
    // Cleanup on error
    if (tempDir) {
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch (cleanupError) {
        addLog(`Cleanup error: ${cleanupError.message}`, 'warning');
      }
    }
    
    throw error;
  }
}

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
  const html = `
    <!DOCTYPE html>
    <html>
    <head>
      <title>Ralawise Sync Dashboard</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          min-height: 100vh;
          padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { 
          color: white; 
          text-align: center;
          margin-bottom: 30px;
          font-size: 2.5em;
          text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        .alert {
          background: #ff4444;
          color: white;
          padding: 15px;
          border-radius: 8px;
          margin-bottom: 20px;
          font-weight: bold;
          animation: pulse 2s infinite;
        }
        .alert button {
          background: white;
          color: #ff4444;
          border: none;
          padding: 8px 16px;
          border-radius: 4px;
          margin-left: 20px;
          cursor: pointer;
        }
        .card { 
          background: white; 
          border-radius: 12px; 
          padding: 25px; 
          margin-bottom: 20px; 
          box-shadow: 0 10px 30px rgba(0,0,0,0.2); 
        }
        .card h2 { 
          color: #333; 
          margin-bottom: 20px;
          border-bottom: 2px solid #667eea;
          padding-bottom: 10px;
        }
        .stats { 
          display: grid; 
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
          gap: 20px; 
        }
        .stat { 
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          padding: 20px; 
          border-radius: 8px;
          text-align: center;
        }
        .stat.error {
          background: linear-gradient(135deg, #ff4444 0%, #cc0000 100%);
        }
        .stat h3 { 
          font-size: 14px; 
          opacity: 0.9;
          margin-bottom: 10px;
          text-transform: uppercase;
          letter-spacing: 1px;
        }
        .stat p { 
          font-size: 32px; 
          font-weight: bold; 
        }
        .stat small { 
          display: block;
          margin-top: 10px;
          opacity: 0.8;
          font-size: 12px;
        }
        button { 
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white; 
          border: none; 
          padding: 12px 24px; 
          border-radius: 8px; 
          cursor: pointer; 
          font-size: 16px; 
          margin-right: 10px;
          margin-bottom: 10px;
          transition: transform 0.2s, box-shadow 0.2s;
          font-weight: 600;
        }
        button:hover { 
          transform: translateY(-2px);
          box-shadow: 0 5px 15px rgba(0,0,0,0.3);
        }
        button:disabled { 
          background: #ccc; 
          cursor: not-allowed;
          transform: none;
        }
        .logs { 
          background: #1e1e1e; 
          color: #fff; 
          padding: 20px; 
          border-radius: 8px; 
          max-height: 400px; 
          overflow-y: auto; 
          font-family: 'Courier New', monospace; 
          font-size: 13px;
          line-height: 1.5;
        }
        .log-entry { 
          margin-bottom: 8px;
          padding: 4px 0;
          border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .log-info { color: #58a6ff; }
        .log-success { color: #56d364; }
        .log-warning { color: #f0ad4e; }
        .log-error { color: #f85149; }
        .log-debug { color: #999; }
        .running { 
          animation: pulse 2s infinite; 
          background: #28a745 !important;
        }
        @keyframes pulse { 
          0% { opacity: 1; } 
          50% { opacity: 0.6; } 
          100% { opacity: 1; } 
        }
        .failsafe-details {
          background: #fff3cd;
          border: 1px solid #ffc107;
          padding: 15px;
          border-radius: 8px;
          margin-top: 10px;
        }
        .failsafe-details h4 {
          margin: 0 0 10px 0;
          color: #856404;
        }
        .failsafe-details pre {
          background: #f8f9fa;
          padding: 10px;
          border-radius: 4px;
          overflow-x: auto;
        }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>🏪 Ralawise Sync Dashboard</h1>
        
        ${failsafeState.triggered ? `
        <div class="alert">
          ⚠️ FAILSAFE ACTIVE - All operations are paused
          <button onclick="clearFailsafe()">Clear Failsafe</button>
        </div>
        <div class="card failsafe-details">
          <h4>Failsafe Details</h4>
          <p><strong>Reason:</strong> ${failsafeState.reason}</p>
          <p><strong>Triggered:</strong> ${failsafeState.timestamp ? new Date(failsafeState.timestamp).toLocaleString() : 'Unknown'}</p>
          ${failsafeState.details ? `<pre>${JSON.stringify(failsafeState.details, null, 2)}</pre>` : ''}
        </div>
        ` : ''}
        
        <div class="card">
          <h2>System Status</h2>
          <div class="stats">
            <div class="stat ${isRunning.inventory ? 'running' : ''} ${failsafeState.triggered ? 'error' : ''}">
              <h3>Inventory Sync</h3>
              <p>${failsafeState.triggered ? '⛔' : (isRunning.inventory ? '🔄' : '✅')}</p>
              <small>${failsafeState.triggered ? 'Blocked' : (isRunning.inventory ? 'Running...' : 'Idle')}</small>
            </div>
            <div class="stat ${isRunning.fullImport ? 'running' : ''} ${failsafeState.triggered ? 'error' : ''}">
              <h3>Full Import</h3>
              <p>${failsafeState.triggered ? '⛔' : (isRunning.fullImport ? '🔄' : '✅')}</p>
              <small>${failsafeState.triggered ? 'Blocked' : (isRunning.fullImport ? 'Running...' : 'Idle')}</small>
            </div>
          </div>
        </div>
        
        <div class="card">
          <h2>Last Run Statistics</h2>
          <div class="stats">
            <div class="stat">
              <h3>Inventory Updated</h3>
              <p>${lastRun.inventory.updated}</p>
              <small>${lastRun.inventory.timestamp ? new Date(lastRun.inventory.timestamp).toLocaleString() : 'Never'}</small>
            </div>
            <div class="stat">
              <h3>Products Tagged</h3>
              <p>${lastRun.inventory.tagged || 0}</p>
              <small>With Supplier:Ralawise</small>
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
          <h2>Manual Controls</h2>
          <button onclick="runInventorySync()" id="invBtn" ${failsafeState.triggered ? 'disabled' : ''}>
            🔄 Run Inventory Sync Now
          </button>
          <button onclick="runFullImport()" id="fullBtn" ${failsafeState.triggered ? 'disabled' : ''}>
            📦 Run Full Import Now
          </button>
          <button onclick="clearLogs()">
            🗑️ Clear Logs
          </button>
          <button onclick="window.location.reload()">
            🔃 Refresh Dashboard
          </button>
          ${failsafeState.triggered ? '<button onclick="clearFailsafe()">⚠️ Clear Failsafe</button>' : ''}
          <p style="margin-top: 15px; color: #666; font-size: 14px;">
            <strong>Automatic Schedule:</strong><br>
            • Inventory sync: Every 60 minutes<br>
            • Full catalog import: Every 2 days at 13:00 UK time<br>
            <br>
            <strong>Failsafe Protection:</strong><br>
            • Max inventory change: ${config.failsafe.maxChangePercentage}%<br>
            • Max products to create: ${config.failsafe.maxProductsToCreate}<br>
            • Max products to discontinue: ${config.failsafe.maxProductsToDiscontinue}<br>
            <br>
            <strong>Pricing Structure:</strong><br>
            • £0-6: ×2.1<br>
            • £7-11: ×1.9<br>
            • £11.01+: ×1.75
          </p>
        </div>
        
        <div class="card">
          <h2>Activity Log</h2>
          <div class="logs" id="logs">
            ${logs.map(log => `
              <div class="log-entry log-${log.type}">
                [${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}
              </div>
            `).join('')}
            ${logs.length === 0 ? '<div class="log-entry">No logs yet. System just started.</div>' : ''}
          </div>
        </div>
      </div>
      
      <script>
        async function runInventorySync() {
          if (!confirm('Run inventory sync now?')) return;
          const btn = document.getElementById('invBtn');
          btn.disabled = true;
          btn.textContent = '⏳ Starting...';
          try {
            const res = await fetch('/api/sync/inventory', { method: 'POST' });
            const data = await res.json();
            if (data.error) {
              alert('Error: ' + data.error);
              btn.disabled = false;
              btn.textContent = '🔄 Run Inventory Sync Now';
            } else {
              btn.textContent = '✅ Started!';
              setTimeout(() => window.location.reload(), 2000);
            }
          } catch (e) {
            alert('Failed to start sync: ' + e.message);
            btn.disabled = false;
            btn.textContent = '🔄 Run Inventory Sync Now';
          }
        }
        
        async function runFullImport() {
          if (!confirm('This will create new products and mark discontinued ones as draft. Continue?')) return;
          const btn = document.getElementById('fullBtn');
          btn.disabled = true;
          btn.textContent = '⏳ Starting...';
          try {
            const res = await fetch('/api/sync/full', { method: 'POST' });
            const data = await res.json();
            if (data.error) {
              alert('Error: ' + data.error);
              btn.disabled = false;
              btn.textContent = '📦 Run Full Import Now';
            } else {
              btn.textContent = '✅ Started!';
              setTimeout(() => window.location.reload(), 2000);
            }
          } catch (e) {
            alert('Failed to start import: ' + e.message);
            btn.disabled = false;
            btn.textContent = '📦 Run Full Import Now';
          }
        }
        
        async function clearLogs() {
          if (!confirm('Clear all logs?')) return;
          await fetch('/api/logs/clear', { method: 'POST' });
          window.location.reload();
        }
        
        async function clearFailsafe() {
          if (!confirm('Clear failsafe and resume operations?')) return;
          try {
            const res = await fetch('/api/failsafe/clear', { method: 'POST' });
            const data = await res.json();
            if (data.success) {
              alert('Failsafe cleared!');
              window.location.reload();
            }
          } catch (e) {
            alert('Failed to clear failsafe: ' + e.message);
          }
        }
        
        // Auto-refresh every 30 seconds if a job is running
        const checkRefresh = () => {
          const isRunning = ${isRunning.inventory || isRunning.fullImport};
          if (isRunning) {
            setTimeout(() => window.location.reload(), 30000);
          } else {
            setTimeout(() => window.location.reload(), 60000);
          }
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

app.get('/api/status', (req, res) => {
  res.json({
    isRunning,
    lastRun,
    failsafeState,
    logs: logs.slice(0, 100),
    config: {
      shopifyDomain: config.shopify.domain,
      ftpHost: config.ftp.host,
      maxInventory: config.ralawise.maxInventory,
      failsafe: config.failsafe
    }
  });
});

app.post('/api/sync/inventory', async (req, res) => {
  if (failsafeState.triggered) {
    return res.json({ error: 'Failsafe is active. Clear it first to continue.' });
  }
  
  if (isRunning.inventory) {
    return res.json({ error: 'Inventory sync already running' });
  }
  
  // Run in background
  syncInventory().catch(error => {
    addLog(`Background inventory sync failed: ${error.message}`, 'error');
    lastRun.inventory.errors++;
  });
  
  res.json({ success: true, message: 'Inventory sync started' });
});

app.post('/api/sync/full', async (req, res) => {
  if (failsafeState.triggered) {
    return res.json({ error: 'Failsafe is active. Clear it first to continue.' });
  }
  
  if (isRunning.fullImport) {
    return res.json({ error: 'Full import already running' });
  }
  
  // Run in background
  syncFullCatalog().catch(error => {
    addLog(`Background full import failed: ${error.message}`, 'error');
    lastRun.fullImport.errors++;
  });
  
  res.json({ success: true, message: 'Full import started' });
});

app.post('/api/logs/clear', (req, res) => {
  logs = [];
  addLog('Logs cleared', 'info');
  res.json({ success: true });
});

app.post('/api/failsafe/clear', (req, res) => {
  const cleared = clearFailsafe();
  res.json({ success: cleared });
});

// ============================================
// SCHEDULED TASKS
// ============================================

// Inventory sync every 60 minutes
cron.schedule('0 * * * *', () => {
  if (!isRunning.inventory && !failsafeState.triggered) {
    addLog('⏰ Starting scheduled inventory sync...', 'info');
    syncInventory().catch(error => {
      addLog(`Scheduled inventory sync failed: ${error.message}`, 'error');
      lastRun.inventory.errors++;
    });
  } else if (failsafeState.triggered) {
    addLog('Scheduled inventory sync skipped - failsafe active', 'warning');
  }
});

// Full import every 2 days at 13:00 UK time
cron.schedule('0 13 */2 * *', () => {
  if (!isRunning.fullImport && !failsafeState.triggered) {
    addLog('⏰ Starting scheduled full catalog import...', 'info');
    syncFullCatalog().catch(error => {
      addLog(`Scheduled full import failed: ${error.message}`, 'error');
      lastRun.fullImport.errors++;
    });
  } else if (failsafeState.triggered) {
    addLog('Scheduled full import skipped - failsafe active', 'warning');
  }
}, {
  timezone: 'Europe/London'
});

// ============================================
// SERVER STARTUP
// ============================================

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  addLog(`✅ Ralawise Sync Server started on port ${PORT}`, 'success');
  addLog('📅 Inventory sync: Every 60 minutes', 'info');
  addLog('📅 Full catalog import: Every 2 days at 13:00 UK time', 'info');
  addLog(`🏪 Shopify domain: ${config.shopify.domain}`, 'info');
  addLog(`📁 FTP host: ${config.ftp.host}`, 'info');
  addLog(`📦 Max inventory cap: ${config.ralawise.maxInventory} units`, 'info');
  addLog(`🛡️ Failsafe: Max ${config.failsafe.maxChangePercentage}% inventory change`, 'info');
  addLog(`💰 Pricing: 0-6=×2.1, 7-11=×1.9, 11+=×1.75`, 'info');
  
  // Send startup notification
  notifyTelegram(`✅ System Started\n\nShopify: ${config.shopify.domain}\nFTP: ${config.ftp.host}\n\nFailsafe limits:\n• Max change: ${config.failsafe.maxChangePercentage}%\n• Max create: ${config.failsafe.maxProductsToCreate}\n• Max discontinue: ${config.failsafe.maxProductsToDiscontinue}`);
  
  // Run initial inventory sync after 10 seconds
  setTimeout(() => {
    if (!failsafeState.triggered) {
      addLog('🚀 Running initial inventory sync...', 'info');
      syncInventory().catch(error => {
        addLog(`Initial inventory sync failed: ${error.message}`, 'error');
        lastRun.inventory.errors++;
      });
    }
  }, 10000);
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================

process.on('SIGTERM', () => {
  addLog('Received SIGTERM, shutting down gracefully...', 'info');
  notifyTelegram('⚠️ System shutting down (SIGTERM)');
  process.exit(0);
});

process.on('SIGINT', () => {
  addLog('Received SIGINT, shutting down gracefully...', 'info');
  notifyTelegram('⚠️ System shutting down (SIGINT)');
  process.exit(0);
});
