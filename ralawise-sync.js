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
  // --- NEW: Failsafe Configuration ---
  failsafe: {
    inventoryChangePercentage: parseInt(process.env.FAILSAFE_INVENTORY_CHANGE_PERCENTAGE || '10')
  }
};

// Validate required config
const requiredConfig = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID', 'FTP_HOST', 'FTP_USERNAME', 'FTP_PASSWORD'];
const missingConfig = requiredConfig.filter(key => !process.env[key]);
if (missingConfig.length > 0) {
  console.error(`Missing required environment variables: ${missingConfig.join(', ')}`);
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

// --- NEW: Failsafe State ---
let failsafe = {
  isTriggered: false,
  reason: '',
  timestamp: null,
  details: {}
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

async function notifyTelegram(message) {
  if (!config.telegram.botToken || !config.telegram.chatId) return;
  try {
    // Truncate message if too long for Telegram
    const maxLength = 4096;
    if (message.length > maxLength) {
        message = message.substring(0, maxLength - 10) + '...';
    }

    await axios.post(
      `https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`,
      {
        chat_id: config.telegram.chatId,
        text: `🏪 Ralawise Sync\n${message}`,
        parse_mode: 'HTML'
      },
      { timeout: 10000 }
    );
  } catch (error) {
    addLog(`Telegram notification failed: ${error.message}`, 'warning');
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// --- NEW: Pricing Logic Function ---
function applyRalawisePricing(originalPrice) {
  if (typeof originalPrice !== 'number' || originalPrice < 0) {
    return '0.00';
  }
  
  let finalPrice;
  if (originalPrice <= 6) {
    finalPrice = originalPrice * 2.1;
  } else if (originalPrice <= 11) {
    finalPrice = originalPrice * 1.9;
  } else {
    finalPrice = originalPrice * 1.75;
  }
  return finalPrice.toFixed(2);
}

// --- NEW: Failsafe Trigger Function ---
function triggerFailsafe(reason, details) {
    if (failsafe.isTriggered) return; // Don't trigger again if already active

    failsafe.isTriggered = true;
    failsafe.reason = reason;
    failsafe.timestamp = new Date().toISOString();
    failsafe.details = details;

    const errorMessage = `🚨 FAILSAFE ACTIVATED 🚨\n\n<b>Reason:</b> ${reason}`;
    addLog(errorMessage, 'error');
    
    let debugMessage = '';
    if (details.inventoryChange) {
        debugMessage += `\n\n<b>Debug Info:</b>\n`;
        debugMessage += `Threshold: <code>${details.inventoryChange.threshold}%</code>\n`;
        debugMessage += `Detected Change: <code>${details.inventoryChange.actualPercentage.toFixed(2)}%</code>\n`;
        debugMessage += `Products to Update: <code>${details.inventoryChange.updatesNeeded} / ${details.inventoryChange.totalProducts}</code>\n\n`;
        debugMessage += `<b>Sample of Changes (SKU: Old -> New):</b>\n<pre>`;
        details.inventoryChange.sample.forEach(item => {
            debugMessage += `\n${item.sku}: ${item.oldQty} -> ${item.newQty}`;
        });
        debugMessage += `</pre>`;
    }

    notifyTelegram(errorMessage + debugMessage);
}


// ============================================
// FTP FUNCTIONS
// ============================================

async function fetchInventoryFromFTP() {
  const client = new ftp.Client();
  client.ftp.verbose = false;
  
  try {
    addLog('Connecting to Ralawise FTP server...', 'info');
    await client.access(config.ftp);
    
    const csvPath = '/Stock/Stock_Update.csv';
    addLog(`Downloading ${csvPath}...`, 'info');
    
    const chunks = [];
    const destination = new Writable({
      write(chunk, encoding, callback) {
        chunks.push(chunk);
        callback();
      }
    });

    await client.downloadTo(destination, csvPath);

    const buffer = Buffer.concat(chunks);
    const stream = Readable.from(buffer);
    
    addLog(`FTP download successful, ${buffer.length} bytes received`, 'success');
    return stream;
  } catch (error) {
    addLog(`FTP error: ${error.message}`, 'error');
    throw error;
  } finally {
    client.close();
  }
}

async function parseInventoryCSV(stream) {
  return new Promise((resolve, reject) => {
    const inventory = new Map();
    let rowCount = 0;
    
    stream
      .pipe(csv({
        headers: ['SKU', 'Quantity'],
        skipLines: 1
      }))
      .on('data', (row) => {
        rowCount++;
        const sku = row.SKU?.trim();
        const qty = parseInt(row.Quantity) || 0;
        if (sku && sku !== 'SKU') {
          const cappedQty = Math.min(qty, config.ralawise.maxInventory);
          inventory.set(sku, cappedQty);
        }
      })
      .on('end', () => {
        addLog(`Parsed ${inventory.size} SKUs from ${rowCount} rows`, 'info');
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
  let pageCount = 0;
  const limit = 250;
  
  try {
    addLog('Fetching all products from Shopify...', 'info');
    
    let url = `/products.json?limit=${limit}&fields=id,handle,title,variants,tags,status`;

    while (url) {
        pageCount++;
        const response = await shopifyClient.get(url);
        const products = response.data.products;
        allProducts.push(...products);

        addLog(`Fetched page ${pageCount}: ${products.length} products (total: ${allProducts.length})`, 'info');

        const linkHeader = response.headers.link;
        if (linkHeader && linkHeader.includes('rel="next"')) {
            const matches = linkHeader.match(/<([^>]+)>/);
            url = matches ? matches[1].replace(config.shopify.baseUrl, '') : null;
        } else {
            url = null;
        }
        await delay(250);
    }
    
    addLog(`Completed fetching ${allProducts.length} products from Shopify`, 'success');
    return allProducts;
  } catch (error) {
    addLog(`Shopify fetch error: ${error.message}`, 'error');
    if (error.response) {
      addLog(`Response status: ${error.response.status}`, 'error');
      addLog(`Response data: ${JSON.stringify(error.response.data)}`, 'error');
    }
    throw error;
  }
}

async function updateInventoryBySKU(inventoryMap) {
  if (isRunning.inventory) {
    addLog('Inventory update already running, skipping...', 'warning');
    return { skipped: true };
  }
  
  isRunning.inventory = true;
  let updated = 0, errors = 0, tagged = 0, notFound = 0;
  
  try {
    addLog('=== STARTING INVENTORY UPDATE ===', 'info');
    
    const shopifyProducts = await getAllShopifyProducts();
    
    const skuToProduct = new Map();
    for (const product of shopifyProducts) {
      for (const variant of (product.variants || [])) {
        if (variant.sku) {
          skuToProduct.set(variant.sku.toUpperCase(), {
            product,
            variant
          });
        }
      }
    }
    
    addLog(`Found ${skuToProduct.size} SKUs in Shopify`, 'info');
    addLog(`Have ${inventoryMap.size} SKUs from Ralawise to process`, 'info');

    // --- NEW: Failsafe Check ---
    const updatesToPerform = [];
    for (const [sku, newQty] of inventoryMap) {
        const match = skuToProduct.get(sku.toUpperCase());
        if (match) {
            const currentQty = match.variant.inventory_quantity || 0;
            if (currentQty !== newQty) {
                updatesToPerform.push({ sku, oldQty: currentQty, newQty, match });
            }
        }
    }

    const totalProducts = skuToProduct.size;
    const updatesNeeded = updatesToPerform.length;
    const changePercentage = totalProducts > 0 ? (updatesNeeded / totalProducts) * 100 : 0;

    addLog(`Inventory change analysis: ${updatesNeeded} updates needed out of ${totalProducts} products (${changePercentage.toFixed(2)}%)`, 'info');

    if (changePercentage > config.failsafe.inventoryChangePercentage) {
        triggerFailsafe(
            `Inventory change of ${changePercentage.toFixed(2)}% exceeds limit of ${config.failsafe.inventoryChangePercentage}%`,
            {
                inventoryChange: {
                    threshold: config.failsafe.inventoryChangePercentage,
                    actualPercentage: changePercentage,
                    updatesNeeded: updatesNeeded,
                    totalProducts: totalProducts,
                    sample: updatesToPerform.slice(0, 10).map(u => ({ sku: u.sku, oldQty: u.oldQty, newQty: u.newQty }))
                }
            }
        );
        isRunning.inventory = false;
        return; // Stop the entire process
    }
    // --- End Failsafe Check ---

    for (const update of updatesToPerform) {
      const { sku, newQty, match } = update;
      const { product, variant } = match;
      
      try {
        if (!product.tags?.includes('Supplier:Ralawise')) {
          const currentTags = product.tags || '';
          const newTags = currentTags ? `${currentTags},Supplier:Ralawise` : 'Supplier:Ralawise';
          await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, tags: newTags } });
          tagged++;
          await delay(200);
        }
        
        if (!variant.inventory_management) {
          await shopifyClient.put(`/variants/${variant.id}.json`, { variant: { id: variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } });
          await delay(200);
        }
        
        await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id }).catch(() => {});
        await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id, available: newQty });
        
        addLog(`Updated ${product.title} (${sku}): ${update.oldQty} → ${newQty}`, 'success');
        updated++;
        
        await delay(250);
      } catch (error) {
        errors++;
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        addLog(`Failed to update ${sku}: ${errorMessage}`, 'error');
        if (error.response?.status === 429) {
          await delay(5000);
        }
      }
    }
    
    notFound = inventoryMap.size - updatesNeeded;
    lastRun.inventory = { updated, errors, tagged, timestamp: new Date().toISOString() };
    const message = `Inventory update complete:\n✅ ${updated} updated\n🏷️ ${tagged} tagged\n❌ ${errors} errors\n❓ ${notFound} SKUs not found in Shopify`;
    addLog(message, 'success');
    if (updated > 0 || tagged > 0) await notifyTelegram(message);
    
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
    const timestamp = Date.now();
    const url = `${config.ralawise.zipUrl}?t=${timestamp}`;
    addLog(`Downloading zip from: ${url}`, 'info');
    
    const response = await axios.get(url, { responseType: 'arraybuffer', timeout: 120000, maxContentLength: 100 * 1024 * 1024 });
    addLog(`Downloaded ${(response.data.length / 1024 / 1024).toFixed(2)} MB`, 'info');
    
    const tempDir = path.join(__dirname, 'temp', `ralawise_${timestamp}`);
    fs.mkdirSync(tempDir, { recursive: true });
    
    const zipPath = path.join(tempDir, 'data.zip');
    fs.writeFileSync(zipPath, response.data);
    
    addLog('Extracting zip file...', 'info');
    const zip = new AdmZip(zipPath);
    zip.extractAllTo(tempDir, true);
    
    const csvFiles = fs.readdirSync(tempDir).filter(file => file.endsWith('.csv')).map(file => path.join(tempDir, file));
    addLog(`Found ${csvFiles.length} CSV files in zip`, 'info');
    fs.unlinkSync(zipPath);
    
    return { tempDir, csvFiles };
  } catch (error) {
    addLog(`Zip download/extract error: ${error.message}`, 'error');
    throw error;
  }
}

async function parseShopifyCSV(filePath) {
  return new Promise((resolve, reject) => {
    const products = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // --- NEW: Apply pricing logic here ---
        const originalPrice = parseFloat(row['Variant Price']) || 0;
        const finalPrice = applyRalawisePricing(originalPrice);

        products.push({
          handle: row['Handle'],
          title: row['Title'],
          body_html: row['Body (HTML)'],
          vendor: row['Vendor'] || 'Ralawise',
          product_type: row['Type'],
          tags: row['Tags'],
          sku: row['Variant SKU'],
          price: finalPrice, // Use the calculated price
          original_price: originalPrice, // Keep original for reference
          compare_at_price: parseFloat(row['Variant Compare At Price']) || 0,
          inventory_qty: parseInt(row['Variant Inventory Qty']) || 0,
          // ... other fields
          option1_name: row['Option1 Name'],
          option1_value: row['Option1 Value'],
          option2_name: row['Option2 Name'],
          option2_value: row['Option2 Value'],
          option3_name: row['Option3 Name'],
          option3_value: row['Option3 Value'],
          weight: parseInt(row['Variant Grams']) || 0,
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
  if (isRunning.fullImport) {
    addLog('Full import already running, skipping...', 'warning');
    return { skipped: true };
  }
  
  isRunning.fullImport = true;
  let created = 0, discontinued = 0, errors = 0;
  
  try {
    addLog('=== STARTING FULL PRODUCT IMPORT ===', 'info');
    
    const allRalawiseProducts = [];
    for (const file of csvFiles) {
      const products = await parseShopifyCSV(file);
      allRalawiseProducts.push(...products);
    }
    
    addLog(`Total rows parsed: ${allRalawiseProducts.length}`, 'info');
    
    const productsByHandle = new Map();
    for (const row of allRalawiseProducts) {
      if (!row.handle) continue;
      if (!productsByHandle.has(row.handle)) {
        productsByHandle.set(row.handle, {
          handle: row.handle, title: row.title, body_html: row.body_html, vendor: row.vendor, product_type: row.product_type,
          tags: `${row.tags || ''},Supplier:Ralawise`.replace(/^,/, ''), status: row.status, images: [], variants: [], options: []
        });
      }
      const product = productsByHandle.get(row.handle);
      if (row.image_src && !product.images.some(img => img.src === row.image_src)) {
        product.images.push({ src: row.image_src, position: parseInt(row.image_position) || product.images.length + 1, alt: row.image_alt || row.title });
      }
      if (row.sku || row.price) {
        const variant = {
          sku: row.sku, price: row.price, compare_at_price: row.compare_at_price, 
          inventory_quantity: Math.min(row.inventory_qty, config.ralawise.maxInventory),
          inventory_management: 'shopify', inventory_policy: 'deny', weight: row.weight, requires_shipping: row.requires_shipping, barcode: row.barcode
        };
        if (row.option1_value) variant.option1 = row.option1_value; if (row.option2_value) variant.option2 = row.option2_value; if (row.option3_value) variant.option3 = row.option3_value;
        product.variants.push(variant);
        if (row.option1_name && !product.options.some(o => o.name === row.option1_name)) product.options.push({ name: row.option1_name, values: new Set() });
        if (row.option2_name && !product.options.some(o => o.name === row.option2_name)) product.options.push({ name: row.option2_name, values: new Set() });
        if (row.option3_name && !product.options.some(o => o.name === row.option3_name)) product.options.push({ name: row.option3_name, values: new Set() });
      }
    }
    for (const product of productsByHandle.values()) {
      for (const variant of product.variants) {
        if (variant.option1 && product.options[0]) product.options[0].values.add(variant.option1);
        if (variant.option2 && product.options[1]) product.options[1].values.add(variant.option2);
        if (variant.option3 && product.options[2]) product.options[2].values.add(variant.option3);
      }
      product.options = product.options.map(o => ({ name: o.name, values: Array.from(o.values) }));
    }
    addLog(`Consolidated to ${productsByHandle.size} unique products`, 'info');
    
    const shopifyProducts = await getAllShopifyProducts();
    const ralawiseProducts = shopifyProducts.filter(p => p.tags && p.tags.includes('Supplier:Ralawise'));
    addLog(`Found ${ralawiseProducts.length} existing Ralawise products in Shopify`, 'info');
    
    const existingHandles = new Set(ralawiseProducts.map(p => p.handle));
    const existingSKUs = new Set();
    ralawiseProducts.forEach(p => p.variants?.forEach(v => { if (v.sku) existingSKUs.add(v.sku.toUpperCase()); }));
    const newHandles = new Set(productsByHandle.keys());
    
    const toCreate = [];
    for (const [handle, product] of productsByHandle) {
      if (!existingHandles.has(handle)) {
        const skuExists = product.variants.some(v => v.sku && existingSKUs.has(v.sku.toUpperCase()));
        if (!skuExists) toCreate.push(product);
      }
    }
    addLog(`Found ${toCreate.length} new products to create`, 'info');
    
    const createLimit = 30;
    const productsToCreate = toCreate.slice(0, createLimit);
    if (toCreate.length > createLimit) addLog(`Creating first ${createLimit} of ${toCreate.length} products`, 'warning');
    
    for (const product of productsToCreate) {
      try {
        const shopifyProduct = { product: {
            title: product.title, body_html: product.body_html || '', handle: product.handle, vendor: product.vendor, product_type: product.product_type,
            tags: product.tags, status: 'active', images: product.images.slice(0, 10), variants: product.variants
        }};
        if (product.options.length > 0) shopifyProduct.product.options = product.options;
        const response = await shopifyClient.post('/products.json', shopifyProduct);
        const createdProduct = response.data.product;
        for (let i = 0; i < createdProduct.variants.length; i++) {
          const variant = createdProduct.variants[i];
          const originalVariant = product.variants[i];
          if (variant.inventory_item_id && originalVariant.inventory_quantity > 0) {
            try {
              await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id }).catch(() => {});
              await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id, available: originalVariant.inventory_quantity });
            } catch (invError) { addLog(`Inventory set failed for ${variant.sku}: ${invError.message}`, 'warning'); }
          }
        }
        // --- NEW: Add price change to log ---
        const sampleVariant = product.variants[0];
        const priceLog = sampleVariant ? `(Price: £${sampleVariant.original_price} -> £${sampleVariant.price})` : '';
        addLog(`✅ Created: ${product.title} ${priceLog}`, 'success');
        created++;
        await delay(1000);
      } catch (error) {
        errors++;
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        addLog(`❌ Failed to create ${product.title}: ${errorMessage}`, 'error');
        if (error.response?.status === 429) await delay(10000);
      }
    }
    
    const toDiscontinue = ralawiseProducts.filter(p => !newHandles.has(p.handle));
    addLog(`Found ${toDiscontinue.length} products to discontinue`, 'info');
    const discontinueLimit = 50;
    const productsToDiscontinue = toDiscontinue.slice(0, discontinueLimit);
    if (toDiscontinue.length > discontinueLimit) addLog(`Discontinuing first ${discontinueLimit} of ${toDiscontinue.length} products`, 'warning');
    
    for (const product of productsToDiscontinue) {
      try {
        await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } });
        for (const variant of (product.variants || [])) {
          if (variant.inventory_item_id) {
            try { await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id, available: 0 }); } catch (invError) {}
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
    
    lastRun.fullImport = { created, discontinued, errors, timestamp: new Date().toISOString() };
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
  // --- NEW: Failsafe Check ---
  if (failsafe.isTriggered) {
    addLog('Inventory sync skipped: Failsafe is active.', 'warning');
    return;
  }

  try {
    addLog('=== INVENTORY SYNC TRIGGERED ===', 'info');
    const stream = await fetchInventoryFromFTP();
    await updateInventoryBySKU(await parseInventoryCSV(stream));
  } catch (error) {
    addLog(`Inventory sync process failed: ${error.message}`, 'error');
    lastRun.inventory.errors++;
  }
}

async function syncFullCatalog() {
  // --- NEW: Failsafe Check ---
  if (failsafe.isTriggered) {
    addLog('Full catalog sync skipped: Failsafe is active.', 'warning');
    return;
  }
  
  let tempDir;
  try {
    addLog('=== FULL CATALOG SYNC TRIGGERED ===', 'info');
    const { tempDir: dir, csvFiles } = await downloadAndExtractZip();
    tempDir = dir;
    await processFullImport(csvFiles);
  } catch (error) {
    addLog(`Full catalog sync process failed: ${error.message}`, 'error');
    lastRun.fullImport.errors++;
  } finally {
    if (tempDir) {
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
        addLog('Cleaned up temporary files', 'info');
      } catch (cleanupError) {
        addLog(`Cleanup error: ${cleanupError.message}`, 'warning');
      }
    }
  }
}

// ============================================
// WEB INTERFACE
// ============================================

app.get('/', (req, res) => {
  const failsafeDetailsHTML = failsafe.details.inventoryChange ? `
    <div class="stat">
      <h3>Threshold</h3><p>${failsafe.details.inventoryChange.threshold}%</p>
    </div>
    <div class="stat">
      <h3>Detected Change</h3><p>${failsafe.details.inventoryChange.actualPercentage.toFixed(2)}%</p>
    </div>
    <div class="stat">
      <h3>Updates Blocked</h3><p>${failsafe.details.inventoryChange.updatesNeeded}</p>
    </div>
    <div class="stat" style="grid-column: 1 / -1;">
        <h3>Sample Changes (SKU: Old -> New)</h3>
        <pre style="text-align: left; background: #eee; padding: 10px; border-radius: 5px; color: #333; max-height: 150px; overflow-y: auto;">${
            failsafe.details.inventoryChange.sample.map(item => `${item.sku}: ${item.oldQty} -> ${item.newQty}`).join('\n')
        }</pre>
    </div>
  ` : '';

  const html = `
    <!DOCTYPE html><html><head><title>Ralawise Sync Dashboard</title><meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f4f7f6; min-height: 100vh; padding: 20px; }
      .container { max-width: 1200px; margin: 0 auto; }
      h1 { color: #333; text-align: center; margin-bottom: 30px; font-size: 2.5em; }
      .card { background: white; border-radius: 12px; padding: 25px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
      .card h2 { color: #333; margin-bottom: 20px; border-bottom: 2px solid #eee; padding-bottom: 10px; }
      .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
      .stat { background: #f9f9f9; border: 1px solid #eee; padding: 20px; border-radius: 8px; text-align: center; }
      .stat h3 { font-size: 14px; color: #666; margin-bottom: 10px; text-transform: uppercase; }
      .stat p { font-size: 32px; font-weight: bold; color: #333; }
      .stat small { display: block; margin-top: 10px; color: #888; font-size: 12px; }
      button { background: #3498db; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; margin-right: 10px; margin-bottom: 10px; transition: background-color 0.2s; font-weight: 600; }
      button:hover { background-color: #2980b9; }
      button:disabled { background: #ccc; cursor: not-allowed; }
      .logs { background: #1e1e1e; color: #fff; padding: 20px; border-radius: 8px; max-height: 400px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 13px; line-height: 1.5; }
      .log-entry { margin-bottom: 8px; padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.1); }
      .log-info { color: #58a6ff; } .log-success { color: #56d364; } .log-warning { color: #f0ad4e; } .log-error { color: #f85149; }
      .status-badge { display: inline-block; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: bold; text-transform: uppercase; }
      .status-idle { background: #2ecc71; color: white; } .status-running { background: #f1c40f; color: #333; animation: pulse 2s infinite; }
      @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
      .failsafe-banner { background: #e74c3c; color: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
      .failsafe-banner h2 { border-bottom-color: rgba(255,255,255,0.3); }
      .failsafe-banner button { background: #c0392b; }
    </style>
    </head><body><div class="container">
      <h1>🏪 Ralawise Sync Dashboard</h1>
      ${failsafe.isTriggered ? `
        <div class="failsafe-banner">
            <h2>🚨 Failsafe Active: All Syncs Paused</h2>
            <p style="margin-bottom: 20px;"><strong>Reason:</strong> ${failsafe.reason}</p>
            <div class="stats">${failsafeDetailsHTML}</div>
            <button onclick="clearFailsafe()" style="margin-top: 20px;">✅ Clear Failsafe & Resume</button>
        </div>
      ` : ''}
      <div class="card">
        <h2>System Status</h2><div class="stats">
          <div class="stat"><h3>Inventory Sync</h3><p class="status-badge ${isRunning.inventory ? 'status-running' : 'status-idle'}">${isRunning.inventory ? 'Running' : 'Idle'}</p></div>
          <div class="stat"><h3>Full Import</h3><p class="status-badge ${isRunning.fullImport ? 'status-running' : 'status-idle'}">${isRunning.fullImport ? 'Running' : 'Idle'}</p></div>
        </div>
      </div>
      <div class="card">
        <h2>Last Run Statistics</h2><div class="stats">
          <div class="stat"><h3>Inventory Updated</h3><p>${lastRun.inventory.updated}</p><small>${lastRun.inventory.timestamp ? new Date(lastRun.inventory.timestamp).toLocaleString() : 'Never'}</small></div>
          <div class="stat"><h3>Products Tagged</h3><p>${lastRun.inventory.tagged || 0}</p><small>With Supplier:Ralawise</small></div>
          <div class="stat"><h3>Products Created</h3><p>${lastRun.fullImport.created}</p><small>${lastRun.fullImport.timestamp ? new Date(lastRun.fullImport.timestamp).toLocaleString() : 'Never'}</small></div>
          <div class="stat"><h3>Discontinued</h3><p>${lastRun.fullImport.discontinued}</p><small>Marked as draft</small></div>
        </div>
      </div>
      <div class="card">
        <h2>Manual Controls</h2>
        <button onclick="runInventorySync()" id="invBtn" ${failsafe.isTriggered ? 'disabled' : ''}>🔄 Run Inventory Sync</button>
        <button onclick="runFullImport()" id="fullBtn" ${failsafe.isTriggered ? 'disabled' : ''}>📦 Run Full Import</button>
        <button onclick="clearLogs()">🗑️ Clear Logs</button>
        <button onclick="window.location.reload()">🔃 Refresh Dashboard</button>
      </div>
      <div class="card">
        <h2>Activity Log</h2>
        <div class="logs" id="logs">${logs.map(log => `<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div>
      </div>
    </div>
    <script>
      async function runInventorySync() { if (!confirm('Run inventory sync now?')) return; const btn = document.getElementById('invBtn'); btn.disabled = true; btn.textContent = '⏳ Starting...'; try { const res = await fetch('/api/sync/inventory', { method: 'POST' }); const data = await res.json(); if (data.error) { alert('Error: ' + data.error); btn.disabled = false; btn.textContent = '🔄 Run Inventory Sync Now'; } else { btn.textContent = '✅ Started!'; setTimeout(() => window.location.reload(), 2000); } } catch (e) { alert('Failed to start sync: ' + e.message); btn.disabled = false; btn.textContent = '🔄 Run Inventory Sync Now'; } }
      async function runFullImport() { if (!confirm('This will create new products and mark discontinued ones. Continue?')) return; const btn = document.getElementById('fullBtn'); btn.disabled = true; btn.textContent = '⏳ Starting...'; try { const res = await fetch('/api/sync/full', { method: 'POST' }); const data = await res.json(); if (data.error) { alert('Error: ' + data.error); btn.disabled = false; btn.textContent = '📦 Run Full Import Now'; } else { btn.textContent = '✅ Started!'; setTimeout(() => window.location.reload(), 2000); } } catch (e) { alert('Failed to start import: ' + e.message); btn.disabled = false; btn.textContent = '📦 Run Full Import Now'; } }
      async function clearLogs() { if (!confirm('Clear all logs?')) return; await fetch('/api/logs/clear', { method: 'POST' }); window.location.reload(); }
      async function clearFailsafe() { if (!confirm('Are you sure you want to clear the failsafe and resume all operations?')) return; await fetch('/api/failsafe/clear', { method: 'POST' }); window.location.reload(); }
      setTimeout(() => window.location.reload(), 30000);
    </script></body></html>
  `;
  res.send(html);
});


// ============================================
// API ENDPOINTS
// ============================================

app.post('/api/sync/inventory', async (req, res) => {
  if (failsafe.isTriggered) return res.status(423).json({ error: 'Failsafe is active. Syncs are paused.' });
  if (isRunning.inventory) return res.status(409).json({ error: 'Inventory sync already running' });
  syncInventory();
  res.json({ success: true, message: 'Inventory sync started' });
});

app.post('/api/sync/full', async (req, res) => {
  if (failsafe.isTriggered) return res.status(423).json({ error: 'Failsafe is active. Syncs are paused.' });
  if (isRunning.fullImport) return res.status(409).json({ error: 'Full import already running' });
  syncFullCatalog();
  res.json({ success: true, message: 'Full import started' });
});

app.post('/api/logs/clear', (req, res) => {
  logs = [];
  addLog('Logs cleared manually', 'info');
  res.json({ success: true });
});

// --- NEW: Failsafe Clear Endpoint ---
app.post('/api/failsafe/clear', (req, res) => {
    addLog('Failsafe manually cleared by user.', 'warning');
    failsafe = { isTriggered: false, reason: '', timestamp: null, details: {} };
    notifyTelegram('✅ Failsafe has been manually cleared. Operations are resuming.');
    res.json({ success: true, message: 'Failsafe cleared.' });
});

// ============================================
// SCHEDULED TASKS
// ============================================

cron.schedule('0 * * * *', () => {
  if (failsafe.isTriggered) { addLog('⏰ Skipped scheduled inventory sync: Failsafe is active.', 'warning'); return; }
  if (!isRunning.inventory) { addLog('⏰ Starting scheduled inventory sync...', 'info'); syncInventory(); } 
  else { addLog('⏰ Skipped scheduled inventory sync, previous job still running.', 'warning'); }
});

cron.schedule('0 13 */2 * *', () => {
  if (failsafe.isTriggered) { addLog('⏰ Skipped scheduled full import: Failsafe is active.', 'warning'); return; }
  if (!isRunning.fullImport) { addLog('⏰ Starting scheduled full catalog import...', 'info'); syncFullCatalog(); } 
  else { addLog('⏰ Skipped scheduled full import, previous job still running.', 'warning'); }
}, { timezone: 'Europe/London' });

// ============================================
// SERVER STARTUP & SHUTDOWN
// ============================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  addLog(`✅ Ralawise Sync Server started on port ${PORT}`, 'success');
  addLog(`🛡️ Failsafe Threshold: ${config.failsafe.inventoryChangePercentage}% inventory change`, 'info');
  
  setTimeout(() => { if (!isRunning.inventory) { addLog('🚀 Running initial inventory sync...', 'info'); syncInventory(); } }, 10000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (error) => { console.error('Uncaught Exception:', error); addLog(`FATAL Uncaught Exception: ${error.message}`, 'error'); });
process.on('unhandledRejection', (reason, promise) => { console.error('Unhandled Rejection at:', promise, 'reason:', reason); addLog(`FATAL Unhandled Rejection: ${reason}`, 'error'); });
