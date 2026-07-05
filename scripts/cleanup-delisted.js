'use strict';

// cleanup-delisted.js
// Checks stale tickers (no trades in last 90 days) against Yahoo Finance.
// Removes ALL trades for tickers Yahoo explicitly confirms are delisted.
// Conservative: only deletes on definitive "symbol may be delisted" response.
// Run weekly via GitHub Actions.

const https  = require('https');
const { createClient } = require('@libsql/client');

const TURSO_URL   = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;
if (!TURSO_URL) { console.error('TURSO_DATABASE_URL not set'); process.exit(1); }

const client = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN || undefined });

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11, 19)}] ${msg}\n`); }

async function dbQuery(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rows.map(row => Object.fromEntries(r.columns.map((c, i) => [c, row[i] ?? null])));
}
async function dbRun(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rowsAffected;
}

function get(url, ms = 10000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/2.0)' },
      timeout: ms,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// Rate limiter: max 5 requests per second to respect Yahoo limits
const _times = [];
async function throttle() {
  const now = Date.now();
  while (_times.length && _times[0] < now - 1000) _times.shift();
  if (_times.length >= 5) await new Promise(r => setTimeout(r, 1000 - (now - _times[0]) + 50));
  _times.push(Date.now());
}

// Returns true ONLY if Yahoo definitively says this ticker is delisted/gone.
// Returns false on timeouts, rate limits, or any ambiguous response.
async function isDefinitelyDelisted(ticker) {
  await throttle();
  try {
    const endTs   = Math.floor(Date.now() / 1000);
    const startTs = endTs - 5 * 365 * 86400; // 5 years back to catch older delistings
    const { status, body } = await get(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&period1=${startTs}&period2=${endTs}`
    );
    if (status !== 200) return false; // Any non-200 = uncertain, don't delete
    const data  = JSON.parse(body);
    const error = data?.chart?.error;
    // Only return true on the explicit "symbol may be delisted" message
    if (error?.code === 'Not Found' && error?.description?.toLowerCase().includes('may be delisted')) {
      return true;
    }
    // If we got valid result data back, it's definitely NOT delisted
    if (data?.chart?.result?.[0]?.timestamp?.length > 0) return false;
    // Any other case: uncertain — don't delete
    return false;
  } catch(_) {
    return false; // Timeout or network error = uncertain, don't delete
  }
}

async function main() {
  log('=== cleanup-delisted start ===');

  // Find tickers that haven't had any new trades in the last 90 days
  // These are candidates: either delisted or just inactive companies
  const stale = await dbQuery(`
    SELECT ticker, COUNT(*) AS trade_count, MAX(trade_date) AS latest_trade
    FROM trades
    WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
    GROUP BY ticker
    HAVING MAX(trade_date) < date('now', '-90 days')
    ORDER BY latest_trade ASC
  `);

  log(`Found ${stale.length} tickers with no trades in last 90 days — checking Yahoo Finance...`);

  let checked = 0, deleted = 0, kept = 0;
  const delistedTickers = [];

  for (const row of stale) {
    const ticker = row.ticker;
    const delisted = await isDefinitelyDelisted(ticker);
    checked++;

    if (delisted) {
      const removed = await dbRun('DELETE FROM trades WHERE ticker = ?', [ticker]);
      delistedTickers.push(ticker);
      deleted += removed;
      log(`  DELISTED: ${ticker} (${row.trade_count} trades removed, last trade ${row.latest_trade})`);
    } else {
      kept++;
    }

    if (checked % 50 === 0) {
      log(`  Progress: ${checked}/${stale.length} checked, ${delistedTickers.length} delisted so far`);
    }
  }

  log(`=== cleanup complete ===`);
  log(`Checked: ${checked} tickers`);
  log(`Delisted and removed: ${delistedTickers.length} tickers (${deleted} trade rows)`);
  log(`Kept (active or uncertain): ${kept} tickers`);
  if (delistedTickers.length > 0) {
    log(`Removed tickers: ${delistedTickers.join(', ')}`);
  }
}

main().catch(e => { log('FATAL: ' + e.message); process.exit(1); });
