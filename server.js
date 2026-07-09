'use strict';

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const http     = require('http');
const path     = require('path');
const crypto   = require('crypto');
const { query, queryOne, run, exec, batch } = require('./lib/db');

const app  = express();
const PORT = process.env.PORT || 3000;

const RESEND_KEY     = process.env.RESEND_KEY          || '';
// Sanitize FROM_EMAIL - strip wrapping quotes/whitespace and fall back if malformed.
// Resend requires "email@domain" or "Name <email@domain>".
const FROM_EMAIL = (() => {
  let v = (process.env.FROM_EMAIL || '').trim().replace(/^["']|["']$/g, '').trim();
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) || /^.+<[^\s@]+@[^\s@]+\.[^\s@]+>$/.test(v);
  return valid ? v : 'InsiderTape <noreply@insidertape.com>';
})();
const SITE_URL       = process.env.SITE_URL            || 'https://www.insidertape.com';
const STRIPE_SECRET         = process.env.STRIPE_SECRET_KEY    || '';
const STRIPE_PRICE_MONTHLY  = process.env.STRIPE_PRICE_ID       || '';
const STRIPE_PRICE_ANNUAL   = process.env.STRIPE_PRICE_ID_ANNUAL|| '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET  || '';
const ADMIN_EMAIL    = process.env.ADMIN_EMAIL         || '';
const TRIAL_DAYS     = parseInt(process.env.TRIAL_DAYS || '7');

// ─── LOGGING ──────────────────────────────────────────────────────────────────
const syncLog = [];
function slog(msg) {
  const line = `[${new Date().toISOString().slice(11, 19)}] ${msg}`;
  console.log(line);
  syncLog.push(line);
  if (syncLog.length > 300) syncLog.shift();
}

process.on('uncaughtException',  e => slog('UNCAUGHT: '  + e.message));
process.on('unhandledRejection', e => slog('UNHANDLED: ' + (e?.message || e)));

// ─── DB SCHEMA INIT ───────────────────────────────────────────────────────────
// Only called explicitly (e.g. via /api/admin/init-schema) - NOT on every cold start.
// The schema is bootstrapped once by the GitHub Actions init job.
async function initSchema() {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
      trade_date TEXT NOT NULL, filing_date TEXT,
      type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER,
      accession TEXT, footnote TEXT,
      UNIQUE(accession, insider, trade_date, type, qty)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_ticker           ON trades(ticker)`,
    `CREATE INDEX IF NOT EXISTS idx_trade_date       ON trades(trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_filing_date      ON trades(filing_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_insider          ON trades(insider)`,
    `CREATE INDEX IF NOT EXISTS idx_ticker_date_price ON trades(ticker, trade_date, price)`,
    `CREATE INDEX IF NOT EXISTS idx_insider_ticker_date ON trades(insider, ticker, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_ticker_type      ON trades(ticker, type, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_type_filing ON trades(type, filing_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_type_trade  ON trades(type, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_filing_type ON trades(filing_date DESC, type, value)`,
    `CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      is_admin INTEGER NOT NULL DEFAULT 0
    )`,
    `CREATE TABLE IF NOT EXISTS magic_tokens (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      token TEXT NOT NULL UNIQUE,
      expires_at TEXT NOT NULL,
      used INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE TABLE IF NOT EXISTS sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      token TEXT NOT NULL UNIQUE,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE TABLE IF NOT EXISTS subscriptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL UNIQUE,
      stripe_customer_id TEXT,
      stripe_subscription_id TEXT,
      stripe_checkout_session_id TEXT,
      plan TEXT DEFAULT 'monthly',
      status TEXT NOT NULL DEFAULT 'inactive',
      current_period_end TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_sessions_token    ON sessions(token)`,
    `CREATE INDEX IF NOT EXISTS idx_magic_tokens_token ON magic_tokens(token)`,
    `CREATE TABLE IF NOT EXISTS alert_prefs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL UNIQUE,
      enabled INTEGER NOT NULL DEFAULT 0,
      min_score INTEGER NOT NULL DEFAULT 70,
      min_value INTEGER NOT NULL DEFAULT 0,
      types TEXT NOT NULL DEFAULT 'conviction,cluster,first_buy,exit_warning',
      tickers TEXT NOT NULL DEFAULT '',
      sectors TEXT NOT NULL DEFAULT '',
      roles TEXT NOT NULL DEFAULT '',
      frequency TEXT NOT NULL DEFAULT 'immediate',
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`,
    `CREATE TABLE IF NOT EXISTS alert_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      ticker TEXT NOT NULL,
      signal_type TEXT NOT NULL,
      signal_key TEXT NOT NULL,
      sent_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(user_id, signal_key)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_alert_log_user ON alert_log(user_id)`,
    `CREATE INDEX IF NOT EXISTS idx_alert_log_key  ON alert_log(signal_key)`,
    `CREATE TABLE IF NOT EXISTS price_cache (
      symbol TEXT PRIMARY KEY,
      bars_json TEXT NOT NULL,
      fetched_at INTEGER NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS sync_log (
      quarter TEXT PRIMARY KEY,
      synced_at TEXT DEFAULT (datetime('now')),
      rows INTEGER
    )`,
    `CREATE TABLE IF NOT EXISTS computed_cache (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      computed_at INTEGER NOT NULL
    )`,
  ];

  for (const sql of stmts) {
    try { await exec(sql); } catch(e) { slog('Schema init warn: ' + e.message); }
  }

  // Cleanup stale data
  try {
    await run(`DELETE FROM trades WHERE ticker IN ('N/A','NA','NONE','NULL','--','-','.','0','FALSE','TRUE','UNKNOWN','TBD') OR ticker NOT GLOB '[A-Z]*' OR LENGTH(ticker) < 1 OR LENGTH(ticker) > 10`);
    await run(`DELETE FROM trades WHERE TRIM(type) NOT IN ('P','S','S-')`);
    await run(`DELETE FROM price_cache WHERE bars_json = '[]'`);
    await run(`DELETE FROM price_cache WHERE fetched_at < ?`, [Date.now() - 12 * 3600000]);
  } catch(e) { slog('Startup cleanup skipped: ' + e.message); }

  slog('Schema ready');
}

// Schema is pre-created by GitHub Actions init job - no startup init needed.

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    } else if (/\.(js|css|woff2?|ttf|eot)$/.test(filePath)) {
      res.setHeader('Cache-Control', 'public, max-age=3600');
    } else if (/\.(png|jpg|jpeg|gif|svg|ico|webp)$/.test(filePath)) {
      res.setHeader('Cache-Control', 'public, max-age=86400');
    }
  }
}));
app.use('/articles', express.static(path.join(__dirname, 'articles'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  }
}));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  if (req.path.startsWith('/api/')) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  }
  next();
});

// Per-request timeout
app.use((req, res, next) => {
  const limit = req.path === '/api/scoreboard' ? 55000
              : (req.path === '/api/drift' || req.path === '/api/proximity') ? 45000
              : req.path === '/api/price' ? 35000
              : 25000;
  const origJson   = res.json.bind(res);
  const origSend   = res.send.bind(res);
  const origStatus = res.status.bind(res);
  res.json   = b => { if (res.headersSent) return res; try { return origJson(b); } catch(_) { return res; } };
  res.send   = b => { if (res.headersSent) return res; try { return origSend(b); } catch(_) { return res; } };
  res.status = c => { if (res.headersSent) return res; try { return origStatus(c); } catch(_) { return res; } };
  res.setTimeout(limit, () => {
    if (!res.headersSent) try { origStatus(503); origJson({ error: 'Request timeout' }); } catch(_) {}
  });
  next();
});

// ─── BOT PROTECTION & RATE LIMITING ──────────────────────────────────────────
const _bannedIPs  = new Set();
const _strikeMap  = new Map();
const _bruteStore = new Map();
const _rlStore    = new Map();
const HEAVY_PATHS = ['/api/drift', '/api/proximity', '/api/scoreboard', '/api/firstbuys', '/api/insider-sentiment', '/api/insider-ratio'];
const BAD_UA_PATTERNS = [
  /python-requests/i, /scrapy/i, /curl\//i, /wget\//i, /httpx/i, /aiohttp/i,
  /go-http-client/i, /java\//i, /libwww/i, /mechanize/i,
  /zgrab/i, /masscan/i, /nmap/i, /nikto/i, /sqlmap/i,
  /semrush/i, /dotbot/i, /ahrefsbot/i, /mj12bot/i,
];
const HONEYPOT_PATHS = ['/admin', '/wp-admin', '/wp-login.php', '/.env', '/config.php', '/phpmyadmin', '/.git/config', '/api/dump', '/api/export', '/shell', '/cmd'];

function getIP(req) {
  return (req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress || 'unknown').replace('::ffff:', '');
}
function strike(ip, n = 1) {
  const s = _strikeMap.get(ip) || { strikes: 0, bannedUntil: 0 };
  s.strikes += n;
  if (s.strikes >= 15)      s.bannedUntil = Date.now() + 86400000;
  else if (s.strikes >= 10) s.bannedUntil = Date.now() + 1800000;
  else if (s.strikes >= 5)  s.bannedUntil = Date.now() + 300000;
  _strikeMap.set(ip, s);
}
function isBanned(ip) {
  if (_bannedIPs.has(ip)) return true;
  const s = _strikeMap.get(ip);
  return s && Date.now() < s.bannedUntil;
}
function rateLimiter(maxReq, windowMs) {
  return (req, res, next) => {
    if (!req.path.startsWith('/api/')) return next();
    const ip = getIP(req), key = ip + '|' + maxReq, now = Date.now();
    let slot = _rlStore.get(key);
    if (!slot || now > slot.resetAt) { slot = { count: 0, resetAt: now + windowMs }; _rlStore.set(key, slot); }
    slot.count++;
    if (slot.count > maxReq) {
      if (slot.count > maxReq * 3) strike(ip);
      res.setHeader('Retry-After', Math.ceil((slot.resetAt - now) / 1000));
      return res.status(429).json({ error: 'Too many requests' });
    }
    next();
  };
}
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _rlStore)    { if (now > v.resetAt)           _rlStore.delete(k); }
  for (const [k, v] of _strikeMap)  { if (now > v.bannedUntil && v.strikes < 5) _strikeMap.delete(k); }
  for (const [k, v] of _bruteStore) { if (now - v.firstAt > 3600000) _bruteStore.delete(k); }
}, 5 * 60 * 1000);

app.use((req, res, next) => {
  const ip = getIP(req), ua = req.headers['user-agent'] || '', p = req.path.toLowerCase();
  if (HONEYPOT_PATHS.some(h => p === h || p.startsWith(h + '/'))) { strike(ip, 10); return res.status(404).send('Not found'); }
  if (isBanned(ip)) return res.status(403).json({ error: 'Forbidden' });
  if (req.path.startsWith('/api/') && BAD_UA_PATTERNS.some(r => r.test(ua))) { strike(ip, 2); return res.status(403).json({ error: 'Forbidden' }); }
  if (req.path.startsWith('/api/') && !ua) { strike(ip); return res.status(403).json({ error: 'Forbidden' }); }
  next();
});
app.use((req, res, next) => {
  const isHeavy = HEAVY_PATHS.some(p => req.path.startsWith(p));
  return isHeavy ? rateLimiter(20, 60000)(req, res, next) : rateLimiter(90, 60000)(req, res, next);
});
function authBruteGuard(req, res, next) {
  const ip = getIP(req), now = Date.now();
  let slot = _bruteStore.get(ip) || { count: 0, firstAt: now };
  if (now - slot.firstAt > 3600000) slot = { count: 0, firstAt: now };
  slot.count++;
  _bruteStore.set(ip, slot);
  if (slot.count > 5) { strike(ip, 2); return res.status(429).json({ error: 'Too many attempts - try again later' }); }
  next();
}

// ─── HTTP HELPER ──────────────────────────────────────────────────────────────
function httpGet(url, ms = 30000, opts = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('http://') ? http : https;
    const headers = Object.assign({ 'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/2.0)' }, opts.headers || {});
    const req = mod.get(url, { headers, timeout: ms }, res => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const next = res.headers.location.startsWith('http') ? res.headers.location : new URL(res.headers.location, url).href;
        return httpGet(next, ms, opts).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks), headers: res.headers }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─── SEARCH INDEX (in-memory) ─────────────────────────────────────────────────
let _searchIndex = [];
async function buildSearchIndex() {
  try {
    const rows = await query(`
      SELECT ticker,
        MIN(CASE WHEN company NOT LIKE '%Option%' AND company NOT LIKE '%Strike%'
                  AND INSTR(company, CHAR(10)) = 0 AND LENGTH(company) > 2
             THEN company END) AS company,
        COUNT(*) AS n
      FROM trades
      WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker ORDER BY n DESC
    `);
    _searchIndex = rows.filter(r => {
      if (!r.company) return true;
      if (r.company.includes('\n') || r.company.includes('\r')) return false;
      if (/option|strike|expires|put|call/i.test(r.company) && r.company.length > 40) return false;
      return true;
    });
    slog(`Search index: ${_searchIndex.length} tickers`);
  } catch(e) { slog('buildSearchIndex error: ' + e.message); }
}
// Built lazily on first /api/search request.

// ─── PRICE CACHE ──────────────────────────────────────────────────────────────
const _priceCache = {};

function getPriceTTL() {
  const now = new Date();
  const janOff = new Date(now.getFullYear(), 0, 1).getTimezoneOffset();
  const julOff = new Date(now.getFullYear(), 6, 1).getTimezoneOffset();
  const isDST  = now.getTimezoneOffset() < Math.max(janOff, julOff);
  const etOff  = isDST ? 4 : 5;
  const etMin  = ((now.getUTCHours() - etOff + 24) % 24) * 60 + now.getUTCMinutes();
  const day    = now.getUTCDay();
  const isMarket = day >= 1 && day <= 5 && etMin >= 570 && etMin < 960;
  return isMarket ? 15 * 60000 : 12 * 3600000;
}

async function getPC(sym) {
  const ttl = getPriceTTL();
  const m = _priceCache[sym];
  if (m && Date.now() - m.fetchedAt <= ttl && m.bars.length > 0) return m.bars;
  try {
    const row = await queryOne('SELECT bars_json, fetched_at FROM price_cache WHERE symbol = ?', [sym]);
    if (row && Date.now() - row.fetched_at <= ttl) {
      const bars = JSON.parse(row.bars_json);
      if (bars.length > 0) { _priceCache[sym] = { bars, fetchedAt: row.fetched_at }; return bars; }
    }
  } catch(e) {}
  return null;
}

// Returns cached bars regardless of age (for stale-while-revalidate).
async function getPCAny(sym) {
  const m = _priceCache[sym];
  if (m && m.bars.length > 0) return m.bars;
  try {
    const row = await queryOne('SELECT bars_json, fetched_at FROM price_cache WHERE symbol = ?', [sym]);
    if (row) {
      const bars = JSON.parse(row.bars_json);
      if (bars.length > 0) { _priceCache[sym] = { bars, fetchedAt: row.fetched_at }; return bars; }
    }
  } catch(e) {}
  return null;
}

async function setPC(sym, bars) {
  const fetchedAt = Date.now();
  _priceCache[sym] = { bars, fetchedAt };
  try {
    await run('INSERT OR REPLACE INTO price_cache (symbol, bars_json, fetched_at) VALUES (?,?,?)',
      [sym, JSON.stringify(bars), fetchedAt]);
  } catch(e) {}
}

function parseYahoo(body) {
  try {
    const d = JSON.parse(body.toString());
    const r = d?.chart?.result?.[0];
    if (!r?.timestamp || !r?.indicators?.quote?.[0]) return null;
    const ts = r.timestamp, q = r.indicators.quote[0];
    const bars = ts.map((t, i) => ({
      time:   new Date(t * 1000).toISOString().slice(0, 10),
      open:   q.open?.[i]   || 0,
      high:   q.high?.[i]   || 0,
      low:    q.low?.[i]    || 0,
      close:  q.close?.[i]  || 0,
      volume: q.volume?.[i] || 0,
    })).filter(b => b.close > 0);
    return bars.length >= 2 ? bars : null;
  } catch(_) { return null; }
}

// Always fetch fresh bars from Yahoo and update the cache (ignores existing cache).
// Concurrent callers for the same symbol share one in-flight promise so nobody
// gets a null just because another request was already fetching it.
const _priceRefreshInFlight = new Map();
function refreshPriceBars(sym) {
  if (_priceRefreshInFlight.has(sym)) return _priceRefreshInFlight.get(sym);
  const endTs   = Math.floor(Date.now() / 1000);
  const startTs = endTs - 1830 * 86400; // ~5 years
  const p = (async () => {
    try {
      const bars = await Promise.any([
        httpGet(`https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&period1=${startTs}&period2=${endTs}`, 10000)
          .then(({ status, body }) => { if (status !== 200) throw new Error('404'); const b = parseYahoo(body); if (!b) throw new Error('no data'); return b; }),
        httpGet(`https://query2.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&period1=${startTs}&period2=${endTs}`, 10000)
          .then(({ status, body }) => { if (status !== 200) throw new Error('404'); const b = parseYahoo(body); if (!b) throw new Error('no data'); return b; }),
      ]);
      const sanitized = bars
        .sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0))
        .filter((b, i, arr) => i === 0 || b.time !== arr[i - 1].time);
      await setPC(sym, sanitized);
      return sanitized;
    } catch(_) {
      return null;
    } finally {
      _priceRefreshInFlight.delete(sym);
    }
  })();
  _priceRefreshInFlight.set(sym, p);
  return p;
}

async function fetchPriceBars(sym) {
  const cached = await getPC(sym);
  if (cached) return cached;
  return refreshPriceBars(sym);
}

// ─── SCREENER ─────────────────────────────────────────────────────────────────
const _screenerCache = new Map();
setInterval(() => { const n = Date.now(); for (const [k,v] of _screenerCache) if (n - v.t > 30000) _screenerCache.delete(k); }, 30000);

app.get('/api/screener', async (req, res) => {
  try {
    const cacheKey = (req.query.days || '30') + '|' + (req.query.limit || '');
    const cached = _screenerCache.get(cacheKey);
    const _reqDays = parseInt(req.query.days || '30');
    const cacheTTL = _reqDays >= 90 ? 120000 : 30000;
    if (cached && Date.now() - cached.t < cacheTTL) return res.json(cached.d);

    // Short windows (≤90d) served from the precomputed 90-day cache: read the blob
    // once, slice to the requested window in memory, reuse via _screenerCache.
    // Avoids scanning thousands of trade rows on every screener load at scale.
    if (_reqDays <= 90 && !req.query.limit) {
      try {
        const cachedRow = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'screener-90d'");
        if (cachedRow && Date.now() - cachedRow.computed_at < 6 * 3600000) {
          let rows = JSON.parse(cachedRow.value_json);
          if (_reqDays < 90) {
            const cutoff = new Date(Date.now() - _reqDays * 86400000).toISOString().slice(0, 10);
            rows = rows.filter(r => (r.trade || '') >= cutoff);
          }
          _screenerCache.set(cacheKey, { d: rows, t: Date.now() });
          return res.json(rows);
        }
      } catch(_) {}
    }

    const count = await queryOne('SELECT COUNT(*) AS n FROM trades');
    const n = count?.n || 0;
    if (n === 0) return res.json({ building: true, message: 'Loading SEC data - data is being ingested, check back in a few minutes.', trades: [] });

    const days  = Math.min(Math.max(parseInt(req.query.days || '30'), 1), 1095);
    const defaultLimit = days <= 7 ? 5000 : days <= 30 ? 10000 : days <= 90 ? 20000 : 50000;
    const limit = Math.min(parseInt(req.query.limit || String(defaultLimit)), 20000);

    let rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC LIMIT ?
    `, [days, limit]);

    if (!rows.length && n > 0) {
      const mx = await queryOne("SELECT MAX(trade_date) AS d FROM trades WHERE trade_date IS NOT NULL");
      if (mx?.d) {
        rows = await query(`
          SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
                 trade_date AS trade, MAX(filing_date) AS filing,
                 TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
                 MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
          FROM trades
          WHERE trade_date >= date(?, '-' || ? || ' days')
            AND TRIM(type) IN ('P','S','S-')
            AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
            AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
          GROUP BY ticker, insider, trade_date, type
          ORDER BY trade_date DESC LIMIT ?
        `, [mx.d, days, limit]);
      }
    }

    _screenerCache.set(cacheKey, { d: rows, t: Date.now() });
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/history', async (req, res) => {
  try {
    const days  = Math.min(Math.max(parseInt(req.query.days || '1825'), 1), 1825);
    const limit = Math.min(parseInt(req.query.limit || '10000'), 25000);
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND insider IS NOT NULL
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC LIMIT ?
    `, [days, limit]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ticker', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  try {
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades WHERE ticker = ? AND TRIM(type) IN ('P','S','S-')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC LIMIT 5000
    `, [sym]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/insider', async (req, res) => {
  const name  = (req.query.name || '').trim();
  const exact = req.query.exact === '1';
  if (!name || name.length < 2) return res.status(400).json({ error: 'name required (min 2 chars)' });
  try {
    const pattern = exact ? name : `%${name}%`;
    const limit   = exact ? 2000 : 500;
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades WHERE UPPER(insider) LIKE UPPER(?) AND TRIM(type) IN ('P','S','S-')
        AND COALESCE(value, 0) <= 5000000000
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC LIMIT ?
    `, [pattern, limit]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/search', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 1) return res.json({ tickers: [], insiders: [] });
  if (!_searchIndex.length) buildSearchIndex().catch(() => {});
  try {
    const upper = q.toUpperCase();
    const prefixMatches = [], companyMatches = [];
    for (const r of _searchIndex) {
      if (r.ticker.startsWith(upper)) prefixMatches.push(r);
      else if (r.company && r.company.toUpperCase().includes(upper)) companyMatches.push(r);
      if (prefixMatches.length >= 8 && companyMatches.length >= 4) break;
    }
    const seen = new Set(prefixMatches.map(r => r.ticker));
    const allTickers = [...prefixMatches, ...companyMatches.filter(r => !seen.has(r.ticker))].slice(0, 8);

    const insiderRows = q.length >= 2 ? await query(`
      SELECT insider, MAX(title) AS title, COUNT(*) AS n
      FROM trades WHERE UPPER(insider) LIKE ? AND insider IS NOT NULL
      GROUP BY insider ORDER BY n DESC LIMIT 6
    `, ['%' + upper + '%']) : [];

    res.json({
      tickers: allTickers.map(r => {
        let co = (r.company || r.ticker).split(/[\n\r]/)[0].trim();
        if (co.length > 60) co = co.slice(0, 60) + '…';
        return { ticker: r.ticker, company: co };
      }),
      insiders: insiderRows.map(r => ({ name: r.insider, title: r.title || '' })),
    });
    allTickers.slice(0, 3).forEach(r => fetchPriceBars(r.ticker).catch(() => {}));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── PRICE ────────────────────────────────────────────────────────────────────
app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  if (req.query.bust === '1') {
    delete _priceCache[sym];
    try { await run('DELETE FROM price_cache WHERE symbol = ?', [sym]); } catch(_) {}
    const fresh = await refreshPriceBars(sym);
    if (!res.headersSent) res.json(fresh || []);
    return;
  }
  // Fresh cache hit - instant
  const fresh = await getPC(sym);
  if (fresh) return res.json(fresh);
  // Stale-while-revalidate: return stale cache instantly, refresh in background
  const stale = await getPCAny(sym);
  if (stale) {
    refreshPriceBars(sym).catch(() => {}); // fire-and-forget
    return res.json(stale);
  }
  // Cold: no cache at all - must fetch now
  const bars = await refreshPriceBars(sym);
  if (res.headersSent) return;
  res.json(bars || []);
});

app.get('/api/prices-bulk', async (req, res) => {
  const raw  = (req.query.symbols || '').toUpperCase().trim();
  if (!raw) return res.status(400).json({ error: 'symbols required' });
  const syms = [...new Set(raw.split(',').map(s => s.trim()).filter(Boolean))].slice(0, 20);
  // Fetch uncached ones in parallel
  const cold = [];
  for (const s of syms) { if (!(await getPC(s))) cold.push(s); }
  if (cold.length) await Promise.allSettled(cold.map(s => fetchPriceBars(s)));
  const result = {};
  for (const s of syms) { const b = await getPC(s); if (b) result[s] = b; }
  res.json(result);
});

app.get('/api/price-highs', async (req, res) => {
  const syms = (req.query.tickers || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean).slice(0, 50);
  if (!syms.length) return res.json({});
  const result = {};
  for (const sym of syms) {
    const bars = await getPC(sym);
    if (!bars || !bars.length) continue;
    const last = bars[bars.length - 1];
    const bars52 = bars.slice(-252);
    const high52 = Math.max(...bars52.map(b => b.high));
    const low52  = Math.min(...bars52.map(b => b.low));
    const range52 = high52 - low52 || 1;
    result[sym] = { high52: +high52.toFixed(2), low52: +low52.toFixed(2), current: +last.close.toFixed(2), pctFromHigh: +((high52 - last.close) / range52).toFixed(3) };
  }
  res.json(result);
});

// ─── INSIDER SENTIMENT ────────────────────────────────────────────────────────
let _sentimentCache = null, _sentimentCacheTime = 0;
app.get('/api/insider-sentiment', async (req, res) => {
  try {
    if (_sentimentCache && Date.now() - _sentimentCacheTime < 3600000) return res.json(_sentimentCache);
    // Serve from precomputed cache (populated by GitHub Actions) - avoids the heavy
    // 120-month aggregation + live S&P fetch on every cold serverless instance.
    try {
      const cached = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'insider-sentiment'");
      if (cached && Date.now() - cached.computed_at < 6 * 3600000) {
        _sentimentCache = JSON.parse(cached.value_json);
        _sentimentCacheTime = cached.computed_at;
        return res.json(_sentimentCache);
      }
    } catch(_) {}
    const months = 120;
    const rows = await query(`
      SELECT strftime('%Y-%m', trade_date) AS month,
             strftime('%Y-%m', trade_date) || '-01' AS month_date,
             SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
             SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
             COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
             COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' months') AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(value, 0) >= 10000
      GROUP BY month HAVING buy_val + sell_val > 0 ORDER BY month ASC
    `, [months]);

    const monthly = rows.map(r => ({
      date: r.month_date, buyPct: r.buy_val / (r.buy_val + r.sell_val),
      buyVal: r.buy_val, sellVal: r.sell_val, buyCount: r.buy_count, sellCount: r.sell_count,
    }));
    const smoothed = monthly.map((m, i) => {
      const sl = monthly.slice(Math.max(0, i - 2), i + 1);
      return { ...m, smoothedBuyPct: sl.reduce((s, x) => s + x.buyPct, 0) / sl.length };
    });
    const vals = smoothed.map(m => m.smoothedBuyPct).sort((a, b) => a - b);
    const p  = n => vals[Math.floor(vals.length * n)] || 0;
    const thresholds = { p10: p(0.10), p25: p(0.25), median: p(0.50), p75: p(0.75), p90: p(0.90) };

    // Fetch S&P 500 monthly
    const endTs = Math.floor(Date.now() / 1000), startTs = endTs - months * 31 * 86400;
    let spxData = [];
    try {
      const { status, body } = await httpGet(`https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1mo&period1=${startTs}&period2=${endTs}`, 15000);
      if (status === 200) {
        const d = JSON.parse(body.toString()), r = d?.chart?.result?.[0];
        if (r?.timestamp) {
          const q = r.indicators.quote[0];
          spxData = r.timestamp.map((t, i) => ({ date: new Date(t * 1000).toISOString().slice(0, 7) + '-01', close: q.close?.[i] || null })).filter(x => x.close);
        }
      }
    } catch(_) {}

    _sentimentCache = { insider: smoothed, spx: spxData, thresholds };
    _sentimentCacheTime = Date.now();
    res.json(_sentimentCache);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/insider-ratio-history', async (req, res) => {
  try {
    const grain  = req.query.grain === 'monthly' ? 'monthly' : 'weekly';
    const weeks  = Math.min(parseInt(req.query.weeks  || '52'), 520);
    const months = Math.min(parseInt(req.query.months || '24'), 120);
    let rows;
    if (grain === 'weekly') {
      rows = await query(`
        SELECT strftime('%Y-%W', trade_date) AS period,
               date(trade_date, 'weekday 0', '-6 days') AS period_date,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
        FROM trades
        WHERE trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
          AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
        GROUP BY period ORDER BY period ASC
      `, [weeks * 7]);
    } else {
      rows = await query(`
        SELECT strftime('%Y-%m', trade_date) AS period,
               strftime('%Y-%m', trade_date) || '-01' AS period_date,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
        FROM trades
        WHERE trade_date >= date('now', '-' || ? || ' months') AND trade_date <= date('now')
          AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
        GROUP BY period ORDER BY period ASC
      `, [months]);
    }
    const data = rows.map(r => ({
      period: r.period_date || r.period, buys: r.buys, sells: r.sells,
      ratio:  r.sells > 0 ? +(r.buys / r.sells).toFixed(3) : null,
    })).filter(r => r.ratio !== null && r.buys + r.sells >= 3);

    const ltRows = await query(`
      SELECT strftime('%Y-%W', trade_date) AS period,
             COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
             COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
      FROM trades WHERE trade_date >= date('now', '-1095 days') AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
      GROUP BY period HAVING sells > 0 ORDER BY period ASC
    `);
    const ltRatios = ltRows.map(r => r.sells > 0 ? r.buys / r.sells : null).filter(r => r !== null && r < 5).sort((a, b) => a - b);
    const pct = p => { const idx = Math.floor(p / 100 * (ltRatios.length - 1)); return +ltRatios[idx].toFixed(3); };
    const stats = ltRatios.length >= 10 ? { p20: pct(20), p40: pct(40), p60: pct(60), p80: pct(80), median: pct(50), mean: +(ltRatios.reduce((s, v) => s + v, 0) / ltRatios.length).toFixed(3), n: ltRatios.length } : null;
    res.json({ data, stats });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/monitor-sentiment', async (req, res) => {
  try {
    const cached = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'monitor-sentiment'");
    if (cached && Date.now() - cached.computed_at < 4 * 3600000) return res.json(JSON.parse(cached.value_json));
  } catch(_) {}
  try {
    const now = new Date(), etOff = -5;
    const etNow = new Date(now.getTime() + etOff * 3600000);
    const dow = etNow.getUTCDay();
    const lastTrade = new Date(etNow);
    if (dow === 0) lastTrade.setUTCDate(etNow.getUTCDate() - 2);
    else if (dow === 6) lastTrade.setUTCDate(etNow.getUTCDate() - 1);
    const dbMax = await queryOne(`SELECT MAX(trade_date) AS d FROM trades WHERE trade_date <= ? AND TRIM(type) IN ('P','S','S-')`, [lastTrade.toISOString().slice(0, 10)]);
    const todayStr = dbMax?.d || lastTrade.toISOString().slice(0, 10);
    const tradeDow = lastTrade.getUTCDay();
    const weekStart = new Date(lastTrade);
    weekStart.setUTCDate(lastTrade.getUTCDate() - (tradeDow === 0 ? 6 : tradeDow - 1));
    const weekStr = weekStart.toISOString().slice(0, 10);
    const monthStart = new Date(lastTrade); monthStart.setUTCDate(lastTrade.getUTCDate() - 30);
    const monthStr = monthStart.toISOString().slice(0, 10);
    const qStartMonth = Math.floor(lastTrade.getUTCMonth() / 3) * 3;
    const quarterStr = `${lastTrade.getUTCFullYear()}-${String(qStartMonth + 1).padStart(2, '0')}-01`;

    async function windowStats(cutStr, endStr) {
      return queryOne(`
        SELECT COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
               COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
               COALESCE(SUM(CASE WHEN TRIM(type)='P' THEN value ELSE 0 END), 0) AS buy_val,
               COALESCE(SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN value ELSE 0 END), 0) AS sell_val,
               COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS unique_buyers,
               COUNT(DISTINCT CASE WHEN TRIM(type) IN ('S','S-') THEN insider END) AS unique_sellers
        FROM trades WHERE trade_date >= ? AND trade_date <= ?
          AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
      `, [cutStr, endStr]);
    }

    const result = {
      today:   { cutStr: todayStr,   ...(await windowStats(todayStr,   todayStr))   },
      week:    { cutStr: weekStr,    ...(await windowStats(weekStr,    todayStr))   },
      month:   { cutStr: monthStr,   ...(await windowStats(monthStr,   todayStr))   },
      quarter: { cutStr: quarterStr, ...(await windowStats(quarterStr, todayStr))   },
    };
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── FIRST BUYS ───────────────────────────────────────────────────────────────
const _firstBuysCache = new Map();
app.get('/api/firstbuys', async (req, res) => {
  try {
    const minGapDays   = parseInt(req.query.mingap   || '365');
    const lookbackDays = parseInt(req.query.lookback || '90');
    const limit        = parseInt(req.query.limit    || '100');

    // Standard tiles are served entirely from precomputed cache - the live CTE is
    // too heavy for a serverless request and was timing out.
    const cacheKey = (minGapDays === 730 && lookbackDays === 92) ? 'firstbuys-monitor'
                   : (minGapDays === 365 && lookbackDays === 90) ? 'firstbuys'
                   : null;
    if (cacheKey) {
      const cached = await queryOne('SELECT value_json FROM computed_cache WHERE key = ?', [cacheKey]);
      if (cached) return res.json(JSON.parse(cached.value_json));
      return res.json([]); // not computed yet - client falls back gracefully
    }
    const fbKey = `${minGapDays}|${lookbackDays}|${limit}`;
    const fbCached = _firstBuysCache.get(fbKey);
    if (fbCached && Date.now() - fbCached.t < 300000) return res.json(fbCached.d);

    const rows = await query(`
      WITH recent_buys AS (
        SELECT DISTINCT insider, ticker FROM trades
        WHERE TRIM(type) = 'P' AND trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
          AND insider IS NOT NULL AND ticker IS NOT NULL
      ),
      latest AS (
        SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title,
               MAX(t.trade_date) AS latest_trade, MAX(t.filing_date) AS latest_filing,
               MAX(t.price) AS latest_price, MAX(t.qty) AS latest_qty,
               MAX(t.value) AS latest_value, MAX(t.owned) AS latest_owned
        FROM trades t JOIN recent_buys rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P' AND t.trade_date >= date('now', '-' || ? || ' days') AND t.trade_date <= date('now')
        GROUP BY t.insider, t.ticker
      ),
      prev AS (
        SELECT t.insider, t.ticker, MAX(t.trade_date) AS prev_trade, MAX(t.owned) AS prev_owned
        FROM trades t JOIN recent_buys rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P' AND t.trade_date < date('now', '-' || ? || ' days')
        GROUP BY t.insider, t.ticker
      )
      SELECT l.ticker, l.company, l.insider, l.title,
             l.latest_trade, l.latest_filing, l.latest_price, l.latest_qty, l.latest_value, l.latest_owned,
             p.prev_trade, p.prev_owned,
             CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
      FROM latest l JOIN prev p ON l.insider = p.insider AND l.ticker = p.ticker
      WHERE CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= ?
      ORDER BY gap_days DESC LIMIT ?
    `, [lookbackDays, lookbackDays, lookbackDays, minGapDays, limit]);

    _firstBuysCache.set(fbKey, { d: rows, t: Date.now() });
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── RANKER ───────────────────────────────────────────────────────────────────
app.get('/api/ranker', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days || '30'), 90);
    const rows = await query(`
      SELECT ticker, MAX(company) AS company,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyer_count,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS total_buy_val,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END) AS latest_buy_date,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS total_sell_val,
        MAX(CASE WHEN TRIM(type)='P' AND (UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR UPPER(title) LIKE '%PRESIDENT%') THEN 1 ELSE 0 END) AS has_exec_buyer
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
      GROUP BY ticker HAVING buy_count > 0 ORDER BY total_buy_val DESC LIMIT 200
    `, [days]);
    res.json(rows.filter(r => r.buyer_count <= 8));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── STOCK LISTS ──────────────────────────────────────────────────────────────
let _stockListsCache = null, _stockListsCacheTime = 0;
const STOCK_LISTS_TTL = 4 * 3600000; // 4 hours

app.get('/api/stock-lists', async (req, res) => {
  // Serve from in-memory cache first
  if (_stockListsCache && Date.now() - _stockListsCacheTime < STOCK_LISTS_TTL) return res.json(_stockListsCache);

  // Fall back to computed_cache in Turso (pre-populated by GitHub Actions)
  try {
    const cached = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'stock-lists'");
    if (cached && Date.now() - cached.computed_at < STOCK_LISTS_TTL) {
      const payload = JSON.parse(cached.value_json);
      _stockListsCache = payload;
      _stockListsCacheTime = cached.computed_at;
      return res.json(payload);
    }
  } catch(e) { slog('stock-lists cache read error: ' + e.message); }
  try {
    const [mostActive, hotBuys, clusterBuys, freshBuys, heavySells] = await Promise.all([
      query(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS insiders,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells,
               SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
               SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
               MAX(trade_date) AS latest_date
             FROM trades WHERE trade_date >= date('now','-14 days') AND trade_date <= date('now')
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
             GROUP BY ticker HAVING (buys >= 1 OR sells >= 1) AND (buy_val >= 1000 OR sell_val >= 1000)
             ORDER BY (buy_val + sell_val) DESC LIMIT 24`),
      query(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyers,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val
             FROM trades WHERE trade_date >= date('now','-30 days') AND trade_date <= date('now')
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL') AND TRIM(type)='P'
             GROUP BY ticker HAVING buyers >= 1 AND buy_val >= 50000
             ORDER BY buy_val DESC LIMIT 16`),
      query(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS buyer_count,
               COUNT(*) AS trade_count, SUM(COALESCE(value,0)) AS total_val, MAX(trade_date) AS latest
             FROM trades WHERE trade_date >= date('now','-14 days') AND trade_date <= date('now')
               AND TRIM(type)='P' AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
             GROUP BY ticker HAVING buyer_count >= 3
             ORDER BY buyer_count DESC, total_val DESC LIMIT 12`),
      query(`SELECT ticker, MAX(company) AS company, MAX(insider) AS insider,
               MAX(value) AS val, MAX(trade_date) AS date
             FROM trades WHERE filing_date >= date('now','-2 days') AND TRIM(type)='P'
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
               AND COALESCE(value,0) >= 25000
             GROUP BY ticker ORDER BY val DESC LIMIT 16`),
      query(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS seller_count,
               SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val
             FROM trades WHERE trade_date >= date('now','-30 days') AND trade_date <= date('now')
               AND TRIM(type) IN ('S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
             GROUP BY ticker HAVING seller_count >= 2 AND sell_val >= 500000
             ORDER BY sell_val DESC LIMIT 12`),
    ]);

    const payload = { hotBuys, clusterBuys, freshBuys, heavySells, mostActive };
    if (mostActive.length || hotBuys.length) { _stockListsCache = payload; _stockListsCacheTime = Date.now(); }
    res.json(payload);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── SECTORS ──────────────────────────────────────────────────────────────────
const TICKER_SECTOR_MAP = {
  'AAPL':['Technology','Consumer Electronics'],'MSFT':['Technology','Software-Infrastructure'],
  'NVDA':['Technology','Semiconductors'],'GOOGL':['Communication Services','Internet Content & Information'],
  'AMZN':['Consumer Cyclical','Internet Retail'],'META':['Communication Services','Internet Content & Information'],
  'TSLA':['Consumer Cyclical','Auto Manufacturers'],'AVGO':['Technology','Semiconductors'],
  'JPM':['Financial Services','Banks-Diversified'],'V':['Financial Services','Credit Services'],
  'MA':['Financial Services','Credit Services'],'UNH':['Healthcare','Healthcare Plans'],
  'XOM':['Energy','Oil & Gas Integrated'],'LLY':['Healthcare','Drug Manufacturers-General'],
  'JNJ':['Healthcare','Drug Manufacturers-General'],'PG':['Consumer Defensive','Household & Personal Products'],
  'HD':['Consumer Cyclical','Home Improvement Retail'],'ABBV':['Healthcare','Drug Manufacturers-General'],
  'MRK':['Healthcare','Drug Manufacturers-General'],'KO':['Consumer Defensive','Beverages-Non-Alcoholic'],
  'PEP':['Consumer Defensive','Beverages-Non-Alcoholic'],'CVX':['Energy','Oil & Gas Integrated'],
  'BAC':['Financial Services','Banks-Diversified'],'WFC':['Financial Services','Banks-Diversified'],
  'COP':['Energy','Oil & Gas E&P'],'COST':['Consumer Defensive','Discount Stores'],
  'TMO':['Healthcare','Diagnostics & Research'],'CSCO':['Technology','Communication Equipment'],
  'ACN':['Technology','Information Technology Services'],'ABT':['Healthcare','Medical Devices'],
  'AMD':['Technology','Semiconductors'],'INTC':['Technology','Semiconductors'],
  'QCOM':['Technology','Semiconductors'],'TXN':['Technology','Semiconductors'],
  'NEE':['Utilities','Utilities-Regulated Electric'],'HON':['Industrials','Conglomerates'],
  'CAT':['Industrials','Farm & Heavy Construction Machinery'],'DE':['Industrials','Farm & Heavy Construction Machinery'],
  'BA':['Industrials','Aerospace & Defense'],'RTX':['Industrials','Aerospace & Defense'],
  'GE':['Industrials','Specialty Industrial Machinery'],'LMT':['Industrials','Aerospace & Defense'],
  'UPS':['Industrials','Integrated Freight & Logistics'],'GS':['Financial Services','Capital Markets'],
  'MS':['Financial Services','Capital Markets'],'BLK':['Financial Services','Asset Management'],
  'AMGN':['Healthcare','Drug Manufacturers-General'],'GILD':['Healthcare','Drug Manufacturers-General'],
  'BIIB':['Healthcare','Drug Manufacturers-General'],'REGN':['Healthcare','Drug Manufacturers-General'],
  'VRTX':['Healthcare','Drug Manufacturers-General'],'MDT':['Healthcare','Medical Devices'],
  'SYK':['Healthcare','Medical Devices'],'BSX':['Healthcare','Medical Devices'],
  'EW':['Healthcare','Medical Devices'],'ZBH':['Healthcare','Medical Devices'],
  'ISRG':['Healthcare','Medical Devices'],'HCA':['Healthcare','Medical Care Facilities'],
  'CI':['Healthcare','Healthcare Plans'],'HUM':['Healthcare','Healthcare Plans'],
  'MCD':['Consumer Cyclical','Restaurants'],'SBUX':['Consumer Cyclical','Restaurants'],
  'NKE':['Consumer Cyclical','Footwear & Accessories'],'LOW':['Consumer Cyclical','Home Improvement Retail'],
  'TGT':['Consumer Cyclical','Discount Stores'],'WMT':['Consumer Defensive','Discount Stores'],
  'BKNG':['Consumer Cyclical','Travel Services'],'MAR':['Consumer Cyclical','Lodging'],
  'F':['Consumer Cyclical','Auto Manufacturers'],'GM':['Consumer Cyclical','Auto Manufacturers'],
  'LIN':['Basic Materials','Specialty Chemicals'],'SHW':['Basic Materials','Specialty Chemicals'],
  'FCX':['Basic Materials','Copper'],'NEM':['Basic Materials','Gold'],
  'HAL':['Energy','Oil & Gas Equipment & Services'],'SLB':['Energy','Oil & Gas Equipment & Services'],
  'DVN':['Energy','Oil & Gas E&P'],'EOG':['Energy','Oil & Gas E&P'],
  'OXY':['Energy','Oil & Gas E&P'],'MPC':['Energy','Oil & Gas Refining & Marketing'],
  'AMT':['Real Estate','REIT-Specialty'],'PLD':['Real Estate','REIT-Industrial'],
  'EQIX':['Real Estate','REIT-Specialty'],'SPG':['Real Estate','REIT-Retail'],
  'CCI':['Real Estate','REIT-Specialty'],'WY':['Real Estate','REIT-Specialty'],
  'SNOW':['Technology','Software-Application'],'CRM':['Technology','Software-Application'],
  'ADBE':['Technology','Software-Application'],'NOW':['Technology','Software-Application'],
  'INTU':['Technology','Software-Application'],'AMAT':['Technology','Semiconductor Equipment & Materials'],
  'MU':['Technology','Semiconductors'],'PANW':['Technology','Software-Infrastructure'],
  'CRWD':['Technology','Software-Infrastructure'],'ZS':['Technology','Software-Infrastructure'],
  'NFLX':['Communication Services','Entertainment'],'DIS':['Communication Services','Entertainment'],
  'CMCSA':['Communication Services','Telecom Services'],'T':['Communication Services','Telecom Services'],
  'VZ':['Communication Services','Telecom Services'],'TMUS':['Communication Services','Telecom Services'],
  'PLTR':['Technology','Software-Application'],'COIN':['Financial Services','Capital Markets'],
  'MSTR':['Financial Services','Capital Markets'],'HOOD':['Financial Services','Capital Markets'],
};
function getTickerSector(t) { return TICKER_SECTOR_MAP[t] || null; }

let _sectorsCache = null, _sectorsCacheTime = 0;
app.get('/api/sectors', async (req, res) => {
  try {
    if (_sectorsCache && Date.now() - _sectorsCacheTime < 120000) return res.json(_sectorsCache);
    const days = [7, 30, 90].includes(parseInt(req.query.days)) ? parseInt(req.query.days) : 30;
    const rows = await query(`
      SELECT ticker, MAX(company) AS company,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyer_count,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date END) AS latest_buy
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days') AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      GROUP BY ticker HAVING buy_count > 0 OR sell_count > 0
      ORDER BY (buy_val + sell_val) DESC LIMIT 500
    `, [days]);

    const sectorMap = {};
    for (const r of rows) {
      const info = getTickerSector(r.ticker);
      if (!info) continue;
      const [sector, industry] = info;
      if (!sectorMap[sector]) sectorMap[sector] = { sector, buy_val: 0, sell_val: 0, buy_count: 0, sell_count: 0, buyer_count: 0, ticker_count: 0, tickers: [], industries: {} };
      const s = sectorMap[sector];
      s.buy_val += r.buy_val || 0; s.sell_val += r.sell_val || 0;
      s.buy_count += r.buy_count || 0; s.sell_count += r.sell_count || 0;
      s.buyer_count += r.buyer_count || 0; s.ticker_count++;
      if (r.buy_val > 0) s.tickers.push({ ticker: r.ticker, company: r.company || r.ticker, buy_val: r.buy_val, buyer_count: r.buyer_count, latest_buy: r.latest_buy });
      if (industry) s.industries[industry] = (s.industries[industry] || 0) + (r.buy_val || 0);
    }
    const sectors = Object.values(sectorMap).filter(s => s.buy_val > 0 || s.sell_val > 0).map(s => ({
      ...s,
      tickers: s.tickers.sort((a, b) => b.buy_val - a.buy_val).slice(0, 10),
      top_industry: Object.entries(s.industries).sort((a, b) => b[1] - a[1])[0]?.[0] || '',
      sentiment: s.buy_val + s.sell_val > 0 ? Math.round(s.buy_val / (s.buy_val + s.sell_val) * 100) : 0,
    })).sort((a, b) => b.buy_val - a.buy_val);
    const result = { sectors, days, total_buy_val: sectors.reduce((s, x) => s + x.buy_val, 0) };
    _sectorsCache = result; _sectorsCacheTime = Date.now();
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── DRIFT (served from pre-computed cache) ───────────────────────────────────
app.get('/api/drift', async (req, res) => {
  try {
    const row = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'drift'");
    if (!row) return res.json({ computing: true, message: 'Drift analysis is being computed - check back in a few minutes.' });
    const age = (Date.now() - row.computed_at) / 3600000;
    if (age > 25) return res.json({ stale: true, message: 'Refreshing drift data…', data: JSON.parse(row.value_json) });
    res.json(JSON.parse(row.value_json));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── PROXIMITY (served from pre-computed cache) ───────────────────────────────
app.get('/api/proximity', async (req, res) => {
  try {
    const row = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'proximity'");
    if (!row) return res.json({ computing: true, message: 'Proximity analysis is being computed - check back shortly.' });
    res.json(JSON.parse(row.value_json));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── SCOREBOARD ───────────────────────────────────────────────────────────────
const _sbCandidatesCache = { data: null, t: 0 };

async function buildScoreboardCandidates() {
  const rows = await query(`
    SELECT insider AS name, MAX(title) AS title, COUNT(*) AS total_buys
    FROM trades WHERE insider IS NOT NULL AND TRIM(type)='P' AND price > 0
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND insider IN (
        SELECT DISTINCT insider FROM trades
        WHERE TRIM(type)='P' AND price > 0 AND COALESCE(value,0) >= 10000
          AND trade_date >= date('now','-90 days')
      )
    GROUP BY insider HAVING total_buys >= 4 ORDER BY MAX(value) DESC LIMIT 80
  `);
  return { candidates: rows };
}

app.get('/api/scoreboard', async (req, res) => {
  try {
    if (!_sbCandidatesCache.data || Date.now() - _sbCandidatesCache.t > 300000) {
      _sbCandidatesCache.data = await buildScoreboardCandidates();
      _sbCandidatesCache.t = Date.now();
    }
    res.json(_sbCandidatesCache.data);
  } catch(e) { res.status(500).json({ error: e.message, candidates: [] }); }
});

// Pre-scored leaderboard (Top Insider Scores + Best Timing) - served from cache
// so the client never has to score dozens of insiders live (which was timing out).
app.get('/api/insider-leaderboard', async (req, res) => {
  try {
    const cached = await queryOne("SELECT value_json FROM computed_cache WHERE key = 'insider-leaderboard'");
    if (cached) return res.json(JSON.parse(cached.value_json));
    return res.json({ accuracy: [], timing: [], computing: true });
  } catch(e) { res.status(500).json({ error: e.message, accuracy: [], timing: [] }); }
});

// ─── INSIDER SCORE ────────────────────────────────────────────────────────────
// Timing Alpha (0–100): how reliably a stock rises shortly after this insider buys.
// Blends three factors instead of the old single-metric formula that saturated at 100:
//   • Magnitude  - average 30-day return, with diminishing returns past ~15% so a
//                  couple of small-cap moonshots don't peg everyone at 100.
//   • Consistency- 30-day win rate (how often buys were up), centered at 50%.
//   • Durability - 90-day return, which penalizes "pop then reverse" patterns.
// Small samples are shrunk toward a neutral 45 so 4-trade records aren't over-trusted.
function computeTimingAlpha(avgRet30, avgRet90, win30Rate, tradeCount) {
  if (avgRet30 === null || avgRet30 === undefined) return null;
  const mag  = avgRet30 >= 0 ? 38 * (1 - Math.exp(-avgRet30 / 10)) : Math.max(-30, avgRet30 * 1.2);
  const cons = (win30Rate !== null && win30Rate !== undefined) ? (win30Rate - 50) * 0.5 : 0;
  const dur  = Math.max(-22, Math.min(14, (avgRet90 || 0) * 0.35));
  let raw = 45 + mag * 0.7 + cons * 0.5 + dur;
  raw = 45 + (raw - 45) * Math.min(1, (tradeCount || 0) / 12);
  return Math.round(Math.max(0, Math.min(100, raw)));
}

const _insiderScoreCache = new Map();
const INSIDER_SCORE_TTL = 30 * 60000;

app.get('/api/insider-score', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });
  const ck = name.toUpperCase();
  const cached = _insiderScoreCache.get(ck);
  if (cached && Date.now() - cached.t < INSIDER_SCORE_TTL) return res.json(cached.data);

  try {
    const rows = await query(`
      SELECT ticker, trade_date AS trade, TRIM(type) AS type,
             COALESCE(price,0) AS price, COALESCE(value,0) AS value
      FROM trades WHERE UPPER(insider) LIKE UPPER(?) AND TRIM(type)='P' AND price > 0
      ORDER BY trade_date DESC LIMIT 500
    `, [name]);

    if (!rows.length) { const p = { error: 'no trades', name }; _insiderScoreCache.set(ck, { data: p, t: Date.now() }); return res.json(p); }

    const tickers = [...new Set(rows.map(r => r.ticker))];
    await Promise.allSettled(tickers.map(sym => fetchPriceBars(sym)));

    const CAP = 100, cap = r => Math.max(-CAP, Math.min(CAP, r));
    const today = new Date().toISOString().slice(0, 10);

    const scored = (await Promise.all(rows.map(async t => {
      const bars = await getPC(t.ticker);
      if (!bars?.length) return null;
      const buyDate = t.trade.slice(0, 10);
      const buyPrice = t.price || bars.find(b => b.time >= buyDate)?.close || 0;
      if (!buyPrice) return null;
      const addDays = (ds, n) => { const d = new Date(ds + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
      if (today < addDays(buyDate, 90)) return null;
      const priceOn = ds => { for (let d = 0; d <= 5; d++) { const dt = new Date(ds + 'T12:00:00Z'); dt.setUTCDate(dt.getUTCDate() + d); const b = bars.find(x => x.time === dt.toISOString().slice(0, 10)); if (b != null) return b.close; } return null; };
      const p30 = priceOn(addDays(buyDate, 30)), p90 = priceOn(addDays(buyDate, 90));
      return { ticker: t.ticker, tradeDate: buyDate, ret30: p30 ? cap((p30 - buyPrice) / buyPrice * 100) : null, ret90: p90 ? cap((p90 - buyPrice) / buyPrice * 100) : null };
    }))).filter(Boolean);

    const completed = scored.filter(s => s.ret90 !== null);
    if (completed.length < 4) { const p = { error: 'insufficient data', completed: completed.length }; _insiderScoreCache.set(ck, { data: p, t: Date.now() }); return res.json(p); }

    const rets90 = completed.map(s => s.ret90), rets30 = completed.filter(s => s.ret30 !== null).map(s => s.ret30);
    const winRate = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
    const avgRet90 = +(rets90.reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
    const avgRet30 = rets30.length ? +(rets30.reduce((a, b) => a + b, 0) / rets30.length).toFixed(1) : null;
    const avgMag   = +(rets90.map(Math.abs).reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
    const median   = [...rets90].sort((a, b) => a - b)[Math.floor(rets90.length / 2)];
    const consist  = Math.round(Math.min(100, Math.max(0, (median / Math.max(avgMag, 1) + 1) * 50)));
    const timingAvg30 = rets30.length ? rets30.reduce((a, b) => a + b, 0) / rets30.length : 0;
    const timingBonus = Math.round(Math.min(20, Math.max(0, (timingAvg30 + 8) / 16 * 20)));
    const baseScore   = winRate * 0.40 + Math.min(35, Math.max(0, avgRet90 / 20 * 35)) + consist * 0.15 + Math.min(10, completed.length * 1.2);
    const accuracyScore = Math.round(Math.min(100, Math.max(0, baseScore * 0.80 + timingBonus)));
    const tier = accuracyScore >= 75 ? 'ELITE' : accuracyScore >= 55 ? 'STRONG' : accuracyScore >= 35 ? 'AVERAGE' : 'WEAK';
    const win30Rate = rets30.length ? Math.round(rets30.filter(r => r > 0).length / rets30.length * 100) : null;
    const timingAlpha = computeTimingAlpha(avgRet30, avgRet90, win30Rate, completed.length);

    const payload = { name, winRate, avgRet90, avgRet30, win30Rate, tradeCount: completed.length, accuracyScore, timingAlpha, tier, tickers: tickers.slice(0, 3).join(', ') };
    _insiderScoreCache.set(ck, { data: payload, t: Date.now() });
    res.json(payload);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── YAHOO CRUMB (for earnings dates) ────────────────────────────────────────
let _yahooCrumb = null, _yahooCookie = null, _yahooCrumbTs = 0, _yahooCrumbBackoff = 0;
const CRUMB_TTL = 12 * 3600000;

async function getYahooCrumb() {
  if (_yahooCrumb && Date.now() - _yahooCrumbTs < CRUMB_TTL) return _yahooCrumb;
  if (_yahooCrumbBackoff && Date.now() < _yahooCrumbBackoff) return null;
  try {
    const r1 = await httpGet('https://fc.yahoo.com', 8000);
    const raw = Array.isArray(r1.headers?.['set-cookie']) ? r1.headers['set-cookie'].join('; ') : (r1.headers?.['set-cookie'] || '');
    const cm = raw.match(/A3=[^;]+/);
    _yahooCookie = cm ? cm[0] : null;
    const opts = _yahooCookie ? { headers: { 'Cookie': _yahooCookie, 'User-Agent': 'Mozilla/5.0' } } : {};
    const r2 = await httpGet('https://query1.finance.yahoo.com/v1/test/getcrumb', 8000, opts);
    if (r2.status === 200) { _yahooCrumb = r2.body.toString().trim(); _yahooCrumbTs = Date.now(); return _yahooCrumb; }
    if (r2.status === 429) _yahooCrumbBackoff = Date.now() + 600000;
  } catch(e) { _yahooCrumbBackoff = Date.now() + 1800000; }
  return null;
}

// ─── AUTH & SUBSCRIPTION ──────────────────────────────────────────────────────
function generateToken(bytes = 32) { return crypto.randomBytes(bytes).toString('hex'); }

async function getSession(req) {
  const header = req.headers['authorization'] || '', cookie = req.headers['cookie'] || '';
  let token = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (!token) { const m = cookie.match(/(?:^|;\s*)it_session=([^;]+)/); if (m) token = decodeURIComponent(m[1]); }
  if (!token) return null;
  try {
    return await queryOne(`
      SELECT s.*, u.email, u.is_admin FROM sessions s
      JOIN users u ON u.id = s.user_id
      WHERE s.token = ? AND s.expires_at > datetime('now')
    `, [token]);
  } catch(_) { return null; }
}

async function getSubscription(userId) {
  try { return await queryOne('SELECT * FROM subscriptions WHERE user_id = ?', [userId]); } catch(_) { return null; }
}

async function isPremium(session) {
  if (!session) return false;
  if (session.is_admin) return true;
  const adminEmail = (ADMIN_EMAIL || '').trim().toLowerCase();
  const sessEmail  = (session.email || '').trim().toLowerCase();
  if (adminEmail && sessEmail && sessEmail === adminEmail) return true;
  const userId = session.user_id;
  if (!userId) return false;
  const sub = await getSubscription(userId);
  if (!sub) return false;
  // active = paid, trialing = Stripe-managed trial with CC on file
  if (sub.status === 'active' || sub.status === 'trialing') return true;
  // Grace period: current_period_end still in the future covers cancel-at-period-end
  if (sub.current_period_end && sub.current_period_end > new Date().toISOString()) return true;
  return false;
}

// Attach session to every request
app.use(async (req, res, next) => {
  req.session   = await getSession(req);
  req.isPremium = req.session ? await isPremium(req.session) : false;
  next();
});

app.get('/api/auth/me', async (req, res) => {
  const session = await getSession(req);
  if (!session) return res.json({ loggedIn: false, isPremium: false });
  const premium = await isPremium(session);
  const sub = premium ? await getSubscription(session.user_id) : null;
  res.json({
    loggedIn:  true,
    email:     session.email,
    isPremium: premium,
    isAdmin:   !!session.is_admin,
    isTrial:   sub?.status === 'trial',
    trialEnds: sub?.status === 'trial' ? sub.current_period_end : null,
  });
});

// Magic link - anyone can request one (new users get a free trial on verify)
app.post('/api/auth/request-link', authBruteGuard, express.json(), async (req, res) => {
  const email = (req.body.email || '').trim().toLowerCase();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'Valid email required' });
  try {
    // Create user row if first visit
    await run('INSERT OR IGNORE INTO users (email) VALUES (?)', [email]);
    const user = await queryOne('SELECT * FROM users WHERE LOWER(email) = ?', [email]);
    if (!user) return res.json({ ok: true });

    const token   = generateToken();
    const expires = new Date(Date.now() + 15 * 60000).toISOString();
    await run('INSERT INTO magic_tokens (email, token, expires_at) VALUES (?, ?, ?)', [email, token, expires]);

    const link = `${SITE_URL}/api/auth/verify?token=${token}`;

    if (RESEND_KEY) {
      const { Resend } = require('resend');
      const { data, error } = await new Resend(RESEND_KEY).emails.send({
        from: FROM_EMAIL, to: email,
        subject: 'Your InsiderTape sign-in link',
        text: `Sign in to InsiderTape\n\nClick the link below to sign in. It expires in 15 minutes.\n\n${link}\n\nIf you didn't request this, you can safely ignore this email.\n\nInsiderTape - insidertape.com`,
        html: `
          <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Roboto,Helvetica,Arial,sans-serif;background:#e3e6eb;padding:32px 16px;margin:0">
            <div style="max-width:480px;margin:0 auto;background:#ffffff;border:1px solid #d0d4db;border-radius:12px;overflow:hidden">
              <div style="padding:32px 36px 24px">
                <div style="font-size:22px;font-weight:800;letter-spacing:2px;color:#1a2030;margin-bottom:4px">INSIDER<span style="color:#2478cc">TAPE</span></div>
                <div style="font-size:11px;letter-spacing:1px;color:#6e7a8a;margin-bottom:28px">FOLLOW THE SMART MONEY</div>
                <div style="font-size:16px;font-weight:600;color:#1a2030;margin-bottom:8px">Sign in to InsiderTape</div>
                <div style="font-size:14px;color:#6e7a8a;line-height:1.6;margin-bottom:26px">Click the button below to sign in. This link expires in <strong style="color:#1a2030">15 minutes</strong>.</div>
                <a href="${link}" style="display:inline-block;background:#2478cc;color:#ffffff;font-weight:700;font-size:14px;letter-spacing:0.5px;padding:14px 34px;border-radius:8px;text-decoration:none">Sign in →</a>
                <div style="margin-top:28px;font-size:12px;color:#6e7a8a;line-height:1.6">Or paste this link into your browser:<br><a href="${link}" style="color:#2478cc;word-break:break-all">${link}</a></div>
              </div>
              <div style="padding:18px 36px;border-top:1px solid #eaecf0;background:#f5f7fa;font-size:11px;color:#8b95a4;line-height:1.6">
                If you didn't request this, you can safely ignore this email.<br>InsiderTape · SEC insider trade tracking · <a href="https://insidertape.com" style="color:#6e7a8a">insidertape.com</a>
              </div>
            </div>
          </div>`,
      });
      if (error) {
        slog('Resend send FAILED: ' + JSON.stringify(error));
        return res.status(500).json({ error: 'Email delivery failed: ' + (error.message || error.name || 'unknown') });
      }
      slog('Magic link sent to ' + email.slice(0, 3) + '*** (id: ' + (data?.id || '?') + ')');
    } else {
      slog('MAGIC LINK (no Resend key): ' + link);
    }
    res.json({ ok: true });
  } catch(e) { slog('request-link error: ' + e.message); res.status(500).json({ error: 'Failed to send link' }); }
});

app.get('/api/auth/verify', authBruteGuard, async (req, res) => {
  const { token } = req.query;
  if (!token) return res.redirect('/signup?error=missing_token');
  try {
    const mt = await queryOne('SELECT * FROM magic_tokens WHERE token = ? AND used = 0 AND expires_at > datetime(\'now\')', [token]);
    if (!mt) return res.redirect('/signup?error=invalid_token');

    await run('UPDATE magic_tokens SET used = 1 WHERE id = ?', [mt.id]);
    await run('INSERT OR IGNORE INTO users (email) VALUES (?)', [mt.email]);
    const user = await queryOne('SELECT * FROM users WHERE email = ?', [mt.email]);

    // No internal trial - Stripe manages the 7-day trial with CC on file

    const sessionToken = generateToken();
    const expires = new Date(Date.now() + 30 * 86400000).toISOString();
    await run('INSERT INTO sessions (user_id, token, expires_at) VALUES (?, ?, ?)', [user.id, sessionToken, expires]);
    res.setHeader('Set-Cookie', `it_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${30 * 86400}; Secure`);

    const sub = await getSubscription(user.id);
    const adminEmail = (ADMIN_EMAIL || '').trim().toLowerCase();
    const alreadyPremium = user.is_admin
      || (adminEmail && (user.email || '').toLowerCase() === adminEmail)
      || (sub && (sub.status === 'active' || sub.status === 'trialing'
          || (sub.current_period_end && sub.current_period_end > new Date().toISOString())));

    if (alreadyPremium) return res.redirect('/?auth=1');
    // New users go straight to Stripe - trial is handled there with CC on file
    return res.redirect('/api/stripe/checkout?session=' + sessionToken);
  } catch(e) { slog('verify error: ' + e.message); res.redirect('/signup?error=server_error'); }
});

app.post('/api/auth/logout', async (req, res) => {
  const s = req.session;
  if (s) { try { await run('DELETE FROM sessions WHERE id = ?', [s.id]); } catch(_) {} }
  res.setHeader('Set-Cookie', 'it_session=; Path=/; Max-Age=0');
  res.json({ ok: true });
});

// ─── STRIPE ───────────────────────────────────────────────────────────────────
app.get('/api/stripe/checkout', async (req, res) => {
  if (!STRIPE_SECRET) return res.redirect('/premium');
  const sessionToken = req.query.session || req.session?.token;
  if (!sessionToken) return res.redirect('/signup');
  const userSession = req.session || await queryOne(`SELECT s.*, u.email, u.is_admin FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = ? AND s.expires_at > datetime('now')`, [sessionToken]);
  if (!userSession) return res.redirect('/signup');
  const plan    = req.query.plan === 'annual' ? 'annual' : 'monthly';
  const priceId = plan === 'annual' ? STRIPE_PRICE_ANNUAL : STRIPE_PRICE_MONTHLY;
  if (!priceId) return res.redirect('/premium');
  try {
    const stripe   = require('stripe')(STRIPE_SECRET);
    const checkout = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'subscription',
      line_items: [{ price: priceId, quantity: 1 }],
      customer_email: userSession.email,
      metadata: { user_id: String(userSession.user_id || userSession.id), plan },
      subscription_data: { trial_period_days: 7 },
      success_url: `${SITE_URL}/api/stripe/success?cs_id={CHECKOUT_SESSION_ID}`,
      cancel_url:  `${SITE_URL}/premium?cancelled=1`,
    });
    res.redirect(checkout.url);
  } catch(e) { slog('Stripe checkout error: ' + e.message); res.redirect('/premium?error=stripe'); }
});

app.get('/api/stripe/success', async (req, res) => {
  const csId = req.query.cs_id;
  if (!csId || !STRIPE_SECRET) return res.redirect('/?premium=1');
  try {
    const stripe  = require('stripe')(STRIPE_SECRET);
    const session = await stripe.checkout.sessions.retrieve(csId, { expand: ['subscription'] });
    if (session.payment_status === 'paid' || session.status === 'complete') {
      const userId = parseInt(session.metadata?.user_id);
      const plan   = session.metadata?.plan || 'monthly';
      const sub    = session.subscription;
      if (userId) {
        const periodEnd = sub?.current_period_end ? new Date(sub.current_period_end * 1000).toISOString() : null;
        await run(`
          INSERT INTO subscriptions (user_id, stripe_customer_id, stripe_subscription_id, stripe_checkout_session_id, plan, status, current_period_end, updated_at)
          VALUES (?, ?, ?, ?, ?, 'active', ?, datetime('now'))
          ON CONFLICT(user_id) DO UPDATE SET
            stripe_customer_id = excluded.stripe_customer_id,
            stripe_subscription_id = excluded.stripe_subscription_id,
            stripe_checkout_session_id = excluded.stripe_checkout_session_id,
            plan = excluded.plan, status = 'active',
            current_period_end = excluded.current_period_end,
            updated_at = datetime('now')
        `, [userId, session.customer, sub?.id || '', csId, plan, periodEnd]);
        slog(`Subscription activated for user ${userId} (${plan})`);
      }
    }
  } catch(e) { slog('Stripe success error: ' + e.message); }
  res.redirect('/?premium=1');
});

app.get('/api/stripe/portal', async (req, res) => {
  const portalSession = req.session;
  if (!portalSession || !STRIPE_SECRET) return res.redirect('/account');
  try {
    const sub = await getSubscription(portalSession.user_id);
    if (!sub?.stripe_customer_id) return res.redirect('/account');
    const portal = await require('stripe')(STRIPE_SECRET).billingPortal.sessions.create({
      customer: sub.stripe_customer_id, return_url: `${SITE_URL}/account`,
    });
    res.redirect(portal.url);
  } catch(e) { slog('Portal error: ' + e.message); res.redirect('/account'); }
});

// ── STRIPE WEBHOOK ────────────────────────────────────────────
// Must use express.raw() - Stripe signature verification needs the raw body bytes.
app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!STRIPE_SECRET || !STRIPE_WEBHOOK_SECRET) return res.status(400).send('Webhook not configured');
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    const stripe = require('stripe')(STRIPE_SECRET);
    event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
  } catch(e) {
    slog('Webhook sig error: ' + e.message);
    return res.status(400).send('Webhook Error: ' + e.message);
  }

  try {
    const stripe = require('stripe')(STRIPE_SECRET);
    const obj = event.data.object;

    async function upsertSub(userId, customerId, subObj, plan, status) {
      if (!userId) return;
      const periodEnd = subObj?.current_period_end
        ? new Date(subObj.current_period_end * 1000).toISOString() : null;
      await run(`
        INSERT INTO subscriptions (user_id, stripe_customer_id, stripe_subscription_id, plan, status, current_period_end, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(user_id) DO UPDATE SET
          stripe_customer_id     = excluded.stripe_customer_id,
          stripe_subscription_id = excluded.stripe_subscription_id,
          plan = excluded.plan, status = excluded.status,
          current_period_end = excluded.current_period_end,
          updated_at = datetime('now')
      `, [userId, customerId, subObj?.id || '', plan, status, periodEnd]);
    }

    async function userIdFromCustomer(customerId) {
      const row = await queryOne('SELECT user_id FROM subscriptions WHERE stripe_customer_id = ?', [customerId]);
      return row?.user_id || null;
    }

    switch (event.type) {

      case 'checkout.session.completed': {
        const userId = parseInt(obj.metadata?.user_id);
        const plan   = obj.metadata?.plan || 'monthly';
        const sub    = obj.subscription ? await stripe.subscriptions.retrieve(obj.subscription) : null;
        await upsertSub(userId, obj.customer, sub, plan, 'active');
        slog(`Webhook: subscription activated for user ${userId} (${plan})`);
        break;
      }

      case 'customer.subscription.updated': {
        const userId = await userIdFromCustomer(obj.customer);
        const plan   = obj.items?.data?.[0]?.price?.id === STRIPE_PRICE_ANNUAL ? 'annual' : 'monthly';
        const status = obj.status === 'active'   ? 'active'
                     : obj.status === 'trialing' ? 'trialing'
                     : obj.status === 'past_due' ? 'past_due' : 'inactive';
        await upsertSub(userId, obj.customer, obj, plan, status);
        slog(`Webhook: subscription updated for customer ${obj.customer} → ${status}`);
        break;
      }

      case 'customer.subscription.deleted': {
        const userId = await userIdFromCustomer(obj.customer);
        if (userId) {
          await run(`UPDATE subscriptions SET status='cancelled', updated_at=datetime('now') WHERE user_id=?`, [userId]);
          slog(`Webhook: subscription cancelled for user ${userId}`);
        }
        break;
      }

      case 'invoice.payment_failed': {
        const userId = await userIdFromCustomer(obj.customer);
        if (userId) {
          await run(`UPDATE subscriptions SET status='past_due', updated_at=datetime('now') WHERE user_id=?`, [userId]);
          slog(`Webhook: payment failed for user ${userId}`);
        }
        break;
      }

      case 'invoice.paid': {
        // Renewal - keep subscription active and update period end
        const sub    = obj.subscription ? await stripe.subscriptions.retrieve(obj.subscription) : null;
        const userId = await userIdFromCustomer(obj.customer);
        if (userId && sub) {
          await upsertSub(userId, obj.customer, sub, null, 'active');
          slog(`Webhook: subscription renewed for user ${userId}`);
        }
        break;
      }
    }
  } catch(e) {
    slog('Webhook processing error: ' + e.message);
    return res.status(500).send('Processing error');
  }

  res.json({ received: true });
});

// ─── ALERT ENGINE ─────────────────────────────────────────────────────────────
function formatVal(n) {
  if (!n) return '$0';
  if (n >= 1e9) return '$' + (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return '$' + (n / 1e3).toFixed(0) + 'K';
  return '$' + n.toFixed(0);
}

function getRoleKey(title) {
  const t = (title || '').toUpperCase();
  if (/\bCEO\b|CHIEF EXECUTIVE/.test(t))  return 'ceo';
  if (/\bCFO\b|CHIEF FINANCIAL/.test(t))  return 'cfo';
  if (/\bCOO\b|CHIEF OPERATING/.test(t))  return 'coo';
  if (/\bCTO\b|CHIEF TECHNOLOGY/.test(t)) return 'cto';
  if (/\bPRESIDENT\b/.test(t))            return 'president';
  if (/\bCHAIRMAN\b/.test(t))             return 'chairman';
  if (/\bDIRECTOR\b/.test(t))             return 'director';
  return 'other';
}

function buildSignalsFromTrades(trades, firstBuyRows = []) {
  const signals = [], byTicker = {};
  for (const t of trades) { if (!byTicker[t.ticker]) byTicker[t.ticker] = []; byTicker[t.ticker].push(t); }
  for (const [ticker, tt] of Object.entries(byTicker)) {
    const company = tt[0].company || ticker;
    const buys    = tt.filter(t => t.type === 'P');
    const sells   = tt.filter(t => t.type === 'S' || t.type === 'S-');
    if (buys.length >= 2) {
      const ui = new Set(buys.map(t => t.insider)).size;
      if (ui >= 2) {
        const totalVal = buys.reduce((s, t) => s + (t.value || 0), 0);
        const score    = Math.min(100, 50 + ui * 10 + (totalVal >= 500000 ? 15 : totalVal >= 100000 ? 8 : 0));
        const date     = buys.sort((a, b) => b.filing_date.localeCompare(a.filing_date))[0].filing_date;
        const sec      = getTickerSector(ticker);
        signals.push({ ticker, company, signal_type: 'CLUSTER', score, value: totalVal, date, sector: sec?.[0] || null, subsector: sec?.[1] || null, role_key: null, headline: ui + ' insiders buying in cluster', detail: ui + ' distinct insiders · ' + formatVal(totalVal) + ' combined' });
      }
    }
    for (const t of buys) {
      const isCsuite = /\b(CEO|CFO|President|Chairman|COO|CTO)\b/i.test(t.title || '');
      if (isCsuite || (t.value || 0) >= 100000) {
        const score  = Math.min(100, 55 + (isCsuite ? 15 : 0) + (t.value >= 500000 ? 15 : t.value >= 100000 ? 8 : 0));
        const sec    = getTickerSector(ticker);
        signals.push({ ticker, company, signal_type: 'CONVICTION', score, value: t.value || 0, date: t.filing_date, sector: sec?.[0] || null, subsector: sec?.[1] || null, role_key: getRoleKey(t.title), headline: 'High conviction buy · ' + (t.title || 'Insider'), detail: (t.insider || '') + ' · ' + formatVal(t.value || 0) });
      }
    }
    if (sells.length >= 2) {
      const us = new Set(sells.map(t => t.insider)).size;
      if (us >= 2) {
        const totalSell = sells.reduce((s, t) => s + (t.value || 0), 0);
        const score     = Math.min(100, 50 + us * 8 + (totalSell >= 1000000 ? 15 : 0));
        const date      = sells.sort((a, b) => b.filing_date.localeCompare(a.filing_date))[0].filing_date;
        const sec       = getTickerSector(ticker);
        signals.push({ ticker, company, signal_type: 'EXIT_WARNING', score, value: totalSell, date, sector: sec?.[0] || null, subsector: sec?.[1] || null, role_key: null, headline: 'Exit warning · ' + us + ' insiders selling', detail: us + ' sellers · ' + formatVal(totalSell) + ' disclosed' });
      }
    }
  }
  for (const fb of firstBuyRows) {
    const gapYears = fb.gap_days ? Math.floor(fb.gap_days / 365) : null;
    const isCsuite = /\b(CEO|CFO|President|Chairman|COO|CTO)\b/i.test(fb.title || '');
    const score    = Math.min(100, 60 + (isCsuite ? 15 : 0) + ((fb.latest_value || 0) >= 100000 ? 10 : 0) + (!fb.prev_trade ? 8 : 0));
    const sec      = getTickerSector(fb.ticker);
    signals.push({ ticker: fb.ticker, company: fb.company || fb.ticker, signal_type: 'FIRST_BUY', score, value: fb.latest_value || 0, date: fb.latest_filing, sector: sec?.[0] || null, subsector: sec?.[1] || null, role_key: getRoleKey(fb.title), headline: 'First buy in years · ' + (fb.title || 'Insider'), detail: (fb.insider || '') + ' · ' + (fb.prev_trade ? gapYears + '+ yr gap' : 'No prior buy') + ' · ' + formatVal(fb.latest_value || 0) });
  }
  return signals.sort((a, b) => b.score - a.score);
}

async function sendAlertEmail(toEmail, signals) {
  if (!RESEND_KEY || !signals.length) return;
  const { Resend } = require('resend');
  const signalRows = signals.map(s => {
    const color = s.signal_type === 'EXIT_WARNING' ? '#ff4466' : s.signal_type === 'CLUSTER' ? '#00d4ff' : s.signal_type === 'FIRST_BUY' ? '#f5a623' : '#00ff88';
    const icon  = s.signal_type === 'EXIT_WARNING' ? '⚠️' : s.signal_type === 'CLUSTER' ? '🔵' : s.signal_type === 'FIRST_BUY' ? '🆕' : '⚡';
    return `<div style="border:1px solid #1e2d3d;border-radius:6px;padding:16px;margin-bottom:12px;background:#0d1117">
      <div style="margin-bottom:8px"><span style="font-size:18px;font-weight:700;color:#00d4ff">${s.ticker}</span>
      <span style="font-size:11px;padding:3px 8px;background:${color}22;border:1px solid ${color}44;border-radius:3px;color:${color};margin-left:8px">${icon} ${s.signal_type.replace('_',' ')}</span></div>
      <div style="font-size:13px;color:#c9d8e8;margin-bottom:4px">${s.company}</div>
      <div style="font-size:12px;color:#e0e6ed;font-weight:600;margin-bottom:4px">${s.headline}</div>
      ${s.detail ? `<div style="font-size:11px;color:#4a6580">${s.detail}</div>` : ''}
      <a href="${SITE_URL}/stock/${s.ticker}" style="font-size:11px;color:#00d4ff;text-decoration:none">VIEW CHART →</a>
    </div>`;
  }).join('');
  const subject = signals.length === 1 ? `InsiderTape Alert: ${signals[0].ticker}` : `InsiderTape: ${signals.length} new insider signals`;
  await new Resend(RESEND_KEY).emails.send({
    from: FROM_EMAIL, to: toEmail, subject,
    html: `<div style="font-family:'Courier New',monospace;max-width:520px;margin:0 auto;background:#080b0f;color:#e0e6ed;padding:32px;border-radius:8px">
      <div style="font-size:20px;font-weight:700;letter-spacing:3px;color:#00d4ff;margin-bottom:4px">INSIDERTAPE</div>
      <div style="font-size:10px;color:#4a6580;margin-bottom:28px">INSIDER SIGNAL ALERT</div>
      ${signalRows}
      <div style="margin-top:24px;padding-top:20px;border-top:1px solid #1e2d3d;font-size:10px;color:#2a3d52">
        Data from SEC Form 4 filings. Not investment advice.<br>
        <a href="${SITE_URL}/account" style="color:#2a3d52">Manage alerts</a> · <a href="${SITE_URL}/account?unsubscribe=1" style="color:#2a3d52">Unsubscribe</a>
      </div></div>`,
  });
}

let _alertRunning = false;
async function runAlertCheck() {
  if (_alertRunning || !RESEND_KEY) return;
  _alertRunning = true;
  try {
    const users = await query(`
      SELECT u.id AS user_id, u.email, p.min_score, p.min_value, p.types, p.tickers, p.sectors, p.roles, p.frequency
      FROM alert_prefs p JOIN users u ON u.id = p.user_id JOIN subscriptions s ON s.user_id = p.user_id
      WHERE p.enabled = 1 AND (s.status = 'active' OR (s.status = 'trial' AND s.current_period_end > datetime('now'))
        OR (s.current_period_end IS NOT NULL AND s.current_period_end > datetime('now')))
    `);
    if (!users.length) return;
    const recentTrades = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, MAX(filing_date) AS filing_date, TRIM(type) AS type,
             MAX(qty) AS qty, MAX(price) AS price, MAX(value) AS value, MAX(owned) AS owned
      FROM trades WHERE filing_date >= date('now','-3 days') AND TRIM(type) IN ('P','S','S-')
        AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
      GROUP BY ticker, insider, trade_date, type ORDER BY filing_date DESC, value DESC
    `);
    if (!recentTrades.length) return;

    const firstBuyRows = await query(`
      WITH rb AS (SELECT DISTINCT insider, ticker FROM trades WHERE TRIM(type)='P' AND filing_date >= date('now','-2 days') AND insider IS NOT NULL),
      latest AS (SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title, MAX(t.trade_date) AS latest_trade, MAX(t.filing_date) AS latest_filing, MAX(t.value) AS latest_value FROM trades t JOIN rb ON t.insider=rb.insider AND t.ticker=rb.ticker WHERE TRIM(t.type)='P' AND t.filing_date >= date('now','-2 days') GROUP BY t.insider, t.ticker),
      prev AS (SELECT t.insider, t.ticker, MAX(t.trade_date) AS prev_trade FROM trades t JOIN rb ON t.insider=rb.insider AND t.ticker=rb.ticker WHERE TRIM(t.type)='P' AND t.filing_date < date('now','-2 days') GROUP BY t.insider, t.ticker)
      SELECT l.ticker, l.company, l.insider, l.title, l.latest_trade, l.latest_filing, l.latest_value, p.prev_trade,
             CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
      FROM latest l LEFT JOIN prev p ON l.insider=p.insider AND l.ticker=p.ticker
      WHERE p.prev_trade IS NULL OR CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= 730
    `);

    const signals = buildSignalsFromTrades(recentTrades, firstBuyRows);
    if (!signals.length) return;

    for (const user of users) {
      const userTickers = user.tickers ? user.tickers.split(',').map(t => t.trim()).filter(Boolean) : [];
      const userTypes   = user.types.split(',').map(t => t.trim().toLowerCase());
      const userSectors = user.sectors ? user.sectors.split(',').map(s => s.trim().toLowerCase()).filter(Boolean) : [];
      const toSend = [];
      for (const sig of signals) {
        if (sig.score < user.min_score) continue;
        if (sig.value < user.min_value) continue;
        if (userTickers.length && !userTickers.includes(sig.ticker)) continue;
        if (!userTypes.includes(sig.signal_type.toLowerCase().replace(' ', '_'))) continue;
        if (userSectors.length) {
          const ts = getTickerSector(sig.ticker);
          if (!ts) continue;
          if (!userSectors.some(s => ts[0].toLowerCase().includes(s) || ts[1].toLowerCase().includes(s))) continue;
        }
        const key = sig.ticker + '|' + sig.signal_type + '|' + sig.date;
        const already = await queryOne('SELECT 1 FROM alert_log WHERE user_id = ? AND signal_key = ?', [user.user_id, key]);
        if (already) continue;
        toSend.push(sig);
        await run('INSERT OR IGNORE INTO alert_log (user_id, ticker, signal_type, signal_key) VALUES (?,?,?,?)', [user.user_id, sig.ticker, sig.signal_type, key]);
      }
      if (toSend.length) { await sendAlertEmail(user.email, toSend); slog('Alert: sent ' + toSend.length + ' signal(s) to ' + user.email); }
    }
  } catch(e) { slog('Alert check error: ' + e.message); } finally { _alertRunning = false; }
}
setInterval(runAlertCheck, 5 * 60000);

// Alert prefs routes
app.get('/api/alerts/prefs', async (req, res) => {
  if (!req.session) return res.status(401).json({ error: 'Not authenticated' });
  if (!req.isPremium) return res.status(403).json({ error: 'Premium required' });
  try {
    let prefs = await queryOne('SELECT * FROM alert_prefs WHERE user_id = ?', [req.session.user_id]);
    if (!prefs) prefs = { enabled: 0, min_score: 70, min_value: 0, types: 'conviction,cluster,first_buy,exit_warning', tickers: '', sectors: '', roles: '', frequency: 'immediate' };
    res.json(prefs);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/alerts/prefs', express.json(), async (req, res) => {
  if (!req.session) return res.status(401).json({ error: 'Not authenticated' });
  if (!req.isPremium) return res.status(403).json({ error: 'Premium required' });
  const { enabled, min_score, min_value, types, tickers, sectors, roles, frequency } = req.body;
  try {
    await run(`
      INSERT INTO alert_prefs (user_id, enabled, min_score, min_value, types, tickers, sectors, roles, frequency, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled, min_score=excluded.min_score,
        min_value=excluded.min_value, types=excluded.types, tickers=excluded.tickers,
        sectors=excluded.sectors, roles=excluded.roles, frequency=excluded.frequency, updated_at=excluded.updated_at
    `, [req.session.user_id, enabled ? 1 : 0, Math.min(Math.max(parseInt(min_score) || 70, 0), 100),
        Math.max(parseInt(min_value) || 0, 0), (types || 'conviction,cluster,first_buy,exit_warning').slice(0, 200),
        (tickers || '').toUpperCase().replace(/[^A-Z,\s]/g, '').slice(0, 500),
        (sectors || '').slice(0, 500), (roles || '').slice(0, 200),
        ['immediate', 'daily'].includes(frequency) ? frequency : 'immediate']);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/alerts/test', async (req, res) => {
  if (!req.session) return res.status(401).json({ error: 'Not authenticated' });
  if (!req.isPremium) return res.status(403).json({ error: 'Premium required' });
  if (!RESEND_KEY) return res.status(500).json({ error: 'RESEND_KEY not configured' });
  try {
    const user = await queryOne('SELECT email FROM users WHERE id = ?', [req.session.user_id]);
    await sendAlertEmail(user.email, [
      { ticker: 'AAPL', company: 'Apple Inc.', signal_type: 'CONVICTION', headline: 'High conviction buy · CEO', detail: 'Test · $2.4M', score: 87, date: new Date().toISOString().slice(0, 10) },
    ]);
    res.json({ ok: true, message: 'Test alert sent to ' + user.email });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── ADMIN ────────────────────────────────────────────────────────────────────
function requireAdminSecret(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret) return false;
  if (req.query.secret !== secret) { res.status(403).json({ error: 'Forbidden' }); return true; }
  return false;
}

app.get('/api/admin/grant-premium', async (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email required' });
  const days = Math.max(1, parseInt(req.query.days || '365'));
  try {
    await run('INSERT OR IGNORE INTO users (email) VALUES (?)', [email]);
    const user = await queryOne('SELECT id, email FROM users WHERE LOWER(email) = ?', [email]);
    const periodEnd = new Date(Date.now() + days * 86400000).toISOString();
    await run(`INSERT INTO subscriptions (user_id, plan, status, current_period_end, updated_at) VALUES (?, 'gifted', 'active', ?, datetime('now'))
      ON CONFLICT(user_id) DO UPDATE SET status='active', current_period_end=?, updated_at=datetime('now')`, [user.id, periodEnd, periodEnd]);
    res.json({ ok: true, message: `Premium granted to ${user.email} for ${days} days` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/check-user', async (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email required' });
  try {
    const user = await queryOne('SELECT id, email, is_admin FROM users WHERE LOWER(email) = ?', [email]);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const sub = await queryOne('SELECT * FROM subscriptions WHERE user_id = ?', [user.id]);
    res.json({ user, subscription: sub || null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/unban', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const ip = req.query.ip;
  if (!ip) { _bannedIPs.clear(); _strikeMap.clear(); return res.json({ ok: true, cleared: true }); }
  _bannedIPs.delete(ip); _strikeMap.delete(ip);
  res.json({ ok: true, unbanned: ip });
});

app.get('/api/debug', async (req, res) => {
  try {
    const total   = await queryOne('SELECT COUNT(*) AS n FROM trades');
    const byType  = await query("SELECT COALESCE(TRIM(type),'NULL') AS type, COUNT(*) AS n FROM trades GROUP BY TRIM(type) ORDER BY n DESC LIMIT 10");
    const dates   = await queryOne("SELECT MAX(trade_date) AS td_latest, MIN(trade_date) AS td_earliest FROM trades WHERE trade_date IS NOT NULL");
    const p7      = await queryOne("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date >= date('now','-7 days')");
    const p30     = await queryOne("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date >= date('now','-30 days')");
    res.json({ total_rows: total?.n || 0, by_type: byType, dates, buys_7d: p7?.n || 0, buys_30d: p30?.n || 0, server_version: '2.0-vercel' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ping', (req, res) => res.json({ ok: true, t: Date.now() }));

// ─── ROBOTS / SITEMAP ─────────────────────────────────────────────────────────
app.get('/robots.txt', (req, res) => {
  res.type('text/plain');
  res.send('User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /members/\nCrawl-delay: 1\n\nSitemap: https://www.insidertape.com/sitemap.xml\n');
});

let _sitemapCache = null, _sitemapCacheTime = 0;
app.get('/sitemap.xml', async (req, res) => {
  if (_sitemapCache && Date.now() - _sitemapCacheTime < 86400000) { res.type('application/xml'); return res.send(_sitemapCache); }
  const now = new Date().toISOString().slice(0, 10), base = 'https://www.insidertape.com';
  const staticPages = [
    { url: '/', priority: '1.0', freq: 'daily' },
    { url: '/screener', priority: '0.9', freq: 'daily' },
    { url: '/stock', priority: '0.8', freq: 'daily' },
    { url: '/insider', priority: '0.8', freq: 'daily' },
    { url: '/premium', priority: '0.7', freq: 'monthly' },
    { url: '/articles/', priority: '0.7', freq: 'weekly' },
  ];
  const articleSlugs = ['cluster-buying-example','how-to-find-stocks-with-insider-buying','how-to-read-sec-form-4','how-to-use-insider-data','insider-buying-vs-analyst-upgrades','legal-vs-illegal-insider-trading','what-is-cluster-buying'];
  const articlePages = articleSlugs.map(s => ({ url: `/articles/${s}.html`, priority: '0.6', freq: 'monthly' }));
  let tickerPages = [];
  try {
    const tickers = await query("SELECT ticker FROM trades WHERE ticker GLOB '[A-Z]*' AND trade_date >= date('now','-1825 days') GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 200");
    tickerPages = tickers.map(r => ({ url: `/stock/${r.ticker}`, priority: '0.5', freq: 'daily' }));
  } catch(_) {}
  const allPages = [...staticPages, ...articlePages, ...tickerPages];
  const urls = allPages.map(p => `\n  <url><loc>${base}${p.url}</loc><lastmod>${now}</lastmod><changefreq>${p.freq}</changefreq><priority>${p.priority}</priority></url>`).join('');
  _sitemapCache = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${urls}\n</urlset>`;
  _sitemapCacheTime = Date.now();
  res.type('application/xml');
  res.send(_sitemapCache);
});

// ─── SPA FALLBACK ─────────────────────────────────────────────────────────────
app.get('*', (req, res) => {
  if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Not found' });
  if (req.path.startsWith('/articles')) {
    const articlePath = req.path === '/articles' || req.path === '/articles/'
      ? path.join(__dirname, 'articles', 'index.html')
      : path.join(__dirname, 'articles', req.path.replace('/articles/', '') + (req.path.endsWith('.html') ? '' : '.html'));
    if (require('fs').existsSync(articlePath)) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      return res.sendFile(articlePath);
    }
  }
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => slog(`Server on port ${PORT}`));
