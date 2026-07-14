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
    `CREATE INDEX IF NOT EXISTS idx_insider_upper    ON trades(UPPER(insider))`,
    `CREATE INDEX IF NOT EXISTS idx_ticker_date_price ON trades(ticker, trade_date, price)`,
    `CREATE INDEX IF NOT EXISTS idx_insider_ticker_date ON trades(insider, ticker, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_ticker_type      ON trades(ticker, type, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_type_filing ON trades(type, filing_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_type_trade  ON trades(type, trade_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_trades_ttype_date  ON trades(TRIM(type), trade_date)`,
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
              : (req.path === '/api/insider' || req.path.startsWith('/insider')) ? 50000
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
    // Exact lookups (the profile pages) use indexed equality so idx_insider_upper
    // kicks in; LIKE would full-scan the whole trades table. SEC stores names
    // last-first ("Austin Rudy Mitchell"), so we also try last-name-first and
    // reversed orderings to tolerate first-last URLs without a scan.
    const limit = exact ? 2000 : 500;
    let matchClause, params;
    if (exact) {
      const toks = name.split(/\s+/).filter(Boolean);
      const cands = [name];
      if (toks.length >= 2) {
        const lastFirst = [toks[toks.length - 1], ...toks.slice(0, -1)].join(' ');
        const reversed  = [...toks].reverse().join(' ');
        if (!cands.includes(lastFirst)) cands.push(lastFirst);
        if (!cands.includes(reversed))  cands.push(reversed);
      }
      matchClause = `UPPER(insider) IN (${cands.map(() => 'UPPER(?)').join(',')})`;
      params = cands;
    } else {
      matchClause = 'UPPER(insider) LIKE UPPER(?)';
      params = [`%${name}%`];
    }
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades WHERE ${matchClause} AND TRIM(type) IN ('P','S','S-')
        AND COALESCE(value, 0) <= 5000000000
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC LIMIT ?
    `, [...params, limit]);
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
    // Use getPCAny (stale-OK): 52-week highs/lows and the last daily close don't
    // need real-time freshness, and getPC's short TTL would drop most tickers.
    const bars = await getPCAny(sym);
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
    // Serve from precomputed cache (refreshed daily by GitHub Actions). Insider
    // sentiment is monthly data that barely moves intraday, so we serve the cached
    // copy for up to 7 days rather than expiring it after a few hours and forcing
    // every serverless request onto the heavy 120-month aggregation + live S&P fetch.
    // The 7-day window is just a self-healing safety if the precompute pipeline breaks.
    try {
      const cached = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'insider-sentiment'");
      if (cached && Date.now() - cached.computed_at < 7 * 24 * 3600000) {
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
// Expanded coverage: top holdings across each sector (major sector-ETF constituents).
Object.assign(TICKER_SECTOR_MAP, {
  // Technology
  'ORCL':['Technology','Software-Infrastructure'],'IBM':['Technology','Information Technology Services'],'ADI':['Technology','Semiconductors'],'LRCX':['Technology','Semiconductor Equipment & Materials'],'KLAC':['Technology','Semiconductor Equipment & Materials'],'SNPS':['Technology','Software-Application'],'CDNS':['Technology','Software-Application'],'ANET':['Technology','Computer Hardware'],'APH':['Technology','Electronic Components'],'TEL':['Technology','Electronic Components'],'MSI':['Technology','Communication Equipment'],'FTNT':['Technology','Software-Infrastructure'],'MRVL':['Technology','Semiconductors'],'NXPI':['Technology','Semiconductors'],'MCHP':['Technology','Semiconductors'],'ON':['Technology','Semiconductors'],'DELL':['Technology','Computer Hardware'],'HPQ':['Technology','Computer Hardware'],'HPE':['Technology','Computer Hardware'],'CTSH':['Technology','Information Technology Services'],'GLW':['Technology','Electronic Components'],'MPWR':['Technology','Semiconductors'],'SMCI':['Technology','Computer Hardware'],'WDC':['Technology','Computer Hardware'],'STX':['Technology','Computer Hardware'],'ADSK':['Technology','Software-Application'],'WDAY':['Technology','Software-Application'],'TEAM':['Technology','Software-Application'],'DDOG':['Technology','Software-Infrastructure'],'NET':['Technology','Software-Infrastructure'],'VRSN':['Technology','Software-Infrastructure'],'FICO':['Technology','Software-Application'],'GRMN':['Technology','Scientific & Technical Instruments'],'IT':['Technology','Information Technology Services'],'CDW':['Technology','Information Technology Services'],'NTAP':['Technology','Computer Hardware'],'KEYS':['Technology','Scientific & Technical Instruments'],'TER':['Technology','Semiconductor Equipment & Materials'],'TYL':['Technology','Software-Application'],'PTC':['Technology','Software-Application'],'AKAM':['Technology','Software-Infrastructure'],'ZM':['Technology','Software-Application'],'DOCU':['Technology','Software-Application'],'HUBS':['Technology','Software-Application'],'MDB':['Technology','Software-Infrastructure'],
  // Communication Services
  'GOOG':['Communication Services','Internet Content & Information'],'CHTR':['Communication Services','Telecom Services'],'WBD':['Communication Services','Entertainment'],'EA':['Communication Services','Electronic Gaming & Multimedia'],'TTWO':['Communication Services','Electronic Gaming & Multimedia'],'OMC':['Communication Services','Advertising Agencies'],'IPG':['Communication Services','Advertising Agencies'],'LYV':['Communication Services','Entertainment'],'MTCH':['Communication Services','Internet Content & Information'],'PINS':['Communication Services','Internet Content & Information'],'SNAP':['Communication Services','Internet Content & Information'],'RBLX':['Communication Services','Electronic Gaming & Multimedia'],'FOXA':['Communication Services','Entertainment'],'PARA':['Communication Services','Entertainment'],'NWSA':['Communication Services','Publishing'],
  // Consumer Cyclical
  'LULU':['Consumer Cyclical','Apparel Retail'],'ORLY':['Consumer Cyclical','Specialty Retail'],'AZO':['Consumer Cyclical','Specialty Retail'],'ROST':['Consumer Cyclical','Apparel Retail'],'TJX':['Consumer Cyclical','Apparel Retail'],'YUM':['Consumer Cyclical','Restaurants'],'CMG':['Consumer Cyclical','Restaurants'],'HLT':['Consumer Cyclical','Lodging'],'RCL':['Consumer Cyclical','Travel Services'],'CCL':['Consumer Cyclical','Travel Services'],'NCLH':['Consumer Cyclical','Travel Services'],'DHI':['Consumer Cyclical','Residential Construction'],'LEN':['Consumer Cyclical','Residential Construction'],'PHM':['Consumer Cyclical','Residential Construction'],'NVR':['Consumer Cyclical','Residential Construction'],'EBAY':['Consumer Cyclical','Internet Retail'],'ETSY':['Consumer Cyclical','Internet Retail'],'APTV':['Consumer Cyclical','Auto Parts'],'LVS':['Consumer Cyclical','Resorts & Casinos'],'WYNN':['Consumer Cyclical','Resorts & Casinos'],'MGM':['Consumer Cyclical','Resorts & Casinos'],'DRI':['Consumer Cyclical','Restaurants'],'GPC':['Consumer Cyclical','Auto Parts'],'TSCO':['Consumer Cyclical','Specialty Retail'],'BBY':['Consumer Cyclical','Specialty Retail'],'ULTA':['Consumer Cyclical','Specialty Retail'],'EXPE':['Consumer Cyclical','Travel Services'],'ABNB':['Consumer Cyclical','Travel Services'],
  // Consumer Defensive
  'PM':['Consumer Defensive','Tobacco'],'MO':['Consumer Defensive','Tobacco'],'MDLZ':['Consumer Defensive','Confectioners'],'CL':['Consumer Defensive','Household & Personal Products'],'KMB':['Consumer Defensive','Household & Personal Products'],'GIS':['Consumer Defensive','Packaged Foods'],'KHC':['Consumer Defensive','Packaged Foods'],'SYY':['Consumer Defensive','Food Distribution'],'STZ':['Consumer Defensive','Beverages-Wineries & Distilleries'],'KDP':['Consumer Defensive','Beverages-Non-Alcoholic'],'MNST':['Consumer Defensive','Beverages-Non-Alcoholic'],'HSY':['Consumer Defensive','Confectioners'],'KR':['Consumer Defensive','Grocery Stores'],'ADM':['Consumer Defensive','Farm Products'],'KVUE':['Consumer Defensive','Household & Personal Products'],'CLX':['Consumer Defensive','Household & Personal Products'],'CHD':['Consumer Defensive','Household & Personal Products'],'MKC':['Consumer Defensive','Packaged Foods'],'HRL':['Consumer Defensive','Packaged Foods'],'TSN':['Consumer Defensive','Packaged Foods'],'EL':['Consumer Defensive','Household & Personal Products'],'DG':['Consumer Defensive','Discount Stores'],'DLTR':['Consumer Defensive','Discount Stores'],'TAP':['Consumer Defensive','Beverages-Brewers'],'K':['Consumer Defensive','Packaged Foods'],
  // Financial Services
  'C':['Financial Services','Banks-Diversified'],'SCHW':['Financial Services','Capital Markets'],'AXP':['Financial Services','Credit Services'],'SPGI':['Financial Services','Financial Data & Stock Exchanges'],'CB':['Financial Services','Insurance-Property & Casualty'],'PGR':['Financial Services','Insurance-Property & Casualty'],'MMC':['Financial Services','Insurance Brokers'],'AON':['Financial Services','Insurance Brokers'],'ICE':['Financial Services','Financial Data & Stock Exchanges'],'CME':['Financial Services','Financial Data & Stock Exchanges'],'USB':['Financial Services','Banks-Regional'],'PNC':['Financial Services','Banks-Regional'],'TFC':['Financial Services','Banks-Regional'],'COF':['Financial Services','Credit Services'],'BK':['Financial Services','Asset Management'],'AIG':['Financial Services','Insurance-Diversified'],'MET':['Financial Services','Insurance-Life'],'PRU':['Financial Services','Insurance-Life'],'TRV':['Financial Services','Insurance-Property & Casualty'],'ALL':['Financial Services','Insurance-Property & Casualty'],'AFL':['Financial Services','Insurance-Life'],'MCO':['Financial Services','Financial Data & Stock Exchanges'],'NDAQ':['Financial Services','Financial Data & Stock Exchanges'],'MSCI':['Financial Services','Financial Data & Stock Exchanges'],'AJG':['Financial Services','Insurance Brokers'],'DFS':['Financial Services','Credit Services'],'SYF':['Financial Services','Credit Services'],'KKR':['Financial Services','Asset Management'],'APO':['Financial Services','Asset Management'],'BX':['Financial Services','Asset Management'],'AMP':['Financial Services','Asset Management'],'TROW':['Financial Services','Asset Management'],'STT':['Financial Services','Asset Management'],'FITB':['Financial Services','Banks-Regional'],'HBAN':['Financial Services','Banks-Regional'],'RF':['Financial Services','Banks-Regional'],'CFG':['Financial Services','Banks-Regional'],'MTB':['Financial Services','Banks-Regional'],'NTRS':['Financial Services','Asset Management'],'ARES':['Financial Services','Asset Management'],
  // Healthcare
  'PFE':['Healthcare','Drug Manufacturers-General'],'BMY':['Healthcare','Drug Manufacturers-General'],'DHR':['Healthcare','Diagnostics & Research'],'ELV':['Healthcare','Healthcare Plans'],'CVS':['Healthcare','Healthcare Plans'],'MCK':['Healthcare','Medical Distribution'],'COR':['Healthcare','Medical Distribution'],'CAH':['Healthcare','Medical Distribution'],'ZTS':['Healthcare','Drug Manufacturers-Specialty & Generic'],'BDX':['Healthcare','Medical Instruments & Supplies'],'DXCM':['Healthcare','Medical Devices'],'IDXX':['Healthcare','Diagnostics & Research'],'IQV':['Healthcare','Diagnostics & Research'],'A':['Healthcare','Diagnostics & Research'],'MRNA':['Healthcare','Biotechnology'],'RMD':['Healthcare','Medical Devices'],'GEHC':['Healthcare','Medical Devices'],'CNC':['Healthcare','Healthcare Plans'],'MOH':['Healthcare','Healthcare Plans'],'HOLX':['Healthcare','Medical Devices'],'BAX':['Healthcare','Medical Instruments & Supplies'],'STE':['Healthcare','Medical Instruments & Supplies'],'WST':['Healthcare','Medical Instruments & Supplies'],'ALGN':['Healthcare','Medical Devices'],'PODD':['Healthcare','Medical Devices'],'WAT':['Healthcare','Diagnostics & Research'],'MTD':['Healthcare','Diagnostics & Research'],'LH':['Healthcare','Diagnostics & Research'],'DGX':['Healthcare','Diagnostics & Research'],'DVA':['Healthcare','Medical Care Facilities'],'UHS':['Healthcare','Medical Care Facilities'],
  // Energy
  'PSX':['Energy','Oil & Gas Refining & Marketing'],'VLO':['Energy','Oil & Gas Refining & Marketing'],'WMB':['Energy','Oil & Gas Midstream'],'KMI':['Energy','Oil & Gas Midstream'],'OKE':['Energy','Oil & Gas Midstream'],'ET':['Energy','Oil & Gas Midstream'],'EPD':['Energy','Oil & Gas Midstream'],'MPLX':['Energy','Oil & Gas Midstream'],'LNG':['Energy','Oil & Gas Midstream'],'BKR':['Energy','Oil & Gas Equipment & Services'],'FANG':['Energy','Oil & Gas E&P'],'HES':['Energy','Oil & Gas E&P'],'CTRA':['Energy','Oil & Gas E&P'],'APA':['Energy','Oil & Gas E&P'],'EQT':['Energy','Oil & Gas E&P'],'TRGP':['Energy','Oil & Gas Midstream'],'TPL':['Energy','Oil & Gas E&P'],
  // Industrials
  'UNP':['Industrials','Railroads'],'CSX':['Industrials','Railroads'],'NSC':['Industrials','Railroads'],'FDX':['Industrials','Integrated Freight & Logistics'],'ETN':['Industrials','Specialty Industrial Machinery'],'EMR':['Industrials','Specialty Industrial Machinery'],'ITW':['Industrials','Specialty Industrial Machinery'],'PH':['Industrials','Specialty Industrial Machinery'],'GD':['Industrials','Aerospace & Defense'],'NOC':['Industrials','Aerospace & Defense'],'TDG':['Industrials','Aerospace & Defense'],'LHX':['Industrials','Aerospace & Defense'],'MMM':['Industrials','Conglomerates'],'TT':['Industrials','Building Products & Equipment'],'JCI':['Industrials','Building Products & Equipment'],'CMI':['Industrials','Specialty Industrial Machinery'],'PCAR':['Industrials','Farm & Heavy Construction Machinery'],'CARR':['Industrials','Building Products & Equipment'],'OTIS':['Industrials','Specialty Industrial Machinery'],'WM':['Industrials','Waste Management'],'RSG':['Industrials','Waste Management'],'FAST':['Industrials','Industrial Distribution'],'GWW':['Industrials','Industrial Distribution'],'URI':['Industrials','Rental & Leasing Services'],'PWR':['Industrials','Engineering & Construction'],'AME':['Industrials','Specialty Industrial Machinery'],'ROK':['Industrials','Specialty Industrial Machinery'],'DOV':['Industrials','Specialty Industrial Machinery'],'IR':['Industrials','Specialty Industrial Machinery'],'XYL':['Industrials','Specialty Industrial Machinery'],'FTV':['Industrials','Scientific & Technical Instruments'],'VRSK':['Industrials','Consulting Services'],'ODFL':['Industrials','Trucking'],'JBHT':['Industrials','Trucking'],'CTAS':['Industrials','Specialty Business Services'],'WAB':['Industrials','Railroads'],'TXT':['Industrials','Aerospace & Defense'],'HII':['Industrials','Aerospace & Defense'],'AXON':['Industrials','Aerospace & Defense'],'EFX':['Industrials','Consulting Services'],'SWK':['Industrials','Tools & Accessories'],'SNA':['Industrials','Tools & Accessories'],'PNR':['Industrials','Specialty Industrial Machinery'],'NDSN':['Industrials','Specialty Industrial Machinery'],'IEX':['Industrials','Specialty Industrial Machinery'],'ALLE':['Industrials','Security & Protection Services'],'MAS':['Industrials','Building Products & Equipment'],'HWM':['Industrials','Aerospace & Defense'],'LII':['Industrials','Building Products & Equipment'],
  // Basic Materials
  'APD':['Basic Materials','Specialty Chemicals'],'ECL':['Basic Materials','Specialty Chemicals'],'DD':['Basic Materials','Specialty Chemicals'],'DOW':['Basic Materials','Chemicals'],'PPG':['Basic Materials','Specialty Chemicals'],'NUE':['Basic Materials','Steel'],'STLD':['Basic Materials','Steel'],'CTVA':['Basic Materials','Agricultural Inputs'],'CF':['Basic Materials','Agricultural Inputs'],'MOS':['Basic Materials','Agricultural Inputs'],'ALB':['Basic Materials','Specialty Chemicals'],'LYB':['Basic Materials','Specialty Chemicals'],'VMC':['Basic Materials','Building Materials'],'MLM':['Basic Materials','Building Materials'],'IFF':['Basic Materials','Specialty Chemicals'],'PKG':['Basic Materials','Packaging & Containers'],'IP':['Basic Materials','Packaging & Containers'],'AMCR':['Basic Materials','Packaging & Containers'],'BALL':['Basic Materials','Packaging & Containers'],'CE':['Basic Materials','Specialty Chemicals'],'EMN':['Basic Materials','Specialty Chemicals'],'FMC':['Basic Materials','Agricultural Inputs'],'AVY':['Basic Materials','Packaging & Containers'],'RPM':['Basic Materials','Specialty Chemicals'],
  // Utilities
  'DUK':['Utilities','Utilities-Regulated Electric'],'SO':['Utilities','Utilities-Regulated Electric'],'D':['Utilities','Utilities-Regulated Electric'],'AEP':['Utilities','Utilities-Regulated Electric'],'EXC':['Utilities','Utilities-Regulated Electric'],'SRE':['Utilities','Utilities-Diversified'],'XEL':['Utilities','Utilities-Regulated Electric'],'PEG':['Utilities','Utilities-Regulated Electric'],'ED':['Utilities','Utilities-Regulated Electric'],'WEC':['Utilities','Utilities-Regulated Electric'],'ES':['Utilities','Utilities-Regulated Electric'],'AEE':['Utilities','Utilities-Regulated Electric'],'DTE':['Utilities','Utilities-Regulated Electric'],'PPL':['Utilities','Utilities-Regulated Electric'],'FE':['Utilities','Utilities-Regulated Electric'],'EIX':['Utilities','Utilities-Regulated Electric'],'ETR':['Utilities','Utilities-Regulated Electric'],'CEG':['Utilities','Utilities-Renewable'],'VST':['Utilities','Utilities-Independent Power Producers'],'NRG':['Utilities','Utilities-Independent Power Producers'],'AWK':['Utilities','Utilities-Regulated Water'],'CMS':['Utilities','Utilities-Regulated Electric'],'CNP':['Utilities','Utilities-Regulated Electric'],'NI':['Utilities','Utilities-Regulated Gas'],'LNT':['Utilities','Utilities-Regulated Electric'],'ATO':['Utilities','Utilities-Regulated Gas'],'PCG':['Utilities','Utilities-Regulated Electric'],
  // Real Estate
  'O':['Real Estate','REIT-Retail'],'PSA':['Real Estate','REIT-Industrial'],'DLR':['Real Estate','REIT-Specialty'],'WELL':['Real Estate','REIT-Healthcare Facilities'],'VICI':['Real Estate','REIT-Diversified'],'AVB':['Real Estate','REIT-Residential'],'EQR':['Real Estate','REIT-Residential'],'EXR':['Real Estate','REIT-Industrial'],'VTR':['Real Estate','REIT-Healthcare Facilities'],'ARE':['Real Estate','REIT-Office'],'SBAC':['Real Estate','REIT-Specialty'],'INVH':['Real Estate','REIT-Residential'],'MAA':['Real Estate','REIT-Residential'],'ESS':['Real Estate','REIT-Residential'],'KIM':['Real Estate','REIT-Retail'],'REG':['Real Estate','REIT-Retail'],'BXP':['Real Estate','REIT-Office'],'HST':['Real Estate','REIT-Hotel & Motel'],'DOC':['Real Estate','REIT-Healthcare Facilities'],'IRM':['Real Estate','REIT-Specialty'],'CBRE':['Real Estate','Real Estate Services'],'CSGP':['Real Estate','Real Estate Services'],'UDR':['Real Estate','REIT-Residential'],'CPT':['Real Estate','REIT-Residential'],
});
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
      FROM trades WHERE UPPER(insider) = UPPER(?) AND TRIM(type)='P' AND price > 0
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

app.get('/api/admin/stats', async (req, res) => {
  if (requireAdminSecret(req, res)) return;
  try {
    const totalUsers = (await queryOne('SELECT COUNT(*) AS n FROM users'))?.n || 0;
    const byStatus   = await query('SELECT status, COUNT(*) AS n FROM subscriptions GROUP BY status ORDER BY n DESC');
    const recentSignups = await query('SELECT email, created_at FROM users ORDER BY id DESC LIMIT 25');
    const recentSubs = await query(`
      SELECT u.email, s.status, s.plan, s.current_period_end, s.updated_at
      FROM subscriptions s JOIN users u ON u.id = s.user_id
      ORDER BY s.updated_at DESC LIMIT 25`);
    res.json({
      totalUsers,
      subscriptionsByStatus: Object.fromEntries(byStatus.map(r => [r.status || 'unknown', r.n])),
      recentSignups,
      recentSubscriptions: recentSubs,
      note: "status trial/trialing = in 7-day trial; active = paying (or gifted/admin). Emails visible only with the admin secret.",
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Review the insider signal sweep (scripts/precompute computeInsiderStudy v4).
// Ranked by 6-month median excess return vs the Russell 2000, with a first-half /
// second-half robustness split. Admin-only.
app.get('/api/admin/signal-sweep', async (req, res) => {
  if (requireAdminSecret(req, res)) return;
  try {
    const row = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'insider-study'");
    if (!row) return res.json({ error: 'not computed yet' });
    const s = JSON.parse(row.value_json);
    if (!s.scenarios) return res.json({ version: s.version, note: 'awaiting v4 sweep (old structure cached)', computed_at: row.computed_at });
    const ranked = Object.entries(s.scenarios).map(([name, x]) => {
      const w6 = x.windows['6M'] || {}, w1 = x.windows['1M'] || {}, w12 = x.windows['12M'] || {}, r = x.robust6M || {};
      return {
        scenario: name, n_6M: w6.n,
        median_6M: w6.medianRet, mean_6M: w6.meanRet, pctUp_6M: w6.pctPositive,
        vsRussell_6M: (w6.rut || {}).medianExcess, beatRussell_6M: (w6.rut || {}).pctBeat,
        vsSP_6M: (w6.spx || {}).medianExcess, beatSP_6M: (w6.spx || {}).pctBeat,
        median_1M: w1.medianRet, vsRussell_1M: (w1.rut || {}).medianExcess,
        vsRussell_12M: (w12.rut || {}).medianExcess,
        robust_1stHalf_6M: r.h1Median, robust_2ndHalf_6M: r.h2Median, h1n: r.h1n, h2n: r.h2n,
      };
    }).sort((a, b) => (b.vsRussell_6M ?? -999) - (a.vsRussell_6M ?? -999));
    res.json({ version: s.version, generated: s.generated, computed_at: row.computed_at, sample: s.sample, benchmarks: s.benchmarks, ranked });
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
    { url: '/biggest-insider-buys', priority: '0.9', freq: 'daily' },
    { url: '/screener', priority: '0.9', freq: 'daily' },
    { url: '/stock', priority: '0.8', freq: 'daily' },
    { url: '/insider', priority: '0.8', freq: 'daily' },
    { url: '/premium', priority: '0.7', freq: 'monthly' },
    { url: '/articles/', priority: '0.7', freq: 'weekly' },
  ];
  const articleSlugs = ['is-insider-buying-bullish','what-it-means-when-a-ceo-buys-stock','insider-buying-near-52-week-lows','what-is-a-10b5-1-plan','first-insider-buy-in-years','real-insider-buys-vs-option-exercises','cluster-buying-example','how-to-find-stocks-with-insider-buying','how-to-read-sec-form-4','how-to-use-insider-data','insider-buying-vs-analyst-upgrades','legal-vs-illegal-insider-trading','what-is-cluster-buying'];
  const articlePages = articleSlugs.map(s => ({ url: `/articles/${s}.html`, priority: '0.6', freq: 'monthly' }));
  // Ticker + insider lists are precomputed by scripts/precompute.js
  // (computeSitemapLists) and read from a single cached row here - running the
  // 5-year GROUP BY scans live cost ~1M rows read each and blew the timeout.
  let tickerPages = [], insiderPages = [];
  try {
    const row = await queryOne("SELECT value_json FROM computed_cache WHERE key = 'sitemap-lists'");
    if (row) {
      const lists = JSON.parse(row.value_json);
      tickerPages = (lists.tickers || []).map(t => ({ url: `/insider-trading/${t}`, priority: '0.5', freq: 'weekly' }));
      const seen = new Set();
      for (const name of (lists.insiders || [])) {
        const slug = _insiderSlug(name);
        if (slug && slug.length >= 2 && !seen.has(slug)) { seen.add(slug); insiderPages.push({ url: `/insider-profile/${slug}`, priority: '0.4', freq: 'weekly' }); }
      }
    }
  } catch(_) {}
  const sectorPages = Object.keys(SECTOR_SLUGS).map(s => ({ url: `/insider-trading/sector/${s}`, priority: '0.5', freq: 'weekly' }));
  const rolePages = Object.keys(ROLE_DEFS).map(s => ({ url: `/insider-trading/role/${s}`, priority: '0.6', freq: 'daily' }));
  const reportPages = []; let _rw = _reportLatest();
  for (let i = 0; i < 12; i++) { reportPages.push({ url: `/insider-buying-report/${_ymd(_rw)}`, priority: i === 0 ? '0.7' : '0.5', freq: i === 0 ? 'daily' : 'monthly' }); _rw = new Date(_rw); _rw.setUTCDate(_rw.getUTCDate() - 7); }
  const allPages = [...staticPages, ...articlePages, ...tickerPages, ...insiderPages, ...sectorPages, ...rolePages, ...reportPages];
  const urls = allPages.map(p => `\n  <url><loc>${base}${p.url}</loc><lastmod>${now}</lastmod><changefreq>${p.freq}</changefreq><priority>${p.priority}</priority></url>`).join('');
  _sitemapCache = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${urls}\n</urlset>`;
  _sitemapCacheTime = Date.now();
  res.type('application/xml');
  res.send(_sitemapCache);
});

// ─── PROGRAMMATIC TICKER PAGES (SEO) ──────────────────────────────────────────
// Server-rendered, indexable pages at /insider-trading/<TICKER>. Unlike the
// client-rendered SPA stock view, these are static HTML Google can crawl, one
// per ticker, targeting long-tail "<company> insider trading" searches. Rendered
// HTML is cached in-memory per ticker to keep Turso rows-read low.
function _esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }
function _fmtV(n) { n = +n || 0; const a = Math.abs(n); if (a >= 1e9) return '$' + (n / 1e9).toFixed(1) + 'B'; if (a >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M'; if (a >= 1e3) return '$' + Math.round(n / 1e3) + 'K'; return '$' + Math.round(n); }
function _fmtQty(n) { n = +n || 0; const a = Math.abs(n); if (a >= 1e6) return (n / 1e6).toFixed(2) + 'M'; if (a >= 1e3) return (n / 1e3).toFixed(1) + 'K'; return String(Math.round(n)); }
function _fmtDate(d) { if (!d) return ''; const dt = new Date(String(d).slice(0, 10) + 'T12:00:00Z'); return isNaN(dt) ? String(d).slice(0, 10) : dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' }); }

function renderTickerPage(ticker, rows, stats) {
  const company = (rows.find(r => r.company && r.company.trim()) || {}).company || ticker;
  const co = _esc(company);
  const url = `https://www.insidertape.com/insider-trading/${ticker}`;
  const buys = stats.buys || 0, sells = stats.sells || 0;
  const posture = buys > sells * 1.5 ? 'net buyers' : sells > buys * 1.5 ? 'net sellers' : 'mixed';
  const intro = `Over the past 12 months, ${stats.insiders || 0} insider${stats.insiders === 1 ? '' : 's'} at ${co} filed ${buys + sells} open-market SEC Form 4 transaction${buys + sells === 1 ? '' : 's'} on ${ticker}: ${buys} purchase${buys === 1 ? '' : 's'} worth ${_fmtV(stats.buyval)} and ${sells} sale${sells === 1 ? '' : 's'} worth ${_fmtV(stats.sellval)}. Insiders have been ${posture} over this period. The most recent filing was on ${_fmtDate(stats.latest)}.`;
  const desc = `Latest insider trading activity for ${company} (${ticker}). Recent SEC Form 4 buys and sells by executives and directors, with dates, share counts, and dollar values.`;

  const tableRows = rows.map(r => {
    const isBuy = r.type === 'P', isSell = r.type === 'S' || r.type === 'S-';
    const badge = isBuy ? '<span class="b buy">BUY</span>' : isSell ? '<span class="b sell">SELL</span>' : '<span class="b">' + _esc(r.type) + '</span>';
    return `<tr>
      <td class="dt">${_fmtDate(r.trade || r.filing)}</td>
      <td class="ins">${r.insider ? `<a href="/insider-profile/${_insiderSlug(r.insider)}"><strong>${_esc(_displayName(r.insider))}</strong></a>` : '<strong>Unknown</strong>'}${r.title ? `<span class="ti">${_esc(r.title)}</span>` : ''}</td>
      <td>${badge}</td>
      <td class="num">${_fmtQty(r.qty)}</td>
      <td class="num">${r.price ? '$' + (+r.price).toFixed(2) : '-'}</td>
      <td class="num val ${isBuy ? 'g' : isSell ? 'r' : ''}">${_fmtV(r.value)}</td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${ticker} Insider Trading - ${co} SEC Form 4 Activity | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="website"><meta property="og:url" content="${url}">
<meta property="og:title" content="${ticker} Insider Trading - ${co}">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'WebPage', name: `${ticker} Insider Trading - ${company}`, description: desc, url })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: `${ticker} Insider Trading`, item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:860px;margin:0 auto;padding:44px 24px 90px}
.crumb{font-size:12px;color:var(--muted);margin-bottom:18px}.crumb a{color:var(--accent);text-decoration:none}
h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:10px}
.sub{font-size:13px;color:var(--muted);margin-bottom:22px}
.intro{font-size:16px;color:#3a4555;line-height:1.8;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.stat .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}
.stat .v{font-size:20px;font-weight:700}.v.g{color:var(--buy)}.v.r{color:var(--sell)}
h2{font-size:18px;font-weight:700;margin:8px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:2px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:top;color:#3a4555}tr:last-child td{border-bottom:none}
.dt{white-space:nowrap;color:var(--muted)}.ins a{text-decoration:none}.ins a:hover strong{color:var(--accent)}.ins strong{color:var(--text);font-weight:600;display:block}.ins .ti{font-size:11px;color:var(--muted)}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}.val.g{color:var(--buy);font-weight:600}.val.r{color:var(--sell);font-weight:600}
.b{font-size:10px;font-weight:700;padding:3px 9px;border-radius:4px;background:#eee;color:var(--muted)}.b.buy{background:rgba(22,122,64,.1);color:var(--buy)}.b.sell{background:rgba(176,48,48,.1);color:var(--sell)}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.rel{margin-top:40px;font-size:13px;color:var(--muted)}.rel a{color:var(--accent);text-decoration:none}.rel li{margin:6px 0}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}table{font-size:12px}th,td{padding:9px 10px}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="crumb"><a href="/">Home</a> &nbsp;/&nbsp; Insider Trading &nbsp;/&nbsp; ${ticker}</div>
  <h1>${co} (${ticker}) Insider Trading Activity</h1>
  <div class="sub">SEC Form 4 open-market purchases and sales by corporate insiders &nbsp;·&nbsp; Sourced from SEC EDGAR</div>
  <p class="intro">${_esc(intro)}</p>
  <div class="stats">
    <div class="stat"><div class="k">Buys (1Y)</div><div class="v g">${buys}</div></div>
    <div class="stat"><div class="k">Sells (1Y)</div><div class="v r">${sells}</div></div>
    <div class="stat"><div class="k">Buy Value</div><div class="v g">${_fmtV(stats.buyval)}</div></div>
    <div class="stat"><div class="k">Insiders</div><div class="v">${stats.insiders || 0}</div></div>
  </div>
  <h2>Recent ${ticker} insider trades</h2>
  <table><thead><tr><th>Date</th><th>Insider</th><th>Type</th><th class="num">Shares</th><th class="num">Price</th><th class="num">Value</th></tr></thead><tbody>${tableRows}</tbody></table>
  <div class="cta">
    <h3>Track ${ticker} insider trades in real time</h3>
    <p>InsiderTape plots every ${co} insider buy and sell on the price chart, with buy/sell pressure, cluster detection, and alerts the moment new Form 4s hit. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/stock/${ticker}" style="font-size:12px;color:var(--muted);text-decoration:none">or open ${ticker} on InsiderTape →</a></div>
  </div>
  <div class="rel">
    <strong>Learn more about insider trading signals:</strong>
    <ul>
      ${(() => { const s = getTickerSector(ticker); const sl = s ? _sectorSlugOf(s[0]) : null; return sl ? `<li><a href="/insider-trading/sector/${sl}">${_esc(s[0])} sector insider trading</a></li>` : ''; })()}
      <li><a href="/articles/is-insider-buying-bullish.html">Is insider buying bullish? What the data says</a></li>
      <li><a href="/articles/what-it-means-when-a-ceo-buys-stock.html">What it means when a CEO buys their own stock</a></li>
      <li><a href="/articles/what-is-cluster-buying.html">What is cluster buying and why it matters</a></li>
    </ul>
  </div>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice. Past insider activity does not predict future results.</footer>
</body></html>`;
}

const _tickerPageCache = new Map(); // ticker -> { html, t }
app.get('/insider-trading/:ticker', async (req, res) => {
  const ticker = (req.params.ticker || '').toUpperCase().replace(/[^A-Z0-9.\-]/g, '').slice(0, 10);
  if (!ticker || !/^[A-Z]/.test(ticker)) return res.redirect(302, '/');
  const hit = _tickerPageCache.get(ticker);
  if (hit && Date.now() - hit.t < 12 * 3600000) { res.type('html'); return res.send(hit.html); }
  try {
    const rows = await query(`
      SELECT company, insider, title, trade_date AS trade, filing_date AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price, MAX(value) AS value
      FROM trades WHERE ticker = ? AND TRIM(type) IN ('P','S','S-')
      GROUP BY insider, trade_date, TRIM(type)
      ORDER BY trade_date DESC, filing_date DESC LIMIT 50`, [ticker]);
    if (!rows.length) {
      res.status(404).type('html');
      return res.send(`<!DOCTYPE html><html><head><meta name="robots" content="noindex"><title>${ticker} | InsiderTape</title><meta http-equiv="refresh" content="0;url=/"></head><body>No insider trading data for ${ticker}. <a href="/">InsiderTape</a></body></html>`);
    }
    const st = await queryOne(`
      SELECT COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
             COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells,
             SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buyval,
             SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sellval,
             COUNT(DISTINCT insider) AS insiders, MAX(trade_date) AS latest
      FROM trades WHERE ticker = ? AND trade_date >= date('now','-365 days') AND TRIM(type) IN ('P','S','S-')`, [ticker]);
    const html = renderTickerPage(ticker, rows, st || {});
    _tickerPageCache.set(ticker, { html, t: Date.now() });
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── PROGRAMMATIC INSIDER PROFILE PAGES (SEO) ─────────────────────────────────
// Server-rendered, indexable pages at /insider-profile/<NAME>. One per corporate
// insider, showing their public open-market Form 4 history as an SEO teaser that
// links into the richer client-rendered profile at /insider/<NAME> and the trial.
// Distinct path from the SPA's /insider/ route so it does not shadow it.
function _titleCaseName(s) {
  return String(s || '').toLowerCase().split(/\s+/).map(w => {
    if (/^(ii|iii|iv|v|vi|vii)$/.test(w)) return w.toUpperCase();
    if (/^(jr|sr)$/.test(w)) return w.charAt(0).toUpperCase() + w.slice(1);
    return w ? w.charAt(0).toUpperCase() + w.slice(1) : w;
  }).join(' ').replace(/\bMc([a-z])/g, (m, c) => 'Mc' + c.toUpperCase()).replace(/\bO'([a-z])/g, (m, c) => "O'" + c.toUpperCase());
}
// Pretty, lowercase-hyphen URL slug for an insider name ("MUSK ELON" -> "musk-elon").
function _insiderSlug(n) { return String(n || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, ''); }
// SEC stores owner names last-first ("MUSK ELON"). For DISPLAY only, flip simple
// person names to natural first-last ("Elon Musk") so titles/headings match how
// people actually search. Entities (funds, corps) and ambiguous names are left as
// filed. Stored data, URLs, and DB lookups stay last-first.
const _ENTITY_RE = /\b(inc|incorporated|llc|llp|lp|ltd|limited|plc|trust|group|partners|partnership|fund|funds|capital|holdings?|management|advisors?|advisers?|corp|corporation|company|ventures?|associates|bank|systems?|technolog\w*|labs?|international|global|financial|securities|investments?|properties|realty|resources|enterprises?|gmbh|ag|nv|foundation|pension|retirement)\b/i;
function _displayName(raw) {
  const s = String(raw || '').trim();
  if (!s) return s;
  if (_ENTITY_RE.test(s)) return _titleCaseName(s); // fund/company: leave order as filed
  // "Last, First Middle" comma form -> "First Middle Last"
  if (s.includes(',')) {
    const [lastPart, restPart = ''] = s.split(',');
    return _titleCaseName(((restPart.trim() + ' ' + lastPart.trim()).trim()) || s);
  }
  let toks = s.split(/\s+/).filter(Boolean);
  const SUFFIX = /^(jr|sr|ii|iii|iv|v)\.?$/i;
  let suffix = '';
  if (toks.length > 2 && SUFFIX.test(toks[toks.length - 1])) suffix = toks.pop();
  // Only reorder unambiguous 2- or 3-token person names; leave the rest as filed
  // (avoids mangling multi-word surnames like "Van Der Beek").
  if (toks.length === 2 || toks.length === 3) {
    const ordered = [...toks.slice(1), toks[0]].join(' ') + (suffix ? ' ' + suffix : '');
    return _titleCaseName(ordered);
  }
  return _titleCaseName(s);
}

function renderInsiderPage(name, rows, stats) {
  const displayName = _displayName(name);
  const dn = _esc(displayName);
  const url = `https://www.insidertape.com/insider-profile/${_insiderSlug(name)}`;
  const buys = stats.buys || 0, sells = stats.sells || 0;
  const companies = stats.companies || 0;
  // Most frequent non-empty title as their headline role.
  const tc = {};
  rows.forEach(r => { const t = (r.title || '').trim(); if (t) tc[t] = (tc[t] || 0) + 1; });
  const role = Object.keys(tc).sort((a, b) => tc[b] - tc[a])[0] || '';
  const posture = buys > sells * 1.5 ? 'a net buyer' : sells > buys * 1.5 ? 'a net seller' : 'a mix of buying and selling';
  const span = stats.first && stats.latest && stats.first !== stats.latest ? `from ${_fmtDate(stats.first)} to ${_fmtDate(stats.latest)}` : `on ${_fmtDate(stats.latest)}`;
  const intro = `${displayName}${role ? `, ${role.toLowerCase().includes('director') || role.toLowerCase().includes('officer') || /ceo|cfo|chief|president|chair/i.test(role) ? '' : 'a '}${role},` : ''} is a corporate insider tracked by InsiderTape across ${companies} ${companies === 1 ? 'company' : 'companies'}. The public record shows ${buys + sells} open-market SEC Form 4 transaction${buys + sells === 1 ? '' : 's'}: ${buys} purchase${buys === 1 ? '' : 's'} worth ${_fmtV(stats.buyval)} and ${sells} sale${sells === 1 ? '' : 's'} worth ${_fmtV(stats.sellval)}, reported ${span}. Over this period they have been ${posture}.`;
  const desc = `Insider trading history for ${displayName}${role ? `, ${role}` : ''}. Recorded SEC Form 4 open-market buys and sells across ${companies} ${companies === 1 ? 'company' : 'companies'}, with dates, share counts, and dollar values.`;

  const tableRows = rows.map(r => {
    const isBuy = r.type === 'P', isSell = r.type === 'S' || r.type === 'S-';
    const badge = isBuy ? '<span class="b buy">BUY</span>' : isSell ? '<span class="b sell">SELL</span>' : '<span class="b">' + _esc(r.type) + '</span>';
    return `<tr>
      <td class="dt">${_fmtDate(r.trade || r.filing)}</td>
      <td class="ins"><a href="/insider-trading/${_esc(r.ticker)}"><strong>${_esc(r.ticker)}</strong></a>${r.company ? `<span class="ti">${_esc(r.company)}</span>` : ''}</td>
      <td>${badge}</td>
      <td class="num">${_fmtQty(r.qty)}</td>
      <td class="num">${r.price ? '$' + (+r.price).toFixed(2) : '-'}</td>
      <td class="num val ${isBuy ? 'g' : isSell ? 'r' : ''}">${_fmtV(r.value)}</td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${dn} Insider Trading - SEC Form 4 History | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="profile"><meta property="og:url" content="${url}">
<meta property="og:title" content="${dn} Insider Trading Activity">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'ProfilePage', mainEntity: { '@type': 'Person', name: displayName, jobTitle: role || undefined }, description: desc, url })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: `${displayName} Insider Trading`, item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:860px;margin:0 auto;padding:44px 24px 90px}
.crumb{font-size:12px;color:var(--muted);margin-bottom:18px}.crumb a{color:var(--accent);text-decoration:none}
h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:10px}
.sub{font-size:13px;color:var(--muted);margin-bottom:22px}
.intro{font-size:16px;color:#3a4555;line-height:1.8;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.stat .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}
.stat .v{font-size:20px;font-weight:700}.v.g{color:var(--buy)}.v.r{color:var(--sell)}
h2{font-size:18px;font-weight:700;margin:8px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:2px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:top;color:#3a4555}tr:last-child td{border-bottom:none}
.dt{white-space:nowrap;color:var(--muted)}.ins a{text-decoration:none}.ins strong{color:var(--accent);font-weight:700;display:block}.ins .ti{font-size:11px;color:var(--muted);max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}.val.g{color:var(--buy);font-weight:600}.val.r{color:var(--sell);font-weight:600}
.b{font-size:10px;font-weight:700;padding:3px 9px;border-radius:4px;background:#eee;color:var(--muted)}.b.buy{background:rgba(22,122,64,.1);color:var(--buy)}.b.sell{background:rgba(176,48,48,.1);color:var(--sell)}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.rel{margin-top:40px;font-size:13px;color:var(--muted)}.rel a{color:var(--accent);text-decoration:none}.rel li{margin:6px 0}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}table{font-size:12px}th,td{padding:9px 10px}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="cta" style="margin-top:0">
    <h3>Follow ${dn}'s trades in real time</h3>
    <p>InsiderTape plots every Form 4 on the price chart and flags cluster buys, CEO conviction, and first buys in years the moment they file. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/insider/${encodeURIComponent(name)}" style="font-size:12px;color:var(--muted);text-decoration:none">or open ${dn}'s full profile on InsiderTape →</a></div>
  </div>
  <div class="rel" style="margin-top:28px;margin-bottom:8px">
    <strong>Learn how to read insider signals:</strong>
    <ul>
      <li><a href="/articles/is-insider-buying-bullish.html">Is insider buying bullish? What the data says</a></li>
      <li><a href="/articles/what-it-means-when-a-ceo-buys-stock.html">What it means when a CEO buys their own stock</a></li>
      <li><a href="/articles/real-insider-buys-vs-option-exercises.html">Real insider buys vs option exercises</a></li>
    </ul>
  </div>
  <div class="crumb" style="margin-top:34px"><a href="/">Home</a> &nbsp;/&nbsp; Insiders &nbsp;/&nbsp; ${dn}</div>
  <h1>${dn} Insider Trading Activity</h1>
  <div class="sub">${role ? _esc(role) + ' &nbsp;·&nbsp; ' : ''}SEC Form 4 open-market purchases and sales &nbsp;·&nbsp; Sourced from SEC EDGAR</div>
  <p class="intro">${_esc(intro)}</p>
  <div class="stats">
    <div class="stat"><div class="k">Buys</div><div class="v g">${buys}</div></div>
    <div class="stat"><div class="k">Sells</div><div class="v r">${sells}</div></div>
    <div class="stat"><div class="k">Buy Value</div><div class="v g">${_fmtV(stats.buyval)}</div></div>
    <div class="stat"><div class="k">Companies</div><div class="v">${companies}</div></div>
  </div>
  <h2>${dn}'s recent insider trades</h2>
  <table><thead><tr><th>Date</th><th>Company</th><th>Type</th><th class="num">Shares</th><th class="num">Price</th><th class="num">Value</th></tr></thead><tbody>${tableRows}</tbody></table>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice. Past insider activity does not predict future results.</footer>
</body></html>`;
}

const _insiderPageCache = new Map(); // upper(name) -> { html, t }
app.get('/insider-profile/:name', async (req, res) => {
  let raw = req.params.name || '';
  try { raw = decodeURIComponent(raw); } catch (_) {}
  raw = raw.replace(/[^A-Za-z0-9 .,'&\/\-]/g, '').replace(/\s+/g, ' ').trim().slice(0, 80);
  if (raw.length < 2) return res.redirect(302, '/');
  // URLs use lowercase-hyphen slugs (musk-elon); convert back to a spaced name for lookup.
  const spaced = raw.replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
  const name = spaced;
  const key = spaced.toUpperCase();
  const hit = _insiderPageCache.get(key);
  if (hit && Date.now() - hit.t < 12 * 3600000) { res.type('html'); return res.send(hit.html); }
  const rowSql = where => `
    SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
           trade_date AS trade, MAX(filing_date) AS filing, TRIM(type) AS type,
           MAX(qty) AS qty, MAX(price) AS price, MAX(value) AS value
    FROM trades WHERE ${where} AND TRIM(type) IN ('P','S','S-') AND COALESCE(value,0) <= 5000000000
    GROUP BY ticker, trade_date, TRIM(type)
    ORDER BY trade_date DESC, filing_date DESC LIMIT 60`;
  try {
    // Indexed via idx_insider_upper (expression index on UPPER(insider)).
    let rows = await query(rowSql('UPPER(insider) = UPPER(?)'), [spaced]);
    // Fall back to the raw slug (with hyphens kept) for genuinely hyphenated surnames.
    if (!rows.length && raw !== spaced) rows = await query(rowSql('UPPER(insider) = UPPER(?)'), [raw]);
    if (!rows.length) {
      res.status(404).type('html');
      return res.send(`<!DOCTYPE html><html><head><meta name="robots" content="noindex"><title>${_esc(name)} | InsiderTape</title><meta http-equiv="refresh" content="0;url=/"></head><body>No insider trading data for ${_esc(name)}. <a href="/">InsiderTape</a></body></html>`);
    }
    const canonical = rows[0].insider || name;
    const st = await queryOne(`
      SELECT SUM(CASE WHEN type='P' THEN 1 ELSE 0 END) AS buys,
             SUM(CASE WHEN type IN ('S','S-') THEN 1 ELSE 0 END) AS sells,
             SUM(CASE WHEN type='P' THEN val ELSE 0 END) AS buyval,
             SUM(CASE WHEN type IN ('S','S-') THEN val ELSE 0 END) AS sellval,
             COUNT(DISTINCT ticker) AS companies, MAX(latest) AS latest, MIN(first) AS first
      FROM (SELECT ticker, TRIM(type) AS type, MAX(COALESCE(value,0)) AS val,
                   MAX(trade_date) AS latest, MIN(trade_date) AS first
            FROM trades WHERE insider = ? AND TRIM(type) IN ('P','S','S-') AND COALESCE(value,0) <= 5000000000
            GROUP BY ticker, trade_date, TRIM(type))`, [canonical]);
    const html = renderInsiderPage(canonical, rows, st || {});
    _insiderPageCache.set(key, { html, t: Date.now() });
    res.type('html').send(html);
  } catch (e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── FREE SHAREABLE PAGE: BIGGEST INSIDER BUYS THIS WEEK ───────────────────────
// Ungated, server-rendered, auto-updating link-bait: the largest open-market
// insider purchases in the last 7 days, ranked by value. Great for sharing on
// social and for SEO; links out to the per-ticker pages and into the trial.
function renderBiggestBuysPage(rows) {
  const today = new Date();
  const updated = today.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  const totalVal = rows.reduce((s, r) => s + (+r.buy_val || 0), 0);
  const totalTrades = rows.reduce((s, r) => s + (+r.trades || 0), 0);
  const url = 'https://www.insidertape.com/biggest-insider-buys';
  const desc = `The biggest open-market insider buys this week: the ${rows.length} companies where executives and directors bought the most stock, ranked by dollar value. Updated daily from SEC Form 4 filings.`;

  const tr = rows.map((r, i) => {
    const co = _esc(r.company || r.ticker);
    return `<tr>
      <td class="rk">${i + 1}</td>
      <td class="tk"><a href="/insider-trading/${_esc(r.ticker)}"><strong>${_esc(r.ticker)}</strong><span class="co">${co}</span></a></td>
      <td class="num">${r.insiders || 0}</td>
      <td class="num">${r.trades || 0}</td>
      <td class="num v">${_fmtV(r.buy_val)}</td>
      <td class="dt">${_fmtDate(r.latest)}</td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Biggest Insider Buys This Week | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="website"><meta property="og:url" content="${url}">
<meta property="og:title" content="Biggest Insider Buys This Week">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'WebPage', name: 'Biggest Insider Buys This Week', description: desc, url, dateModified: today.toISOString().slice(0, 10) })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: 'Biggest Insider Buys This Week', item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:880px;margin:0 auto;padding:44px 24px 90px}
.tag{display:inline-block;padding:3px 10px;background:rgba(22,122,64,.08);border:1px solid rgba(22,122,64,.2);border-radius:20px;font-size:10px;font-weight:700;color:var(--buy);letter-spacing:.5px;text-transform:uppercase;margin-bottom:16px}
h1{font-size:clamp(28px,5vw,42px);font-weight:800;letter-spacing:-.5px;line-height:1.12;margin-bottom:12px}
.sub{font-size:15px;color:#3a4555;line-height:1.7;margin-bottom:8px;max-width:620px}
.upd{font-size:12px;color:var(--muted);margin-bottom:26px}
.summary{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:30px}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.card .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}.card .v{font-size:22px;font-weight:800}.card .v.g{color:var(--buy)}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:14px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:12px 14px;border-bottom:2px solid var(--border)}
td{padding:12px 14px;border-bottom:1px solid var(--border);vertical-align:middle}tr:last-child td{border-bottom:none}tr:hover td{background:rgba(36,120,204,.03)}
.rk{color:var(--muted);font-weight:700;width:38px;font-variant-numeric:tabular-nums}
.tk a{text-decoration:none;color:inherit;display:flex;flex-direction:column}.tk strong{color:var(--accent);font-weight:700;font-size:15px}.tk .co{font-size:11px;color:var(--muted);max-width:230px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;color:#3a4555}.num.v{color:var(--buy);font-weight:700}
.dt{text-align:right;white-space:nowrap;color:var(--muted);font-size:12px}
.note{font-size:13px;color:var(--muted);margin:22px 0 0;line-height:1.7}.note a{color:var(--accent);text-decoration:none}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:38px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.soft{margin-top:12px}.soft a{font-size:12px;color:var(--muted);text-decoration:none}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.summary{grid-template-columns:1fr}table{font-size:12px}th,td{padding:9px 8px}.tk .co{max-width:130px}th:nth-child(4),td:nth-child(4){display:none}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/biggest-insider-buys">Top Buys</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="tag">Updated Daily &nbsp;·&nbsp; Free</div>
  <h1>Biggest Insider Buys This Week</h1>
  <p class="sub">The largest open-market insider purchases filed with the SEC over the past 7 days, ranked by dollar value. Only genuine open-market buys, with option exercises, grants, and plan sales stripped out.</p>
  <div class="upd">Updated ${updated}</div>
  <div class="share-row" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:0 0 26px">
    <span style="font-size:11px;color:#6e7a8a;letter-spacing:1px;text-transform:uppercase;font-weight:600">Share</span>
    <a href="#" onclick="return sx('x')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">Post on X</a>
    <a href="#" onclick="return sx('reddit')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">Reddit</a>
    <a href="#" onclick="return sx('linkedin')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">LinkedIn</a>
    <button type="button" onclick="sx('copy',this)" style="font-size:12px;font-weight:600;color:#1a2030;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer;font-family:inherit">Copy link</button>
  </div>
  <div class="summary">
    <div class="card"><div class="k">Companies</div><div class="v">${rows.length}</div></div>
    <div class="card"><div class="k">Buy Transactions</div><div class="v">${totalTrades}</div></div>
    <div class="card"><div class="k">Total Insider Buying</div><div class="v g">${_fmtV(totalVal)}</div></div>
  </div>
  <table><thead><tr><th>#</th><th>Company</th><th class="num">Insiders</th><th class="num">Buys</th><th class="num">Total Bought</th><th class="dt">Latest</th></tr></thead><tbody>${tr}</tbody></table>
  <p class="note">These are open-market purchases: shares insiders chose to buy at the market price with their own money, which historically carries a far stronger signal than grants or option exercises. See the <a href="/insider-buying-report">weekly insider buying report</a> for the full breakdown, or read <a href="/articles/is-insider-buying-bullish.html">whether insider buying is bullish</a> and <a href="/articles/what-is-cluster-buying.html">what cluster buying means</a>.</p>
  <div class="cta">
    <h3>See these buys plotted on the chart</h3>
    <p>InsiderTape tracks every SEC Form 4 in real time and flags cluster buys, CEO conviction, first buys in years, and buying at the lows the moment they file. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div class="soft"><a href="/">or explore the live screener free →</a></div>
  </div>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice</footer>
<script>function sx(k,el){var u=encodeURIComponent(location.href.split('#')[0]);var t=encodeURIComponent((document.title||'').split('|')[0].trim());var m={x:'https://twitter.com/intent/tweet?text='+t+'&url='+u,reddit:'https://www.reddit.com/submit?url='+u+'&title='+t,linkedin:'https://www.linkedin.com/sharing/share-offsite/?url='+u};if(k==='copy'){try{navigator.clipboard.writeText(location.href.split('#')[0]);}catch(e){}if(el){var o=el.textContent;el.textContent='Copied!';setTimeout(function(){el.textContent=o;},1500);}return false;}window.open(m[k],'_blank','noopener,noreferrer,width=600,height=520');return false;}</script>
</body></html>`;
}

let _biggestBuysCache = null;
app.get('/biggest-insider-buys', async (req, res) => {
  if (_biggestBuysCache && Date.now() - _biggestBuysCache.t < 2 * 3600000) { res.type('html'); return res.send(_biggestBuysCache.html); }
  try {
    const rows = await query(`
      SELECT ticker, MAX(company) AS company,
             COUNT(DISTINCT insider) AS insiders, COUNT(*) AS trades,
             SUM(COALESCE(value,0)) AS buy_val, MAX(trade_date) AS latest
      FROM trades
      WHERE TRIM(type)='P' AND trade_date >= date('now','-7 days')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(value,0) >= 10000
      GROUP BY ticker HAVING buy_val > 0
      ORDER BY buy_val DESC LIMIT 30`);
    const html = renderBiggestBuysPage(rows || []);
    _biggestBuysCache = { html, t: Date.now() };
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── PROGRAMMATIC SECTOR PAGES (SEO) ──────────────────────────────────────────
// Server-rendered, indexable pages at /insider-trading/sector/<slug>, one per
// GICS-style sector, targeting "<sector> insider buying" searches. Aggregates
// open-market Form 4 activity across the mapped tickers in each sector.
const SECTOR_SLUGS = {
  'technology': 'Technology', 'healthcare': 'Healthcare', 'financial-services': 'Financial Services',
  'energy': 'Energy', 'consumer-cyclical': 'Consumer Cyclical', 'consumer-defensive': 'Consumer Defensive',
  'industrials': 'Industrials', 'communication-services': 'Communication Services',
  'basic-materials': 'Basic Materials', 'utilities': 'Utilities', 'real-estate': 'Real Estate',
};
const _sectorSlugOf = name => Object.keys(SECTOR_SLUGS).find(s => SECTOR_SLUGS[s] === name);

function renderSectorPage(sector, slug, rows, stats) {
  const co = _esc(sector);
  const url = `https://www.insidertape.com/insider-trading/sector/${slug}`;
  const buys = stats.buys || 0, sells = stats.sells || 0;
  const posture = stats.buyval > stats.sellval * 1.5 ? 'net buyers' : stats.sellval > stats.buyval * 1.5 ? 'net sellers' : 'mixed';
  const intro = `Over the past 12 months, corporate insiders across ${stats.companies || 0} ${co} companies filed ${buys + sells} open-market SEC Form 4 transaction${buys + sells === 1 ? '' : 's'}: ${buys} purchase${buys === 1 ? '' : 's'} worth ${_fmtV(stats.buyval)} and ${sells} sale${sells === 1 ? '' : 's'} worth ${_fmtV(stats.sellval)}. Insiders in the ${co} sector have been ${posture} over this period.`;
  const desc = `${sector} sector insider trading: which ${sector} stocks executives and directors are buying and selling on the open market, ranked by insider buy value. Live SEC Form 4 data.`;

  const tableRows = rows.map(r => {
    const sentiment = (r.buy_val + r.sell_val) > 0 ? Math.round(r.buy_val / (r.buy_val + r.sell_val) * 100) : 0;
    return `<tr>
      <td class="tk"><a href="/insider-trading/${_esc(r.ticker)}"><strong>${_esc(r.ticker)}</strong><span class="co">${_esc(r.company || r.ticker)}</span></a></td>
      <td class="num g">${r.buy_count || 0}</td>
      <td class="num r">${r.sell_count || 0}</td>
      <td class="num v">${_fmtV(r.buy_val)}</td>
      <td class="num" style="color:${sentiment >= 50 ? 'var(--buy)' : 'var(--muted)'}">${sentiment}%</td>
      <td class="dt">${_fmtDate(r.latest)}</td>
    </tr>`;
  }).join('');

  const otherSectors = Object.keys(SECTOR_SLUGS).filter(s => s !== slug)
    .map(s => `<a href="/insider-trading/sector/${s}">${_esc(SECTOR_SLUGS[s])}</a>`).join(' &nbsp;·&nbsp; ');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${co} Insider Buying - Which ${co} Stocks Insiders Are Buying | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="website"><meta property="og:url" content="${url}">
<meta property="og:title" content="${co} Sector Insider Buying &amp; Selling">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'WebPage', name: `${sector} Sector Insider Trading`, description: desc, url })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: `${sector} Insider Trading`, item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:880px;margin:0 auto;padding:44px 24px 90px}
.crumb{font-size:12px;color:var(--muted);margin-bottom:18px}.crumb a{color:var(--accent);text-decoration:none}
h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:10px}
.sub{font-size:13px;color:var(--muted);margin-bottom:22px}
.intro{font-size:16px;color:#3a4555;line-height:1.8;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.stat .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}
.stat .v{font-size:20px;font-weight:700}.v.g{color:var(--buy)}.v.r{color:var(--sell)}
h2{font-size:18px;font-weight:700;margin:8px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:2px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:middle;color:#3a4555}tr:last-child td{border-bottom:none}
.tk a{text-decoration:none;color:inherit;display:flex;flex-direction:column}.tk strong{color:var(--accent);font-weight:700;font-size:14px}.tk .co{font-size:11px;color:var(--muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}.num.g{color:var(--buy)}.num.r{color:var(--sell)}.num.v{color:var(--buy);font-weight:700}
.dt{text-align:right;white-space:nowrap;color:var(--muted);font-size:12px}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.rel{margin-top:40px;font-size:13px;color:var(--muted);line-height:1.9}.rel a{color:var(--accent);text-decoration:none}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}table{font-size:12px}th,td{padding:9px 10px}th:nth-child(5),td:nth-child(5){display:none}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/biggest-insider-buys">Top Buys</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="cta" style="margin-top:0">
    <h3>Track ${co} insider buys in real time</h3>
    <p>InsiderTape plots every ${co} insider buy and sell on the price chart and flags cluster buys, CEO conviction, and first buys in years the moment they file. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/" style="font-size:12px;color:var(--muted);text-decoration:none">or explore the live screener free →</a></div>
  </div>
  <div class="crumb" style="margin-top:34px"><a href="/">Home</a> &nbsp;/&nbsp; Insider Trading &nbsp;/&nbsp; ${co} Sector</div>
  <h1>${co} Sector Insider Trading</h1>
  <div class="sub">Open-market SEC Form 4 buying and selling by insiders at ${co} companies &nbsp;·&nbsp; Last 12 months</div>
  <p class="intro">${_esc(intro)}</p>
  <div class="stats">
    <div class="stat"><div class="k">Buys</div><div class="v g">${buys}</div></div>
    <div class="stat"><div class="k">Sells</div><div class="v r">${sells}</div></div>
    <div class="stat"><div class="k">Buy Value</div><div class="v g">${_fmtV(stats.buyval)}</div></div>
    <div class="stat"><div class="k">Companies</div><div class="v">${stats.companies || 0}</div></div>
  </div>
  <h2>${co} stocks with insider activity</h2>
  ${rows.length ? `<table><thead><tr><th>Company</th><th class="num">Buys</th><th class="num">Sells</th><th class="num">Buy Value</th><th class="num">Bullish</th><th class="dt">Latest</th></tr></thead><tbody>${tableRows}</tbody></table>`
    : `<p style="color:var(--muted);font-size:14px">No open-market insider transactions recorded for tracked ${co} companies in the last 12 months.</p>`}
  <div class="rel">
    <strong>Insider trading by sector:</strong><br>${otherSectors}
    <div style="margin-top:18px"><strong>Learn more:</strong> <a href="/articles/is-insider-buying-bullish.html">Is insider buying bullish?</a> &nbsp;·&nbsp; <a href="/articles/what-is-cluster-buying.html">What is cluster buying?</a> &nbsp;·&nbsp; <a href="/biggest-insider-buys">Biggest insider buys this week</a></div>
  </div>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice. Sector membership covers major tracked tickers.</footer>
</body></html>`;
}

const _sectorPageCache = new Map(); // slug -> { html, t }
app.get('/insider-trading/sector/:slug', async (req, res) => {
  const slug = (req.params.slug || '').toLowerCase().replace(/[^a-z-]/g, '');
  const sector = SECTOR_SLUGS[slug];
  if (!sector) return res.redirect(302, '/');
  const hit = _sectorPageCache.get(slug);
  if (hit && Date.now() - hit.t < 12 * 3600000) { res.type('html'); return res.send(hit.html); }
  try {
    const tickers = Object.keys(TICKER_SECTOR_MAP).filter(t => TICKER_SECTOR_MAP[t][0] === sector);
    if (!tickers.length) { res.type('html'); return res.send(renderSectorPage(sector, slug, [], {})); }
    const ph = tickers.map(() => '?').join(',');
    const rows = await query(`
      SELECT ticker, MAX(company) AS company,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
        MAX(trade_date) AS latest
      FROM trades
      WHERE ticker IN (${ph}) AND trade_date >= date('now','-365 days')
        AND TRIM(type) IN ('P','S','S-') AND COALESCE(value,0) >= 10000
      GROUP BY ticker HAVING buy_count > 0 OR sell_count > 0
      ORDER BY buy_val DESC, sell_val DESC`, tickers);
    const stats = rows.reduce((a, r) => {
      a.buys += r.buy_count || 0; a.sells += r.sell_count || 0;
      a.buyval += r.buy_val || 0; a.sellval += r.sell_val || 0;
      if ((r.buy_count || 0) > 0 || (r.sell_count || 0) > 0) a.companies++;
      return a;
    }, { buys: 0, sells: 0, buyval: 0, sellval: 0, companies: 0 });
    const html = renderSectorPage(sector, slug, rows, stats);
    _sectorPageCache.set(slug, { html, t: Date.now() });
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── PROGRAMMATIC ROLE PAGES (SEO) ────────────────────────────────────────────
// Server-rendered pages at /insider-trading/role/<slug> targeting "<role> buying
// stocks" searches (e.g. "CEO insider buying"). Aggregates recent open-market
// purchases across the whole database by insiders whose title matches the role.
const ROLE_DEFS = {
  'ceo':              { name: 'CEO',        plural: 'CEOs',            where: "(UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CHIEF EXECUTIVE%')" },
  'cfo':              { name: 'CFO',        plural: 'CFOs',            where: "(UPPER(title) LIKE '%CFO%' OR UPPER(title) LIKE '%CHIEF FINANCIAL%')" },
  'coo':              { name: 'COO',        plural: 'COOs',            where: "(UPPER(title) LIKE '%COO%' OR UPPER(title) LIKE '%CHIEF OPERATING%')" },
  'president':        { name: 'President',  plural: 'presidents',      where: "(UPPER(title) LIKE '%PRESIDENT%' AND UPPER(title) NOT LIKE '%VICE PRESIDENT%')" },
  'chairman':        { name: 'Chairman',   plural: 'chairs',          where: "(UPPER(title) LIKE '%CHAIRMAN%' OR UPPER(title) LIKE '%CHAIRPERSON%')" },
  'director':        { name: 'Director',   plural: 'directors',       where: "(UPPER(title) LIKE '%DIRECTOR%' AND UPPER(title) NOT LIKE '%MANAGING DIRECTOR%')" },
  '10-percent-owner': { name: '10% Owner',  plural: '10% owners',      where: "(UPPER(title) LIKE '%10%OWNER%' OR UPPER(title) LIKE '%TEN PERCENT%')" },
};

function renderRolePage(slug, def, rows, stats) {
  const role = def.name;
  const url = `https://www.insidertape.com/insider-trading/role/${slug}`;
  const intro = `Over the past 90 days, ${stats.insiders || 0} ${def.plural} made ${stats.buys || 0} open-market purchase${stats.buys === 1 ? '' : 's'} worth ${_fmtV(stats.buyval)} across ${stats.companies || 0} ${stats.companies === 1 ? 'company' : 'companies'}. These are ${role} buys filed with the SEC on Form 4, with option exercises and awards stripped out so only genuine open-market conviction is shown.`;
  const desc = `Which stocks ${def.plural} are buying: recent open-market ${role} insider purchases from SEC Form 4 filings, ranked by value. Company, insider, shares, and dollar value.`;

  const tableRows = rows.map(r => `<tr>
      <td class="dt">${_fmtDate(r.trade || r.filing)}</td>
      <td class="ins"><a href="/insider-profile/${_insiderSlug(r.insider)}"><strong>${_esc(_displayName(r.insider))}</strong></a>${r.title ? `<span class="ti">${_esc(r.title)}</span>` : ''}</td>
      <td class="tk"><a href="/insider-trading/${_esc(r.ticker)}"><strong>${_esc(r.ticker)}</strong><span class="co">${_esc(r.company || r.ticker)}</span></a></td>
      <td class="num">${_fmtQty(r.qty)}</td>
      <td class="num">${r.price ? '$' + (+r.price).toFixed(2) : '-'}</td>
      <td class="num v">${_fmtV(r.value)}</td>
    </tr>`).join('');

  const otherRoles = Object.keys(ROLE_DEFS).filter(s => s !== slug)
    .map(s => `<a href="/insider-trading/role/${s}">${_esc(ROLE_DEFS[s].name)} buys</a>`).join(' &nbsp;·&nbsp; ');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${role} Insider Buying - Stocks ${def.plural.charAt(0).toUpperCase() + def.plural.slice(1)} Are Buying | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="website"><meta property="og:url" content="${url}">
<meta property="og:title" content="${role} Insider Buying - What ${def.plural} Are Buying">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'WebPage', name: `${role} Insider Buying`, description: desc, url })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: `${role} Insider Buying`, item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:900px;margin:0 auto;padding:44px 24px 90px}
.crumb{font-size:12px;color:var(--muted);margin-bottom:18px}.crumb a{color:var(--accent);text-decoration:none}
h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:10px}
.sub{font-size:13px;color:var(--muted);margin-bottom:22px}
.intro{font-size:16px;color:#3a4555;line-height:1.8;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.stat .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}
.stat .v{font-size:20px;font-weight:700}.v.g{color:var(--buy)}
h2{font-size:18px;font-weight:700;margin:8px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:2px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:top;color:#3a4555}tr:last-child td{border-bottom:none}
.dt{white-space:nowrap;color:var(--muted)}
.ins a,.tk a{text-decoration:none;color:inherit}.ins strong{color:var(--text);font-weight:600;display:block}.ins .ti{font-size:11px;color:var(--muted);display:block;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tk strong{color:var(--accent);font-weight:700;display:block}.tk .co{font-size:11px;color:var(--muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}.num.v{color:var(--buy);font-weight:700}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.rel{margin-top:40px;font-size:13px;color:var(--muted);line-height:1.9}.rel a{color:var(--accent);text-decoration:none}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}table{font-size:12px}th,td{padding:9px 10px}th:nth-child(4),td:nth-child(4),th:nth-child(5),td:nth-child(5){display:none}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/biggest-insider-buys">Top Buys</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="cta" style="margin-top:0">
    <h3>Get ${role} buys the moment they file</h3>
    <p>InsiderTape flags ${role} conviction buys, cluster buying, and first buys in years in real time, plotted on the price chart. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/" style="font-size:12px;color:var(--muted);text-decoration:none">or explore the live screener free →</a></div>
  </div>
  <div class="crumb" style="margin-top:34px"><a href="/">Home</a> &nbsp;/&nbsp; Insider Trading &nbsp;/&nbsp; ${role} Buying</div>
  <h1>${role} Insider Buying</h1>
  <div class="sub">Recent open-market ${role} stock purchases from SEC Form 4 filings &nbsp;·&nbsp; Last 90 days</div>
  <p class="intro">${_esc(intro)}</p>
  <div class="stats">
    <div class="stat"><div class="k">${role} Buys</div><div class="v g">${stats.buys || 0}</div></div>
    <div class="stat"><div class="k">Total Value</div><div class="v g">${_fmtV(stats.buyval)}</div></div>
    <div class="stat"><div class="k">Companies</div><div class="v">${stats.companies || 0}</div></div>
    <div class="stat"><div class="k">${role}s</div><div class="v">${stats.insiders || 0}</div></div>
  </div>
  <h2>Recent ${role} open-market buys</h2>
  ${rows.length ? `<table><thead><tr><th>Date</th><th>Insider</th><th>Company</th><th class="num">Shares</th><th class="num">Price</th><th class="num">Value</th></tr></thead><tbody>${tableRows}</tbody></table>`
    : `<p style="color:var(--muted);font-size:14px">No open-market ${role} purchases recorded in the last 90 days.</p>`}
  <div class="rel">
    <strong>Insider buying by role:</strong><br>${otherRoles}
    <div style="margin-top:18px"><strong>Learn more:</strong> <a href="/articles/what-it-means-when-a-ceo-buys-stock.html">What it means when a CEO buys stock</a> &nbsp;·&nbsp; <a href="/articles/is-insider-buying-bullish.html">Is insider buying bullish?</a> &nbsp;·&nbsp; <a href="/biggest-insider-buys">Biggest insider buys this week</a></div>
  </div>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice. Past insider activity does not predict future results.</footer>
</body></html>`;
}

const _rolePageCache = new Map(); // slug -> { html, t }
app.get('/insider-trading/role/:slug', async (req, res) => {
  const slug = (req.params.slug || '').toLowerCase().replace(/[^a-z0-9-]/g, '');
  const def = ROLE_DEFS[slug];
  if (!def) return res.redirect(302, '/');
  const hit = _rolePageCache.get(slug);
  if (hit && Date.now() - hit.t < 12 * 3600000) { res.type('html'); return res.send(hit.html); }
  try {
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             MAX(qty) AS qty, MAX(price) AS price, MAX(value) AS value
      FROM trades
      WHERE TRIM(type)='P' AND ${def.where} AND trade_date >= date('now','-90 days')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6 AND COALESCE(value,0) >= 10000
      GROUP BY ticker, insider, trade_date
      ORDER BY value DESC LIMIT 40`);
    const st = await queryOne(`
      SELECT COUNT(*) AS buys, SUM(value) AS buyval, COUNT(DISTINCT ticker) AS companies, COUNT(DISTINCT insider) AS insiders
      FROM (SELECT ticker, insider, trade_date, MAX(COALESCE(value,0)) AS value
            FROM trades
            WHERE TRIM(type)='P' AND ${def.where} AND trade_date >= date('now','-90 days')
              AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6 AND COALESCE(value,0) >= 10000
            GROUP BY ticker, insider, trade_date)`);
    const html = renderRolePage(slug, def, rows || [], st || {});
    _rolePageCache.set(slug, { html, t: Date.now() });
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── WEEKLY INSIDER BUYING REPORT (SEO + shareable, backlink asset) ────────────
// Permanent dated pages at /insider-buying-report/<YYYY-MM-DD> (week ending that
// Sunday), computed on demand from historical Form 4 data. Public data only:
// biggest buys, most-bought stocks, sector breakdown, headline stats. No premium
// signals (no cluster buying, no first-buy-in-years).
function _ymd(d) { return new Date(d).toISOString().slice(0, 10); }
function _reportSunday(d) { const x = new Date(d); x.setUTCHours(12, 0, 0, 0); x.setUTCDate(x.getUTCDate() - x.getUTCDay()); return x; }
function _reportLatest() { return _reportSunday(new Date()); }
function _ymdAdd(ymd, days) { const d = new Date(ymd + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + days); return _ymd(d); }
function _humanRange(startYmd, endYmd) {
  const s = new Date(startYmd + 'T12:00:00Z'), e = new Date(endYmd + 'T12:00:00Z');
  const sMonth = s.toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
  const eMonth = e.toLocaleDateString('en-US', { month: 'long', timeZone: 'UTC' });
  const sDay = s.getUTCDate(), eDay = e.getUTCDate(), y = e.getUTCFullYear();
  return sMonth === eMonth ? `${sMonth} ${sDay}–${eDay}, ${y}` : `${sMonth} ${sDay} – ${eMonth} ${eDay}, ${y}`;
}
function _weekLabel(endYmd) { return _humanRange(_ymdAdd(endYmd, -6), endYmd); }

function renderReportPage(endYmd, startYmd, data) {
  const rangeLabel = _humanRange(startYmd, endYmd);
  const url = `https://www.insidertape.com/insider-buying-report/${endYmd}`;
  const desc = `Insider buying report for the week of ${rangeLabel}: the biggest open-market insider purchases, most-bought stocks, and which sectors insiders bought, from SEC Form 4 filings. ${data.companies} companies, ${_fmtV(data.buyval)} in insider buying.`;
  const posture = data.buys > 0 ? `Insiders bought ${_fmtV(data.buyval)} of stock across ${data.companies} ${data.companies === 1 ? 'company' : 'companies'}` : 'Insider buying was quiet';
  const topSector = data.sectors[0];
  const intro = data.buys > 0
    ? `${posture} the week of ${rangeLabel}, in ${data.buys} open-market purchase${data.buys === 1 ? '' : 's'} by ${data.buyers} insider${data.buyers === 1 ? '' : 's'}.${topSector ? ` The ${topSector.sector} sector saw the most insider buying, at ${_fmtV(topSector.buyval)}.` : ''} These are genuine open-market buys filed on SEC Form 4, with option exercises and awards stripped out.`
    : `Open-market insider buying was light the week of ${rangeLabel}. These figures cover genuine open-market purchases filed on SEC Form 4, with option exercises and awards stripped out.`;

  const bigRows = data.biggest.map((r, i) => `<tr>
      <td class="rk">${i + 1}</td>
      <td class="tk"><a href="/insider-trading/${_esc(r.ticker)}"><strong>${_esc(r.ticker)}</strong><span class="co">${_esc(r.company || r.ticker)}</span></a></td>
      <td class="ins"><a href="/insider-profile/${_insiderSlug(r.insider)}">${_esc(_displayName(r.insider))}</a>${r.title ? `<span class="ti">${_esc(r.title)}</span>` : ''}</td>
      <td class="dt">${_fmtDate(r.trade_date)}</td>
      <td class="num v">${_fmtV(r.value)}</td>
    </tr>`).join('');

  const mostRows = data.mostBought.map(t => `<tr>
      <td class="tk"><a href="/insider-trading/${_esc(t.ticker)}"><strong>${_esc(t.ticker)}</strong><span class="co">${_esc(t.company || t.ticker)}</span></a></td>
      <td class="num">${t.buyers}</td>
      <td class="num">${t.buys}</td>
      <td class="num v">${_fmtV(t.buyval)}</td>
    </tr>`).join('');

  const sectorRows = data.sectors.map(s => `<tr>
      <td><a href="/insider-trading/sector/${Object.keys(SECTOR_SLUGS).find(k => SECTOR_SLUGS[k] === s.sector) || ''}" style="color:var(--accent);text-decoration:none">${_esc(s.sector)}</a></td>
      <td class="num">${s.buys}</td>
      <td class="num v">${_fmtV(s.buyval)}</td>
    </tr>`).join('');

  // Recent weeks nav (last 10 weeks from latest), for internal linking.
  const weeks = []; let wd = _reportLatest();
  for (let i = 0; i < 10; i++) { const y = _ymd(wd); weeks.push(y); wd = new Date(wd); wd.setUTCDate(wd.getUTCDate() - 7); }
  const weeksNav = weeks.map(y => y === endYmd ? `<strong style="color:var(--text)">${_weekLabel(y)}</strong>` : `<a href="/insider-buying-report/${y}">${_weekLabel(y)}</a>`).join(' &nbsp;·&nbsp; ');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Insider Buying Report: Week of ${rangeLabel} | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="article"><meta property="og:url" content="${url}">
<meta property="og:title" content="Insider Buying Report: Week of ${rangeLabel}">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'Article', headline: `Insider Buying Report: Week of ${rangeLabel}`, description: desc, url, datePublished: endYmd, author: { '@type': 'Organization', name: 'InsiderTape' }, publisher: { '@type': 'Organization', name: 'InsiderTape' } })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: 'Insider Buying Report', item: 'https://www.insidertape.com/insider-buying-report' }, { '@type': 'ListItem', position: 3, name: `Week of ${rangeLabel}`, item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:900px;margin:0 auto;padding:44px 24px 90px}
.tag{display:inline-block;padding:3px 10px;background:rgba(22,122,64,.08);border:1px solid rgba(22,122,64,.2);border-radius:20px;font-size:10px;font-weight:700;color:var(--buy);letter-spacing:.5px;text-transform:uppercase;margin-bottom:16px}
.crumb{font-size:12px;color:var(--muted);margin-bottom:14px}.crumb a{color:var(--accent);text-decoration:none}
h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.5px;line-height:1.15;margin-bottom:12px}
.intro{font-size:16px;color:#3a4555;line-height:1.8;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:34px}
.stat{background:var(--bg2);border:1px solid var(--border);border-radius:9px;padding:14px 16px}
.stat .k{font-size:10px;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-bottom:6px}
.stat .v{font-size:20px;font-weight:800}.v.g{color:var(--buy)}
h2{font-size:18px;font-weight:700;margin:34px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px;margin-bottom:6px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:2px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:top;color:#3a4555}tr:last-child td{border-bottom:none}
.rk{color:var(--muted);font-weight:700;width:34px;font-variant-numeric:tabular-nums}
.tk a,.ins a{text-decoration:none;color:inherit}.tk strong{color:var(--accent);font-weight:700;display:block}.tk .co{font-size:11px;color:var(--muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block}
.ins a{color:var(--text);font-weight:600}.ins .ti{font-size:11px;color:var(--muted);display:block;max-width:190px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dt{white-space:nowrap;color:var(--muted);font-size:12px}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}.num.v{color:var(--buy);font-weight:700}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
.rel{margin-top:36px;font-size:13px;color:var(--muted);line-height:1.9}.rel a{color:var(--accent);text-decoration:none}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){.stats{grid-template-columns:1fr 1fr}table{font-size:12px}th,td{padding:9px 9px}.tk .co{max-width:120px}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/biggest-insider-buys">Top Buys</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="cta" style="margin-top:0">
    <h3>See these buys plotted on the chart</h3>
    <p>InsiderTape tracks every SEC Form 4 in real time and shows each insider buy and sell right on the price chart. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/biggest-insider-buys" style="font-size:12px;color:var(--muted);text-decoration:none">or see the biggest insider buys this week →</a></div>
  </div>
  <div class="rel" style="margin-top:28px;margin-bottom:6px">
    <strong>Past reports:</strong><br>${weeksNav}
  </div>
  <div class="tag" style="margin-top:34px">Weekly Report</div>
  <div class="crumb"><a href="/">Home</a> &nbsp;/&nbsp; <a href="/insider-buying-report">Insider Buying Report</a> &nbsp;/&nbsp; ${rangeLabel}</div>
  <h1>Insider Buying Report: ${rangeLabel}</h1>
  <p class="intro">${_esc(intro)}</p>
  <div class="share-row" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:0 0 30px">
    <span style="font-size:11px;color:#6e7a8a;letter-spacing:1px;text-transform:uppercase;font-weight:600">Share</span>
    <a href="#" onclick="return sx('x')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">Post on X</a>
    <a href="#" onclick="return sx('reddit')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">Reddit</a>
    <a href="#" onclick="return sx('linkedin')" style="font-size:12px;font-weight:600;color:#1a2030;text-decoration:none;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer">LinkedIn</a>
    <button type="button" onclick="sx('copy',this)" style="font-size:12px;font-weight:600;color:#1a2030;background:#fff;border:1px solid #d0d4db;border-radius:6px;padding:6px 12px;cursor:pointer;font-family:inherit">Copy link</button>
  </div>
  <div class="stats">
    <div class="stat"><div class="k">Total Insider Buying</div><div class="v g">${_fmtV(data.buyval)}</div></div>
    <div class="stat"><div class="k">Open-Market Buys</div><div class="v">${data.buys}</div></div>
    <div class="stat"><div class="k">Companies</div><div class="v">${data.companies}</div></div>
    <div class="stat"><div class="k">Insiders Buying</div><div class="v">${data.buyers}</div></div>
  </div>
  ${data.biggest.length ? `<h2>Biggest insider buys</h2>
  <table><thead><tr><th>#</th><th>Company</th><th>Insider</th><th class="dt">Date</th><th class="num">Value</th></tr></thead><tbody>${bigRows}</tbody></table>` : ''}
  ${data.mostBought.length ? `<h2>Most-bought stocks</h2>
  <table><thead><tr><th>Company</th><th class="num">Insiders</th><th class="num">Buys</th><th class="num">Total Bought</th></tr></thead><tbody>${mostRows}</tbody></table>` : ''}
  ${data.sectors.length ? `<h2>Insider buying by sector</h2>
  <table><thead><tr><th>Sector</th><th class="num">Buys</th><th class="num">Total Bought</th></tr></thead><tbody>${sectorRows}</tbody></table>` : ''}
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice</footer>
<script>function sx(k,el){var u=encodeURIComponent(location.href.split('#')[0]);var t=encodeURIComponent((document.title||'').split('|')[0].trim());var m={x:'https://twitter.com/intent/tweet?text='+t+'&url='+u,reddit:'https://www.reddit.com/submit?url='+u+'&title='+t,linkedin:'https://www.linkedin.com/sharing/share-offsite/?url='+u};if(k==='copy'){try{navigator.clipboard.writeText(location.href.split('#')[0]);}catch(e){}if(el){var o=el.textContent;el.textContent='Copied!';setTimeout(function(){el.textContent=o;},1500);}return false;}window.open(m[k],'_blank','noopener,noreferrer,width=600,height=520');return false;}</script>
</body></html>`;
}

const _reportCache = new Map(); // endYmd -> { html, t }
app.get('/insider-buying-report', (req, res) => res.redirect(302, '/insider-buying-report/' + _ymd(_reportLatest())));
app.get('/insider-buying-report/:date', async (req, res) => {
  const raw = (req.params.date || '').slice(0, 10);
  const d = new Date(raw + 'T12:00:00Z');
  if (isNaN(d.getTime())) return res.redirect(302, '/insider-buying-report');
  const sun = _reportSunday(d), latest = _reportLatest();
  if (sun > latest) return res.redirect(302, '/insider-buying-report/' + _ymd(latest));
  const endYmd = _ymd(sun);
  if (endYmd !== raw) return res.redirect(301, '/insider-buying-report/' + endYmd); // canonical Sunday
  const startYmd = _ymdAdd(endYmd, -6);
  const isLatest = endYmd === _ymd(latest);
  const hit = _reportCache.get(endYmd);
  const ttl = isLatest ? 6 * 3600000 : 30 * 24 * 3600000;
  if (hit && Date.now() - hit.t < ttl) { res.type('html'); return res.send(hit.html); }
  try {
    const rows = await query(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title, trade_date,
             MAX(COALESCE(value,0)) AS value, MAX(qty) AS qty, MAX(price) AS price
      FROM trades
      WHERE TRIM(type)='P' AND trade_date BETWEEN ? AND ?
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6 AND COALESCE(value,0) >= 10000
      GROUP BY ticker, insider, trade_date`, [startYmd, endYmd]);
    const buyval = rows.reduce((s, r) => s + (+r.value || 0), 0);
    const companies = new Set(rows.map(r => r.ticker)).size;
    const buyers = new Set(rows.map(r => r.insider)).size;
    const biggest = [...rows].sort((a, b) => b.value - a.value).slice(0, 15);
    const tk = {};
    for (const r of rows) { const t = tk[r.ticker] || (tk[r.ticker] = { ticker: r.ticker, company: r.company, buyval: 0, buyers: new Set(), buys: 0 }); t.buyval += +r.value || 0; t.buyers.add(r.insider); t.buys++; }
    const mostBought = Object.values(tk).map(t => ({ ...t, buyers: t.buyers.size })).sort((a, b) => b.buyval - a.buyval).slice(0, 10);
    const sec = {};
    for (const r of rows) { const info = getTickerSector(r.ticker); if (!info) continue; const s = sec[info[0]] || (sec[info[0]] = { sector: info[0], buyval: 0, buys: 0 }); s.buyval += +r.value || 0; s.buys++; }
    const sectors = Object.values(sec).sort((a, b) => b.buyval - a.buyval).slice(0, 8);
    const html = renderReportPage(endYmd, startYmd, { buys: rows.length, buyval, companies, buyers, biggest, mostBought, sectors });
    _reportCache.set(endYmd, { html, t: Date.now() });
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── DATA STUDY: WHAT STOCKS DO AFTER INSIDERS BUY (SEO + backlink asset) ──────
// Evergreen analysis rendered from the precomputed 'insider-study' cache
// (scripts/precompute.js computeInsiderStudy). Public data only.
function renderStudyPage(s) {
  const url = 'https://www.insidertape.com/insider-buying-study';
  const sp = v => (v >= 0 ? '+' : '') + Number(v || 0).toFixed(1) + '%';
  const fdY = d => { if (!d) return ''; const dt = new Date(String(d).slice(0, 10) + 'T12:00:00Z'); return isNaN(dt) ? String(d).slice(0, 10) : dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }); };
  const cls = v => (v || 0) >= 0 ? 'g' : 'r';
  const co = s.cohorts || {};
  const COH = [['ceo', 'CEO buys'], ['cluster', 'Cluster buys'], ['allBuys', 'All insider buys']];
  const nAll = (s.sample?.allBuys || 0).toLocaleString('en-US');
  const nCl = (s.sample?.clusterEvents || 0).toLocaleString('en-US');
  const nCeo = (s.sample?.ceoBuys || 0).toLocaleString('en-US');
  const cmin = s.clusterMin || 3, wdays = s.windowDays || 30;
  const fromY = (s.sample?.from || '').slice(0, 4);
  const g = (key, win) => (co[key] || {})[win] || {};
  const ceo6 = g('ceo', '6M'), cl6 = g('cluster', '6M'), all6 = g('allBuys', '6M');
  const desc = `Market-wide study of ${nAll} insider buys over five years: how CEO buys, cluster buys, and all insider buys performed at 1, 3, 6, and 12 months versus the Russell 2000 and the S&P 500. Median and average returns plus win rates.`;
  const intro = `We measured what stocks did after insiders bought, across ${nAll} open-market SEC Form 4 purchases market-wide since ${fromY}, split three ways: all insider buys, cluster buys (${cmin}+ insiders within ${wdays} days), and CEO buys (${nCeo} of them). Each is measured 1, 3, 6, and 12 months later against both the Russell 2000 (a fairer yardstick for the small and mid caps where insiders actually buy) and the S&P 500.`;

  const rowFor = win => ([key, label]) => { const x = g(key, win), r = x.rut || {}, p = x.spx || {}; return `<tr>
      <td><strong>${label}</strong></td>
      <td class="num" style="color:var(--muted)">${(x.n || 0).toLocaleString('en-US')}</td>
      <td class="num ${cls(x.medianRet)}">${sp(x.medianRet)}</td>
      <td class="num ${cls(x.meanRet)}">${sp(x.meanRet)}</td>
      <td class="num">${x.pctPositive || 0}%</td>
      <td class="num ${cls(r.medianExcess)}">${sp(r.medianExcess)}</td>
      <td class="num ${cls(p.medianExcess)}">${sp(p.medianExcess)}</td>
    </tr>`; };
  const tbl = win => `<table><thead><tr><th>Cohort</th><th class="num">Sample</th><th class="num">Median</th><th class="num">Mean</th><th class="num">% up</th><th class="num">vs Russell</th><th class="num">vs S&amp;P</th></tr></thead><tbody>${COH.map(rowFor(win)).join('')}</tbody></table>`;

  const faq = [
    { q: 'Do stocks rise after insiders buy?', a: `Across ${nAll} insider buys, over the six months after the purchase the median stock returned ${sp(all6.medianRet)} and ${all6.pctPositive || 0}% were higher.` },
    { q: 'Do CEO buys perform better than other insider buys?', a: `Over six months, CEO buys returned a median ${sp(ceo6.medianRet)} (${ceo6.pctPositive || 0}% positive) versus ${sp(all6.medianRet)} for all insider buys, and ${(ceo6.rut || {}).pctBeat || 0}% beat the Russell 2000.` },
    { q: 'Do cluster buys beat a single insider buying?', a: `Over six months, cluster buys (${cmin}+ insiders within ${wdays} days) returned a median ${sp(cl6.medianRet)} versus ${sp(all6.medianRet)} for a typical single insider buy.` },
  ];

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>How Stocks Perform After Insiders Buy: CEO, Cluster &amp; All Buys | InsiderTape</title>
<meta name="description" content="${_esc(desc)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${url}">
<meta property="og:type" content="article"><meta property="og:url" content="${url}">
<meta property="og:title" content="How Stocks Perform After Insiders Buy: CEO, Cluster &amp; All Buys">
<meta property="og:description" content="${_esc(desc)}">
<meta property="og:image" content="https://www.insidertape.com/og-image.png">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://www.insidertape.com/og-image.png">
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'Article', headline: `How Stocks Perform After Insiders Buy: CEO, Cluster and All Buys`, description: desc, url, datePublished: s.generated, dateModified: s.generated, author: { '@type': 'Organization', name: 'InsiderTape' }, publisher: { '@type': 'Organization', name: 'InsiderTape' } })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'FAQPage', mainEntity: faq.map(f => ({ '@type': 'Question', name: f.q, acceptedAnswer: { '@type': 'Answer', text: f.a } })) })}</script>
<script type="application/ld+json">${JSON.stringify({ '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Home', item: 'https://www.insidertape.com/' }, { '@type': 'ListItem', position: 2, name: 'Insider Buying Study', item: url }] })}</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='32' fill='%230f172a'/%3E%3Ccircle cx='32' cy='32' r='14' fill='none' stroke='%2300d4ff' stroke-width='1.5' opacity='0.5'/%3E%3Ccircle cx='32' cy='32' r='3' fill='%2300d4ff'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap">
<style>
:root{--bg:#f0f2f5;--bg2:#fff;--border:#d0d4db;--text:#1a2030;--muted:#6e7a8a;--accent:#2478cc;--accent2:#1a5fa8;--buy:#167a40;--sell:#b03030}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:16px;line-height:1.7}
header{position:sticky;top:0;z-index:10;height:60px;background:rgba(255,255,255,.97);backdrop-filter:blur(10px);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.logo{font-size:17px;font-weight:800;letter-spacing:3px;color:var(--text);text-decoration:none}.logo span{color:var(--accent)}
header nav a{color:var(--muted);font-size:12px;font-weight:500;text-decoration:none;padding:7px 14px;border:1px solid transparent;border-radius:5px}header nav a:hover{color:var(--text);border-color:var(--border)}
.wrap{max-width:820px;margin:0 auto;padding:44px 24px 90px}
.tag{display:inline-block;padding:3px 10px;background:rgba(36,120,204,.08);border:1px solid rgba(36,120,204,.2);border-radius:20px;font-size:10px;font-weight:700;color:var(--accent);letter-spacing:.5px;text-transform:uppercase;margin-bottom:16px}
h1{font-size:clamp(26px,4.4vw,40px);font-weight:800;letter-spacing:-.5px;line-height:1.14;margin-bottom:12px}
.meta{font-size:12px;color:var(--muted);margin-bottom:24px}
.intro{font-size:17px;color:#3a4555;line-height:1.8;margin-bottom:20px}
p{margin-bottom:16px;color:#3a4555}
h2{font-size:20px;font-weight:700;margin:36px 0 14px}
table{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px;margin-bottom:10px}
th{text-align:left;font-size:10px;letter-spacing:.5px;text-transform:uppercase;color:var(--muted);padding:11px 13px;border-bottom:2px solid var(--border)}
td{padding:11px 13px;border-bottom:1px solid var(--border);vertical-align:middle;color:#3a4555}tr:last-child td{border-bottom:none}
.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;font-weight:600}.num.g{color:var(--buy)}.num.r{color:var(--sell)}
.callout{background:var(--bg2);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:8px;padding:18px 20px;margin:20px 0;font-size:14px;color:#3a4555}
.method{font-size:13px;color:var(--muted);line-height:1.8}
.cta{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:30px;text-align:center;margin-top:40px}
.cta h3{font-size:20px;font-weight:700;margin-bottom:8px}.cta p{color:var(--muted);font-size:14px;margin-bottom:18px}
.btn{display:inline-block;background:var(--accent);color:#fff;padding:11px 26px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none}.btn:hover{background:var(--accent2)}
footer{border-top:1px solid var(--border);padding:28px 24px;text-align:center;font-size:11px;color:var(--muted);background:var(--bg2)}footer a{color:var(--accent);text-decoration:none}
@media(max-width:640px){table{font-size:12px}th,td{padding:9px 8px}}
</style></head><body>
<header><a class="logo" href="/">INSIDER<span>TAPE</span></a><nav><a href="/">Screener</a><a href="/biggest-insider-buys">Top Buys</a><a href="/articles/">Learn</a></nav></header>
<div class="wrap">
  <div class="tag">Data Study</div>
  <h1>How Stocks Perform After Insiders Buy</h1>
  <div class="meta">InsiderTape research &nbsp;·&nbsp; ${nAll} buys, ${fdY(s.sample?.from)} to ${fdY(s.sample?.to)} &nbsp;·&nbsp; Updated ${fdY(s.generated)}</div>
  <p class="intro">${_esc(intro)}</p>
  <div class="callout">Reading the tables: <strong>Median</strong> is the typical stock, <strong>Mean</strong> the average (pulled up by big winners), <strong>% up</strong> the share that rose. <strong>vs Russell / vs S&amp;P</strong> is the median return above or below that index over the same dates - positive means it beat the benchmark.</div>
  <h2>Six months after the buy</h2>
  ${tbl('6M')}
  <h2>One month after the buy</h2>
  ${tbl('1M')}
  <h2>Three months after the buy</h2>
  ${tbl('3M')}
  <h2>One year after the buy</h2>
  ${tbl('12M')}
  <h2>Methodology</h2>
  <p class="method">Starting from every open-market purchase (SEC Form 4, code P) of ${_fmtV(s.sample?.minValue || 10000)} or more filed over the last five years across the US market, we entered each at the closing price on or just after the transaction date and measured its return 1, 3, 6, and 12 months later against both the Russell 2000 and the S&amp;P 500 over the same dates. "Cluster buys" are cases where ${cmin} or more different insiders bought the same company within ${wdays} days (each wave counted once); "CEO buys" are purchases whose filed title identifies the buyer as the CEO. Option exercises, grants, and obvious price-data errors are excluded, as are tickers with no available daily price history. Longer windows have smaller samples because recent events have not completed a full year. This is analysis of past filings, not a prediction or investment advice.</p>
  <div class="cta">
    <h3>Track insider buys as they happen</h3>
    <p>InsiderTape flags CEO buys, cluster buying, and first buys in years in real time, plotted on the price chart. Start a free 7-day trial, cancel anytime.</p>
    <a class="btn" href="/premium">START FREE TRIAL →</a>
    <div style="margin-top:12px"><a href="/biggest-insider-buys" style="font-size:12px;color:var(--muted);text-decoration:none">or see the biggest insider buys this week →</a></div>
  </div>
</div>
<footer><a href="/">InsiderTape</a> &nbsp;·&nbsp; Insider data sourced from SEC EDGAR (Form 4) &nbsp;·&nbsp; Not financial advice. Past performance does not predict future results.</footer>
</body></html>`;
}

let _studyCache = { html: null, t: 0 };
app.get('/insider-buying-study', async (req, res) => {
  try {
    const row = await queryOne("SELECT value_json, computed_at FROM computed_cache WHERE key = 'insider-study'");
    const study = row ? (() => { try { return JSON.parse(row.value_json); } catch(_) { return null; } })() : null;
    // Show the placeholder until the new cohort-structured study has been computed.
    if (!study || !study.cohorts) {
      return res.type('html').send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="robots" content="noindex"><title>Cluster Buying Study | InsiderTape</title><meta name="viewport" content="width=device-width, initial-scale=1.0"><style>body{background:#f0f2f5;color:#1a2030;font-family:Inter,system-ui,sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center;text-align:center;padding:24px}a{color:#2478cc}</style></head><body><div><h1 style="font-size:22px">Cluster Buying Study</h1><p style="color:#6e7a8a">Our cluster-buy performance analysis is being compiled and will appear here shortly.</p><p><a href="/">Back to InsiderTape</a></p></div></body></html>`);
    }
    if (_studyCache.html && _studyCache.t === row.computed_at) { res.type('html'); return res.send(_studyCache.html); }
    const html = renderStudyPage(study);
    _studyCache = { html, t: row.computed_at };
    res.type('html').send(html);
  } catch(e) { res.status(500).type('html').send('<!DOCTYPE html><html><body>Temporarily unavailable. <a href="/">InsiderTape</a></body></html>'); }
});

// ─── SPA FALLBACK ─────────────────────────────────────────────────────────────
app.get('*', (req, res) => {
  if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Not found' });
  if (req.path.startsWith('/articles')) {
    const rel = req.path.replace('/articles/', '');
    // Serve files that already have an extension (e.g. img/*.png) as-is; only
    // append .html for extension-less article slugs.
    const articlePath = (req.path === '/articles' || req.path === '/articles/')
      ? path.join(__dirname, 'articles', 'index.html')
      : path.join(__dirname, 'articles', /\.[a-z0-9]+$/i.test(rel) ? rel : rel + '.html');
    if (require('fs').existsSync(articlePath)) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      return res.sendFile(articlePath);
    }
  }
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => slog(`Server on port ${PORT}`));
