'use strict';

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const path     = require('path');
const fs       = require('fs');
const { spawn } = require('child_process');
const Database = require('better-sqlite3');

const app  = express();
const PORT = process.env.PORT || 3000;
const FMP            = process.env.FMP_KEY             || '';
const TIINGO         = process.env.TIINGO_KEY          || '';
const POLYGON        = process.env.POLYGON_KEY         || '';
const RESEND_KEY     = process.env.RESEND_KEY          || '';
const FROM_EMAIL     = process.env.FROM_EMAIL          || 'noreply@insidertape.com';
const SESSION_SECRET = process.env.SESSION_SECRET      || 'dev-secret-change-in-prod';
const SITE_URL       = process.env.SITE_URL            || 'https://insidertape.com';
const STRIPE_SECRET  = process.env.STRIPE_SECRET_KEY   || '';
const STRIPE_PRICE_MONTHLY = process.env.STRIPE_PRICE_ID        || '';
const STRIPE_PRICE_ANNUAL  = process.env.STRIPE_PRICE_ID_ANNUAL || '';
const ADMIN_EMAIL    = process.env.ADMIN_EMAIL         || '';

// ─── DB PATH — prefer Render persistent disk, fall back to local ./data ──────
// /var/data exists on Render but can throw disk I/O errors if the disk is
// unhealthy, full, or not yet attached. We try it first, and if it fails we
// fall back to a local directory so the server stays up rather than crash-looping.
function resolveDiskState() {
  const candidates = ['/var/data', path.join(__dirname, 'data')];
  for (const dir of candidates) {
    try {
      fs.mkdirSync(dir, { recursive: true });
      // Probe: try writing a tiny test file to confirm the disk is actually writable
      const probe = path.join(dir, '.write_probe');
      fs.writeFileSync(probe, String(Date.now()));
      fs.unlinkSync(probe);
      return { dir, ok: true };
    } catch(e) {
      console.warn(`Disk probe failed for ${dir}: ${e.message} — trying next`);
    }
  }
  return { dir: path.join(__dirname, 'data'), ok: false };
}
const { dir: DATA_DIR, ok: DISK_OK } = resolveDiskState();
if (!DISK_OK) console.error('WARNING: all disk probes failed — DB may not persist across restarts');
const DB_PATH = path.join(DATA_DIR, 'trades.db');
console.log(`DB path: ${DB_PATH} (disk ok: ${DISK_OK})`);

app.use(cors());
// Serve static files — cache JS/CSS/images aggressively, but never cache HTML
// so users always get the latest version on page load
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      // HTML: always revalidate so users get fresh content
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    } else if (/\.(js|css|woff2?|ttf|eot)$/.test(filePath)) {
      // JS/CSS/fonts: cache 1 hour (short enough to get updates, long enough to help)
      res.setHeader('Cache-Control', 'public, max-age=3600');
    } else if (/\.(png|jpg|jpeg|gif|svg|ico|webp)$/.test(filePath)) {
      // Images: cache 24 hours
      res.setHeader('Cache-Control', 'public, max-age=86400');
    }
  }
}));

// Serve articles — no-cache on HTML so new posts are always fresh
app.use('/articles', express.static(path.join(__dirname, 'articles'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  }
}));

// ─── DISK DIAGNOSTIC (call /api/disk to see exactly what's wrong) ────────────
app.get('/api/disk', (req, res) => {
  const report = { db_path: DB_PATH, disk_ok: DISK_OK, checks: [] };
  // Check /var/data
  try {
    const stat = fs.statSync('/var/data');
    report.checks.push({ path: '/var/data', exists: true, isDir: stat.isDirectory() });
    // Try write probe
    try {
      fs.writeFileSync('/var/data/.probe', 'test');
      fs.unlinkSync('/var/data/.probe');
      report.checks.push({ path: '/var/data write', ok: true });
    } catch(e) {
      report.checks.push({ path: '/var/data write', ok: false, error: e.message });
    }
    // Check DB file
    try {
      const dbs = fs.statSync(DB_PATH);
      report.checks.push({ path: DB_PATH, exists: true, sizeBytes: dbs.size, sizeMB: +(dbs.size/1024/1024).toFixed(1) });
    } catch(e) {
      report.checks.push({ path: DB_PATH, exists: false, error: e.message });
    }
  } catch(e) {
    report.checks.push({ path: '/var/data', exists: false, error: e.message });
  }
  // Check local data dir
  try {
    const localDir = path.join(__dirname, 'data');
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(path.join(localDir, '.probe'), 'test');
    fs.unlinkSync(path.join(localDir, '.probe'));
    report.checks.push({ path: localDir, writable: true });
  } catch(e) {
    report.checks.push({ path: path.join(__dirname, 'data'), writable: false, error: e.message });
  }
  // DB status
  if (db) {
    try {
      const n = _stmtTradeCount.get().n;
      report.db_status = { ok: true, total_trades: n, using_path: DB_PATH };
    } catch(e) {
      report.db_status = { ok: false, error: e.message };
    }
  } else {
    report.db_status = { ok: false, error: 'db not initialized' };
  }
  res.json(report);
});

// Global safety net — log crashes but keep the process alive
process.on('uncaughtException',  err => console.error('UNCAUGHT:', err.message));
process.on('unhandledRejection', err => console.error('UNHANDLED:', err?.message || err));

// Per-request timeout: kill any request that takes > 55s so it returns a proper error
// rather than hanging until the platform kills the whole dyno.
// Scoreboard needs ~10-15s for parallel price fetches; 55s gives comfortable headroom.
app.use((req, res, next) => {
  const limit = req.path === '/api/scoreboard' ? 55000
              : (req.path === '/api/drift' || req.path === '/api/proximity') ? 45000
              : (req.path === '/api/screener' || req.path === '/api/firstbuys' || req.path === '/api/monitor-sentiment') ? 45000
              : req.path === '/api/price' ? 35000
              : 25000;
  res.setTimeout(limit, () => {
    if (!res.headersSent) res.status(503).json({ error: 'Request timeout' });
  });
  next();
});

// ─── RATE LIMITER ─────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════
// BOT PROTECTION + BRUTE FORCE DEFENSE
// ═══════════════════════════════════════════════════════════════

// ── IP ban list (in-memory, resets on restart) ──────────────────
const _bannedIPs  = new Set();  // permanent session bans
const _strikeMap  = new Map();  // ip → { strikes, bannedUntil }
const _bruteStore = new Map();  // ip → { count, firstAt } for auth endpoints

function getIP(req) {
  return (req.headers['x-forwarded-for']?.split(',')[0].trim()
    || req.socket.remoteAddress
    || 'unknown').replace('::ffff:', '');
}

function strike(ip, count = 1) {
  const s = _strikeMap.get(ip) || { strikes: 0, bannedUntil: 0 };
  s.strikes += count;
  // Progressive bans: 5 strikes = 5min, 10 = 30min, 15+ = 24hr
  if (s.strikes >= 15)     s.bannedUntil = Date.now() + 24 * 3600000;
  else if (s.strikes >= 10) s.bannedUntil = Date.now() + 30 * 60000;
  else if (s.strikes >= 5)  s.bannedUntil = Date.now() + 5  * 60000;
  _strikeMap.set(ip, s);
}

function isBanned(ip) {
  if (_bannedIPs.has(ip)) return true;
  const s = _strikeMap.get(ip);
  return s && Date.now() < s.bannedUntil;
}

// ── Known bad user-agents ───────────────────────────────────────
const BAD_UA_PATTERNS = [
  /python-requests/i, /scrapy/i, /curl\//i, /wget\//i, /httpx/i,
  /aiohttp/i, /go-http-client/i, /java\//i, /libwww/i, /lwp-/i,
  /mechanize/i, /twisted/i, /axios\/0\./i,
  /zgrab/i, /masscan/i, /nmap/i, /nikto/i, /sqlmap/i,
  /semrush/i, /dotbot/i, /ahrefsbot/i, /mj12bot/i, /blexbot/i,
  /petalbot/i, /serpstatbot/i, /seokicks/i,
];

// ── Honeypot paths — no legit user ever visits these ───────────
// Hitting one gets your IP banned for 24hr
const HONEYPOT_PATHS = [
  '/admin', '/wp-admin', '/wp-login.php', '/.env', '/config.php',
  '/phpmyadmin', '/mysql', '/adminer', '/.git/config',
  '/api/v1/users', '/api/dump', '/api/export',
  '/backup', '/db', '/database', '/shell', '/cmd',
];

// ── Rate limit store ────────────────────────────────────────────
const _rlStore = new Map();
const HEAVY_PATHS = [
  '/api/drift', '/api/proximity', '/api/scoreboard', '/api/firstbuys',
  '/api/insider-sentiment', '/api/insider-ratio',
];

function rateLimiter(maxReq, windowMs) {
  return (req, res, next) => {
    if (!req.path.startsWith('/api/')) return next();
    const ip  = getIP(req);
    const key = ip + '|' + maxReq;
    const now = Date.now();
    let slot = _rlStore.get(key);
    if (!slot || now > slot.resetAt) {
      slot = { count: 0, resetAt: now + windowMs };
      _rlStore.set(key, slot);
    }
    slot.count++;
    if (slot.count > maxReq) {
      // Add a strike after sustained hammering
      if (slot.count > maxReq * 3) strike(ip, 1);
      res.setHeader('Retry-After', Math.ceil((slot.resetAt - now) / 1000));
      return res.status(429).json({ error: 'Too many requests' });
    }
    next();
  };
}

// Clean up expired entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _rlStore)   { if (now > v.resetAt)    _rlStore.delete(k); }
  for (const [k, v] of _strikeMap) { if (now > v.bannedUntil && v.strikes < 5) _strikeMap.delete(k); }
  for (const [k, v] of _bruteStore){ if (now - v.firstAt > 3600000) _bruteStore.delete(k); }
}, 5 * 60 * 1000);

// ── Main protection middleware (runs on every request) ──────────
app.use((req, res, next) => {
  const ip = getIP(req);
  const ua = req.headers['user-agent'] || '';
  const path = req.path.toLowerCase();

  // 1. Honeypot — instant 24hr ban
  if (HONEYPOT_PATHS.some(p => path === p || path.startsWith(p + '/'))) {
    _bannedIPs.has(ip) || strike(ip, 10); // fast-track to ban
    slog(`[HONEYPOT] ${ip} hit ${req.path}`);
    return res.status(404).send('Not found');
  }

  // 2. Banned IP
  if (isBanned(ip)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  // 3. Bad user-agent (API calls only — don't block browsers fetching assets)
  if (req.path.startsWith('/api/') && BAD_UA_PATTERNS.some(p => p.test(ua))) {
    strike(ip, 2);
    slog(`[BOT-UA] ${ip} "${ua.slice(0,60)}"`);
    return res.status(403).json({ error: 'Forbidden' });
  }

  // 4. Missing user-agent on API calls (bots often omit it)
  if (req.path.startsWith('/api/') && !ua) {
    strike(ip, 1);
    return res.status(403).json({ error: 'Forbidden' });
  }

  next();
});

// ── Rate limiting ───────────────────────────────────────────────
app.use((req, res, next) => {
  const isHeavy = HEAVY_PATHS.some(p => req.path.startsWith(p));
  return isHeavy
    ? rateLimiter(20, 60000)(req, res, next)   // heavy: 20/min
    : rateLimiter(90, 60000)(req, res, next);  // normal: 90/min
});

// ── Brute force protection for auth endpoints ───────────────────
// Max 5 auth attempts per IP per hour
function authBruteGuard(req, res, next) {
  const ip  = getIP(req);
  const now = Date.now();
  let slot  = _bruteStore.get(ip) || { count: 0, firstAt: now };
  if (now - slot.firstAt > 3600000) slot = { count: 0, firstAt: now }; // reset hourly
  slot.count++;
  _bruteStore.set(ip, slot);
  if (slot.count > 5) {
    strike(ip, 2);
    slog(`[BRUTE] ${ip} — ${slot.count} auth attempts`);
    return res.status(429).json({ error: 'Too many attempts — try again later' });
  }
  next();
}

// ─── DB ───────────────────────────────────────────────────────
let db;
try {
  db = new Database(DB_PATH);
  console.log(`Opened DB at ${DB_PATH}`);
  // Drop sc13 tables if they still exist (migrated away from 13D/G)
  try { db.exec('DROP TABLE IF EXISTS sc13_transactions'); } catch(_) {}
  try { db.exec('DROP TABLE IF EXISTS sc13_quarter_log'); } catch(_) {}

  // Drop f13 tables (removed from product)
  try { db.exec('DROP TABLE IF EXISTS f13_changes'); } catch(_) {}
  try { db.exec('DROP TABLE IF EXISTS f13_cusip_cache'); } catch(_) {}
  try { db.exec('DROP TABLE IF EXISTS f13_quarter_log'); } catch(_) {}

  // Create 13F tables if not yet created by f13-worker
  db.exec(`
  `);
} catch (e) {
  console.error(`Cannot open DB at ${DB_PATH}: ${e.message}`);
  // Last-resort fallback: try opening in the local directory
  const fallbackPath = path.join(__dirname, 'data', 'trades.db');
  try {
    fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
    db = new Database(fallbackPath);
    console.warn(`Fallback: opened DB at ${fallbackPath} — data will NOT persist on Render`);
  } catch(e2) {
    console.error(`FATAL: cannot open DB anywhere. Last error: ${e2.message}`);
    process.exit(1);
  }
}

try { db.pragma('journal_mode = WAL'); } catch(e) { console.warn('WAL pragma failed (read-only disk?):', e.message); }
try { db.pragma('busy_timeout = 15000'); } catch(e) {} // wait up to 15s if congress worker holds write lock

// Each table/index created separately so existing DBs get new tables too
// All wrapped in try/catch — on Render the persistent disk can be briefly
// unavailable during a new deploy; a write failure here must NOT crash the server.
try {
  db.exec(`CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL, company TEXT, insider TEXT, title TEXT,
    trade_date TEXT NOT NULL, filing_date TEXT,
    type TEXT, qty INTEGER, price REAL, value INTEGER, owned INTEGER, accession TEXT,
    footnote TEXT,
    UNIQUE(accession, insider, trade_date, type, qty)
  )`);
  // v1.6: add footnote column for DRIP detection (safe to run repeatedly)
  try { db.exec(`ALTER TABLE trades ADD COLUMN footnote TEXT`); } catch(e) {} // already exists = fine
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ticker      ON trades(ticker)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_date  ON trades(trade_date DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_filing_date ON trades(filing_date DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_insider     ON trades(insider)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ticker_date_price ON trades(ticker, trade_date, price)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_insider_ticker_date ON trades(insider, ticker, trade_date DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_ticker_type ON trades(ticker, type, trade_date DESC)`); // speeds up /api/ticker
} catch(e) { console.warn('Schema init warning:', e.message); }

// ─── AUTH / SUBSCRIPTION TABLES ──────────────────────────────
try {
  db.exec(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    is_admin INTEGER NOT NULL DEFAULT 0
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS magic_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS subscriptions (
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
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_magic_tokens_token ON magic_tokens(token)`);

  // ── v1.7: Alert preferences ────────────────────────────────────
  db.exec(`CREATE TABLE IF NOT EXISTS alert_prefs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL UNIQUE,
    enabled     INTEGER NOT NULL DEFAULT 1,
    min_score   INTEGER NOT NULL DEFAULT 70,   -- only fire for signals scored >= this
    min_value   INTEGER NOT NULL DEFAULT 0,    -- only fire for trades >= this dollar value
    types       TEXT    NOT NULL DEFAULT 'conviction,cluster,first_buy,exit_warning', -- comma-separated
    tickers     TEXT    NOT NULL DEFAULT '',   -- comma-separated ticker filter, empty = all
    sectors     TEXT    NOT NULL DEFAULT '',   -- comma-separated sector filter, empty = all
    roles       TEXT    NOT NULL DEFAULT '',   -- comma-separated role filter, empty = all
    frequency   TEXT    NOT NULL DEFAULT 'immediate', -- 'immediate' | 'daily'
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS alert_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    ticker       TEXT NOT NULL,
    signal_type  TEXT NOT NULL,
    signal_key   TEXT NOT NULL,  -- dedup key: ticker|type|date to prevent re-sending
    sent_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, signal_key)
  )`);

  db.exec(`CREATE INDEX IF NOT EXISTS idx_alert_log_user ON alert_log(user_id)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alert_log_key  ON alert_log(signal_key)`);

  // v1.7.1 migrations — add columns if upgrading from earlier v1.7
  for (const col of ['sectors TEXT NOT NULL DEFAULT ""', 'roles TEXT NOT NULL DEFAULT ""']) {
    try { db.exec(`ALTER TABLE alert_prefs ADD COLUMN ${col}`); } catch(_) {}
  }
} catch(e) { console.warn('Auth schema init warning:', e.message); }

// ─── Clean up bad/invalid trade records ──────────────────────
// These are idempotent maintenance tasks — safe to skip if the disk is busy.
try {
  // Remove invalid tickers
  const r1 = db.prepare(`
    DELETE FROM trades
    WHERE ticker IN ('N/A','NA','NONE','NULL','--','-','.','0','FALSE','TRUE','UNKNOWN','TBD')
       OR ticker NOT GLOB '[A-Z]*'
       OR LENGTH(ticker) < 1 OR LENGTH(ticker) > 10
       OR COALESCE(company,'') IN ('N/A','NA','None','NULL','--','-')
  `).run();
  if (r1.changes > 0) console.log(`Cleaned up ${r1.changes} invalid ticker records`);

  // Belt-and-suspenders: explicitly nuke any NONE/NULL ticker rows
  // These come from EDGAR Form 4 filings where issuerTradingSymbol='NONE'
  const rNone = db.prepare(`DELETE FROM trades WHERE ticker = 'NONE' OR ticker = 'NULL'`).run();
  if (rNone.changes > 0) console.log(`Removed ${rNone.changes} NONE/NULL ticker records`);

  // Remove non-open-market transaction codes.
  // Only keep P (open-market buy), S (open-market sell), S- (sale under 10b5-1 plan).
  // Conversions (C), exercises (M), awards (A), gifts (G), tax withholding (F),
  // disposals (D), inheritance (W), trust transfers (Z), etc. are not market trades
  // and produce fabricated values (qty * exercise_price = billions of fake dollars).
  const r2 = db.prepare(`
    DELETE FROM trades
    WHERE TRIM(type) NOT IN ('P', 'S', 'S-')
  `).run();
  if (r2.changes > 0) console.log(`Removed ${r2.changes} non-market transaction records (conversions, exercises, awards, etc.)`);

  // Remove records with implausible values that slipped through
  const r3 = db.prepare(`
    DELETE FROM trades
    WHERE value > 2000000000
       OR price > 1500000
       OR qty   > 50000000
  `).run();
  if (r3.changes > 0) console.log(`Removed ${r3.changes} records with implausible values (likely derivative artifacts)`);
} catch(e) { console.warn('Startup cleanup skipped (disk busy?):', e.message); }

try {
  db.exec(`CREATE TABLE IF NOT EXISTS price_cache (
    symbol    TEXT PRIMARY KEY,
    bars_json TEXT NOT NULL,
    fetched_at INTEGER NOT NULL
  )`);
  // Only clear entries older than 12h — do NOT wipe everything on deploy
  try { db.prepare(`DELETE FROM price_cache WHERE fetched_at < ?`).run(Date.now() - 12 * 60 * 60 * 1000); } catch(_) {}
  db.exec(`CREATE TABLE IF NOT EXISTS sync_log (
    quarter TEXT PRIMARY KEY, synced_at TEXT DEFAULT (datetime('now')), rows INTEGER
  )`);
} catch(e) { console.warn('sync_log table init warning:', e.message); }

// ─── SC 13D/G ownership transaction table ────────────────────────────────
try {
  // Composite indexes for the heaviest screener queries
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trades_type_filing  ON trades(type, filing_date DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trades_type_trade   ON trades(type, trade_date DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trades_filing_type  ON trades(filing_date DESC, type, value)`);
  // Migrate existing DB
} catch(e) { console.warn('SC13 schema init warning:', e.message); }

// ─── SYNC via child process ────────────────────────────────────
let syncRunning = false;
const syncLog   = [];

function slog(msg) {
  const line = `[${new Date().toISOString().slice(11,19)}] ${msg}`;
  console.log(line);
  syncLog.push(line);
  if (syncLog.length > 300) syncLog.shift();
}

function runSync(numQ = 4) {
  if (syncRunning) { slog('sync already running'); return; }
  syncRunning = true;
  slog(`=== spawning sync-worker (${numQ} quarters) ===`);

  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=400', path.join(__dirname, 'sync-worker.js'), String(numQ)],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog(l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('ERR: '+l)));
  worker.on('exit', code => {
    syncRunning = false;
    slog(`=== worker exited (code ${code}) ===`);
  });
}

// ─── HTTP helper ──────────────────────────────────────────────
const http = require('http');

function get(url, ms=30000, opts={}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('http://') ? http : https;
    const reqHeaders = Object.assign(
      { 'User-Agent': 'Mozilla/5.0 (compatible; InsiderTape/1.0)' },
      opts.headers || {}
    );
    const req = mod.get(url, { headers: reqHeaders, timeout: ms }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms, opts).then(resolve).catch(reject);
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


// ─── DAILY INGESTION (recent Form 4s from EDGAR daily index) ────
let dailyRunning = false;

function runDaily(daysBack = 3) {
  if (dailyRunning) return;

  // Smart scheduling: only poll during market hours (Mon–Fri, 7am–8pm ET)
  // Outside those hours, sleep until the next morning open
  const now = new Date();
  // Convert to US Eastern time
  // Detect DST: EDT=UTC-4 (Mar-Nov), EST=UTC-5 (Nov-Mar)
  const etOffset = (() => {
    const y = now.getUTCFullYear();
    // DST starts 2nd Sunday of March, ends 1st Sunday of November
    const dstStart = new Date(Date.UTC(y, 2, 8));  // March 8 (earliest possible 2nd Sun)
    dstStart.setUTCDate(8 + (7 - dstStart.getUTCDay()) % 7);
    dstStart.setUTCHours(7, 0, 0, 0); // 2am ET = 7am UTC in EST
    const dstEnd = new Date(Date.UTC(y, 10, 1));   // November 1 (earliest possible 1st Sun)
    dstEnd.setUTCDate(1 + (7 - dstEnd.getUTCDay()) % 7);
    dstEnd.setUTCHours(6, 0, 0, 0);   // 2am ET = 6am UTC in EDT
    return (now >= dstStart && now < dstEnd) ? -4 : -5;
  })();
  const etHour = (now.getUTCHours() + 24 + etOffset) % 24;
  const etDay  = new Date(now.getTime() + etOffset * 3600000).getUTCDay(); // 0=Sun,6=Sat
  const isWeekday = etDay >= 1 && etDay <= 5;
  const isMarketHours = etHour >= 7 && etHour < 20; // 7am–8pm ET (4 hrs after 4pm close)

  if (!isWeekday || !isMarketHours) {
    // Sleep until 7:05am ET next weekday
    const msUntilOpen = msUntilNextOpen(now, etOffset);
    slog(`=== daily-worker sleeping ${Math.round(msUntilOpen/60000)}min until market hours ===`);
    setTimeout(() => runDaily(2), msUntilOpen);
    return;
  }

  dailyRunning = true;
  slog(`=== spawning daily-worker (RSS poll mode, ${daysBack} days backfill) ===`);

  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=200', path.join(__dirname, 'daily-worker.js'), String(daysBack), 'poll'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );

  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[daily] ' + l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[daily] ERR: ' + l)));
  worker.on('exit', code => {
    dailyRunning = false;
    if (code !== 0) {
      // Unexpected exit — restart after 5 min
      slog(`=== daily-worker exited (code ${code}) — restarting in 5min ===`);
      setTimeout(() => runDaily(2), 5 * 60 * 1000);
    } else {
      // Clean exit shouldn't happen in poll mode, but restart after 1 hour just in case
      slog(`=== daily-worker exited cleanly — restarting in 1hr ===`);
      setTimeout(() => runDaily(2), 60 * 60 * 1000);
    }
  });
}

function msUntilNextOpen(now, etOffset) {
  // Find next weekday 7:05am ET
  const target = new Date(now);
  target.setUTCHours(7 - etOffset, 5, 0, 0); // 7:05am ET in UTC
  if (target <= now) target.setUTCDate(target.getUTCDate() + 1);
  // Skip weekends
  while (true) {
    const day = new Date(target.getTime() + etOffset * 3600000).getUTCDay();
    if (day >= 1 && day <= 5) break;
    target.setUTCDate(target.getUTCDate() + 1);
  }
  return Math.max(target - now, 60000); // at least 1 min
}

// ─── PRICE CACHE (see full implementation near fetchPriceBars) ──────────────

// ─── API ROUTES ───────────────────────────────────────────────

// SCREENER
// Screener cache: keyed by days+limit, 30s TTL — same data for all users in that window
const _screenerCache = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k,v] of _screenerCache) if (now - v.t > 30000) _screenerCache.delete(k);
}, 30000);

app.get('/api/screener', (req, res) => {
  try {
    const cacheKey = (req.query.days||'30') + '|' + (req.query.limit||'');
    const cached = _screenerCache.get(cacheKey);
    const _reqDays = parseInt(req.query.days||'30');
    const cacheTTL = _reqDays >= 90 ? 120000 : 30000; // 90+ day queries cached 2min; short queries 30s
    if (cached && Date.now() - cached.t < cacheTTL) return res.json(cached.d);

    const n = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    if (n === 0) {
      // DB is empty — auto-trigger daily ingestion if not already running
      if (!syncRunning && !dailyRunning) {
        slog('Screener hit empty DB — auto-triggering daily ingestion');
        runDaily(7);
      }
      return res.json({ building: true, message: 'Loading SEC data — this takes 2–3 minutes on first run…', trades: [] });
    }

    const days  = Math.min(Math.max(parseInt(req.query.days || '30'), 1), 1095);
    // Scale limit by window so every range returns a meaningfully different dataset.
    // 7d → 1000, 30d → 2000, 90d → 5000, 365d → 15000
    const defaultLimit = days <= 7 ? 1000 : days <= 30 ? 2000 : days <= 90 ? 5000 : 15000;
    const limit = Math.min(parseInt(req.query.limit || String(defaultLimit)), 20000);

        let rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned,
             MAX(accession) AS accession
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
      AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT ?
    `).all(days, limit);

    if (!rows.length && n > 0) {
      // Smart fallback: use the most recent available window relative to MAX(trade_date)
      // This handles the case where the daily worker hasn't inserted recent trades
      const maxDate = db.prepare('SELECT MAX(trade_date) AS d FROM trades WHERE trade_date IS NOT NULL').get().d;
      if (maxDate) {
        rows = db.prepare(`
          SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
                 trade_date AS trade, MAX(filing_date) AS filing,
                 TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
                 MAX(value) AS value, MAX(owned) AS owned,
                 MAX(accession) AS accession
          FROM trades
          WHERE trade_date >= date(?, '-' || ? || ' days')
            AND TRIM(type) IN ('P','S','S-')
            AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
            AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
            AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
          GROUP BY ticker, insider, trade_date, type
          ORDER BY trade_date DESC, filing_date DESC
          LIMIT ?
        `).all(maxDate, days, limit);
      }
    }

    const etag = '"' + rows.length + '-' + (rows[0]?.filing || '') + '"';
    _screenerCache.set(cacheKey, { d: rows, t: Date.now(), etag });
    res.setHeader('ETag', etag);
    if (req.headers['if-none-match'] === etag) return res.status(304).end();
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// HISTORY — like screener but uses trade_date (not filing_date) for the window
// Used by analysis tools that need historical data: drift, regime shift, first-buy
const _historyCache = new Map();
setInterval(() => { const n=Date.now(); for(const [k,v] of _historyCache) if(n-v.t>120000) _historyCache.delete(k); }, 60000);

app.get('/api/history', (req, res) => {
  try {
    const days  = Math.min(Math.max(parseInt(req.query.days || '1825'), 1), 1825);
    const limit = Math.min(parseInt(req.query.limit || '10000'), 25000);

    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
      AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        AND COALESCE(company,'') NOT IN ('N/A','NA','None','NULL','--','-','')
        AND insider IS NOT NULL
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC
      LIMIT ?
    `).all(days, limit);

    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// INSIDER BUY/SELL RATIO HISTORY — monthly rolling counts for the ratio chart
// Transaction count based (not dollar), officers/directors only excluded 10%-only owners
// ── INSIDER SENTIMENT INDEX — for Peak/Trough chart ─────────────────────────
// Returns monthly insider buy% (smoothed) + S&P 500 monthly OHLC via Yahoo
let _sentimentCache = null;
let _sentimentCacheTime = 0;

app.get('/api/insider-sentiment', async (req, res) => {
  try {
    if (_sentimentCache && Date.now() - _sentimentCacheTime < 3600000) { // 1hr cache
      return res.json(_sentimentCache);
    }
    const months = 120; // 10 years
    // Monthly insider data: buy value / (buy + sell value) per month
    const rows = db.prepare(`
      SELECT
        strftime('%Y-%m', trade_date) AS month,
        strftime('%Y-%m', trade_date) || '-01' AS month_date,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' months')
        AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(value, 0) >= 10000
      GROUP BY month
      HAVING buy_val + sell_val > 0
      ORDER BY month ASC
    `).all(months);

    // Compute raw buy% per month
    const monthly = rows.map(r => ({
      date: r.month_date,
      buyPct: r.buy_val / (r.buy_val + r.sell_val),
      buyVal: r.buy_val,
      sellVal: r.sell_val,
      buyCount: r.buy_count,
      sellCount: r.sell_count,
    }));

    // 3-month rolling average to smooth noise
    const smoothed = monthly.map((m, i) => {
      const slice = monthly.slice(Math.max(0, i-2), i+1);
      const avgBuyPct = slice.reduce((s,x) => s + x.buyPct, 0) / slice.length;
      return { ...m, smoothedBuyPct: avgBuyPct };
    });

    // Compute percentile thresholds for calibration
    const vals = smoothed.map(m => m.smoothedBuyPct).sort((a,b) => a-b);
    const p10 = vals[Math.floor(vals.length * 0.10)] || 0.15;
    const p25 = vals[Math.floor(vals.length * 0.25)] || 0.20;
    const p75 = vals[Math.floor(vals.length * 0.75)] || 0.40;
    const p90 = vals[Math.floor(vals.length * 0.90)] || 0.50;
    const median = vals[Math.floor(vals.length * 0.50)] || 0.30;

    // Fetch S&P 500 monthly data from Yahoo Finance
    const endTs   = Math.floor(Date.now() / 1000);
    const startTs = endTs - months * 31 * 86400;
    let spxData = [];
    try {
      const { status, body } = await get(
        `https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1mo&period1=${startTs}&period2=${endTs}`,
        15000
      );
      if (status === 200) {
        const d = JSON.parse(body.toString());
        const r = d?.chart?.result?.[0];
        if (r?.timestamp && r?.indicators?.quote?.[0]) {
          const q = r.indicators.quote[0];
          spxData = r.timestamp.map((t, i) => ({
            date: new Date(t * 1000).toISOString().slice(0, 7) + '-01',
            close: q.close?.[i] || null,
            open:  q.open?.[i]  || null,
            high:  q.high?.[i]  || null,
            low:   q.low?.[i]   || null,
          })).filter(x => x.close);
        }
      }
    } catch(e) { slog('SPX fetch error: ' + e.message); }

    const sentimentResult = { insider: smoothed, spx: spxData, thresholds: { p10, p25, median, p75, p90 } };
    _sentimentCache = sentimentResult;
    _sentimentCacheTime = Date.now();
    res.json(sentimentResult);
  } catch(e) {
    slog('insider-sentiment error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

const _ratioHistCache = new Map();

app.get('/api/insider-ratio-history', (req, res) => {
  try {
    const grain  = req.query.grain === 'monthly' ? 'monthly' : 'weekly';
    const weeks  = Math.min(parseInt(req.query.weeks  || '52'), 520); // up to 10 years
    const months = Math.min(parseInt(req.query.months || '24'), 120); // up to 10 years

    let rows;
    if (grain === 'weekly') {
      rows = db.prepare(`
        SELECT
          strftime('%Y-%W', trade_date) AS period,
          date(trade_date, 'weekday 0', '-6 days') AS period_date,
          COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
          COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
        FROM trades
        WHERE trade_date >= date('now', '-' || ? || ' days')
          AND trade_date <= date('now')
          AND TRIM(type) IN ('P','S','S-')
          AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
          AND COALESCE(value, 0) > 0
        GROUP BY period
        ORDER BY period ASC
      `).all(weeks * 7);
    } else {
      rows = db.prepare(`
        SELECT
          strftime('%Y-%m', trade_date) AS period,
          strftime('%Y-%m', trade_date) || '-01' AS period_date,
          COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
          COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
        FROM trades
        WHERE trade_date >= date('now', '-' || ? || ' months')
          AND trade_date <= date('now')
          AND TRIM(type) IN ('P','S','S-')
          AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
          AND COALESCE(value, 0) > 0
        GROUP BY period
        ORDER BY period ASC
      `).all(months);
    }

    const data = rows.map(r => ({
      period: r.period_date || r.period,
      buys:   r.buys,
      sells:  r.sells,
      ratio:  r.sells > 0 ? +(r.buys / r.sells).toFixed(3) : null,
    })).filter(r => r.ratio !== null && r.buys + r.sells >= 3);

    // Compute long-term percentile thresholds using 3 years of weekly data
    // These calibrate the Fear/Greed zones against actual historical distribution
    const ltRows = db.prepare(`
      SELECT
        strftime('%Y-%W', trade_date) AS period,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells
      FROM trades
      WHERE trade_date >= date('now', '-1095 days')
        AND trade_date <= date('now')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(value, 0) > 0
      GROUP BY period
      HAVING sells > 0
      ORDER BY period ASC
    `).all();

    const ltRatios = ltRows
      .map(r => r.sells > 0 ? r.buys / r.sells : null)
      .filter(r => r !== null && r < 5) // exclude extreme outliers
      .sort((a, b) => a - b);

    const pct = (p) => {
      const idx = Math.floor(p / 100 * (ltRatios.length - 1));
      return +ltRatios[idx].toFixed(3);
    };

    const stats = ltRatios.length >= 10 ? {
      p20:    pct(20),   // Fear threshold
      p40:    pct(40),   // Below average
      p60:    pct(60),   // Above average
      p80:    pct(80),   // Greed threshold
      median: pct(50),
      mean:   +(ltRatios.reduce((s,v) => s+v, 0) / ltRatios.length).toFixed(3),
      n:      ltRatios.length,
    } : null;

    res.json({ data, stats });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
// Returns buy/sell counts and values for today, week, month, quarter windows
// Much faster than fetching all trades client-side — does aggregation in SQL
let _monitorSentCache = null, _monitorSentTime = 0;
app.get('/api/monitor-sentiment', (req, res) => {
  try {
    if (_monitorSentCache && Date.now() - _monitorSentTime < 60000) return res.json(_monitorSentCache);
    const now = new Date();
    const etOffset = -5;
    const etNow = new Date(now.getTime() + etOffset * 3600000);
    const dow = etNow.getUTCDay(); // 0=Sun, 6=Sat

    // Last trading day — skip weekends
    const lastTrade = new Date(etNow);
    if (dow === 0) lastTrade.setUTCDate(etNow.getUTCDate() - 2); // Sun → Fri
    else if (dow === 6) lastTrade.setUTCDate(etNow.getUTCDate() - 1); // Sat → Fri

    // Use DB max trade date as ceiling (handles holidays too)
    const dbMax = db.prepare(`SELECT MAX(trade_date) AS d FROM trades WHERE trade_date <= ? AND TRIM(type) IN ('P','S','S-')`).get(lastTrade.toISOString().slice(0, 10));
    const todayStr = (dbMax && dbMax.d) || lastTrade.toISOString().slice(0, 10);

    // Week start (Monday of last trading week)
    const tradeDow = lastTrade.getUTCDay();
    const weekStart = new Date(lastTrade);
    weekStart.setUTCDate(lastTrade.getUTCDate() - (tradeDow === 0 ? 6 : tradeDow - 1));
    const weekStr = weekStart.toISOString().slice(0, 10);

    // Month: 30 days back from last trading day
    const monthStart = new Date(lastTrade);
    monthStart.setUTCDate(lastTrade.getUTCDate() - 30);
    const monthStr = monthStart.toISOString().slice(0, 10);

    // Quarter start
    const qStartMonth = Math.floor(lastTrade.getUTCMonth() / 3) * 3;
    const quarterStr = `${lastTrade.getUTCFullYear()}-${String(qStartMonth + 1).padStart(2, '0')}-01`;

    function windowStats(cutStr, endStr) {
      const row = db.prepare(`
        SELECT
          COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
          COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
          COALESCE(SUM(CASE WHEN TRIM(type)='P' THEN value ELSE 0 END), 0) AS buy_val,
          COALESCE(SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN value ELSE 0 END), 0) AS sell_val,
          COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS unique_buyers,
          COUNT(DISTINCT CASE WHEN TRIM(type) IN ('S','S-') THEN insider END) AS unique_sellers
        FROM trades
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND TRIM(type) IN ('P','S','S-')
          AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
          AND COALESCE(value, 0) > 0
      `).get(cutStr, endStr);
      return row;
    }

    const monitorResult = {
      today:   { cutStr: todayStr,   ...windowStats(todayStr,   todayStr)   },
      week:    { cutStr: weekStr,    ...windowStats(weekStr,    todayStr)   },
      month:   { cutStr: monthStr,   ...windowStats(monthStr,   todayStr)   },
      quarter: { cutStr: quarterStr, ...windowStats(quarterStr, todayStr)   },
    };
    _monitorSentCache = monitorResult;
    _monitorSentTime = Date.now();
    res.json(monitorResult);
  } catch(e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/ticker', (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  try {
    // Sort by transaction date, filter derivatives, deduplicate amendments
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades
      WHERE ticker = ?
        AND TRIM(type) IN ('P','S','S-')
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC, filing_date DESC
      LIMIT 5000
    `).all(sym);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// SC 13D/G — returns all beneficial ownership filings for a given ticker
// Recent SC 13D/G filings across all tickers — used by screener 5% Owner filter
// ── 13F Holdings API ─────────────────────────────────────────────────────────











app.get('/api/insider', (req, res) => {
  const name  = (req.query.name  || '').trim();
  const exact = req.query.exact === '1';
  if (!name) return res.status(400).json({ error: 'name required' });
  if (name.length < 2) return res.status(400).json({ error: 'name too short' });
  try {
    const pattern = exact ? name : `%${name}%`;
    const limit = exact ? 2000 : 500;
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date AS trade, MAX(filing_date) AS filing,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
      FROM trades
      WHERE UPPER(insider) LIKE UPPER(?)
        AND TRIM(type) IN ('P','S','S-')
        AND COALESCE(value, 0) <= 2000000000
      GROUP BY ticker, insider, trade_date, type
      ORDER BY trade_date DESC LIMIT ?
    `).all(pattern, limit);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PRICE — FMP with Yahoo Finance fallback
// SEARCH AUTOCOMPLETE — ticker prefix + company name + insider name
// In-memory ticker/company index for instant autocomplete — built at startup
let _searchIndex = []; // [{ticker, company, n}] sorted by trade count desc
function buildSearchIndex() {
  try {
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company, COUNT(*) AS n
      FROM trades
      WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      GROUP BY ticker ORDER BY n DESC
    `).all();
    _searchIndex = rows;
    slog(`Search index built: ${rows.length} tickers`);
  } catch(e) { slog('buildSearchIndex error: ' + e.message); }
}

app.get('/api/search', (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 1) return res.json({ tickers: [], insiders: [] });
  try {
    const upper = q.toUpperCase();

    // Instant ticker lookup from in-memory index
    const prefixMatches = [], companyMatches = [];
    for (const r of _searchIndex) {
      if (r.ticker.startsWith(upper)) prefixMatches.push(r);
      else if (r.company && r.company.toUpperCase().includes(upper)) companyMatches.push(r);
      if (prefixMatches.length >= 8 && companyMatches.length >= 4) break;
    }
    const seenTickers = new Set(prefixMatches.map(r => r.ticker));
    const allTickers = [...prefixMatches, ...companyMatches.filter(r => !seenTickers.has(r.ticker))].slice(0, 8);

    // Insider name match still hits DB but only when needed
    const insiderRows = q.length >= 2 ? db.prepare(`
      SELECT insider, MAX(title) AS title, COUNT(*) AS n
      FROM trades
      WHERE UPPER(insider) LIKE ? AND insider IS NOT NULL
      GROUP BY insider ORDER BY n DESC LIMIT 6
    `).all('%' + upper + '%') : [];

    res.json({
      tickers: allTickers.map(r => ({ ticker: r.ticker, company: r.company || r.ticker })),
      insiders: insiderRows.map(r => ({ name: r.insider, title: r.title || '' })),
    });

    // Pre-warm price cache for searched tickers in background
    allTickers.slice(0, 3).forEach(r => {
      if (!getPC(r.ticker)) fetchPriceBars(r.ticker).catch(() => {});
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// FIRST BUY IN YEARS — scans entire DB history, no date filter
// Uses window functions to find insiders whose most recent buy on a ticker
// came after a long gap since their previous buy on that same ticker.
const _firstBuysCache = new Map(); // key=mingap|lookback|limit → {d, t}

app.get('/api/firstbuys', (req, res) => {
  try {
    const minGapDays   = parseInt(req.query.mingap   || '180');
    const lookbackDays = parseInt(req.query.lookback || '90');
    const limit        = parseInt(req.query.limit    || '100');
    const fbKey = `${minGapDays}|${lookbackDays}|${limit}`;
    const fbCached = _firstBuysCache.get(fbKey);
    if (fbCached && Date.now() - fbCached.t < 300000) return res.json(fbCached.d); // 5min TTL

    // Optimised approach: only consider insiders who have a buy in the lookback window.
    // Then for each such (insider, ticker) pair find the previous buy date.
    // This avoids scanning the entire trades table with ROW_NUMBER().
    const rows = db.prepare(`
      WITH recent_buys AS (
        -- Only insiders with a buy inside the lookback window (uses idx_trade_date)
        SELECT DISTINCT insider, ticker
        FROM trades
        WHERE TRIM(type) = 'P'
          AND trade_date >= date('now', '-' || ? || ' days')
          AND trade_date <= date('now')
          AND insider IS NOT NULL AND ticker IS NOT NULL
      ),
      latest AS (
        -- Latest buy per insider+ticker from the recent window
        SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title,
               MAX(t.trade_date) AS latest_trade,
               MAX(t.filing_date) AS latest_filing,
               MAX(t.price) AS latest_price,
               MAX(t.qty) AS latest_qty,
               MAX(t.value) AS latest_value,
               MAX(t.owned) AS latest_owned
        FROM trades t
        JOIN recent_buys rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P'
          AND t.trade_date >= date('now', '-' || ? || ' days')
          AND t.trade_date <= date('now')
        GROUP BY t.insider, t.ticker
      ),
      prev AS (
        -- Most recent buy BEFORE the lookback window for each (insider, ticker) pair
        SELECT t.insider, t.ticker,
               MAX(t.trade_date) AS prev_trade,
               MAX(t.owned)      AS prev_owned
        FROM trades t
        JOIN recent_buys rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P'
          AND t.trade_date < date('now', '-' || ? || ' days')
        GROUP BY t.insider, t.ticker
      )
      SELECT
        l.ticker, l.company, l.insider, l.title,
        l.latest_trade, l.latest_filing,
        l.latest_price, l.latest_qty, l.latest_value, l.latest_owned,
        p.prev_trade, p.prev_owned,
        CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
      FROM latest l
      JOIN prev p ON l.insider = p.insider AND l.ticker = p.ticker
      WHERE CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= ?
      ORDER BY gap_days DESC
      LIMIT ?
    `).all(lookbackDays, lookbackDays, lookbackDays, minGapDays, limit);

    res.json(rows);
  } catch(e) {
    slog('firstbuys error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// OPPORTUNITY RANKER — per-ticker aggregates for frontend scoring
app.get('/api/ranker', (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days || '30'), 90);
    const rows = db.prepare(`
      SELECT
        ticker,
        MAX(company) AS company,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END)                            AS buy_count,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END)             AS buyer_count,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END)       AS total_buy_val,
        MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)           AS latest_buy_date,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END)                  AS sell_count,
        COUNT(DISTINCT CASE WHEN TRIM(type) IN ('S','S-') THEN insider END)   AS seller_count,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS total_sell_val,
        MAX(CASE WHEN TRIM(type) IN ('S','S-') THEN trade_date ELSE NULL END) AS latest_sell_date,
        MAX(CASE WHEN TRIM(type)='P' AND (
          UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR
          UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%' OR
          UPPER(title) LIKE '%FOUNDER%' OR UPPER(title) LIKE '%COO%'
        ) THEN 1 ELSE 0 END)                                            AS has_exec_buyer,
        CAST(julianday('now') - julianday(
          MAX(CASE WHEN TRIM(type)='P' THEN trade_date ELSE NULL END)
        ) AS INTEGER)                                                    AS days_since_buy,
        MAX(CASE WHEN TRIM(type)='P' AND owned>qty AND qty>0
          THEN CAST(qty*100.0/(owned-qty) AS INTEGER) ELSE 0 END)       AS max_stake_pct
      FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
      AND trade_date <= date('now')
      GROUP BY ticker
      HAVING buy_count > 0
      ORDER BY total_buy_val DESC
      LIMIT 200
    `).all(days);
    // Filter out likely ESPP: tickers with 8+ buyers are almost always stock plans
    // Real insider clusters rarely exceed 6-7 independent buyers in 30 days
    const filtered = rows.filter(r => r.buyer_count <= 8);
    res.json(filtered);
  } catch(e) {
    slog('ranker error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// GOV TRADES — congressional trades for a ticker
// GOV TRADES BULK IMPORT — called by GitHub Actions worker
// Requires IMPORT_SECRET env var to match X-Import-Token header
app.post('/api/gov-import', express.json({ limit: '10mb' }), (req, res) => {
  const token = req.headers['x-import-token'];
  const secret = process.env.IMPORT_SECRET;
  if (!secret || token !== secret) return res.status(401).json({ error: 'unauthorized' });

  const rows = req.body;
  if (!Array.isArray(rows) || !rows.length) return res.json({ inserted: 0 });

  try {
    const ins = db.prepare(`
      INSERT OR IGNORE INTO gov_trades
        (chamber,member,ticker,asset_description,transaction_type,
         transaction_date,disclosure_date,amount_range,owner,filing_url,doc_id)
      VALUES
        (@chamber,@member,@ticker,@asset_description,@transaction_type,
         @transaction_date,@disclosure_date,@amount_range,@owner,@filing_url,@doc_id)
    `);
    const insertMany = db.transaction(rs => {
      let n = 0;
      for (const r of rs) n += ins.run(r).changes;
      return n;
    });
    const inserted = insertMany(rows);
    slog(`gov-import: ${inserted} new trades from ${rows.length} rows`);
    res.json({ inserted, total: rows.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/gov', (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.json([]);
  try {
    const rows = db.prepare(`
      SELECT member, chamber, ticker, transaction_type, transaction_date,
             disclosure_date, amount_range, owner, filing_url
      FROM gov_trades
      WHERE ticker = ? AND ticker != '--'
        AND transaction_date IS NOT NULL
        AND transaction_type IN ('P','S')
      ORDER BY transaction_date DESC
      LIMIT 200
    `).all(sym);
    res.json(rows);
  } catch(e) {
    res.json([]);
  }
});

// LEADERBOARD — top insiders by open-market buy activity (enough history to score)
// ── SECTORS ─────────────────────────────────────────────────────────────────
// Static sector/industry map. Zero external calls — instant lookups.
// Covers S&P 500 + Russell 1000 + high-insider-activity names (~750 tickers).
const TICKER_SECTOR_MAP = {
  'AAPL':['Technology','Consumer Electronics'],'MSFT':['Technology','Software—Infrastructure'],
  'NVDA':['Technology','Semiconductors'],'AVGO':['Technology','Semiconductors'],
  'ORCL':['Technology','Software—Infrastructure'],'CSCO':['Technology','Communication Equipment'],
  'ADBE':['Technology','Software—Application'],'CRM':['Technology','Software—Application'],
  'AMD':['Technology','Semiconductors'],'QCOM':['Technology','Semiconductors'],
  'TXN':['Technology','Semiconductors'],'INTC':['Technology','Semiconductors'],
  'IBM':['Technology','Information Technology Services'],'NOW':['Technology','Software—Application'],
  'INTU':['Technology','Software—Application'],'AMAT':['Technology','Semiconductor Equipment & Materials'],
  'MU':['Technology','Semiconductors'],'LRCX':['Technology','Semiconductor Equipment & Materials'],
  'KLAC':['Technology','Semiconductor Equipment & Materials'],'ADI':['Technology','Semiconductors'],
  'MRVL':['Technology','Semiconductors'],'PANW':['Technology','Software—Infrastructure'],
  'SNPS':['Technology','Software—Application'],'CDNS':['Technology','Software—Application'],
  'FTNT':['Technology','Software—Infrastructure'],'HPQ':['Technology','Computer Hardware'],
  'HPE':['Technology','Computer Hardware'],'DELL':['Technology','Computer Hardware'],
  'STX':['Technology','Computer Hardware'],'WDC':['Technology','Computer Hardware'],
  'NTAP':['Technology','Computer Hardware'],'GLW':['Technology','Electronic Components'],
  'TEL':['Technology','Electronic Components'],'KEYS':['Technology','Scientific & Technical Instruments'],
  'ANSS':['Technology','Software—Application'],'PTC':['Technology','Software—Application'],
  'CTSH':['Technology','Information Technology Services'],'ACN':['Technology','Information Technology Services'],
  'IT':['Technology','Information Technology Services'],'GPN':['Technology','Software—Infrastructure'],
  'FISV':['Technology','Software—Infrastructure'],'FIS':['Technology','Software—Infrastructure'],
  'FI':['Technology','Software—Infrastructure'],'PYPL':['Technology','Software—Infrastructure'],
  'ADSK':['Technology','Software—Application'],'WDAY':['Technology','Software—Application'],
  'ZS':['Technology','Software—Infrastructure'],'CRWD':['Technology','Software—Infrastructure'],
  'OKTA':['Technology','Software—Infrastructure'],'DDOG':['Technology','Software—Application'],
  'NET':['Technology','Software—Infrastructure'],'MDB':['Technology','Software—Application'],
  'SNOW':['Technology','Software—Application'],'HUBS':['Technology','Software—Application'],
  'GTLB':['Technology','Software—Application'],'APP':['Technology','Software—Application'],
  'RBLX':['Technology','Software—Application'],'FFIV':['Technology','Software—Infrastructure'],
  'JNPR':['Technology','Communication Equipment'],'MSCI':['Technology','Information Technology Services'],
  'BR':['Technology','Information Technology Services'],'NXPI':['Technology','Semiconductors'],
  'ON':['Technology','Semiconductors'],'MPWR':['Technology','Semiconductors'],
  'SWKS':['Technology','Semiconductors'],'MCHP':['Technology','Semiconductors'],
  'SMCI':['Technology','Computer Hardware'],'PSTG':['Technology','Computer Hardware'],
  'TWLO':['Technology','Software—Application'],'ZM':['Technology','Software—Application'],
  'DOCU':['Technology','Software—Application'],'BOX':['Technology','Software—Infrastructure'],
  'DBX':['Technology','Software—Infrastructure'],'PCOR':['Technology','Software—Application'],
  'SPSC':['Technology','Software—Application'],'BRZE':['Technology','Software—Application'],
  'BILL':['Technology','Software—Application'],'DT':['Technology','Software—Application'],
  'AI':['Technology','Software—Application'],'NCNO':['Technology','Software—Application'],
  'TOST':['Technology','Software—Application'],'AMBA':['Technology','Semiconductors'],
  'SLAB':['Technology','Semiconductors'],'ONTO':['Technology','Semiconductor Equipment & Materials'],
  'IPGP':['Technology','Electronic Components'],'VIAV':['Technology','Communication Equipment'],
  'CIEN':['Technology','Communication Equipment'],'CALX':['Technology','Communication Equipment'],
  'EPAM':['Technology','Information Technology Services'],'GDDY':['Technology','Software—Infrastructure'],
  'AKAM':['Technology','Software—Infrastructure'],'CDW':['Technology','Information Technology Services'],
  'NTDOY':['Technology','Electronic Gaming & Multimedia'],'ACLS':['Technology','Semiconductor Equipment & Materials'],

  'JPM':['Financial Services','Banks—Diversified'],'BAC':['Financial Services','Banks—Diversified'],
  'WFC':['Financial Services','Banks—Diversified'],'GS':['Financial Services','Capital Markets'],
  'MS':['Financial Services','Capital Markets'],'BLK':['Financial Services','Asset Management'],
  'C':['Financial Services','Banks—Diversified'],'AXP':['Financial Services','Credit Services'],
  'V':['Financial Services','Credit Services'],'MA':['Financial Services','Credit Services'],
  'COF':['Financial Services','Credit Services'],'DFS':['Financial Services','Credit Services'],
  'SYF':['Financial Services','Credit Services'],'USB':['Financial Services','Banks—Regional'],
  'PNC':['Financial Services','Banks—Regional'],'TFC':['Financial Services','Banks—Regional'],
  'MTB':['Financial Services','Banks—Regional'],'KEY':['Financial Services','Banks—Regional'],
  'RF':['Financial Services','Banks—Regional'],'CFG':['Financial Services','Banks—Regional'],
  'HBAN':['Financial Services','Banks—Regional'],'FITB':['Financial Services','Banks—Regional'],
  'WBS':['Financial Services','Banks—Regional'],'BOKF':['Financial Services','Banks—Regional'],
  'SNV':['Financial Services','Banks—Regional'],'FHN':['Financial Services','Banks—Regional'],
  'WAL':['Financial Services','Banks—Regional'],'COLB':['Financial Services','Banks—Regional'],
  'EWBC':['Financial Services','Banks—Regional'],'FFIN':['Financial Services','Banks—Regional'],
  'CVBF':['Financial Services','Banks—Regional'],'FULT':['Financial Services','Banks—Regional'],
  'INDB':['Financial Services','Banks—Regional'],'NTRS':['Financial Services','Asset Management'],
  'STT':['Financial Services','Asset Management'],'BK':['Financial Services','Asset Management'],
  'SCHW':['Financial Services','Capital Markets'],'RJF':['Financial Services','Capital Markets'],
  'LPLA':['Financial Services','Capital Markets'],'PFG':['Financial Services','Insurance—Diversified'],
  'LNC':['Financial Services','Insurance—Life'],'UNM':['Financial Services','Insurance—Life'],
  'GL':['Financial Services','Insurance—Life'],'MET':['Financial Services','Insurance—Life'],
  'PRU':['Financial Services','Insurance—Life'],'AIG':['Financial Services','Insurance—Diversified'],
  'CB':['Financial Services','Insurance—Property & Casualty'],'PGR':['Financial Services','Insurance—Property & Casualty'],
  'ALL':['Financial Services','Insurance—Property & Casualty'],'HIG':['Financial Services','Insurance—Property & Casualty'],
  'TRV':['Financial Services','Insurance—Property & Casualty'],'CNA':['Financial Services','Insurance—Property & Casualty'],
  'WRB':['Financial Services','Insurance—Property & Casualty'],'CINF':['Financial Services','Insurance—Property & Casualty'],
  'MKL':['Financial Services','Insurance—Property & Casualty'],'ERIE':['Financial Services','Insurance—Property & Casualty'],
  'ICE':['Financial Services','Financial Data & Stock Exchanges'],'CME':['Financial Services','Financial Data & Stock Exchanges'],
  'CBOE':['Financial Services','Financial Data & Stock Exchanges'],'MCO':['Financial Services','Financial Data & Stock Exchanges'],
  'SPGI':['Financial Services','Financial Data & Stock Exchanges'],'FDS':['Financial Services','Financial Data & Stock Exchanges'],
  'NDAQ':['Financial Services','Financial Data & Stock Exchanges'],'ALLY':['Financial Services','Credit Services'],
  'CACC':['Financial Services','Credit Services'],'OMF':['Financial Services','Credit Services'],
  'ENVA':['Financial Services','Credit Services'],'SLM':['Financial Services','Credit Services'],
  'NAVI':['Financial Services','Credit Services'],'UWMC':['Financial Services','Mortgage Finance'],
  'AMG':['Financial Services','Asset Management'],'IVZ':['Financial Services','Asset Management'],
  'AMP':['Financial Services','Asset Management'],'BEN':['Financial Services','Asset Management'],
  'TROW':['Financial Services','Asset Management'],'WTW':['Financial Services','Insurance Brokers'],
  'AON':['Financial Services','Insurance Brokers'],'MMC':['Financial Services','Insurance Brokers'],
  'AJG':['Financial Services','Insurance Brokers'],'RYAN':['Financial Services','Insurance Brokers'],
  'BRP':['Financial Services','Insurance Brokers'],'NMIH':['Financial Services','Insurance—Property & Casualty'],
  'ESNT':['Financial Services','Insurance—Property & Casualty'],'RDN':['Financial Services','Insurance—Property & Casualty'],
  'MTG':['Financial Services','Insurance—Property & Casualty'],'ORI':['Financial Services','Insurance—Diversified'],
  'HOOD':['Financial Services','Capital Markets'],'COIN':['Financial Services','Capital Markets'],
  'MSTR':['Financial Services','Capital Markets'],'WEX':['Financial Services','Credit Services'],
  'ARCC':['Financial Services','Asset Management'],'MAIN':['Financial Services','Asset Management'],
  'HTGC':['Financial Services','Asset Management'],'CSWC':['Financial Services','Asset Management'],
  'OBDC':['Financial Services','Asset Management'],'BXSL':['Financial Services','Asset Management'],
  'OPBK':['Financial Services','Banks—Regional'],'FFBC':['Financial Services','Banks—Regional'],
  'HOMB':['Financial Services','Banks—Regional'],'TOWN':['Financial Services','Banks—Regional'],
  'SMBC':['Financial Services','Banks—Regional'],'HOPE':['Financial Services','Banks—Regional'],
  'BANF':['Financial Services','Banks—Regional'],'SFNC':['Financial Services','Banks—Regional'],
  'HTLF':['Financial Services','Banks—Regional'],'WSFS':['Financial Services','Banks—Regional'],
  'HAFC':['Financial Services','Banks—Regional'],'NBTB':['Financial Services','Banks—Regional'],
  'NWBI':['Financial Services','Banks—Regional'],'KMPR':['Financial Services','Insurance—Life'],
  'FG':['Financial Services','Insurance—Life'],'SIGI':['Financial Services','Insurance—Property & Casualty'],
  'ROOT':['Financial Services','Insurance—Property & Casualty'],'PRAA':['Financial Services','Credit Services'],
  'ECPG':['Financial Services','Credit Services'],'ATLC':['Financial Services','Credit Services'],
  'PMTS':['Financial Services','Credit Services'],'PSFE':['Financial Services','Credit Services'],

  'LLY':['Healthcare','Drug Manufacturers—General'],'JNJ':['Healthcare','Drug Manufacturers—General'],
  'ABBV':['Healthcare','Drug Manufacturers—General'],'MRK':['Healthcare','Drug Manufacturers—General'],
  'PFE':['Healthcare','Drug Manufacturers—General'],'BMY':['Healthcare','Drug Manufacturers—General'],
  'AMGN':['Healthcare','Drug Manufacturers—General'],'GILD':['Healthcare','Drug Manufacturers—General'],
  'BIIB':['Healthcare','Drug Manufacturers—General'],'REGN':['Healthcare','Drug Manufacturers—General'],
  'VRTX':['Healthcare','Drug Manufacturers—General'],'MRNA':['Healthcare','Drug Manufacturers—General'],
  'BNTX':['Healthcare','Drug Manufacturers—General'],'ALNY':['Healthcare','Biotechnology'],
  'IDXX':['Healthcare','Diagnostics & Research'],'ILMN':['Healthcare','Diagnostics & Research'],
  'TMO':['Healthcare','Diagnostics & Research'],'DHR':['Healthcare','Diagnostics & Research'],
  'A':['Healthcare','Diagnostics & Research'],'BIO':['Healthcare','Diagnostics & Research'],
  'HOLX':['Healthcare','Medical Devices'],'MDT':['Healthcare','Medical Devices'],
  'ABT':['Healthcare','Medical Devices'],'SYK':['Healthcare','Medical Devices'],
  'BSX':['Healthcare','Medical Devices'],'EW':['Healthcare','Medical Devices'],
  'ZBH':['Healthcare','Medical Devices'],'ISRG':['Healthcare','Medical Devices'],
  'DXCM':['Healthcare','Medical Devices'],'PODD':['Healthcare','Medical Devices'],
  'INSP':['Healthcare','Medical Devices'],'SWAV':['Healthcare','Medical Devices'],
  'NVCR':['Healthcare','Medical Devices'],'RMD':['Healthcare','Medical Devices'],
  'ALGN':['Healthcare','Medical Devices'],'BDX':['Healthcare','Medical Devices'],
  'BAX':['Healthcare','Medical Devices'],'XRAY':['Healthcare','Medical Devices'],
  'GKOS':['Healthcare','Medical Devices'],'LMAT':['Healthcare','Medical Devices'],
  'IRTC':['Healthcare','Medical Devices'],'NARI':['Healthcare','Medical Devices'],
  'TMDX':['Healthcare','Medical Devices'],'VCEL':['Healthcare','Medical Devices'],
  'CNMD':['Healthcare','Medical Devices'],'MMSI':['Healthcare','Medical Devices'],
  'ATRC':['Healthcare','Medical Devices'],'AMED':['Healthcare','Medical Care Facilities'],
  'HCA':['Healthcare','Medical Care Facilities'],'THC':['Healthcare','Medical Care Facilities'],
  'CYH':['Healthcare','Medical Care Facilities'],'UHS':['Healthcare','Medical Care Facilities'],
  'ENSG':['Healthcare','Medical Care Facilities'],'PGNY':['Healthcare','Medical Care Facilities'],
  'ACHC':['Healthcare','Medical Care Facilities'],'CERT':['Healthcare','Medical Care Facilities'],
  'UNH':['Healthcare','Healthcare Plans'],'CVS':['Healthcare','Healthcare Plans'],
  'CI':['Healthcare','Healthcare Plans'],'HUM':['Healthcare','Healthcare Plans'],
  'MOH':['Healthcare','Healthcare Plans'],'CNC':['Healthcare','Healthcare Plans'],
  'ELV':['Healthcare','Healthcare Plans'],'MCK':['Healthcare','Medical Distribution'],
  'CAH':['Healthcare','Medical Distribution'],'ABC':['Healthcare','Medical Distribution'],
  'PDCO':['Healthcare','Medical Distribution'],'AMPH':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'PRGO':['Healthcare','Drug Manufacturers—Specialty & Generic'],'TEVA':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'VTRS':['Healthcare','Drug Manufacturers—Specialty & Generic'],'JAZZ':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'SUPN':['Healthcare','Drug Manufacturers—Specialty & Generic'],'HIMS':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'IOVA':['Healthcare','Biotechnology'],'NTLA':['Healthcare','Biotechnology'],
  'BEAM':['Healthcare','Biotechnology'],'CRSP':['Healthcare','Biotechnology'],
  'EDIT':['Healthcare','Biotechnology'],'RCKT':['Healthcare','Biotechnology'],
  'BLUE':['Healthcare','Biotechnology'],'ACAD':['Healthcare','Biotechnology'],
  'AXSM':['Healthcare','Biotechnology'],'APLS':['Healthcare','Biotechnology'],
  'IMVT':['Healthcare','Biotechnology'],'PRAX':['Healthcare','Biotechnology'],
  'KRYS':['Healthcare','Biotechnology'],'TGTX':['Healthcare','Biotechnology'],
  'RXRX':['Healthcare','Biotechnology'],'RVMD':['Healthcare','Biotechnology'],
  'IONS':['Healthcare','Biotechnology'],'SRPT':['Healthcare','Biotechnology'],
  'BMRN':['Healthcare','Biotechnology'],'PTCT':['Healthcare','Biotechnology'],
  'RARE':['Healthcare','Biotechnology'],'FOLD':['Healthcare','Biotechnology'],
  'KYMR':['Healthcare','Biotechnology'],'RCUS':['Healthcare','Biotechnology'],
  'IMCR':['Healthcare','Biotechnology'],'KRTX':['Healthcare','Biotechnology'],
  'NUVL':['Healthcare','Biotechnology'],'VRNA':['Healthcare','Biotechnology'],
  'ARCT':['Healthcare','Biotechnology'],'SAVA':['Healthcare','Biotechnology'],
  'NVAX':['Healthcare','Drug Manufacturers—General'],'NTRA':['Healthcare','Diagnostics & Research'],
  'EXAS':['Healthcare','Diagnostics & Research'],'PACB':['Healthcare','Diagnostics & Research'],
  'NKTR':['Healthcare','Drug Manufacturers—Specialty & Generic'],'MNKD':['Healthcare','Drug Manufacturers—Specialty & Generic'],
  'ACCD':['Healthcare','Healthcare Plans'],'CLOV':['Healthcare','Healthcare Plans'],
  'OSCR':['Healthcare','Healthcare Plans'],'OPRX':['Healthcare','Healthcare Plans'],

  'AMZN':['Consumer Cyclical','Internet Retail'],'TSLA':['Consumer Cyclical','Auto Manufacturers'],
  'HD':['Consumer Cyclical','Home Improvement Retail'],'LOW':['Consumer Cyclical','Home Improvement Retail'],
  'MCD':['Consumer Cyclical','Restaurants'],'SBUX':['Consumer Cyclical','Restaurants'],
  'NKE':['Consumer Cyclical','Footwear & Accessories'],'TGT':['Consumer Cyclical','Discount Stores'],
  'BKNG':['Consumer Cyclical','Travel Services'],'ABNB':['Consumer Cyclical','Travel Services'],
  'EXPE':['Consumer Cyclical','Travel Services'],'MAR':['Consumer Cyclical','Lodging'],
  'HLT':['Consumer Cyclical','Lodging'],'H':['Consumer Cyclical','Lodging'],
  'MGM':['Consumer Cyclical','Resorts & Casinos'],'WYNN':['Consumer Cyclical','Resorts & Casinos'],
  'LVS':['Consumer Cyclical','Resorts & Casinos'],'CZR':['Consumer Cyclical','Resorts & Casinos'],
  'DKNG':['Consumer Cyclical','Gambling'],'F':['Consumer Cyclical','Auto Manufacturers'],
  'GM':['Consumer Cyclical','Auto Manufacturers'],'RIVN':['Consumer Cyclical','Auto Manufacturers'],
  'LCID':['Consumer Cyclical','Auto Manufacturers'],'AZO':['Consumer Cyclical','Auto Parts'],
  'ORLY':['Consumer Cyclical','Auto Parts'],'AAP':['Consumer Cyclical','Auto Parts'],
  'AN':['Consumer Cyclical','Auto & Truck Dealerships'],'KMX':['Consumer Cyclical','Auto & Truck Dealerships'],
  'PAG':['Consumer Cyclical','Auto & Truck Dealerships'],'LAD':['Consumer Cyclical','Auto & Truck Dealerships'],
  'ABG':['Consumer Cyclical','Auto & Truck Dealerships'],'GPI':['Consumer Cyclical','Auto & Truck Dealerships'],
  'SAH':['Consumer Cyclical','Auto & Truck Dealerships'],'CVNA':['Consumer Cyclical','Auto & Truck Dealerships'],
  'EBAY':['Consumer Cyclical','Internet Retail'],'ETSY':['Consumer Cyclical','Internet Retail'],
  'W':['Consumer Cyclical','Internet Retail'],'BBY':['Consumer Cyclical','Specialty Retail'],
  'FIVE':['Consumer Cyclical','Specialty Retail'],'DLTR':['Consumer Cyclical','Discount Stores'],
  'DG':['Consumer Cyclical','Discount Stores'],'ULTA':['Consumer Cyclical','Specialty Retail'],
  'RL':['Consumer Cyclical','Apparel Retail'],'PVH':['Consumer Cyclical','Apparel Manufacturing'],
  'HBI':['Consumer Cyclical','Apparel Manufacturing'],'UAA':['Consumer Cyclical','Athletic & Outdoor Clothing'],
  'VFC':['Consumer Cyclical','Apparel Manufacturing'],'TPR':['Consumer Cyclical','Luxury Goods'],
  'CPRI':['Consumer Cyclical','Luxury Goods'],'TJX':['Consumer Cyclical','Apparel Retail'],
  'ROST':['Consumer Cyclical','Apparel Retail'],'GPS':['Consumer Cyclical','Apparel Retail'],
  'M':['Consumer Cyclical','Department Stores'],'JWN':['Consumer Cyclical','Department Stores'],
  'KSS':['Consumer Cyclical','Department Stores'],'DKS':['Consumer Cyclical','Specialty Retail'],
  'BOOT':['Consumer Cyclical','Specialty Retail'],'YUM':['Consumer Cyclical','Restaurants'],
  'QSR':['Consumer Cyclical','Restaurants'],'DPZ':['Consumer Cyclical','Restaurants'],
  'CMG':['Consumer Cyclical','Restaurants'],'TXRH':['Consumer Cyclical','Restaurants'],
  'SHAK':['Consumer Cyclical','Restaurants'],'WEN':['Consumer Cyclical','Restaurants'],
  'JACK':['Consumer Cyclical','Restaurants'],'CAKE':['Consumer Cyclical','Restaurants'],
  'CCL':['Consumer Cyclical','Travel Services'],'RCL':['Consumer Cyclical','Travel Services'],
  'NCLH':['Consumer Cyclical','Travel Services'],'LUV':['Consumer Cyclical','Airlines'],
  'DAL':['Consumer Cyclical','Airlines'],'UAL':['Consumer Cyclical','Airlines'],
  'AAL':['Consumer Cyclical','Airlines'],'ALK':['Consumer Cyclical','Airlines'],
  'JBLU':['Consumer Cyclical','Airlines'],'PLNT':['Consumer Cyclical','Leisure'],
  'PENN':['Consumer Cyclical','Resorts & Casinos'],'RRR':['Consumer Cyclical','Resorts & Casinos'],
  'CHDN':['Consumer Cyclical','Gambling'],'AEO':['Consumer Cyclical','Apparel Retail'],
  'ANF':['Consumer Cyclical','Apparel Retail'],'URBN':['Consumer Cyclical','Apparel Retail'],
  'VSCO':['Consumer Cyclical','Apparel Retail'],'BURL':['Consumer Cyclical','Apparel Retail'],
  'CROX':['Consumer Cyclical','Footwear & Accessories'],'DECK':['Consumer Cyclical','Footwear & Accessories'],
  'SKX':['Consumer Cyclical','Footwear & Accessories'],'ONON':['Consumer Cyclical','Footwear & Accessories'],
  'POOL':['Consumer Cyclical','Specialty Retail'],'OLLI':['Consumer Cyclical','Discount Stores'],
  'CHH':['Consumer Cyclical','Lodging'],'WH':['Consumer Cyclical','Lodging'],
  'IHG':['Consumer Cyclical','Lodging'],

  'WMT':['Consumer Defensive','Discount Stores'],'COST':['Consumer Defensive','Discount Stores'],
  'KR':['Consumer Defensive','Grocery Stores'],'SFM':['Consumer Defensive','Grocery Stores'],
  'ACI':['Consumer Defensive','Grocery Stores'],'GO':['Consumer Defensive','Grocery Stores'],
  'PG':['Consumer Defensive','Household & Personal Products'],'CL':['Consumer Defensive','Household & Personal Products'],
  'CHD':['Consumer Defensive','Household & Personal Products'],'EL':['Consumer Defensive','Household & Personal Products'],
  'KMB':['Consumer Defensive','Household & Personal Products'],'CLX':['Consumer Defensive','Household & Personal Products'],
  'ELF':['Consumer Defensive','Household & Personal Products'],'COTY':['Consumer Defensive','Household & Personal Products'],
  'KO':['Consumer Defensive','Beverages—Non-Alcoholic'],'PEP':['Consumer Defensive','Beverages—Non-Alcoholic'],
  'MNST':['Consumer Defensive','Beverages—Non-Alcoholic'],'CELH':['Consumer Defensive','Beverages—Non-Alcoholic'],
  'FIZZ':['Consumer Defensive','Beverages—Non-Alcoholic'],'TAP':['Consumer Defensive','Beverages—Brewers'],
  'SAM':['Consumer Defensive','Beverages—Brewers'],'STZ':['Consumer Defensive','Beverages—Wineries & Distilleries'],
  'MO':['Consumer Defensive','Tobacco'],'PM':['Consumer Defensive','Tobacco'],'BTI':['Consumer Defensive','Tobacco'],
  'MKC':['Consumer Defensive','Packaged Foods'],'GIS':['Consumer Defensive','Packaged Foods'],
  'CPB':['Consumer Defensive','Packaged Foods'],'HRL':['Consumer Defensive','Packaged Foods'],
  'SJM':['Consumer Defensive','Packaged Foods'],'MDLZ':['Consumer Defensive','Confectioners'],
  'HSY':['Consumer Defensive','Confectioners'],'POST':['Consumer Defensive','Packaged Foods'],
  'TSN':['Consumer Defensive','Packaged Foods'],'CAG':['Consumer Defensive','Packaged Foods'],
  'PPC':['Consumer Defensive','Packaged Foods'],'USFD':['Consumer Defensive','Food Distribution'],
  'SYY':['Consumer Defensive','Food Distribution'],'PFGC':['Consumer Defensive','Food Distribution'],
  'HAIN':['Consumer Defensive','Packaged Foods'],'SMPL':['Consumer Defensive','Packaged Foods'],
  'FRPT':['Consumer Defensive','Packaged Foods'],'WBA':['Consumer Defensive','Pharmaceutical Retailers'],

  'CAT':['Industrials','Farm & Heavy Construction Machinery'],'DE':['Industrials','Farm & Heavy Construction Machinery'],
  'HON':['Industrials','Conglomerates'],'MMM':['Industrials','Conglomerates'],
  'GE':['Industrials','Specialty Industrial Machinery'],'GEV':['Industrials','Specialty Industrial Machinery'],
  'EMR':['Industrials','Specialty Industrial Machinery'],'ETN':['Industrials','Specialty Industrial Machinery'],
  'PH':['Industrials','Specialty Industrial Machinery'],'ROK':['Industrials','Specialty Industrial Machinery'],
  'IR':['Industrials','Specialty Industrial Machinery'],'AME':['Industrials','Specialty Industrial Machinery'],
  'ROP':['Industrials','Specialty Industrial Machinery'],'ITW':['Industrials','Specialty Industrial Machinery'],
  'DOV':['Industrials','Specialty Industrial Machinery'],'IEX':['Industrials','Specialty Industrial Machinery'],
  'GNRC':['Industrials','Specialty Industrial Machinery'],'AOS':['Industrials','Specialty Industrial Machinery'],
  'NDSN':['Industrials','Specialty Industrial Machinery'],'CSWI':['Industrials','Specialty Industrial Machinery'],
  'XYL':['Industrials','Specialty Industrial Machinery'],'MAS':['Industrials','Specialty Industrial Machinery'],
  'FTV':['Industrials','Specialty Industrial Machinery'],'AAON':['Industrials','Building Products & Equipment'],
  'AWI':['Industrials','Building Products & Equipment'],'MHK':['Industrials','Building Products & Equipment'],
  'OC':['Industrials','Building Products & Equipment'],'CARR':['Industrials','Building Products & Equipment'],
  'TT':['Industrials','Building Products & Equipment'],'JCI':['Industrials','Building Products & Equipment'],
  'WMS':['Industrials','Building Products & Equipment'],'TREX':['Industrials','Building Products & Equipment'],
  'AZEK':['Industrials','Building Products & Equipment'],'BLDR':['Industrials','Building Products & Equipment'],
  'IBP':['Industrials','Building Products & Equipment'],'NVR':['Industrials','Residential Construction'],
  'PHM':['Industrials','Residential Construction'],'DHI':['Industrials','Residential Construction'],
  'LEN':['Industrials','Residential Construction'],'TOL':['Industrials','Residential Construction'],
  'MDC':['Industrials','Residential Construction'],'TMHC':['Industrials','Residential Construction'],
  'MHO':['Industrials','Residential Construction'],'GRBK':['Industrials','Residential Construction'],
  'LGIH':['Industrials','Residential Construction'],'SKY':['Industrials','Residential Construction'],
  'CVCO':['Industrials','Residential Construction'],'BA':['Industrials','Aerospace & Defense'],
  'RTX':['Industrials','Aerospace & Defense'],'LMT':['Industrials','Aerospace & Defense'],
  'NOC':['Industrials','Aerospace & Defense'],'GD':['Industrials','Aerospace & Defense'],
  'HII':['Industrials','Aerospace & Defense'],'TDG':['Industrials','Aerospace & Defense'],
  'HEI':['Industrials','Aerospace & Defense'],'LDOS':['Industrials','Aerospace & Defense'],
  'SAIC':['Industrials','Aerospace & Defense'],'BAH':['Industrials','Aerospace & Defense'],
  'CACI':['Industrials','Aerospace & Defense'],'MANT':['Industrials','Aerospace & Defense'],
  'AXON':['Industrials','Aerospace & Defense'],'CW':['Industrials','Aerospace & Defense'],
  'KTOS':['Industrials','Aerospace & Defense'],'DRS':['Industrials','Aerospace & Defense'],
  'TDY':['Industrials','Aerospace & Defense'],'MRCY':['Industrials','Aerospace & Defense'],
  'UPS':['Industrials','Integrated Freight & Logistics'],'FDX':['Industrials','Integrated Freight & Logistics'],
  'XPO':['Industrials','Trucking'],'ODFL':['Industrials','Trucking'],'SAIA':['Industrials','Trucking'],
  'ARCB':['Industrials','Trucking'],'JBHT':['Industrials','Trucking'],'KNX':['Industrials','Trucking'],
  'WERN':['Industrials','Trucking'],'CHRW':['Industrials','Integrated Freight & Logistics'],
  'EXPD':['Industrials','Integrated Freight & Logistics'],'GXO':['Industrials','Integrated Freight & Logistics'],
  'NSC':['Industrials','Railroads'],'CSX':['Industrials','Railroads'],'UNP':['Industrials','Railroads'],
  'CP':['Industrials','Railroads'],'CNI':['Industrials','Railroads'],'WAB':['Industrials','Railroads'],
  'URI':['Industrials','Rental & Leasing Services'],'AL':['Industrials','Rental & Leasing Services'],
  'R':['Industrials','Rental & Leasing Services'],'WCC':['Industrials','Industrial Distribution'],
  'MSM':['Industrials','Industrial Distribution'],'GWW':['Industrials','Industrial Distribution'],
  'FAST':['Industrials','Industrial Distribution'],'SITE':['Industrials','Industrial Distribution'],
  'BECN':['Industrials','Industrial Distribution'],'VMC':['Industrials','Building Materials'],
  'MLM':['Industrials','Building Materials'],'SUM':['Industrials','Building Materials'],
  'EXP':['Industrials','Building Materials'],'FELE':['Industrials','Specialty Industrial Machinery'],

  'XOM':['Energy','Oil & Gas Integrated'],'CVX':['Energy','Oil & Gas Integrated'],
  'COP':['Energy','Oil & Gas E&P'],'EOG':['Energy','Oil & Gas E&P'],
  'DVN':['Energy','Oil & Gas E&P'],'FANG':['Energy','Oil & Gas E&P'],
  'MPC':['Energy','Oil & Gas Refining & Marketing'],'VLO':['Energy','Oil & Gas Refining & Marketing'],
  'PSX':['Energy','Oil & Gas Refining & Marketing'],'HES':['Energy','Oil & Gas E&P'],
  'OXY':['Energy','Oil & Gas E&P'],'APA':['Energy','Oil & Gas E&P'],
  'MRO':['Energy','Oil & Gas E&P'],'OVV':['Energy','Oil & Gas E&P'],
  'SM':['Energy','Oil & Gas E&P'],'CIVI':['Energy','Oil & Gas E&P'],
  'MTDR':['Energy','Oil & Gas E&P'],'CPE':['Energy','Oil & Gas E&P'],
  'MGY':['Energy','Oil & Gas E&P'],'CRGY':['Energy','Oil & Gas E&P'],
  'NOG':['Energy','Oil & Gas E&P'],'VTLE':['Energy','Oil & Gas E&P'],
  'PR':['Energy','Oil & Gas E&P'],'TALO':['Energy','Oil & Gas E&P'],
  'HAL':['Energy','Oil & Gas Equipment & Services'],'SLB':['Energy','Oil & Gas Equipment & Services'],
  'BKR':['Energy','Oil & Gas Equipment & Services'],'OII':['Energy','Oil & Gas Equipment & Services'],
  'CHX':['Energy','Oil & Gas Equipment & Services'],'LBRT':['Energy','Oil & Gas Equipment & Services'],
  'KMI':['Energy','Oil & Gas Midstream'],'WMB':['Energy','Oil & Gas Midstream'],
  'OKE':['Energy','Oil & Gas Midstream'],'TRGP':['Energy','Oil & Gas Midstream'],
  'ET':['Energy','Oil & Gas Midstream'],'MPLX':['Energy','Oil & Gas Midstream'],
  'LNG':['Energy','Oil & Gas Midstream'],'CTRA':['Energy','Oil & Gas E&P'],
  'AR':['Energy','Oil & Gas E&P'],'RRC':['Energy','Oil & Gas E&P'],
  'EQT':['Energy','Oil & Gas E&P'],'CHK':['Energy','Oil & Gas E&P'],
  'FSLR':['Energy','Solar'],'ENPH':['Energy','Solar'],'ARRY':['Energy','Solar'],
  'NOVA':['Energy','Solar'],'MAXN':['Energy','Solar'],'SHLS':['Energy','Solar'],
  'BE':['Energy','Electrical Equipment & Parts'],'PLUG':['Energy','Electrical Equipment & Parts'],

  'META':['Communication Services','Internet Content & Information'],
  'GOOGL':['Communication Services','Internet Content & Information'],
  'GOOG':['Communication Services','Internet Content & Information'],
  'NFLX':['Communication Services','Entertainment'],'DIS':['Communication Services','Entertainment'],
  'CMCSA':['Communication Services','Telecom Services'],'CHTR':['Communication Services','Telecom Services'],
  'T':['Communication Services','Telecom Services'],'VZ':['Communication Services','Telecom Services'],
  'TMUS':['Communication Services','Telecom Services'],'LUMN':['Communication Services','Telecom Services'],
  'DISH':['Communication Services','Telecom Services'],'SIRI':['Communication Services','Entertainment'],
  'LYV':['Communication Services','Entertainment'],'WMG':['Communication Services','Entertainment'],
  'PARA':['Communication Services','Entertainment'],'WBD':['Communication Services','Entertainment'],
  'FOX':['Communication Services','Entertainment'],'FOXA':['Communication Services','Entertainment'],
  'NYT':['Communication Services','Publishing'],'NWSA':['Communication Services','Publishing'],
  'IAC':['Communication Services','Internet Content & Information'],
  'PINS':['Communication Services','Internet Content & Information'],
  'SNAP':['Communication Services','Internet Content & Information'],
  'RDDT':['Communication Services','Internet Content & Information'],
  'MTCH':['Communication Services','Internet Content & Information'],
  'TTD':['Communication Services','Advertising Agencies'],'DV':['Communication Services','Advertising Agencies'],
  'MGNI':['Communication Services','Advertising Agencies'],'YELP':['Communication Services','Internet Content & Information'],
  'Z':['Communication Services','Internet Content & Information'],'ZG':['Communication Services','Internet Content & Information'],
  'TRIP':['Communication Services','Internet Content & Information'],
  'IDT':['Communication Services','Telecom Services'],

  'AMT':['Real Estate','REIT—Specialty'],'PLD':['Real Estate','REIT—Industrial'],
  'EQIX':['Real Estate','REIT—Specialty'],'CCI':['Real Estate','REIT—Specialty'],
  'SBAC':['Real Estate','REIT—Specialty'],'SPG':['Real Estate','REIT—Retail'],
  'O':['Real Estate','REIT—Retail'],'WPC':['Real Estate','REIT—Diversified'],
  'VICI':['Real Estate','REIT—Diversified'],'GLPI':['Real Estate','REIT—Diversified'],
  'EQR':['Real Estate','REIT—Residential'],'AVB':['Real Estate','REIT—Residential'],
  'ESS':['Real Estate','REIT—Residential'],'UDR':['Real Estate','REIT—Residential'],
  'CPT':['Real Estate','REIT—Residential'],'MAA':['Real Estate','REIT—Residential'],
  'NNN':['Real Estate','REIT—Retail'],'EPRT':['Real Estate','REIT—Retail'],
  'ADC':['Real Estate','REIT—Retail'],'KIM':['Real Estate','REIT—Retail'],
  'REG':['Real Estate','REIT—Retail'],'FRT':['Real Estate','REIT—Retail'],
  'BXP':['Real Estate','REIT—Office'],'VNO':['Real Estate','REIT—Office'],
  'SLG':['Real Estate','REIT—Office'],'HIW':['Real Estate','REIT—Office'],
  'ARE':['Real Estate','REIT—Office'],'EXR':['Real Estate','REIT—Industrial'],
  'PSA':['Real Estate','REIT—Industrial'],'CUBE':['Real Estate','REIT—Industrial'],
  'STAG':['Real Estate','REIT—Industrial'],'REXR':['Real Estate','REIT—Industrial'],
  'EGP':['Real Estate','REIT—Industrial'],'FR':['Real Estate','REIT—Industrial'],
  'TRNO':['Real Estate','REIT—Industrial'],'COLD':['Real Estate','REIT—Industrial'],
  'DRH':['Real Estate','REIT—Hotel & Motel'],'PK':['Real Estate','REIT—Hotel & Motel'],
  'HST':['Real Estate','REIT—Hotel & Motel'],'RHP':['Real Estate','REIT—Hotel & Motel'],
  'WELL':['Real Estate','REIT—Healthcare Facilities'],'VTR':['Real Estate','REIT—Healthcare Facilities'],
  'PEAK':['Real Estate','REIT—Healthcare Facilities'],'OHI':['Real Estate','REIT—Healthcare Facilities'],
  'NHI':['Real Estate','REIT—Healthcare Facilities'],'CTRE':['Real Estate','REIT—Healthcare Facilities'],
  'MPW':['Real Estate','REIT—Healthcare Facilities'],'HR':['Real Estate','REIT—Healthcare Facilities'],
  'INVH':['Real Estate','REIT—Residential'],'AMH':['Real Estate','REIT—Residential'],
  'AIRC':['Real Estate','REIT—Residential'],'IRM':['Real Estate','REIT—Specialty'],
  'DLR':['Real Estate','REIT—Specialty'],'WY':['Real Estate','REIT—Specialty'],
  'RYN':['Real Estate','REIT—Specialty'],'IIPR':['Real Estate','REIT—Industrial'],
  'NTST':['Real Estate','REIT—Retail'],'PINE':['Real Estate','REIT—Retail'],
  'GNL':['Real Estate','REIT—Diversified'],'PDM':['Real Estate','REIT—Office'],
  'SBRA':['Real Estate','REIT—Healthcare Facilities'],'CHCT':['Real Estate','REIT—Healthcare Facilities'],
  'DHC':['Real Estate','REIT—Healthcare Facilities'],'GMRE':['Real Estate','REIT—Healthcare Facilities'],

  'NEE':['Utilities','Utilities—Regulated Electric'],'DUK':['Utilities','Utilities—Regulated Electric'],
  'SO':['Utilities','Utilities—Regulated Electric'],'D':['Utilities','Utilities—Regulated Electric'],
  'AEP':['Utilities','Utilities—Regulated Electric'],'EXC':['Utilities','Utilities—Regulated Electric'],
  'XEL':['Utilities','Utilities—Regulated Electric'],'ED':['Utilities','Utilities—Regulated Electric'],
  'WEC':['Utilities','Utilities—Regulated Electric'],'ETR':['Utilities','Utilities—Regulated Electric'],
  'PPL':['Utilities','Utilities—Regulated Electric'],'CNP':['Utilities','Utilities—Regulated Electric'],
  'AES':['Utilities','Utilities—Diversified'],'ES':['Utilities','Utilities—Regulated Electric'],
  'FE':['Utilities','Utilities—Regulated Electric'],'NRG':['Utilities','Utilities—Independent Power Producers'],
  'VST':['Utilities','Utilities—Independent Power Producers'],'CEG':['Utilities','Utilities—Independent Power Producers'],
  'PCG':['Utilities','Utilities—Regulated Electric'],'SRE':['Utilities','Utilities—Diversified'],
  'CMS':['Utilities','Utilities—Regulated Electric'],'LNT':['Utilities','Utilities—Regulated Electric'],
  'EVRG':['Utilities','Utilities—Regulated Electric'],'OGE':['Utilities','Utilities—Regulated Electric'],
  'POR':['Utilities','Utilities—Regulated Electric'],'OTTR':['Utilities','Utilities—Regulated Electric'],
  'MGEE':['Utilities','Utilities—Regulated Electric'],'IDA':['Utilities','Utilities—Regulated Electric'],
  'AWK':['Utilities','Utilities—Regulated Water'],'NI':['Utilities','Utilities—Regulated Gas'],
  'ATO':['Utilities','Utilities—Regulated Gas'],'NWN':['Utilities','Utilities—Regulated Gas'],
  'CWEN':['Utilities','Utilities—Renewable'],'BEP':['Utilities','Utilities—Renewable'],

  'LIN':['Basic Materials','Specialty Chemicals'],'APD':['Basic Materials','Specialty Chemicals'],
  'SHW':['Basic Materials','Specialty Chemicals'],'ECL':['Basic Materials','Specialty Chemicals'],
  'PPG':['Basic Materials','Specialty Chemicals'],'DD':['Basic Materials','Specialty Chemicals'],
  'DOW':['Basic Materials','Specialty Chemicals'],'EMN':['Basic Materials','Specialty Chemicals'],
  'CE':['Basic Materials','Specialty Chemicals'],'RPM':['Basic Materials','Specialty Chemicals'],
  'HUN':['Basic Materials','Specialty Chemicals'],'OLN':['Basic Materials','Specialty Chemicals'],
  'WLK':['Basic Materials','Specialty Chemicals'],'AXTA':['Basic Materials','Specialty Chemicals'],
  'FMC':['Basic Materials','Agricultural Inputs'],'MOS':['Basic Materials','Agricultural Inputs'],
  'CF':['Basic Materials','Agricultural Inputs'],'NTR':['Basic Materials','Agricultural Inputs'],
  'CTVA':['Basic Materials','Agricultural Inputs'],'NEM':['Basic Materials','Gold'],
  'GOLD':['Basic Materials','Gold'],'AEM':['Basic Materials','Gold'],'KGC':['Basic Materials','Gold'],
  'PAAS':['Basic Materials','Silver'],'FCX':['Basic Materials','Copper'],'SCCO':['Basic Materials','Copper'],
  'AA':['Basic Materials','Aluminum'],'CENX':['Basic Materials','Aluminum'],
  'X':['Basic Materials','Steel'],'NUE':['Basic Materials','Steel'],'STLD':['Basic Materials','Steel'],
  'CLF':['Basic Materials','Steel'],'CMC':['Basic Materials','Steel'],'RS':['Basic Materials','Steel'],
  'ATI':['Basic Materials','Steel'],'ZEUS':['Basic Materials','Steel'],
  'IP':['Basic Materials','Paper & Paper Products'],'GPK':['Basic Materials','Packaging & Containers'],
  'PKG':['Basic Materials','Packaging & Containers'],'SEE':['Basic Materials','Packaging & Containers'],
  'SON':['Basic Materials','Packaging & Containers'],'BERY':['Basic Materials','Packaging & Containers'],
  'AMCR':['Basic Materials','Packaging & Containers'],'WRK':['Basic Materials','Packaging & Containers'],
};

function getTickerSector(ticker) {
  return TICKER_SECTOR_MAP[ticker] || null;
}

// Per-window cache: { 7: {result, time}, 30: {result, time}, 90: {result, time} }
const _sectorResultCache = {};
const SECTOR_RESULT_TTL = 30 * 60 * 1000;

function buildSectorResult(days) {
  const rows = db.prepare(`
    SELECT
      ticker, MAX(company) AS company,
      SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END)           AS buy_val,
      SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
      COUNT(CASE WHEN TRIM(type)='P' THEN 1 END)                                 AS buy_count,
      COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END)                       AS sell_count,
      COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END)                  AS buyer_count,
      MAX(CASE WHEN TRIM(type)='P' THEN trade_date END)                          AS latest_buy
    FROM trades
    WHERE trade_date >= date('now', '-' || ? || ' days')
    AND trade_date <= date('now')
      AND TRIM(type) IN ('P','S','S-')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
    GROUP BY ticker
    HAVING buy_count > 0 OR sell_count > 0
    ORDER BY (buy_val + sell_val) DESC
    LIMIT 500
  `).all(days);

  const sectorMap = {};
  let mapped = 0, skipped = 0;
  rows.forEach(r => {
    const info = getTickerSector(r.ticker);
    if (!info) { skipped++; return; }
    mapped++;
    const [sector, industry] = info;
    if (!sectorMap[sector]) {
      sectorMap[sector] = {
        sector, buy_val: 0, sell_val: 0, buy_count: 0, sell_count: 0,
        buyer_count: 0, ticker_count: 0, tickers: [], industries: {}
      };
    }
    const s = sectorMap[sector];
    s.buy_val     += r.buy_val  || 0;
    s.sell_val    += r.sell_val || 0;
    s.buy_count   += r.buy_count  || 0;
    s.sell_count  += r.sell_count || 0;
    s.buyer_count += r.buyer_count || 0;
    s.ticker_count++;
    if (r.buy_val > 0) s.tickers.push({ ticker: r.ticker, company: r.company || r.ticker, buy_val: r.buy_val, buyer_count: r.buyer_count, latest_buy: r.latest_buy });
    if (industry) s.industries[industry] = (s.industries[industry] || 0) + (r.buy_val || 0);
  });

  slog(`Sectors(${days}d): ${rows.length} tickers, ${mapped} mapped, ${skipped} no-sector`);

  const sectors = Object.values(sectorMap)
    .filter(s => s.buy_val > 0 || s.sell_val > 0)
    .map(s => ({
      ...s,
      tickers: s.tickers.sort((a, b) => b.buy_val - a.buy_val).slice(0, 10),
      top_industry: Object.entries(s.industries).sort((a, b) => b[1] - a[1])[0]?.[0] || '',
      sentiment: s.buy_val + s.sell_val > 0
        ? Math.round(s.buy_val / (s.buy_val + s.sell_val) * 100)
        : 0,
    }))
    .sort((a, b) => b.buy_val - a.buy_val);

  return { sectors, days, total_buy_val: sectors.reduce((s, x) => s + x.buy_val, 0) };
}

let _sectorsCache = null, _sectorsCacheTime = 0;
app.get('/api/sectors', (req, res) => {
  try {
    if (_sectorsCache && Date.now() - _sectorsCacheTime < 120000) return res.json(_sectorsCache);
    const days = [7, 30, 90].includes(parseInt(req.query.days)) ? parseInt(req.query.days) : 30;
    const now  = Date.now();
    const entry = _sectorResultCache[days];
    if (entry && now - entry.time < SECTOR_RESULT_TTL) {
      return res.json(entry.result);
    }
    const result = buildSectorResult(days);
    _sectorResultCache[days] = { result, time: now };
    res.json(result);
  } catch(e) {
    slog('sectors error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── DEBUG ENDPOINT — shows DB stats for diagnosing data issues ───────────
app.get('/api/ping', (req, res) => res.json({ ok: true, t: Date.now() }));

app.get('/api/debug', (req, res) => {
  try {
    const total = db.prepare('SELECT COUNT(*) AS n FROM trades').get().n;
    const byType = db.prepare("SELECT COALESCE(TRIM(type),'NULL') AS type, COUNT(*) AS n FROM trades GROUP BY TRIM(type) ORDER BY n DESC LIMIT 20").all();
    const dates = db.prepare("SELECT MAX(trade_date) AS td_latest, MIN(trade_date) AS td_earliest, MAX(filing_date) AS fd_latest, MIN(filing_date) AS fd_earliest FROM trades WHERE trade_date IS NOT NULL").get();
    const p7_trade   = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-7 days')").get().n;
    const p30_trade  = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-30 days')").get().n;
    const p30_filing = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND filing_date >= date('now','-30 days')").get().n;
    const p3yr_trade = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date  >= date('now','-1095 days')").get().n;
    const ranker30   = db.prepare("SELECT COUNT(DISTINCT ticker) AS n FROM trades WHERE filing_date >= date('now','-30 days')").get().n;
    const history1095 = db.prepare("SELECT COUNT(*) AS n FROM trades WHERE TRIM(type)='P' AND trade_date >= date('now','-1095 days')").get().n;

    // Test the exact screener query to confirm it works
    let screenerTest = null, screenerError = null;
    try {
      const rows = db.prepare(`
        SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
               trade_date AS trade, MAX(filing_date) AS filing,
               TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
               MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
        FROM trades
        WHERE trade_date >= date('now', '-7 days')
        AND trade_date <= date('now')
          AND TRIM(type) IN ('P','S','S-')
          AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
        GROUP BY ticker, insider, trade_date, type
        ORDER BY trade_date DESC LIMIT 5
      `).all();
      screenerTest = { ok: true, rows: rows.length };
    } catch(e) { screenerError = e.message; }

    res.json({
      db_path: DB_PATH,
      total_rows: total,
      by_type: byType,
      dates,
      buys_last_7d_by_trade_date: p7_trade,
      buys_last_30d_by_trade_date: p30_trade,
      buys_last_30d_by_filing_date: p30_filing,
      buys_3yr: p3yr_trade,
      tickers_filed_30d: ranker30,
      history_3yr_buys: history1095,
      screener_query_test: screenerTest || { ok: false, error: screenerError },
      server_version: '2026-03-11',
      sync_running: syncRunning,
      daily_running: dailyRunning,
    });
    _sectorsCacheTime = Date.now();
    res.json(_sectorsCache);
  } catch(e) { res.status(500).json({ error: e.message, db_path: DB_PATH }); }
});

// ── PRICE FETCH HELPER ──────────────────────────────────────────────────────
// In-memory LRU-style price cache: ticker → { bars, fetchedAt }
// bars = [{time:'YYYY-MM-DD', open, high, low, close, volume}, ...]
const _priceCache = {}; // in-memory layer on top of DB

// TTL is market-hours-aware:
//   • During US market hours (9:30–16:00 ET, Mon–Fri) → 15 minutes
//     so today's candle stays current for active users.
//   • Outside market hours (evenings, nights, weekends) → 12 hours
//     since daily bars won't change until the next session opens.
function getPriceTTL() {
  const now = new Date();
  // Convert current UTC time to US Eastern (ET = UTC-5 standard, UTC-4 daylight)
  // We approximate by checking if DST is in effect for New York
  const janOffset = new Date(now.getFullYear(), 0, 1).getTimezoneOffset();
  const julOffset = new Date(now.getFullYear(), 6, 1).getTimezoneOffset();
  const isDST     = now.getTimezoneOffset() < Math.max(janOffset, julOffset);
  const etOffset  = isDST ? 4 : 5; // hours behind UTC
  const etHour    = (now.getUTCHours() - etOffset + 24) % 24;
  const etMin     = now.getUTCMinutes();
  const etTotalMin = etHour * 60 + etMin;
  const day       = now.getUTCDay(); // 0=Sun, 6=Sat; adjust for ET midnight rollover
  // Market open = 9:30 ET = 570 min, close = 16:00 ET = 960 min, Mon–Fri
  const isWeekday = day >= 1 && day <= 5;
  const isMarketHours = isWeekday && etTotalMin >= 570 && etTotalMin < 960;
  return isMarketHours
    ? 15 * 60 * 1000        // 15 minutes during market hours
    : 12 * 60 * 60 * 1000;  // 12 hours outside market hours
}

function getPC(sym) {
  const ttl = getPriceTTL();
  // Check memory first
  const m = _priceCache[sym];
  if (m && Date.now() - m.fetchedAt <= ttl && m.bars.length > 0) return m.bars;
  // Fall back to DB
  try {
    const row = db.prepare('SELECT bars_json, fetched_at FROM price_cache WHERE symbol=?').get(sym);
    if (row && Date.now() - row.fetched_at <= ttl) {
      const bars = JSON.parse(row.bars_json);
      if (bars.length > 0) {
        _priceCache[sym] = { bars, fetchedAt: row.fetched_at };
        return bars;
      }
    }
  } catch(e) {}
  return null;
}
function setPC(sym, bars) {
  const fetchedAt = Date.now();
  _priceCache[sym] = { bars, fetchedAt };
  // Persist to DB asynchronously — don't block the response
  try {
    db.prepare('INSERT OR REPLACE INTO price_cache (symbol, bars_json, fetched_at) VALUES (?,?,?)')
      .run(sym, JSON.stringify(bars), fetchedAt);
  } catch(e) {}
}

// Fetch 2 years of daily OHLCV bars.
// Chain: Tiingo -> Polygon -> Yahoo query1 -> Yahoo query2
// All sources require free API keys (TIINGO_KEY, POLYGON_KEY) set as Render env vars.
// Yahoo kept as keyless fallback but is unreliable for server-side requests.
async function fetchPriceBars(sym) {
  const cached = getPC(sym);
  if (cached) return cached;
  let _rateLimited = false;  // track 429s so we don't cache misses caused by rate limits

  function parseYahoo(body) {
    const data   = JSON.parse(body.toString());
    const result = data && data.chart && data.chart.result && data.chart.result[0];
    if (!result) return null;
    const ts = result.timestamp || [];
    const q  = (result.indicators && result.indicators.quote && result.indicators.quote[0]) || {};
    const bars = ts.map((t, i) => ({
      time:   new Date(t * 1000).toISOString().slice(0, 10),
      open:   (q.open   && q.open[i])   || 0,
      high:   (q.high   && q.high[i])   || 0,
      low:    (q.low    && q.low[i])    || 0,
      close:  (q.close  && q.close[i])  || 0,
      volume: (q.volume && q.volume[i]) || 0,
    })).filter(b => b.close > 0);
    return bars.length >= 2 ? bars : null;
  }

  const end   = new Date().toISOString().slice(0, 10);
  const start = new Date(Date.now() - 20 * 365 * 86400000).toISOString().slice(0, 10);
  const endTs = Math.floor(Date.now() / 1000);
  const startTs = endTs - 20 * 365 * 86400;

  // Race all sources — return as soon as the first one responds with valid data
  const makeSource = (promise) => promise.then(result => {
    if (!result) throw new Error('no data');
    return result;
  });

  try {
    const bars = await Promise.any([
      // Tiingo (primary)
      TIINGO ? makeSource(get('https://api.tiingo.com/tiingo/daily/' + sym + '/prices?startDate=' + start + '&endDate=' + end + '&format=json&resampleFreq=daily&token=' + TIINGO, 5000).then(({ status, body }) => {
        if (status === 429) { _rateLimited = true; return null; }
        if (status !== 200) return null;
        const data = JSON.parse(body.toString());
        if (!Array.isArray(data) || data.length < 2) return null;
        const bars = data.map(d => ({ time: d.date.slice(0,10), open: d.open||d.close||0, high: d.high||d.close||0, low: d.low||d.close||0, close: d.close||0, volume: d.volume||0 })).filter(b => b.close > 0);
        return bars.length >= 2 ? bars : null;
      })) : Promise.reject(new Error('no tiingo')),

      // Polygon (secondary)
      POLYGON ? makeSource(get('https://api.polygon.io/v2/aggs/ticker/' + sym + '/range/1/day/' + start + '/' + end + '?adjusted=true&sort=asc&limit=5200&apiKey=' + POLYGON, 5000).then(({ status, body }) => {
        if (status !== 200) return null;
        const data = JSON.parse(body.toString());
        if (!data.results || data.results.length < 2) return null;
        const bars = data.results.map(d => ({ time: new Date(d.t).toISOString().slice(0,10), open: d.o||0, high: d.h||0, low: d.l||0, close: d.c||0, volume: d.v||0 })).filter(b => b.close > 0);
        return bars.length >= 2 ? bars : null;
      })) : Promise.reject(new Error('no polygon')),

      // Yahoo query1 (fallback)
      makeSource(get('https://query1.finance.yahoo.com/v8/finance/chart/' + sym + '?interval=1d&period1=' + startTs + '&period2=' + endTs, 5000).then(({ status, body }) => {
        if (status !== 200) return null;
        return parseYahoo(body);
      }).catch(() => null)),

      // Yahoo query2 (fallback)
      makeSource(get('https://query2.finance.yahoo.com/v8/finance/chart/' + sym + '?interval=1d&period1=' + startTs + '&period2=' + endTs, 5000).then(({ status, body }) => {
        if (status !== 200) return null;
        return parseYahoo(body);
      }).catch(() => null)),
    ]);
    // Sanitize: sort ascending by date and remove duplicate timestamps.
    // LightweightCharts requires strictly-ascending time order with no dupes;
    // Yahoo Finance (and occasionally other sources) can return out-of-order
    // or duplicate bars (e.g. for tickers with corporate actions or sparse
    // coverage), which causes setData() to throw and leaves the chart blank.
    const sanitized = bars
      .sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0))
      .filter((b, i, arr) => i === 0 || b.time !== arr[i - 1].time);
    setPC(sym, sanitized);
    return sanitized;
  } catch (_) {
    // All sources failed — do NOT cache the failure, allow retry on next request
    return null;
  }
}

// Warm price cache for the most-active insider tickers at startup
async function warmPriceCache() {
  try {
    // Form 4 most-active tickers
    const form4Rows = db.prepare(`
      SELECT ticker FROM trades
      WHERE TRIM(type) IN ('P','S','S-') AND trade_date >= date('now','-365 days')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 75
    `).all();

    const allTickers = [...new Set(form4Rows.map(r => r.ticker))].slice(0, 75);
    slog(`Warming price cache for ${allTickers.length} tickers...`);
    for (let i = 0; i < allTickers.length; i += 10) {
      const batch = allTickers.slice(i, i + 10);
      await Promise.allSettled(batch.map(sym => fetchPriceBars(sym)));
      if (i + 10 < allTickers.length) await new Promise(r => setTimeout(r, 400));
    }
    slog('Price cache warm-up complete');
  } catch(e) { slog('warmPriceCache error: ' + e.message); }
}

app.get('/api/price', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  if (req.query.bust === '1') {
    delete _priceCache[sym];
    try { db.prepare('DELETE FROM price_cache WHERE symbol=?').run(sym); } catch(_) {}
  }
  const t0 = Date.now();
  const bars = await fetchPriceBars(sym);
  if (res.headersSent) return;
  const ms = Date.now() - t0;
  if (!bars || !bars.length) {
    slog(`price MISS ${sym}: ${ms}ms — tiingo=${!!TIINGO} polygon=${!!POLYGON}`);
  } else {
    slog(`price OK ${sym}: ${bars.length} bars in ${ms}ms`);
  }
  res.json(bars || []);
});

// Returns {TICKER: bars[], ...} for up to 20 tickers in one call
app.get('/api/prices-bulk', async (req, res) => {
  const raw  = (req.query.symbols || '').toUpperCase().trim();
  if (!raw) return res.status(400).json({ error: 'symbols required' });
  const syms = [...new Set(raw.split(',').map(s => s.trim()).filter(Boolean))].slice(0, 20);
  const cold = syms.filter(s => !getPC(s));
  if (cold.length) await Promise.allSettled(cold.map(s => fetchPriceBars(s)));
  const result = {};
  syms.forEach(s => { const b = getPC(s); if (b) result[s] = b; });
  res.json(result);
});

// ── POST-BUY DRIFT ───────────────────────────────────────────────────────────
// Measures average D+30 / D+90 / D+180 returns after every insider buy.
// Uses the price already stored in each trade row (SEC-reported price) plus
// the warm price-bar cache for forward price lookup — no extra API calls.
let _driftServerCache     = null;
let _driftServerCacheTime = 0;
const DRIFT_TTL = 6 * 60 * 60 * 1000;

let _driftRunning = false;
async function preComputeDrift() {
  if (_driftRunning) return;
  _driftRunning = true;
  try {
    slog('Pre-computing drift...');
    const rows = db.prepare(`
      SELECT ticker, MAX(company) AS company,
        GROUP_CONCAT(trade_date || '|' || COALESCE(price,0), ';;') AS trade_series,
        COUNT(*) AS buy_count,
        MAX(trade_date) AS last_buy
      FROM trades
      WHERE TRIM(type)='P'
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND trade_date >= date('now','-1825 days')
        AND trade_date <= date('now','-95 days')
        AND price > 0
      GROUP BY ticker
      HAVING buy_count >= 3
      ORDER BY buy_count DESC
      LIMIT 50
    `).all();

    if (!rows.length) { slog('Drift pre-compute: no rows'); return; }

    // Fetch price bars for all tickers
    await Promise.allSettled(rows.map(r => fetchPriceBars(r.ticker)));

    const results = [];
    rows.forEach(r => {
      try {
        const bars = getPC(r.ticker);
        if (!bars || bars.length < 30) return;

        const trades = (r.trade_series || '').split(';;').map(s => {
          const [dt, prStr] = s.split('|');
          return { date: dt?.slice(0, 10), price: parseFloat(prStr) || 0 };
        }).filter(t => t.date && t.price > 0);

        if (trades.length < 2) return;

        const fwd = (buyDate, buyPrice, days) => {
          const fd = new Date(buyDate + 'T12:00:00Z');
          fd.setUTCDate(fd.getUTCDate() + days);
          const fs = fd.toISOString().slice(0, 10);
          const bar = bars.find(b => b.time >= fs);
          const raw = bar ? +((bar.close - buyPrice) / buyPrice * 100).toFixed(2) : null;
          return raw !== null ? Math.max(-500, Math.min(500, raw)) : null; // cap split artifacts
        };

        const scored = trades.map(t => ({
          ret1:   fwd(t.date, t.price, 30),   // ~D+30 calendar = ~D+1 trading name
          ret5:   fwd(t.date, t.price, 90),   // ~D+90
          ret20:  fwd(t.date, t.price, 180),  // ~D+180
          ret60:  fwd(t.date, t.price, 365),  // ~D+365
        }));

        const avg = (cp) => {
          const vals = scored.map(s => s[cp]).filter(v => v !== null)
                             .map(v => Math.max(-500, Math.min(500, v))); // cap splits
          return vals.length ? +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(1) : null;
        };

        const avgObj = { 1: avg('ret1'), 5: avg('ret5'), 20: avg('ret20'), 60: avg('ret60') };
        const wins = scored.filter(s => s.ret5 !== null && s.ret5 > 0).length;
        const total = scored.filter(s => s.ret5 !== null).length;
        const winRate = total > 0 ? Math.round(wins / total * 100) : 0;

        // Weighted drift score
        const weights = { 1: 0.10, 5: 0.20, 20: 0.30, 60: 0.40 };
        let wscore = 0, wsum = 0;
        Object.entries(weights).forEach(([cp, w]) => {
          const v = avgObj[parseInt(cp)];
          if (v !== null) { wscore += v * w; wsum += w; }
        });
        const weightedAvg = wsum > 0 ? +(wscore / wsum).toFixed(1) : 0;

        // Score 0-100: base from weighted avg + win rate bonus + acceleration bonus
        const accelerates = avgObj[5] !== null && avgObj[20] !== null && avgObj[20] > avgObj[5];
        const base  = Math.min(60, Math.max(0, weightedAvg * 3 + 30));
        const wrBonus = Math.min(25, winRate * 0.25);
        const accBonus = accelerates ? 15 : 0;
        const score = Math.min(100, Math.round(base + wrBonus + accBonus));

        if (score < 10) return;

        results.push({
          ticker: r.ticker, company: r.company || r.ticker,
          avg: avgObj, score, winRate, weightedAvg,
          buyCount: trades.length, lastBuy: r.last_buy,
          accelerates
        });
      } catch(e) {}
    });

    results.sort((a, b) => b.score - a.score);
    _driftServerCache     = results;
    _driftServerCacheTime = Date.now();
    slog(`Drift pre-computed: ${results.length} tickers`);
  } catch(e) { slog('preComputeDrift error: ' + e.message); } finally { _driftRunning = false; }
}

app.get('/api/drift', async (req, res) => {
  if (_driftServerCache && Date.now() - _driftServerCacheTime < DRIFT_TTL) {
    return res.json(_driftServerCache);
  }
  if (!_driftRunning) await preComputeDrift();
  // If still running from startup chain, wait up to 30s for it to finish
  if (_driftRunning) {
    for (let i = 0; i < 60; i++) {
      await new Promise(r => setTimeout(r, 500));
      if (!_driftRunning) break;
    }
  }
  if (res.headersSent) return;
  res.json(_driftServerCache || []);
});

// ── INSIDER EVENT PROXIMITY ──────────────────────────────────────────────────
// Finds insider buys within 120 days before a known upcoming event (earnings,
// 8-K, regulatory) and scores them by proximity.
let _proximityServerCache     = null;  // cleared on deploy — will recompute with fixed logic
let _proximityServerCacheTime = 0;
const PROXIMITY_TTL = 3 * 60 * 60 * 1000; // 3h

// Earnings date cache: ticker -> { date, confirmed, fetched }
const _earningsCache = {};
const EARNINGS_CACHE_TTL = 24 * 60 * 60 * 1000; // 24h

// Fetch earnings dates from Yahoo Finance quote summary API
// Uses /quoteSummary with calendarEvents module — more reliable than chart events
// Yahoo crumb/cookie state — refreshed every 12h
let _yahooCrumb   = null;
let _yahooCookie  = null;
let _yahooCrumbTs = 0;
const CRUMB_TTL   = 12 * 60 * 60 * 1000;

let _yahooCrumbBackoff = 0; // timestamp until which crumb fetches are suppressed
async function getYahooCrumb() {
  if (_yahooCrumb && Date.now() - _yahooCrumbTs < CRUMB_TTL) return _yahooCrumb;
  if (_yahooCrumbBackoff && Date.now() < _yahooCrumbBackoff) return null; // rate limited
  try {
    // Step 1: hit fc.yahoo.com to get the consent cookie
    const r1 = await get('https://fc.yahoo.com', 8000);
    const raw = Array.isArray(r1.headers?.['set-cookie'])
      ? r1.headers['set-cookie'].join('; ')
      : (r1.headers?.['set-cookie'] || '');
    // Extract A3 cookie value which is what Yahoo needs
    const cookieMatch = raw.match(/A3=[^;]+/);
    _yahooCookie = cookieMatch ? cookieMatch[0] : null;

    // Step 2: fetch the crumb using the cookie
    const crumbOpts = _yahooCookie
      ? { headers: { 'Cookie': _yahooCookie, 'User-Agent': 'Mozilla/5.0' } }
      : { headers: { 'User-Agent': 'Mozilla/5.0' } };
    const r2 = await get('https://query1.finance.yahoo.com/v1/test/getcrumb', 8000, crumbOpts);
    if (r2.status === 200) {
      _yahooCrumb   = r2.body.toString().trim();
      _yahooCrumbTs = Date.now();
      slog(`Yahoo crumb acquired: ${_yahooCrumb.slice(0,8)}...`);
      return _yahooCrumb;
    }
    slog(`Yahoo crumb HTTP ${r2.status}`);
    if (r2.status === 429) { _yahooCrumbBackoff = Date.now() + 10 * 60 * 1000; }
  } catch(e) {
    slog(`Yahoo crumb error: ${e.message}`);
  }
  return null;
}

async function fetchConfirmedEarnings(ticker) {
  const cached = _earningsCache[ticker];
  if (cached && Date.now() - cached.fetched < EARNINGS_CACHE_TTL) return cached;
  // Don't attempt if crumb is in backoff period
  if (_yahooCrumbBackoff && Date.now() < _yahooCrumbBackoff) return null;
  try {
    const today  = new Date().toISOString().slice(0, 10);
    const crumb  = await getYahooCrumb();
    if (!crumb) return null; // backoff active, don't spam
    const crumbQ = crumb ? `&crumb=${encodeURIComponent(crumb)}` : '';
    const cookie = _yahooCookie ? _yahooCookie : '';
    const url    = `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(ticker)}?modules=calendarEvents${crumbQ}`;
    const opts   = cookie ? { headers: { 'Cookie': cookie, 'User-Agent': 'Mozilla/5.0' } } : {};
    const { status, body } = await get(url, 8000, opts);
    if (status === 200) {
      const data = JSON.parse(body.toString());
      const cal  = data?.quoteSummary?.result?.[0]?.calendarEvents;
      if (cal) {
        const dates = cal?.earnings?.earningsDate || [];
        const upcoming = dates
          .map(d => d.fmt || new Date(d.raw * 1000).toISOString().slice(0, 10))
          .filter(d => d && d >= today)
          .sort()[0];
        if (upcoming) {
          const entry = { date: upcoming, confirmed: true, fetched: Date.now() };
          _earningsCache[ticker] = entry;
          return entry;
        }
      }
    } else if (status === 401) {
      // Crumb expired — force refresh on next call
      _yahooCrumb = null;
      _yahooCrumbTs = 0;
      _yahooCrumbBackoff = Date.now() + 5 * 60 * 1000; // wait 5min before re-fetching crumb
      slog(`Yahoo earnings ${ticker}: 401, crumb reset`);
    } else {
      slog(`Yahoo earnings ${ticker}: HTTP ${status}`);
    }
  } catch(e) {
    slog(`Yahoo earnings ${ticker} error: ${e.message}`);
  }
  const miss = { date: null, confirmed: false, fetched: Date.now() };
  _earningsCache[ticker] = miss;
  return null;
}

// Estimate next earnings from fiscal calendar (fallback)
function estimateNextEarnings(buyDate) {
  try {
    const buyDt = new Date(buyDate + 'T12:00:00Z');
    const yr    = buyDt.getUTCFullYear();
    const qEnds = [
      new Date(Date.UTC(yr,   2, 31)),
      new Date(Date.UTC(yr,   5, 30)),
      new Date(Date.UTC(yr,   8, 30)),
      new Date(Date.UTC(yr,  11, 31)),
      new Date(Date.UTC(yr+1, 2, 31)),
    ];
    const nextQEnd = qEnds.find(d => d > buyDt);
    if (!nextQEnd) return null;
    const estReport = new Date(nextQEnd);
    estReport.setUTCDate(estReport.getUTCDate() + 45);
    return { date: estReport.toISOString().slice(0,10), confirmed: false };
  } catch(_) { return null; }
}

// Get next earnings date for a ticker — uses cache populated by preComputeProximity
function predictNextEarnings(ticker, buyDate) {
  const cached = _earningsCache[ticker];
  const src = (cached?.date) ? cached : estimateNextEarnings(buyDate);
  if (!src?.date) return null;
  const buyDt  = new Date(buyDate + 'T12:00:00Z');
  const evtDt  = new Date(src.date + 'T12:00:00Z');
  const daysTo = Math.round((evtDt - buyDt) / 86400000);
  if (daysTo <= 0 || daysTo > 120) return null;
  return {
    date: src.date,
    type: 'QUARTERLY',
    label: src.confirmed ? '✓ Confirmed Earnings' : 'Est. Earnings',
    predicted: !src.confirmed,
    confirmed: src.confirmed || false,
  };
}

let _proximityRunning = false;
async function preComputeProximity() {
  if (_proximityRunning) return;
  _proximityRunning = true;
  try {
    slog('Pre-computing proximity...');
    const today = new Date().toISOString().slice(0, 10);
    const since = new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10);
    const future = new Date(Date.now() + 120 * 86400000).toISOString().slice(0, 10);

    const rows = db.prepare(`
      SELECT
        t.ticker, t.company, t.insider, t.title,
        t.trade_date AS buyDate,
        t.value AS buyVal,
        t.filing_date
      FROM trades t
      WHERE TRIM(t.type) = 'P'
        AND t.trade_date >= ?
        AND t.trade_date <= ?
        AND t.ticker GLOB '[A-Z]*' AND LENGTH(t.ticker) BETWEEN 1 AND 6
        AND t.value > 0
      ORDER BY t.trade_date DESC
      LIMIT 400
    `).all(since, today);

    // Pre-fetch earnings dates from Yahoo Finance for all unique tickers
    // Pre-warm the crumb once so all ticker requests use it immediately
    const uniqueTickers = [...new Set(rows.map(r => r.ticker))].slice(0, 80);
    slog(`Fetching earnings dates for ${uniqueTickers.length} tickers from Yahoo...`);
    await getYahooCrumb();  // warm crumb before batch
    const BATCH = 5;
    for (let i = 0; i < uniqueTickers.length; i += BATCH) {
      const batch = uniqueTickers.slice(i, i + BATCH);
      await Promise.allSettled(batch.map(t => fetchConfirmedEarnings(t)));
      if (i + BATCH < uniqueTickers.length) await new Promise(r => setTimeout(r, 150));
    }
    const confirmed = Object.values(_earningsCache).filter(e => e.confirmed && e.date).length;
    slog(`Earnings dates: ${confirmed}/${uniqueTickers.length} confirmed from Yahoo, rest estimated`);

    const results = [];
    const seenPairs = new Set(); // deduplicate ticker+insider

    rows.forEach(row => {
      try {
        const pairKey = `${row.ticker}|${row.insider}`;
        if (seenPairs.has(pairKey)) return;
        seenPairs.add(pairKey);

        // Predict next event
        const event = predictNextEarnings(row.ticker, row.buyDate);
        if (!event) return;

        const buyDt  = new Date(row.buyDate + 'T12:00:00Z');
        const evtDt  = new Date(event.date + 'T12:00:00Z');
        const daysTo = Math.round((evtDt - buyDt) / 86400000);

        if (daysTo < 0 || daysTo > 120) return;

        // Score
        let score = 0;
        if (daysTo <= 7)       score += 40;
        else if (daysTo <= 14) score += 28;
        else if (daysTo <= 21) score += 18;
        else if (daysTo <= 30) score += 10;
        else                   score += 5;

        if (event.type === 'QUARTERLY')      score += 10;
        else if (event.type === 'REGULATORY') score += 8;
        else                                  score += 5;

        const titleL = (row.title || '').toLowerCase();
        const isCsuite = /\b(ceo|cfo|coo|cto|president|chairman|chief)\b/.test(titleL);
        const isDir    = /\bdirector\b/.test(titleL);
        if (isCsuite) score += 8;
        else if (isDir) score += 4;

        if (row.buyVal >= 5000000) score += 12;
        else if (row.buyVal >= 1000000) score += 8;
        else if (row.buyVal >= 500000) score += 5;

        // Check repeat pattern: did this insider buy before prior predicted events?
        const priorBuys = db.prepare(`
          SELECT COUNT(*) AS n FROM trades
          WHERE insider = ? AND ticker = ? AND TRIM(type) = 'P'
            AND trade_date < ? AND trade_date >= date(?,' -365 days')
        `).get(row.insider, row.ticker, row.buyDate, row.buyDate);
        const repeatPattern = (priorBuys?.n || 0) >= 2;
        if (repeatPattern) score += 12;

        score = Math.min(100, score);

        // Abnormal: any insider buying within 21 days of confirmed earnings
        // OR C-suite/Director buying within 45 days of confirmed earnings
        const isAbnormal = event.confirmed && (
          daysTo <= 21 ||
          ((isCsuite || isDir) && daysTo <= 45)
        );
        const proximityColor = daysTo <= 7 ? 'var(--sell)' : daysTo <= 14 ? 'var(--option)' : daysTo <= 30 ? 'var(--accent)' : 'var(--muted)';

        results.push({
          ticker: row.ticker,
          company: row.company || row.ticker,
          insider: row.insider || '—',
          title: row.title || '—',
          buyDate: row.buyDate,
          buyVal: row.buyVal || 0,
          buyValue: row.buyVal || 0,
          nextEvent: event,
          allUpcoming: [event],
          daysTo,
          score,
          isAbnormal,
          repeatPattern,
          proximityColor,
        });
      } catch(_) {}
    });

    results.sort((a, b) => b.score - a.score);
    _proximityServerCache     = results;
    _proximityServerCacheTime = Date.now();
    slog(`Proximity pre-computed: ${results.length} results`);
  } catch(e) { slog('preComputeProximity error: ' + e.message); } finally { _proximityRunning = false; }
}

const _bootTime = Date.now();
const STARTUP_GRACE_MS = 90000; // 90s — let warm-up chain finish before user-triggered computes

app.get('/api/proximity', async (req, res) => {
  if (_proximityServerCache && Date.now() - _proximityServerCacheTime < PROXIMITY_TTL) {
    return res.json(_proximityServerCache);
  }
  // During startup grace period, return empty rather than triggering a compute
  // that would compete with the warm-up chain
  if (!_proximityServerCache && Date.now() - _bootTime < STARTUP_GRACE_MS) {
    return res.json({ computing: true, results: [] });
  }
  // Don't trigger a second compute if one is already running
  if (!_proximityRunning) preComputeProximity().catch(e => slog('proximity err: ' + e.message));
  // Wait up to 30s for it to finish
  for (let i = 0; i < 60; i++) {
    await new Promise(r => setTimeout(r, 500));
    if (!_proximityRunning) break;
  }
  if (res.headersSent) return;
  res.json(_proximityServerCache || []);
});

// ── INSIDER SCOREBOARD ───────────────────────────────────────────────────────
// Scores every insider with 4+ completed buy trades against actual forward
// price returns at D+30 / D+90 / D+180. Uses the warm price cache.
let _scoreboardCache     = null;
let _scoreboardCacheTime = 0;
let _scoreboardRunning   = false;   // C3: prevent parallel precompute runs
const SCOREBOARD_TTL = 6 * 60 * 60 * 1000; // 6 hours

async function preComputeScoreboard() {
  if (_scoreboardCache) return;
  if (_scoreboardRunning) return;   // already in-flight — don't double-run
  _scoreboardRunning = true;
  try {
    slog('Pre-computing scoreboard...');
    const minBuys = 3, limit = 150;
    const rows = db.prepare(`
      SELECT
        insider, MAX(title) AS title, COUNT(*) AS buy_count,
        GROUP_CONCAT(ticker || '|' || trade_date || '|' || COALESCE(price,0) || '|' || COALESCE(value,0), ';;') AS trade_data,
        GROUP_CONCAT(DISTINCT ticker) AS tickers_csv,
        CAST(julianday(MAX(trade_date)) - julianday(MIN(trade_date)) AS INTEGER) AS span_days
      FROM trades
      WHERE TRIM(type) = 'P'
        AND insider IS NOT NULL AND ticker IS NOT NULL AND price > 0
        AND trade_date >= date('now', '-1825 days')
        AND trade_date <= date('now', '-95 days')
      GROUP BY insider
      HAVING buy_count >= ? AND span_days >= 30
      ORDER BY buy_count DESC, span_days DESC LIMIT ?
    `).all(minBuys, limit);

    if (!rows.length) { slog('Scoreboard pre-compute: no qualifying rows'); return; }

    const allTickers = [...new Set(
      rows.flatMap(r => (r.tickers_csv || '').split(',').filter(Boolean))
    )].slice(0, 80);

    const priceEntries = await Promise.allSettled(
      allTickers.map(async sym => [sym, await fetchPriceBars(sym)])
    );
    const priceCache = Object.fromEntries(
      priceEntries.filter(r => r.status === 'fulfilled' && r.value[1]).map(r => r.value)
    );

    const accuracyResults = [], timingResults = [];

    rows.forEach(leader => {
      try {
        const rawTrades = (leader.trade_data || '').split(';;').map(s => {
          const [ticker, trade_date, priceStr, valueStr] = s.split('|');
          return { ticker, trade: trade_date, price: parseFloat(priceStr) || 0, value: parseFloat(valueStr) || 0 };
        }).filter(t => t.ticker && t.trade);

        if (rawTrades.length < 4) return;

        const scored = rawTrades.map(t => {
          const bars = priceCache[t.ticker] || [];
          if (!bars.length) return null;
          const buyDate  = t.trade.slice(0, 10);
          const entryBar = bars.find(b => b.time >= buyDate);
          const buyPrice = t.price > 0 ? t.price : (entryBar?.close || 0);
          if (!buyPrice || buyPrice <= 0) return null;
          const fwd = days => {
            const fd = new Date(buyDate + 'T12:00:00Z');
            fd.setUTCDate(fd.getUTCDate() + days);
            const fs  = fd.toISOString().slice(0, 10);
            const bar = bars.find(b => b.time >= fs);
            return bar ? +((bar.close - buyPrice) / buyPrice * 100).toFixed(2) : null;
          };
          return { ticker: t.ticker, tradeDate: buyDate, buyPrice,
            ret30: fwd(30), ret90: fwd(90), ret180: fwd(180) };
        }).filter(Boolean);

        const completed = scored.filter(s => s.ret90 !== null);
        if (completed.length < 3) return;

        // Cap returns at ±500% to eliminate reverse-split artifacts
        // (e.g. Ault Global has done multiple reverse splits causing 7000%+ fake returns)
        const CAP = 200; // cap at 200% to eliminate split artifacts
        const capRet = r => Math.max(-CAP, Math.min(CAP, r));

        const rets90   = completed.map(s => capRet(s.ret90));
        const avgRet90 = +(rets90.reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const rets30   = completed.filter(s => s.ret30 !== null).map(s => capRet(s.ret30));
        const avgRet30 = rets30.length ? +(rets30.reduce((a, b) => a + b, 0) / rets30.length).toFixed(1) : null;
        const winRate  = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
        const avgMag   = +(rets90.map(Math.abs).reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
        const sortedR  = [...rets90].sort((a, b) => a - b);
        const median   = sortedR[Math.floor(sortedR.length / 2)];
        const consist  = Math.round(Math.min(100, Math.max(0, (median / Math.max(avgMag, 1) + 1) * 50)));
        const accScore = Math.round(Math.min(100, Math.max(0,
          winRate * 0.40 + Math.min(35, Math.max(0, avgRet90 / 20 * 35))
          + consist * 0.15 + Math.min(10, completed.length * 1.2)
        )));
        const tier     = accScore >= 75 ? 'ELITE' : accScore >= 55 ? 'STRONG' : accScore >= 35 ? 'AVERAGE' : 'WEAK';
        const tickers3 = [...new Set(rawTrades.map(t => t.ticker))].slice(0, 3).join(', ');

        if (accScore >= 35) {  // only include insiders with a meaningful accuracy score
          accuracyResults.push({ name: leader.insider, title: leader.title || '',
            accuracyScore: accScore, tier, winRate, avgRet90, avgRet30,
            tradeCount: completed.length, tickers: tickers3 });
        }

        // Timing alpha — measures quality of entry price relative to 12M range
        let nearLowCount = 0, ret90sum = 0, ret180sum = 0, retN = 0;
        scored.forEach(s => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars = bars.filter(b =>
            b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate
          );
          if (yr1Bars.length < 20) return;
          const yr1Lo = Math.min(...yr1Bars.map(b => b.low || b.close));
          if ((s.buyPrice - yr1Lo) / yr1Lo * 100 <= 20) nearLowCount++;
          if (s.ret90  !== null) { ret90sum  += Math.max(-200, Math.min(200, s.ret90));  retN++; }
          if (s.ret180 !== null)   ret180sum += Math.max(-200, Math.min(200, s.ret180));
        });

        const n         = scored.length;
        const nearLowPct = n > 0 ? Math.round(nearLowCount / n * 100) : 0;
        const avgFwd90  = retN > 0 ? +(ret90sum  / retN).toFixed(1) : null;
        const avgFwd180 = retN > 0 ? +(ret180sum / retN).toFixed(1) : null;

        // Compute avg position in annual range
        const avgPos = scored.reduce((acc, s) => {
          const bars = priceCache[s.ticker] || [];
          if (!bars.length || !s.buyPrice) return acc;
          const yr1Start = new Date(s.tradeDate + 'T12:00:00Z');
          yr1Start.setUTCFullYear(yr1Start.getUTCFullYear() - 1);
          const yr1Bars = bars.filter(b =>
            b.time >= yr1Start.toISOString().slice(0, 10) && b.time <= s.tradeDate
          );
          if (yr1Bars.length < 20) return acc;
          const yr1Lo = Math.min(...yr1Bars.map(b => b.low  || b.close));
          const yr1Hi = Math.max(...yr1Bars.map(b => b.high || b.close));
          const rng = yr1Hi - yr1Lo;
          return rng > 0.01 ? { sum: acc.sum + (s.buyPrice - yr1Lo) / rng, n: acc.n + 1 } : acc;
        }, { sum: 0, n: 0 });
        const avgPosVal = avgPos.n > 0 ? avgPos.sum / avgPos.n : 0.5;

        const compLow  = Math.round(Math.min(25, (nearLowPct / 100) * 50));
        const compPos  = Math.round(Math.min(25, Math.max(0, (0.7 - avgPosVal) / 0.4 * 25)));
        const compFwd  = avgFwd90 !== null ? Math.round(Math.min(25, Math.max(0, avgFwd90 / 10 * 25))) : 12;
        const compSell = 12;
        const timingAlpha = Math.min(100, Math.max(0, compLow + compPos + compFwd + compSell));

        let verdict;
        if      (timingAlpha >= 80 && nearLowPct >= 50) verdict = 'Buys near bottoms';
        else if (timingAlpha >= 80)                     verdict = 'Elite forward returns';
        else if (timingAlpha >= 60)                     verdict = 'Above-average timing';
        else if (timingAlpha >= 40)                     verdict = 'Mixed timing signals';
        else                                            verdict = 'Tends to buy high';

        if (timingAlpha >= 35) {
          timingResults.push({ name: leader.insider, title: leader.title || '',
            timingAlpha, nearLowPct, avgRet90: avgFwd90, avgRet180: avgFwd180,
            verdict, tradeCount: completed.length, tickers: tickers3 });
        }
      } catch(e) { slog('scoreboard insider err: ' + e.message); }
    });

    accuracyResults.sort((a, b) => b.accuracyScore - a.accuracyScore);
    timingResults.sort((a, b) => b.timingAlpha - a.timingAlpha);

    const result = { accuracy: accuracyResults, timing: timingResults };
    _scoreboardCache     = result;
    _scoreboardCacheTime = Date.now();
    slog(`Scoreboard pre-computed: ${accuracyResults.length} accuracy, ${timingResults.length} timing`);
  } catch(e) { slog('preComputeScoreboard error: ' + e.message); }
  finally { _scoreboardRunning = false; }   // C3: always release lock
}

// C2: Non-blocking route — never awaits preCompute inline.
// Returns {computing:true} immediately if not ready; client retries.
app.get('/api/scoreboard', (req, res) => {
  if (_scoreboardCache && Date.now() - _scoreboardCacheTime < SCOREBOARD_TTL) {
    return res.json(_scoreboardCache);
  }
  if (!_scoreboardCache && Date.now() - _bootTime < STARTUP_GRACE_MS) {
    return res.json({ computing: true, accuracy: [], timing: [] });
  }
  // Trigger compute in background (guard prevents double-runs)
  preComputeScoreboard().catch(e => slog('scoreboard bg err: ' + e.message));
  if (_scoreboardCache) return res.json(_scoreboardCache);
  return res.json({ computing: true, accuracy: [], timing: [] });
});

// ── 13F WORKER ───────────────────────────────────────────────────────────────


// Schedule quarterly — run on the 20th of Feb, May, Aug, Nov (45+ days after quarter end)
// Also run once at startup if not yet processed current quarter

// ── STARTUP PRECOMPUTES ──────────────────────────────────────────────────────
// Start daily ingestion immediately on boot (handles market-hours check internally)
runDaily(3);

// ── Congressional trades (STOCK Act PTRs) ────────────────────────────────────
// Congress worker disabled — re-enable when FMP data source is configured



// Pre-prepared statements for hot paths
let _stmtTradeCount;
function initPreparedStatements() {
  _stmtTradeCount = db.prepare('SELECT COUNT(*) AS n FROM trades');
}
 // 20s delay — lets daily-worker finish startup writes first

// H5: Sequential chain — price warm → drift → proximity → scoreboard
// Prevents all three from hammering external price APIs simultaneously.
// Clear empty/failed price cache entries immediately so they retry
try { db.prepare(`DELETE FROM price_cache WHERE bars_json = '[]'`).run(); } catch(_) {}

// Build search index immediately — must be ready before first user request
buildSearchIndex();

// Pre-warm popular large-caps immediately (no delay) — these are searched by users
// but rarely have insider buys so they'd miss the 60s warm-up otherwise
const POPULAR_TICKERS = ['AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','JPM','V','CRM',
  'JNJ','UNH','XOM','PG','MA','HD','ABBV','BAC','WMT','SNOW','GS','AMD','PLTR','COIN'];
setTimeout(() => {
  Promise.allSettled(POPULAR_TICKERS.map(sym => fetchPriceBars(sym)))
    .then(() => slog('Popular ticker price cache warmed'))
    .catch(() => {});
}, 5000); // 5s after startup — just enough for DB to be ready

setTimeout(() => {
  // Pre-warm stock-lists cache so Stock View landing loads instantly
  try {
    const http = require('http');
    http.get('http://localhost:' + (process.env.PORT || 10000) + '/api/stock-lists', r => {
      let d = '';
      r.on('data', c => d += c);
      r.on('end', () => {
        try {
          _stockListsCache = JSON.parse(d);
          _stockListsCacheTime = Date.now();
          slog('Stock-lists pre-warmed: mostActive=' + (_stockListsCache.mostActive||[]).length + ' hotBuys=' + (_stockListsCache.hotBuys||[]).length);
        } catch(_) {}
      });
    }).on('error', () => {});
  } catch(e) {}
}, 30000);

setTimeout(() => {
  // Pre-warm monitor data so Monitor page loads instantly
  try {
    const http = require('http');
    const port = process.env.PORT || 10000;
    const warmUrl = `/api/screener?days=92`;
    http.get('http://localhost:' + port + warmUrl, r => {
      r.resume(); // drain response
      slog('Monitor screener pre-warmed');
    }).on('error', () => {});
  } catch(e) {}
}, 45000);

setTimeout(() => {
  warmPriceCache()
    .then(() => preComputeDrift())
    .then(() => preComputeProximity())
    .then(() => preComputeScoreboard())
    .catch(e => slog('startup precompute err: ' + e.message));
}, 60000);

// Run alert check every 5 minutes — same cadence as RSS poll in daily-worker
// Offset by 3 minutes so it runs after new trades are likely ingested
setTimeout(() => {
  runAlertCheck();
  setInterval(runAlertCheck, 5 * 60 * 1000);
}, 3 * 60 * 1000);

// Refresh every 12h — intentionally does NOT null the cache before starting.
// preComputeScoreboard skips if cache is still fresh; the _scoreboardRunning
// guard prevents double-runs. Stale-but-valid data stays available to users
// during the refresh window rather than showing {computing:true}.
setInterval(() => {
  _scoreboardCache     = null;  // expire so preComputeScoreboard re-runs
  _scoreboardCacheTime = 0;
  warmPriceCache()
    .then(() => preComputeDrift())
    .then(() => preComputeProximity())
    .then(() => preComputeScoreboard())
    .catch(e => slog('refresh err: ' + e.message));
}, 12 * 60 * 60 * 1000);

// ── ADMIN TRIGGER ROUTES ─────────────────────────────────────────────────────
// Protected by ADMIN_SECRET env var — set this in your Render environment variables.
// Call as: /api/admin/sync?secret=YOUR_SECRET
function requireAdminSecret(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret) return false; // if not set, allow (backwards compat on first deploy)
  if (req.query.secret !== secret) {
    res.status(403).json({ error: 'Forbidden — set ?secret=YOUR_ADMIN_SECRET' });
    return true;
  }
  return false;
}
// POST /api/admin/sync — trigger a full historical sync (4 quarters)
// ── F13 admin endpoints ───────────────────────────────────────────────────────


app.get('/api/admin/unban', (req, res) => {
  if (req.query.secret !== process.env.ADMIN_SECRET) return res.status(403).json({ error: 'Forbidden' });
  const ip = req.query.ip;
  if (!ip) {
    // Clear all bans
    const count = _bannedIPs.size + _strikeMap.size;
    _bannedIPs.clear();
    _strikeMap.clear();
    return res.json({ ok: true, cleared: count });
  }
  _bannedIPs.delete(ip);
  _strikeMap.delete(ip);
  res.json({ ok: true, unbanned: ip });
});

app.get('/api/admin/grant-premium', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email param required' });
  try {
    const user = db.prepare('SELECT id, email FROM users WHERE LOWER(email) = ?').get(email);
    if (!user) return res.status(404).json({ error: 'User not found: ' + email });
    const existing = db.prepare('SELECT * FROM subscriptions WHERE user_id = ?').get(user.id);
    const periodEnd = new Date(Date.now() + 365 * 24 * 3600 * 1000).toISOString();
    if (existing) {
      db.prepare(`UPDATE subscriptions SET status='active', current_period_end=?, updated_at=datetime('now') WHERE user_id=?`).run(periodEnd, user.id);
    } else {
      db.prepare(`INSERT INTO subscriptions (user_id, plan, status, current_period_end, updated_at) VALUES (?, 'monthly', 'active', ?, datetime('now'))`).run(user.id, periodEnd);
    }
    res.json({ ok: true, message: `Premium granted to ${user.email} until ${periodEnd}` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/check-user', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const email = (req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'email param required' });
  try {
    const user = db.prepare('SELECT id, email, is_admin FROM users WHERE LOWER(email) = ?').get(email);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const sub = db.prepare('SELECT * FROM subscriptions WHERE user_id = ?').get(user.id);
    res.json({ user, subscription: sub || null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/sync', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  runSync(parseInt(req.query.q || '4'));
  res.json({ ok: true, message: 'Sync triggered' });
});
// POST /api/admin/daily — trigger daily ingestion now regardless of market hours
app.get('/api/admin/daily', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  if (dailyRunning) return res.json({ ok: false, message: 'Daily worker already running' });
  dailyRunning = true;
  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=200', path.join(__dirname, 'daily-worker.js'), '7', 'poll'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[manual-daily] ' + l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[manual-daily] ERR: ' + l)));
  worker.on('exit', code => { dailyRunning = false; slog(`manual-daily exited (${code})`); });
  res.json({ ok: true, message: 'Daily ingestion triggered (7-day backfill)' });
});

// GET /api/admin/reingest-recent?days=30&confirm=1
// Deletes all trades from the last N days and triggers a fresh re-ingest.
// This time footnotes are stored and DRIP/offering filters apply cleanly.
// Without confirm=1 returns a preview of what would be deleted.
app.get('/api/admin/reingest-recent', (req, res) => {
  const days    = Math.min(parseInt(req.query.days || '30'), 90);
  const confirm = req.query.confirm === '1';
  try {
    const count = db.prepare(`
      SELECT COUNT(*) AS n FROM trades
      WHERE trade_date >= date('now', '-' || ? || ' days')
    `).get(days).n;

    if (!confirm) {
      return res.json({
        mode: 'preview',
        days,
        would_delete: count,
        message: 'Add &confirm=1 to delete and trigger re-ingest'
      });
    }

    // Delete recent trades
    const result = db.prepare(`
      DELETE FROM trades WHERE trade_date >= date('now', '-' || ? || ' days')
    `).run(days);

    // Invalidate caches
    _stockListsCache = null;

    // Trigger daily worker re-ingest for the same window
    if (!dailyRunning) {
      dailyRunning = true;
      const worker = spawn(
        process.execPath,
        ['--max-old-space-size=200', path.join(__dirname, 'daily-worker.js'), String(days), 'backfill'],
        { stdio: ['ignore', 'pipe', 'pipe'] }
      );
      worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[reingest] ' + l)));
      worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[reingest] ERR: ' + l)));
      worker.on('exit', code => { dailyRunning = false; slog('reingest worker exited (' + code + ')'); });
    }

    res.json({
      mode: 'deleted_and_reingesting',
      days,
      deleted: result.changes,
      message: 'Deleted ' + result.changes + ' trades. Re-ingest running in background — check /api/debug in a few minutes.'
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/deep-backfill?days=365&confirm=1
// Fills historical gaps WITHOUT deleting existing data — safe to run anytime.
// Uses backfill mode which does INSERT OR IGNORE so duplicates are skipped.
app.get('/api/admin/deep-backfill', (req, res) => {
  const days    = Math.min(parseInt(req.query.days || '365'), 730);
  const confirm = req.query.confirm === '1';

  const existing = db.prepare(`SELECT COUNT(*) AS n FROM trades`).get().n;
  const oldest   = db.prepare(`SELECT MIN(trade_date) AS d FROM trades`).get().d;

  if (!confirm) {
    return res.json({
      mode: 'preview',
      days,
      existing_trades: existing,
      oldest_trade_date: oldest,
      message: `Will backfill ${days} days from EDGAR form.idx. Add &confirm=1 to run. Does NOT delete existing data.`
    });
  }

  if (dailyRunning) return res.json({ ok: false, message: 'Worker already running — try again in a few minutes.' });

  dailyRunning = true;
  const worker = spawn(
    process.execPath,
    ['--max-old-space-size=300', path.join(__dirname, 'daily-worker.js'), String(days), 'backfill'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  worker.stdout.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[deep-backfill] ' + l)));
  worker.stderr.on('data', d => d.toString().trim().split('\n').forEach(l => slog('[deep-backfill] ERR: ' + l)));
  worker.on('exit', code => { dailyRunning = false; slog('deep-backfill worker exited (' + code + ')'); });

  res.json({
    mode: 'running',
    days,
    existing_trades: existing,
    oldest_before: oldest,
    message: `Deep backfill running for ${days} days. This may take 5–15 minutes. Check /api/debug for progress.`
  });
});

// GET /api/admin/purge-nondiscretionary?confirm=1
// Bulk-removes all non-discretionary 'P' trades already in the DB whose stored
// footnote matches DRIP or offering-participation keywords.
// Without confirm=1 returns a preview count and sample.
app.get('/api/admin/purge-nondiscretionary', (req, res) => {
  const confirm = req.query.confirm === '1';

  const keywords = [
    // DRIP / compensation plan
    'dividend reinvest', 'drip', 'reinvestment plan', 'director compensation',
    'compensation plan', 'deferred compensation', 'prior election',
    'stock purchase plan', 'employee stock purchase', 'espp',
    // Offering participation
    'public offering', 'underwritten offering', 'registered offering',
    'private placement', 'subscription agreement', 'securities purchase agreement',
    'placement agent', 'prospectus supplement', 'direct offering', 'pipe offering',
  ];

  // Build LIKE conditions for SQLite (case-insensitive via LOWER())
  const conditions = keywords.map(() => "LOWER(COALESCE(footnote,'')) LIKE ?").join(' OR ');
  const params     = keywords.map(k => '%' + k + '%');

  try {
    if (!confirm) {
      const count = db.prepare(`
        SELECT COUNT(*) AS n FROM trades
        WHERE TRIM(type) = 'P' AND footnote IS NOT NULL AND (${conditions})
      `).get(...params).n;

      const sample = db.prepare(`
        SELECT ticker, insider, trade_date, value,
               SUBSTR(footnote, 1, 120) AS footnote_preview
        FROM trades
        WHERE TRIM(type) = 'P' AND footnote IS NOT NULL AND (${conditions})
        ORDER BY trade_date DESC
        LIMIT 30
      `).all(...params);

      return res.json({
        mode: 'preview',
        would_delete: count,
        sample,
        message: 'Add &confirm=1 to execute'
      });
    }

    const result = db.prepare(`
      DELETE FROM trades
      WHERE TRIM(type) = 'P' AND footnote IS NOT NULL AND (${conditions})
    `).run(...params);

    _stockListsCache = null;
    res.json({ mode: 'deleted', deleted: result.changes });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/reingest-accession?accession=XXXX&filing_date=2026-03-17
// Re-fetches a specific Form 4 accession from EDGAR and inserts it,
// bypassing the footnote filter. Use for verified legitimate trades
// that were incorrectly dropped by the DRIP/offering filter.
app.get('/api/admin/reingest-accession', async (req, res) => {
  const accession   = (req.query.accession || '').trim();
  const filingDate  = (req.query.filing_date || new Date().toISOString().slice(0,10)).trim();
  if (!accession.match(/^\d{10}-\d{2}-\d{6}$/)) {
    return res.status(400).json({ error: 'Invalid accession format. Use XXXXXXXXXX-XX-XXXXXX' });
  }
  try {
    const acc = accession.replace(/-/g, '');
    const filerCik = parseInt(acc.slice(0, 10), 10).toString();

    async function tryUrl(url) {
      try {
        const r = await get(url, 15000, { headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' } });
        if (r.status !== 200) return null;
        const text = r.body.toString('utf8');
        return text.includes('ownershipDocument') ? text : null;
      } catch(e) { return null; }
    }

    let xml = null;
    // Try common filename patterns
    for (const name of ['xslF345X05/form4.xml', 'form4.xml', 'wf-form4.xml', accession + '.xml']) {
      xml = await tryUrl('https://www.sec.gov/Archives/edgar/data/' + filerCik + '/' + acc + '/' + name);
      if (xml) break;
    }
    if (!xml) {
      // Try JSON index to find the XML filename
      try {
        const idxR = await get('https://www.sec.gov/Archives/edgar/data/' + filerCik + '/' + acc + '/' + accession + '-index.json', 10000, { headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' } });
        if (idxR.status === 200) {
          const data = JSON.parse(idxR.body.toString('utf8'));
          const doc  = (data.documents || []).find(d => d.document && d.document.match(/\.xml$/i));
          if (doc) xml = await tryUrl('https://www.sec.gov/Archives/edgar/data/' + filerCik + '/' + acc + '/' + doc.document);
        }
      } catch(e) {}
    }
    if (!xml) return res.status(404).json({ error: 'Could not fetch XML from EDGAR — check accession number and try again' });

    // Parse WITHOUT the footnote filter — this is a manually verified clean trade
    const ticker  = (xml.match(/<issuerTradingSymbol[^>]*>\s*([^<]+)/i) || [])[1]?.trim().toUpperCase() || '';
    const company = (xml.match(/<issuerName[^>]*>\s*([^<]+)/i) || [])[1]?.trim() || '';
    const insider = (xml.match(/<rptOwnerName[^>]*>\s*([^<]+)/i) || [])[1]?.trim() || '';
    const title   = (xml.match(/<officerTitle[^>]*>\s*([^<]+)/i) || [])[1]?.trim() || '';

    const rows = [];
    const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
    let m;
    while ((m = ndRe.exec(xml))) {
      const block = m[1];
      const code  = (block.match(/<transactionCode[^>]*>\s*([^<]+)/i) || [])[1]?.trim() || '';
      if (!['P','S','S-'].includes(code)) continue;
      const dateStr = (block.match(/<transactionDate[^>]*>[\s\S]*?<value>\s*([^<]+)/i) || [])[1]?.trim() || filingDate;
      const date    = dateStr.slice(0,10);
      const qty     = Math.round(Math.abs(parseFloat((block.match(/<transactionShares[^>]*>[\s\S]*?<value>\s*([^<]+)/i) || [])[1] || '0') || 0));
      const price   = Math.abs(parseFloat((block.match(/<transactionPricePerShare[^>]*>[\s\S]*?<value>\s*([^<]+)/i) || [])[1] || '0') || 0);
      const owned   = Math.round(Math.abs(parseFloat((block.match(/<sharesOwnedFollowingTransaction[^>]*>[\s\S]*?<value>\s*([^<]+)/i) || [])[1] || '0') || 0));
      const value   = Math.round(qty * price);
      if (qty > 50000000 || price > 1500000 || value > 2000000000) continue;
      rows.push([ticker, company, insider, title, date, filingDate, code, qty, +price.toFixed(4), value, owned, accession, null]);
    }

    if (!rows.length) return res.json({ inserted: 0, message: 'No valid P/S/S- transactions found in filing' });

    const stmt = db.prepare('INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession,footnote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
    let inserted = 0;
    const txn = db.transaction(rs => { for (const r of rs) inserted += stmt.run(r).changes; });
    txn(rows);
    _stockListsCache = null;
    res.json({ inserted, rows: rows.length, ticker, insider, message: 'Done — no footnote filter applied' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/insert-trade?ticker=SCM&insider=Huskinson+W.+Todd&title=CFO+and+CCO&date=2026-03-13&filing_date=2026-03-17&type=P&qty=5700&price=8.785&owned=54297
// Manually inserts a single verified trade, bypassing all filters.
// Use only for confirmed legitimate trades dropped by the footnote filter.
app.get('/api/admin/insert-trade', (req, res) => {
  const ticker      = (req.query.ticker      || '').toUpperCase().trim();
  const insider     = (req.query.insider     || '').trim();
  const title       = (req.query.title       || '').trim();
  const date        = (req.query.date        || '').trim();
  const filingDate  = (req.query.filing_date || date).trim();
  const type        = (req.query.type        || 'P').trim();
  const qty         = Math.round(Math.abs(parseFloat(req.query.qty   || '0')));
  const price       = Math.abs(parseFloat(req.query.price || '0'));
  const owned       = Math.round(Math.abs(parseFloat(req.query.owned || '0')));
  const accession   = (req.query.accession   || 'manual-' + Date.now()).trim();
  const company     = (req.query.company     || ticker).trim();

  if (!ticker || !insider || !date || !qty) {
    return res.status(400).json({ error: 'Required: ticker, insider, date, qty' });
  }
  if (!['P','S','S-'].includes(type)) {
    return res.status(400).json({ error: 'type must be P, S, or S-' });
  }

  const value = Math.round(qty * price);
  try {
    const stmt = db.prepare('INSERT OR IGNORE INTO trades (ticker,company,insider,title,trade_date,filing_date,type,qty,price,value,owned,accession,footnote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
    const result = stmt.run(ticker, company, insider, title, date, filingDate, type, qty, +price.toFixed(4), value, owned, accession, null);
    _stockListsCache = null;
    res.json({
      inserted: result.changes,
      ticker, insider, date, type, qty, price, value,
      message: result.changes ? 'Trade inserted' : 'Already exists (duplicate key)'
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/purge-drip?ticker=GABC&date=2026-03-17&confirm=1
// Surgically removes confirmed DRIP trades for a specific ticker and date.
// Without confirm=1, returns a preview of what would be deleted.
app.get('/api/admin/purge-drip', (req, res) => {
  const ticker  = (req.query.ticker || '').toUpperCase().trim();
  const date    = (req.query.date   || '').trim();
  const confirm = req.query.confirm === '1';
  if (!ticker || !date) return res.status(400).json({ error: 'ticker and date required' });
  try {
    const preview = db.prepare(`
      SELECT ticker, insider, trade_date, value, title
      FROM trades
      WHERE ticker = ? AND trade_date = ? AND TRIM(type) = 'P'
      ORDER BY value DESC
    `).all(ticker, date);

    if (!confirm) {
      return res.json({
        mode: 'preview',
        ticker, date,
        would_delete: preview.length,
        rows: preview,
        message: 'Add &confirm=1 to execute the deletion'
      });
    }

    const result = db.prepare(`
      DELETE FROM trades WHERE ticker = ? AND trade_date = ? AND TRIM(type) = 'P'
    `).run(ticker, date);

    _stockListsCache = null;
    res.json({ mode: 'deleted', ticker, date, deleted: result.changes });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/free-disk — delete the bloated /var/data/trades.db to free disk space.
app.get('/api/admin/free-disk', (req, res) => {
  if (requireAdminSecret(req, res)) return;
  const varDb = '/var/data/trades.db';
  const varWal = '/var/data/trades.db-wal';
  const varShm = '/var/data/trades.db-shm';
  const results = [];
  for (const f of [varDb, varWal, varShm]) {
    try {
      if (fs.existsSync(f)) {
        const size = fs.statSync(f).size;
        fs.unlinkSync(f);
        results.push({ file: f, deleted: true, freedBytes: size, freedMB: +(size/1024/1024).toFixed(1) });
      } else {
        results.push({ file: f, skipped: true, reason: 'does not exist' });
      }
    } catch(e) {
      results.push({ file: f, deleted: false, error: e.message });
    }
  }
  const freed = results.filter(r => r.deleted).reduce((s, r) => s + r.freedBytes, 0);
  res.json({
    ok: true,
    freed_mb: +(freed/1024/1024).toFixed(1),
    files: results,
    next_step: freed > 0
      ? 'Disk space freed. Redeploy the server — it will now open /var/data/trades.db fresh and re-sync from SEC EDGAR.'
      : 'No files deleted. Check /api/disk for current disk state.'
  });
});


// H1: 15-min server-side cache — prevents 5 heavy GROUP BY queries on every Stock View open
let _stockListsCache     = null;
let _stockListsCacheTime = 0;
const STOCK_LISTS_TTL    = 15 * 60 * 1000;

app.get('/api/stock-lists', (req, res) => {
  if (_stockListsCache && Date.now() - _stockListsCacheTime < STOCK_LISTS_TTL) {
    slog('stock-lists served from cache: mostActive=' + (_stockListsCache.mostActive||[]).length);
    return res.json(_stockListsCache);
  }
  try {
    // Quick diagnostic: what's the most recent trade_date in the DB?
    const latestTrade = db.prepare("SELECT MAX(trade_date) AS d, COUNT(*) AS n FROM trades WHERE trade_date >= date('now','-30 days')").get();
    slog('stock-lists: latest_trade=' + latestTrade.d + ' trades_30d=' + latestTrade.n);

    // Most-active tickers: unique insiders, buy + sell counts, last 14 days
    const mostActive = db.prepare(`
      SELECT
        ticker, MAX(company) AS company,
        COUNT(DISTINCT insider) AS insiders,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
        COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
        SUM(COALESCE(value,0)) AS total_val,
        MAX(trade_date) AS latest_date,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buy_insiders,
        MAX(CASE WHEN TRIM(type)='P' AND (
          UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR
          UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%' OR
          UPPER(title) LIKE '%10%'
        ) THEN 1 ELSE 0 END) AS exec_buy
      FROM trades
      WHERE trade_date >= date('now','-14 days')
      AND trade_date <= date('now')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL','--')
      GROUP BY ticker
      HAVING (buys >= 1 OR sells >= 1)
        AND (buy_val >= 10000 OR sell_val >= 10000)
      ORDER BY (buy_val + sell_val) DESC
      LIMIT 24
    `).all();

    // Top insider buying: last 30 days, sorted by total buy value
    const hotBuys = db.prepare(`
      SELECT
        ticker, MAX(company) AS company,
        COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyers,
        COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
        SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
        MAX(CASE WHEN TRIM(type)='P' AND (
          UPPER(title) LIKE '%CEO%' OR UPPER(title) LIKE '%CFO%' OR
          UPPER(title) LIKE '%PRESIDENT%' OR UPPER(title) LIKE '%CHAIRMAN%'
        ) THEN 1 ELSE 0 END) AS exec_buy
      FROM trades
      WHERE trade_date >= date('now','-30 days')
      AND trade_date <= date('now')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL','--')
        AND TRIM(type) = 'P'
      GROUP BY ticker
      HAVING buyers >= 1 AND buy_val >= 50000
      ORDER BY buy_val DESC
      LIMIT 16
    `).all();

    // Cluster buys: 3+ distinct insiders buying same ticker in 10 days
    const clusterBuys = db.prepare(`
      SELECT
        ticker, MAX(company) AS company,
        COUNT(DISTINCT insider) AS buyer_count,
        COUNT(*) AS trade_count,
        SUM(COALESCE(value,0)) AS total_val,
        MAX(trade_date) AS latest
      FROM trades
      WHERE trade_date >= date('now','-14 days')
      AND trade_date <= date('now')
        AND TRIM(type) = 'P'
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL','--')
      GROUP BY ticker
      HAVING buyer_count >= 3
      ORDER BY buyer_count DESC, total_val DESC
      LIMIT 12
    `).all();

    // Fresh buys: brand-new insider buys filed today/yesterday
    const freshBuys = db.prepare(`
      SELECT ticker, MAX(company) AS company, MAX(insider) AS insider,
             MAX(value) AS val, MAX(trade_date) AS date
      FROM trades
      WHERE filing_date >= date('now','-2 days')
        AND TRIM(type) = 'P'
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL','--')
        AND COALESCE(value,0) >= 25000
      GROUP BY ticker
      ORDER BY val DESC
      LIMIT 16
    `).all();

    // Heavy sells: last 30 days
    const heavySells = db.prepare(`
      SELECT
        ticker, MAX(company) AS company,
        COUNT(DISTINCT insider) AS seller_count,
        SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val
      FROM trades
      WHERE trade_date >= date('now','-30 days')
      AND trade_date <= date('now')
        AND TRIM(type) IN ('S','S-')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL','--')
      GROUP BY ticker
      HAVING seller_count >= 2 AND sell_val >= 500000
      ORDER BY sell_val DESC
      LIMIT 12
    `).all();

    const payload = { hotBuys, clusterBuys, freshBuys, heavySells, mostActive };
    _stockListsCache     = payload;
    _stockListsCacheTime = Date.now();
    res.json(payload);
  } catch(e) {
    slog('stock-lists error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});


// ═══════════════════════════════════════════════════════════════
// ─── AUTH & BILLING ─────────────────────────────────════════════
// ═══════════════════════════════════════════════════════════════

const crypto = require('crypto');

// ── Helpers ──────────────────────────────────────────────────
function generateToken(bytes=32) {
  return crypto.randomBytes(bytes).toString('hex');
}

function getSession(req) {
  const header = req.headers['authorization'] || '';
  const cookie = req.headers['cookie'] || '';
  // Check Authorization: Bearer <token>
  let token = header.startsWith('Bearer ') ? header.slice(7) : null;
  // Fall back to cookie
  if (!token) {
    const m = cookie.match(/(?:^|;\s*)it_session=([^;]+)/);
    if (m) token = decodeURIComponent(m[1]);
  }
  if (!token) return null;
  try {
    const row = db.prepare(`
      SELECT s.*, u.email, u.is_admin FROM sessions s
      JOIN users u ON u.id = s.user_id
      WHERE s.token = ? AND s.expires_at > datetime('now')
    `).get(token);
    return row || null;
  } catch(_) { return null; }
}

function getSubscription(userId) {
  try {
    return db.prepare(`SELECT * FROM subscriptions WHERE user_id = ?`).get(userId);
  } catch(_) { return null; }
}

function isPremium(session) {
  if (!session) return false;
  if (session.is_admin) return true;
  // ADMIN_EMAIL bypass — normalize both sides for safe comparison
  const adminEmail = (ADMIN_EMAIL || '').trim().toLowerCase();
  const sessEmail  = (session.email || '').trim().toLowerCase();
  if (adminEmail && sessEmail && sessEmail === adminEmail) return true;
  // session row: s.* gives s.id (session id), s.user_id (the user's id)
  const userId = session.user_id;
  if (!userId) return false;
  const sub = getSubscription(userId);
  if (!sub) return false;
  if (sub.status === 'active') return true;
  // Grace: allow access if period_end is in the future (covers annual cancel-at-period-end)
  if (sub.current_period_end && sub.current_period_end > new Date().toISOString()) return true;
  return false;
}

// ── Middleware: attach session to req ────────────────────────
app.use((req, res, next) => {
  req.session = getSession(req);
  req.isPremium = isPremium(req.session);
  next();
});

// ── GET /api/auth/me ─────────────────────────────────────────
app.get('/api/auth/me', (req, res) => {
  const session = getSession(req);
  if (!session) return res.json({ loggedIn: false });
  res.json({
    loggedIn:  true,
    email:     session.email,
    isPremium: isPremium(session),
    isAdmin:   !!session.is_admin,
  });
});

// ── POST /api/auth/request-link ──────────────────────────────
app.post('/api/auth/request-link', authBruteGuard, express.json(), async (req, res) => {
  const email = (req.body.email || '').trim().toLowerCase();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Valid email required' });
  }
  try {
    // Upsert user
    db.prepare(`INSERT OR IGNORE INTO users (email) VALUES (?)`).run(email);
    const user = db.prepare(`SELECT * FROM users WHERE email = ?`).get(email);

    // Create magic token — expires in 15 minutes
    const token = generateToken();
    const expires = new Date(Date.now() + 15 * 60 * 1000).toISOString();
    db.prepare(`INSERT INTO magic_tokens (email, token, expires_at) VALUES (?, ?, ?)`).run(email, token, expires);

    const link = `${SITE_URL}/api/auth/verify?token=${token}`;

    // Send via Resend
    if (RESEND_KEY) {
      const { Resend } = require('resend');
      const resend = new Resend(RESEND_KEY);
      await resend.emails.send({
        from: FROM_EMAIL,
        to: email,
        subject: 'Your InsiderTape login link',
        html: `
          <div style="font-family:'Courier New',monospace;max-width:480px;margin:0 auto;background:#080b0f;color:#e0e6ed;padding:40px;border-radius:8px">
            <div style="font-size:22px;font-weight:700;letter-spacing:3px;color:#00d4ff;margin-bottom:8px">INSIDERTAPE</div>
            <div style="font-size:12px;color:#4a6580;margin-bottom:32px">FOLLOW THE SMART MONEY</div>
            <div style="font-size:15px;color:#e0e6ed;margin-bottom:24px">Click the button below to sign in. This link expires in <strong>15 minutes</strong>.</div>
            <a href="${link}" style="display:inline-block;background:#00d4ff;color:#000;font-family:'Courier New',monospace;font-weight:700;font-size:13px;letter-spacing:1px;padding:14px 32px;border-radius:6px;text-decoration:none">SIGN IN TO INSIDERTAPE</a>
            <div style="margin-top:32px;font-size:11px;color:#4a6580">If you didn't request this, you can safely ignore this email.<br>Link: ${link}</div>
          </div>
        `,
      });
    } else {
      // Dev fallback — log to console
      slog('MAGIC LINK (no Resend key): ' + link);
    }

    res.json({ ok: true });
  } catch(e) {
    console.error('request-link error:', e.message);
    res.status(500).json({ error: 'Failed to send link' });
  }
});

// ── GET /api/auth/verify ─────────────────────────────────────
app.get('/api/auth/verify', authBruteGuard, (req, res) => {
  const { token, next: nextUrl } = req.query;
  if (!token) return res.redirect('/signup?error=missing_token');
  try {
    const mt = db.prepare(`
      SELECT * FROM magic_tokens
      WHERE token = ? AND used = 0 AND expires_at > datetime('now')
    `).get(token);
    if (!mt) return res.redirect('/signup?error=invalid_token');

    // Mark token used
    db.prepare(`UPDATE magic_tokens SET used = 1 WHERE id = ?`).run(mt.id);

    // Get or create user
    db.prepare(`INSERT OR IGNORE INTO users (email) VALUES (?)`).run(mt.email);
    const user = db.prepare(`SELECT * FROM users WHERE email = ?`).get(mt.email);

    // Create 30-day session
    const sessionToken = generateToken();
    const expires = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
    db.prepare(`INSERT INTO sessions (user_id, token, expires_at) VALUES (?, ?, ?)`).run(user.id, sessionToken, expires);

    // Set cookie + redirect
    res.setHeader('Set-Cookie', `it_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${30*24*3600}; Secure`);

    // If already premium or admin, go straight to app
    const sub = getSubscription(user.id);
    const adminEmail = (ADMIN_EMAIL || '').trim().toLowerCase();
    const alreadyPremium = user.is_admin || (adminEmail && (user.email||'').toLowerCase() === adminEmail) ||
      (sub && (sub.status === 'active' || (sub.current_period_end && sub.current_period_end > new Date().toISOString())));

    if (alreadyPremium) return res.redirect('/?auth=1');
    // Otherwise go to Stripe checkout
    return res.redirect('/api/stripe/checkout?session=' + sessionToken);
  } catch(e) {
    console.error('verify error:', e.message);
    res.redirect('/signup?error=server_error');
  }
});

// ── GET /api/auth/logout ─────────────────────────────────────
app.post('/api/auth/logout', (req, res) => {
  const s = req.session;
  if (s) {
    try { db.prepare(`DELETE FROM sessions WHERE id = ?`).run(s.id); } catch(_) {}
  }
  res.setHeader('Set-Cookie', 'it_session=; Path=/; Max-Age=0');
  res.json({ ok: true });
});

// ── GET /api/stripe/checkout ─────────────────────────────────
// ── ALERT ENGINE ─────────────────────────────────────────────────────────────

async function sendAlertEmail(toEmail, signals) {
  if (!RESEND_KEY || !signals.length) return;
  const { Resend } = require('resend');
  const resend = new Resend(RESEND_KEY);

  const signalRows = signals.map(s => {
    const color = s.signal_type === 'EXIT_WARNING' ? '#ff4466' : s.signal_type === 'CLUSTER' ? '#00d4ff' : s.signal_type === 'FIRST_BUY' ? '#f5a623' : '#00ff88';
    const icon  = s.signal_type === 'EXIT_WARNING' ? '⚠️' : s.signal_type === 'CLUSTER' ? '🔵' : s.signal_type === 'FIRST_BUY' ? '🆕' : '⚡';
    return `
      <div style="border:1px solid #1e2d3d;border-radius:6px;padding:16px;margin-bottom:12px;background:#0d1117">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
          <span style="font-family:'Courier New',monospace;font-size:18px;font-weight:700;color:#00d4ff">${s.ticker}</span>
          <span style="font-family:'Courier New',monospace;font-size:11px;padding:3px 8px;background:${color}22;border:1px solid ${color}44;border-radius:3px;color:${color}">${icon} ${s.signal_type.replace('_',' ')}</span>
        </div>
        <div style="font-size:13px;color:#c9d8e8;margin-bottom:4px">${s.company}${s.subsector ? ` <span style="font-size:10px;color:#2a4a5a;margin-left:6px">${s.subsector}</span>` : ''}</div>
        <div style="font-size:12px;color:#e0e6ed;font-weight:600;margin-bottom:4px">${s.headline}</div>
        ${s.detail ? `<div style="font-size:11px;color:#4a6580;margin-bottom:8px">${s.detail}</div>` : ''}
        <a href="${SITE_URL}/stock/${s.ticker}" style="font-family:'Courier New',monospace;font-size:11px;color:#00d4ff;text-decoration:none">VIEW CHART →</a>
      </div>`;
  }).join('');

  const subject = signals.length === 1
    ? `InsiderTape Alert: ${signals[0].ticker} — ${signals[0].signal_type.replace('_',' ')}`
    : `InsiderTape: ${signals.length} new insider signals`;

  await resend.emails.send({
    from: FROM_EMAIL,
    to: toEmail,
    subject,
    html: `
      <div style="font-family:'Courier New',monospace;max-width:520px;margin:0 auto;background:#080b0f;color:#e0e6ed;padding:32px;border-radius:8px">
        <div style="font-size:20px;font-weight:700;letter-spacing:3px;color:#00d4ff;margin-bottom:4px">INSIDERTAPE</div>
        <div style="font-size:10px;color:#4a6580;letter-spacing:2px;margin-bottom:28px">INSIDER SIGNAL ALERT</div>
        ${signalRows}
        <div style="margin-top:24px;padding-top:20px;border-top:1px solid #1e2d3d;font-size:10px;color:#2a3d52">
          All data sourced from SEC Form 4 filings. Not investment advice.<br>
          <a href="${SITE_URL}/account" style="color:#2a3d52">Manage alert settings</a> · 
          <a href="${SITE_URL}/account?unsubscribe=1" style="color:#2a3d52">Unsubscribe</a>
        </div>
      </div>`,
  });
}

// Runs after each RSS poll — checks for new signals matching user alert prefs
// and sends emails for anything not already sent
let _alertRunning = false;
async function runAlertCheck() {
  if (_alertRunning || !RESEND_KEY) return;
  _alertRunning = true;
  try {
    // Get all premium users with alerts enabled
    const users = db.prepare(`
      SELECT u.id AS user_id, u.email, p.min_score, p.min_value, p.types, p.tickers,
             p.sectors, p.roles, p.frequency
      FROM alert_prefs p
      JOIN users u ON u.id = p.user_id
      JOIN subscriptions s ON s.user_id = p.user_id
      WHERE p.enabled = 1
        AND (s.status = 'active' OR (s.current_period_end IS NOT NULL AND s.current_period_end > datetime('now')))
    `).all();
    if (!users.length) return;

    // Get recent trades from last 48h (captures anything new since last check)
    // Look back 72h to catch insiders who file late (SEC allows 2 business days)
    const recentTrades = db.prepare(`
      SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
             trade_date, MAX(filing_date) AS filing_date,
             TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
             MAX(value) AS value, MAX(owned) AS owned
      FROM trades
      WHERE filing_date >= date('now', '-3 days')
        AND TRIM(type) IN ('P','S','S-')
        AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
        AND COALESCE(value,0) > 0
      GROUP BY ticker, insider, trade_date, type
      ORDER BY filing_date DESC, value DESC
    `).all();

    if (!recentTrades.length) return;

    // First-buy detection: insiders who filed a buy in last 48h AND had a 2yr+ gap
    // Uses same CTE logic as /api/firstbuys but scoped to recent filings only
    const firstBuyRows = db.prepare(`
      WITH recent_buyers AS (
        SELECT DISTINCT insider, ticker
        FROM trades
        WHERE TRIM(type) = 'P'
          AND filing_date >= date('now', '-2 days')
          AND insider IS NOT NULL AND ticker IS NOT NULL
      ),
      latest AS (
        SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title,
               MAX(t.trade_date) AS latest_trade, MAX(t.filing_date) AS latest_filing,
               MAX(t.value) AS latest_value
        FROM trades t
        JOIN recent_buyers rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P' AND t.filing_date >= date('now', '-2 days')
        GROUP BY t.insider, t.ticker
      ),
      prev AS (
        SELECT t.insider, t.ticker, MAX(t.trade_date) AS prev_trade
        FROM trades t
        JOIN recent_buyers rb ON t.insider = rb.insider AND t.ticker = rb.ticker
        WHERE TRIM(t.type) = 'P' AND t.filing_date < date('now', '-2 days')
        GROUP BY t.insider, t.ticker
      )
      SELECT l.ticker, l.company, l.insider, l.title,
             l.latest_trade, l.latest_filing, l.latest_value,
             p.prev_trade,
             CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
      FROM latest l
      LEFT JOIN prev p ON l.insider = p.insider AND l.ticker = p.ticker
      WHERE p.prev_trade IS NULL    -- truly first ever buy in DB
         OR CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= 730
    `).all();

    // Build simple signals from trades (server-side signal detection)
    const signals = buildSignalsFromTrades(recentTrades, firstBuyRows);
    if (!signals.length) return;

    for (const user of users) {
      const userTickers = user.tickers ? user.tickers.split(',').map(t => t.trim()).filter(Boolean) : [];
      const userTypes   = user.types.split(',').map(t => t.trim().toLowerCase());
      const userSectors = user.sectors ? user.sectors.split(',').map(s => s.trim().toLowerCase()).filter(Boolean) : [];
      const userRoles   = user.roles   ? user.roles.split(',').map(r => r.trim().toLowerCase()).filter(Boolean)   : [];

      const toSend = [];
      for (const sig of signals) {
        // Apply user filters
        if (sig.score < user.min_score) continue;
        if (sig.value < user.min_value) continue;
        if (userTickers.length && !userTickers.includes(sig.ticker)) continue;
        if (!userTypes.includes(sig.signal_type.toLowerCase().replace(' ','_'))) continue;

        // Sector filter — use TICKER_SECTOR_MAP; skip if ticker not in map and sector filter is active
        if (userSectors.length) {
          const tickerSector = getTickerSector(sig.ticker);
          if (!tickerSector) continue; // unknown sector — skip when filter is active
          const [sector, subsector] = tickerSector;
          const matchesSector = userSectors.some(s =>
            sector.toLowerCase().includes(s) || subsector.toLowerCase().includes(s)
          );
          if (!matchesSector) continue;
        }

        // Role filter — check sig.role_key set during signal building
        if (userRoles.length && sig.role_key) {
          if (!userRoles.includes(sig.role_key)) continue;
        }

        // Dedup — don't resend what was already sent
        const key = sig.ticker + '|' + sig.signal_type + '|' + sig.date;
        const already = db.prepare('SELECT 1 FROM alert_log WHERE user_id = ? AND signal_key = ?').get(user.user_id, key);
        if (already) continue;

        toSend.push(sig);
        // Mark as sent
        db.prepare('INSERT OR IGNORE INTO alert_log (user_id, ticker, signal_type, signal_key) VALUES (?,?,?,?)')
          .run(user.user_id, sig.ticker, sig.signal_type, key);
      }

      if (toSend.length) {
        await sendAlertEmail(user.email, toSend);
        slog('Alert: sent ' + toSend.length + ' signal(s) to ' + user.email);
      }
    }
  } catch(e) {
    slog('Alert check error: ' + e.message);
  } finally {
    _alertRunning = false;
  }
}

// Server-side signal builder — lightweight version of the client scoring
function buildSignalsFromTrades(trades, firstBuyRows = []) {
  const signals = [];
  const byTicker = {};

  // Helper: normalize title to role_key
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

  // Group trades by ticker
  for (const t of trades) {
    if (!byTicker[t.ticker]) byTicker[t.ticker] = [];
    byTicker[t.ticker].push(t);
  }

  for (const [ticker, tickerTrades] of Object.entries(byTicker)) {
    const company = tickerTrades[0].company || ticker;
    const buys    = tickerTrades.filter(t => t.type === 'P');
    const sells   = tickerTrades.filter(t => t.type === 'S' || t.type === 'S-');

    // CLUSTER: 2+ distinct insiders buying same ticker within 2 days
    if (buys.length >= 2) {
      const uniqueInsiders = new Set(buys.map(t => t.insider)).size;
      if (uniqueInsiders >= 2) {
        const totalVal = buys.reduce((s,t) => s + (t.value||0), 0);
        const score    = Math.min(100, 50 + uniqueInsiders * 10 + (totalVal >= 500000 ? 15 : totalVal >= 100000 ? 8 : 0));
        const date     = buys.sort((a,b) => b.filing_date.localeCompare(a.filing_date))[0].filing_date;
        const sector   = getTickerSector(ticker);
        signals.push({ ticker, company, signal_type: 'CLUSTER', score, value: totalVal, date,
          sector: sector ? sector[0] : null, subsector: sector ? sector[1] : null,
          role_key: null, // clusters span multiple roles
          headline: uniqueInsiders + ' insiders buying in cluster',
          detail: uniqueInsiders + ' distinct insiders · ' + formatVal(totalVal) + ' combined' });
      }
    }

    // CONVICTION: single large buy (CEO/CFO or big value)
    for (const t of buys) {
      const isCsuite = /\b(CEO|CFO|President|Chairman|COO|CTO)\b/i.test(t.title || '');
      const bigBuy   = (t.value || 0) >= 100000;
      if (isCsuite || bigBuy) {
        const score   = Math.min(100, 55 + (isCsuite ? 15 : 0) + (t.value >= 500000 ? 15 : t.value >= 100000 ? 8 : 0));
        const sector  = getTickerSector(ticker);
        const roleKey = getRoleKey(t.title);
        signals.push({ ticker, company, signal_type: 'CONVICTION', score, value: t.value || 0, date: t.filing_date,
          sector: sector ? sector[0] : null, subsector: sector ? sector[1] : null,
          role_key: roleKey,
          headline: 'High conviction buy · ' + (t.title || 'Insider'),
          detail: (t.insider || '') + ' · ' + formatVal(t.value || 0) });
      }
    }

    // EXIT_WARNING: 2+ distinct insiders selling
    if (sells.length >= 2) {
      const uniqueSellers = new Set(sells.map(t => t.insider)).size;
      if (uniqueSellers >= 2) {
        const totalSell = sells.reduce((s,t) => s + (t.value||0), 0);
        const score     = Math.min(100, 50 + uniqueSellers * 8 + (totalSell >= 1000000 ? 15 : 0));
        const date      = sells.sort((a,b) => b.filing_date.localeCompare(a.filing_date))[0].filing_date;
        const sector    = getTickerSector(ticker);
        signals.push({ ticker, company, signal_type: 'EXIT_WARNING', score, value: totalSell, date,
          sector: sector ? sector[0] : null, subsector: sector ? sector[1] : null,
          role_key: null,
          headline: 'Exit warning · ' + uniqueSellers + ' insiders selling',
          detail: uniqueSellers + ' distinct sellers · ' + formatVal(totalSell) + ' disclosed' });
      }
    }
  }

  // FIRST_BUY: insider returning after 2+ year gap (or no prior history)
  for (const fb of firstBuyRows) {
    const gapYears = fb.gap_days ? Math.floor(fb.gap_days / 365) : null;
    const gapLabel = fb.prev_trade === null
      ? 'No prior purchase on record'
      : gapYears + '+ year gap since last buy';
    const isCsuite = /\b(CEO|CFO|President|Chairman|COO|CTO)\b/i.test(fb.title || '');
    const score    = Math.min(100, 60 + (isCsuite ? 15 : 0) + ((fb.latest_value||0) >= 100000 ? 10 : 0) + (!fb.prev_trade ? 8 : 0));
    const sector   = getTickerSector(fb.ticker);
    const roleKey  = getRoleKey(fb.title);
    signals.push({
      ticker:      fb.ticker,
      company:     fb.company || fb.ticker,
      signal_type: 'FIRST_BUY',
      score,
      value:       fb.latest_value || 0,
      date:        fb.latest_filing,
      sector:      sector ? sector[0] : null,
      subsector:   sector ? sector[1] : null,
      role_key:    roleKey,
      headline:    'First buy in years · ' + (fb.title || 'Insider'),
      detail:      (fb.insider || '') + ' · ' + gapLabel + ' · ' + formatVal(fb.latest_value || 0),
    });
  }

  return signals.sort((a,b) => b.score - a.score);
}

function formatVal(n) {
  if (!n) return '$0';
  if (n >= 1e9) return '$' + (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return '$' + (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return '$' + (n/1e3).toFixed(0) + 'K';
  return '$' + n.toFixed(0);
}

// ── ALERT PREFERENCES ────────────────────────────────────────────────
// GET /api/alerts/prefs — get current user's alert preferences
app.get('/api/alerts/prefs', (req, res) => {
  const session = getSession(req);
  if (!session) return res.status(401).json({ error: 'Not authenticated' });
  if (!isPremium(session)) return res.status(403).json({ error: 'Premium required' });
  try {
    let prefs = db.prepare('SELECT * FROM alert_prefs WHERE user_id = ?').get(session.user_id);
    if (!prefs) {
      // Return defaults if never saved
      prefs = { enabled: 1, min_score: 70, min_value: 0, types: 'conviction,cluster,first_buy,exit_warning', tickers: '', sectors: '', roles: '', frequency: 'immediate' };
    }
    res.json(prefs);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/alerts/prefs — save alert preferences
app.post('/api/alerts/prefs', express.json(), (req, res) => {
  const session = getSession(req);
  if (!session) return res.status(401).json({ error: 'Not authenticated' });
  if (!isPremium(session)) return res.status(403).json({ error: 'Premium required' });
  const { enabled, min_score, min_value, types, tickers, sectors, roles, frequency } = req.body;
  try {
    db.prepare(`
      INSERT INTO alert_prefs (user_id, enabled, min_score, min_value, types, tickers, sectors, roles, frequency, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(user_id) DO UPDATE SET
        enabled    = excluded.enabled,
        min_score  = excluded.min_score,
        min_value  = excluded.min_value,
        types      = excluded.types,
        tickers    = excluded.tickers,
        sectors    = excluded.sectors,
        roles      = excluded.roles,
        frequency  = excluded.frequency,
        updated_at = excluded.updated_at
    `).run(
      session.user_id,
      enabled ? 1 : 0,
      Math.min(Math.max(parseInt(min_score) || 70, 0), 100),
      Math.max(parseInt(min_value) || 0, 0),
      (types || 'conviction,cluster,first_buy,exit_warning').slice(0, 200),
      (tickers || '').toUpperCase().replace(/[^A-Z,\s]/g, '').slice(0, 500),
      (sectors || '').slice(0, 500),
      (roles || '').slice(0, 200),
      ['immediate', 'daily'].includes(frequency) ? frequency : 'immediate'
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/alerts/test — send a test alert email to the current user
app.post('/api/alerts/test', async (req, res) => {
  const session = getSession(req);
  if (!session) return res.status(401).json({ error: 'Not authenticated' });
  if (!isPremium(session)) return res.status(403).json({ error: 'Premium required' });
  try {
    if (!RESEND_KEY) return res.status(500).json({ error: 'Email not configured — RESEND_KEY missing on server' });
    const user = db.prepare('SELECT email FROM users WHERE id = ?').get(session.user_id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    await sendAlertEmail(user.email, [{
      ticker: 'AAPL',
      company: 'Apple Inc.',
      signal_type: 'CONVICTION',
      headline: 'High conviction buy · Score 87',
      detail: 'CEO added to position (15% increase) · $2.4M disclosed',
      score: 87,
      date: new Date().toISOString().slice(0, 10),
    }, {
      ticker: 'NVDA',
      company: 'NVIDIA Corporation',
      signal_type: 'FIRST_BUY',
      headline: 'First buy in years · Director',
      detail: 'John Smith · 4+ year gap since last buy · $180K',
      score: 72,
      date: new Date().toISOString().slice(0, 10),
    }]);
    res.json({ ok: true, message: 'Test alert sent to ' + user.email });
  } catch(e) {
    slog('Test alert error: ' + e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/stripe/checkout', async (req, res) => {
  if (!STRIPE_SECRET) return res.redirect('/premium');
  const sessionToken = req.query.session || (req.session && req.session.token);
  if (!sessionToken) return res.redirect('/signup');

  const userSession = req.session || (() => {
    try {
      return db.prepare(`
        SELECT s.*, u.email, u.is_admin FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = ? AND s.expires_at > datetime('now')
      `).get(sessionToken);
    } catch(_) { return null; }
  })();

  if (!userSession) return res.redirect('/signup');

  const plan = req.query.plan === 'annual' ? 'annual' : 'monthly';
  const priceId = plan === 'annual' ? STRIPE_PRICE_ANNUAL : STRIPE_PRICE_MONTHLY;
  if (!priceId) return res.redirect('/premium');

  try {
    const Stripe = require('stripe');
    const stripe = Stripe(STRIPE_SECRET);
    const checkout = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'subscription',
      line_items: [{ price: priceId, quantity: 1 }],
      customer_email: userSession.email,
      metadata: { user_id: String(userSession.user_id || userSession.id), plan },
      success_url: `${SITE_URL}/api/stripe/success?cs_id={CHECKOUT_SESSION_ID}`,
      cancel_url:  `${SITE_URL}/premium?cancelled=1`,
    });
    res.redirect(checkout.url);
  } catch(e) {
    console.error('Stripe checkout error:', e.message);
    res.redirect('/premium?error=stripe');
  }
});

// ── GET /api/stripe/success ──────────────────────────────────
// Called after Stripe redirects back — verify payment and activate subscription
app.get('/api/stripe/success', async (req, res) => {
  const csId = req.query.cs_id;
  if (!csId || !STRIPE_SECRET) return res.redirect('/?premium=1');
  try {
    const Stripe = require('stripe');
    const stripe = Stripe(STRIPE_SECRET);
    const session = await stripe.checkout.sessions.retrieve(csId, {
      expand: ['subscription'],
    });
    if (session.payment_status === 'paid' || session.status === 'complete') {
      const userId = parseInt(session.metadata?.user_id);
      const plan   = session.metadata?.plan || 'monthly';
      const sub    = session.subscription;
      if (userId) {
        const periodEnd = sub?.current_period_end
          ? new Date(sub.current_period_end * 1000).toISOString() : null;
        db.prepare(`
          INSERT INTO subscriptions (user_id, stripe_customer_id, stripe_subscription_id, stripe_checkout_session_id, plan, status, current_period_end, updated_at)
          VALUES (?, ?, ?, ?, ?, 'active', ?, datetime('now'))
          ON CONFLICT(user_id) DO UPDATE SET
            stripe_customer_id = excluded.stripe_customer_id,
            stripe_subscription_id = excluded.stripe_subscription_id,
            stripe_checkout_session_id = excluded.stripe_checkout_session_id,
            plan = excluded.plan, status = 'active',
            current_period_end = excluded.current_period_end,
            updated_at = datetime('now')
        `).run(userId, session.customer, sub?.id || '', csId, plan, periodEnd);
        slog(`Subscription activated for user ${userId} (${plan})`);
      }
    }
  } catch(e) { console.error('Stripe success error:', e.message); }
  res.redirect('/?premium=1');
});

// ── GET /api/stripe/portal ───────────────────────────────────
// Redirects logged-in user to Stripe billing portal to manage/cancel
app.get('/api/stripe/portal', async (req, res) => {
  const portalSession = getSession(req);
  if (!portalSession || !STRIPE_SECRET) return res.redirect('/account');
  try {
    const sub = getSubscription(portalSession.user_id);
    if (!sub?.stripe_customer_id) return res.redirect('/account');
    const Stripe = require('stripe');
    const stripe = Stripe(STRIPE_SECRET);
    const portal = await stripe.billingPortal.sessions.create({
      customer: sub.stripe_customer_id,
      return_url: `${SITE_URL}/account`,
    });
    res.redirect(portal.url);
  } catch(e) {
    console.error('Portal error:', e.message);
    res.redirect('/account');
  }
});


// ── DEDUP DIAGNOSTIC — shows cascade filing extent ───────────
app.get('/api/dedup-check', (req, res) => {
  try {
    // Find accession numbers with multiple distinct insiders (cascade filings)
    const cascades = db.prepare(`
      SELECT accession, ticker, trade_date, TRIM(type) AS type,
             CAST(value AS INTEGER) AS value,
             COUNT(DISTINCT insider) AS insider_count,
             GROUP_CONCAT(insider, ' | ') AS insiders
      FROM trades
      WHERE accession IS NOT NULL AND value > 0
      GROUP BY accession, ticker, trade_date, TRIM(type), CAST(value AS INTEGER)
      HAVING insider_count > 1
      ORDER BY value DESC
      LIMIT 30
    `).all();

    // Total trade count vs deduped count
    const totals = db.prepare(`
      SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT accession || '|' || ticker || '|' || trade_date || '|' || TRIM(type) || '|' || CAST(value AS INTEGER)) AS unique_economic_trades
      FROM trades
      WHERE accession IS NOT NULL
    `).get();

    // Top insider buy totals with and without dedup (last 30 days)
    const withDupe = db.prepare(`
      SELECT insider, SUM(COALESCE(value,0)) AS total
      FROM trades
      WHERE TRIM(type)='P'
        AND trade_date >= date('now','-30 days')
        AND trade_date <= date('now')
        AND insider IS NOT NULL
      GROUP BY insider ORDER BY total DESC LIMIT 10
    `).all();

    const withoutDupe = db.prepare(`
      SELECT insider, SUM(COALESCE(value,0)) AS total
      FROM trades
      WHERE TRIM(type)='P'
        AND trade_date >= date('now','-30 days')
        AND trade_date <= date('now')
        AND insider IS NOT NULL
      GROUP BY insider ORDER BY total DESC LIMIT 10
    `).all();

    res.json({ totals, cascades, withDupe, withoutDupe });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ── EARNINGS DEBUG — /api/earnings-debug?symbol=AAPL ────────────
app.get('/api/earnings-debug', async (req, res) => {
  const sym = (req.query.symbol || 'AAPL').toUpperCase();
  const results = { ticker: sym, cache: _earningsCache[sym] || null, crumb: _yahooCrumb, hasCookie: !!_yahooCookie };
  try {
    // Force fresh crumb
    _yahooCrumb = null; _yahooCrumbTs = 0;
    const crumb = await getYahooCrumb();
    results.freshCrumb = crumb;
    const crumbQ = crumb ? `&crumb=${encodeURIComponent(crumb)}` : '';
    const opts = _yahooCookie ? { headers: { 'Cookie': _yahooCookie, 'User-Agent': 'Mozilla/5.0' } } : {};
    const url = `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(sym)}?modules=calendarEvents${crumbQ}`;
    const { status, body } = await get(url, 8000, opts);
    results.httpStatus = status;
    const bodyStr = body.toString();
    results.bodyPreview = bodyStr.slice(0, 800);
    if (status === 200) {
      try {
        const data = JSON.parse(bodyStr);
        results.parsed = {
          error: data?.quoteSummary?.error,
          hasResult: !!(data?.quoteSummary?.result?.[0]),
          calendarEvents: data?.quoteSummary?.result?.[0]?.calendarEvents || null,
        };
      } catch(e) { results.parseError = e.message; }
    }
  } catch(e) { results.fetchError = e.message; }
  res.json(results);
});

// ── PRICE DEBUG — /api/price-debug?symbol=KRRO ───────────────
app.get('/api/price-debug', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'symbol required' });
  const results = {};
  const end   = new Date().toISOString().slice(0, 10);
  const start = new Date(Date.now() - 6 * 365 * 86400000).toISOString().slice(0, 10);

  // Tiingo
  try {
    if (TIINGO) {
      const url = 'https://api.tiingo.com/tiingo/daily/' + sym + '/prices?startDate=' + start + '&endDate=' + end + '&format=json&resampleFreq=daily&token=' + TIINGO;
      const { status, body } = await get(url, 10000);
      const data = status === 200 ? JSON.parse(body.toString()) : null;
      results.tiingo = { status, bars: Array.isArray(data) ? data.length : 0, sample: Array.isArray(data) ? data.slice(-2) : data };
    }
  } catch(e) { results.tiingo = { error: e.message }; }

  // Polygon
  try {
    if (POLYGON) {
      const url = 'https://api.polygon.io/v2/aggs/ticker/' + sym + '/range/1/day/' + start + '/' + end + '?adjusted=true&sort=asc&limit=5200&apiKey=' + POLYGON;
      const { status, body } = await get(url, 10000);
      const data = status === 200 ? JSON.parse(body.toString()) : null;
      results.polygon = { status, bars: data?.results?.length || 0, resultsCount: data?.resultsCount, ticker: data?.ticker };
    }
  } catch(e) { results.polygon = { error: e.message }; }

  // Yahoo
  try {
    const endTs = Math.floor(Date.now() / 1000);
    const startTs = endTs - 6 * 365 * 86400;
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + sym + '?interval=1d&period1=' + startTs + '&period2=' + endTs;
    const { status, body } = await get(url, 8000);
    const data = status === 200 ? JSON.parse(body.toString()) : null;
    const ts = data?.chart?.result?.[0]?.timestamp || [];
    results.yahoo = { status, bars: ts.length };
  } catch(e) { results.yahoo = { error: e.message }; }

  res.json({ sym, start, end, results });
});


// ── PRICE HIGHS — batch 52-week high/low/current for exit warning scoring ──
app.get('/api/price-highs', (req, res) => {
  const syms = (req.query.tickers || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean).slice(0, 50);
  if (!syms.length) return res.json({});
  const result = {};
  syms.forEach(sym => {
    const bars = getPC(sym);
    if (!bars || !bars.length) return;
    const last = bars[bars.length - 1];
    const bars52 = bars.slice(-252);
    const high52 = Math.max(...bars52.map(b => b.high));
    const low52  = Math.min(...bars52.map(b => b.low));
    const current = last.close;
    // How far is current price from 52w high (0=at high, 1=at low)
    const range52 = high52 - low52 || 1;
    const pctFromHigh = (high52 - current) / range52; // 0 = at high, 1 = at low
    result[sym] = { high52: +high52.toFixed(2), low52: +low52.toFixed(2), current: +current.toFixed(2), pctFromHigh: +pctFromHigh.toFixed(3) };
  });
  res.json(result);
});

app.get('*', (req, res) => {
  if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Not found' });
  // Serve article pages directly — don't fall through to SPA
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
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Security headers on every response
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  if (req.path.startsWith('/api/')) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
  }
  next();
});

app.listen(PORT, () => console.log(`Server on port ${PORT}`));
