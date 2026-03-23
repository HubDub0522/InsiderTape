'use strict';

// sc13-worker.js — v2
// Ingests Schedule 13D and 13G filings from EDGAR.
//
// Two-phase approach:
//
//   HISTORICAL (form.idx):
//     Walks every quarterly form.idx file from 2004 to present.
//     Stores metadata-only rows (filer, date, accession, SEC link)
//     WITHOUT fetching each document individually — too many filings.
//     This gives chart markers for all historical filings instantly.
//     Completed quarters are tracked in sc13_quarter_log so restarts
//     skip already-processed quarters.
//
//   RECENT (Atom feed, last 90 days):
//     Uses browse-edgar Atom feed — same proven approach as daily-worker.js.
//     Runs on startup and every 4 hours to catch new filings.
//     Ticker/company hints are extracted from the Atom entry titles.
//
//   TICKER ENRICHMENT:
//     For recent rows (last 2 years) where ticker is blank, a background
//     enrichment pass fetches each filing's index page to resolve the
//     issuer ticker and company name.
//
//   SEC LINK:
//     Every record stores the direct EDGAR index URL so users can click
//     through to the full filing. Same pattern as Form 4 links in the UI:
//     https://www.sec.gov/Archives/edgar/data/{CIK}/{acc}/{acc}-index.htm

const https    = require('https');
const fs       = require('fs');
const path     = require('path');
const Database = require('better-sqlite3');

const DATA_DIR = fs.existsSync('/var/data') ? '/var/data' : path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH  = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');
log(`DB path: ${DB_PATH}`);

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// ── Schema ────────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS sc13_transactions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT NOT NULL DEFAULT '',
    company      TEXT,
    filer        TEXT,
    filing_type  TEXT,
    filed_date   TEXT NOT NULL,
    period_date  TEXT,
    pct_owned    REAL,
    shares_owned INTEGER,
    shares_delta INTEGER,
    accession    TEXT UNIQUE,
    url          TEXT
  )
`);
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_sc13_ticker     ON sc13_transactions(ticker)`); } catch(_) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_sc13_filed_date ON sc13_transactions(filed_date DESC)`); } catch(_) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_sc13_filer      ON sc13_transactions(filer)`); } catch(_) {}

// Track completed quarters so restarts skip them
db.exec(`
  CREATE TABLE IF NOT EXISTS sc13_quarter_log (
    quarter   TEXT PRIMARY KEY,
    synced_at TEXT DEFAULT (datetime('now')),
    rows      INTEGER DEFAULT 0
  )
`);

const insertSc13 = db.prepare(`
  INSERT OR IGNORE INTO sc13_transactions
    (ticker, company, filer, filing_type, filed_date, period_date,
     pct_owned, shares_owned, shares_delta, accession, url)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
`);
const insertMany = db.transaction(rows => {
  let n = 0;
  for (const r of rows) n += insertSc13.run(...r).changes;
  return n;
});

// ── Logging ───────────────────────────────────────────────────────────────
function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

// ── Rate-limited HTTPS GET (max 8 req/sec, respects SEC limits) ───────────
const _reqTimes = [];
async function get(url, ms = 30000, _hops = 0) {
  if (_hops > 5) throw new Error('Too many redirects');
  const now = Date.now();
  while (_reqTimes.length && _reqTimes[0] < now - 1000) _reqTimes.shift();
  if (_reqTimes.length >= 8) {
    await new Promise(r => setTimeout(r, 1000 - (now - _reqTimes[0]) + 10));
  }
  _reqTimes.push(Date.now());

  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return get(next, ms, _hops + 1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ── Build EDGAR index URL from accession number ───────────────────────────
// Matches the Form 4 SEC link pattern used in index.html:
//   https://www.sec.gov/Archives/edgar/data/{CIK}/{acc-no-dashes}/{acc}-index.htm
function edgarIndexUrl(accession, cik) {
  const acc = accession.replace(/-/g, '');
  const resolvedCik = cik ? parseInt(cik, 10).toString() : parseInt(acc.slice(0, 10), 10).toString();
  return `https://www.sec.gov/Archives/edgar/data/${resolvedCik}/${acc}/${accession}-index.htm`;
}

// SC 13D/G form type variants as they appear in form.idx
const SC13_TYPES = new Set([
  'SC 13D', 'SC 13G', 'SC 13D/A', 'SC 13G/A',
  'SC13D',  'SC13G',  'SC13D/A',  'SC13G/A',
  'SC 13D/A', 'SC 13G/A',
]);

// ── HISTORICAL BACKFILL via EDGAR quarterly form.idx ─────────────────────
//
// form.idx is a fixed-width text file listing every SEC filing for a quarter.
// Format:
//   Form Type  Company Name              CIK         Date Filed  Filename
//   ---------- ------------------------- ----------- ----------- ----------------------------
//   SC 13D     PALE FIRE CAPITAL SE      0001922318  2026-02-19  edgar/data/1922318/0001922318-26-000511.txt
//
// For SC 13D/G the "Company Name" is the FILER (the investor making the
// disclosure), which is exactly what we want. The subject company ticker
// is not in form.idx but is resolved later by enrichRecentTickers().

async function fetchQuarterIndex(year, q) {
  const key = `${year}Q${q}`;
  const already = db.prepare('SELECT rows FROM sc13_quarter_log WHERE quarter=?').get(key);
  if (already) {
    log(`${key}: already synced (${already.rows} rows), skipping`);
    return 0;
  }

  const url = `https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${q}/form.idx`;
  log(`${key}: fetching form.idx...`);

  let bodyText;
  try {
    const { status, body } = await get(url, 90000);
    if (status !== 200) { log(`${key}: HTTP ${status} — skipping`); return 0; }
    bodyText = body.toString('utf8');
  } catch(e) { log(`${key}: fetch error — ${e.message}`); return 0; }

  const lines = bodyText.split('\n');
  let pastHeader = false;
  const batch = [];
  let scanned = 0;

  for (const line of lines) {
    if (!pastHeader) {
      if (/^-{5,}/.test(line.trim())) { pastHeader = true; }
      continue;
    }
    if (line.length < 40) continue;

    // Form type is left-aligned in the first ~12 characters
    const formType = line.slice(0, 12).trim();
    if (!SC13_TYPES.has(formType)) continue;
    scanned++;

    // Date filed — ISO YYYY-MM-DD
    const dateM = line.match(/(\d{4}-\d{2}-\d{2})/);
    if (!dateM) continue;
    const filedDate = dateM[1];
    if (filedDate < '2000-01-01') continue;

    // Filename column: edgar/data/{CIK}/{accession}.txt
    const fnM = line.match(/edgar\/data\/(\d+)\/([\d-]+)\.txt/i);
    if (!fnM) continue;
    const cik   = fnM[1];
    const parts = fnM[2].split('-');
    if (parts.length !== 3) continue;
    const accDash = `${parts[0].padStart(10,'0')}-${parts[1]}-${parts[2]}`;

    // Filer name: text between form type end (col 12) and where CIK starts
    // CIK column is right-justified in a fixed-width field — find it
    const cikPadded = cik.padStart(10, '0');
    const cikPos    = line.indexOf(cikPadded);
    const filerRaw  = cikPos > 12 ? line.slice(12, cikPos) : '';
    const filer     = filerRaw.replace(/\s{2,}/g, ' ').trim();

    batch.push([
      '',          // ticker — resolved later by enrichRecentTickers()
      '',          // company — resolved later
      filer,       // filer = investor (directly available in form.idx)
      formType,
      filedDate,
      filedDate,
      null,        // pct_owned — not in form.idx
      null,        // shares_owned
      null,        // shares_delta
      accDash,
      edgarIndexUrl(accDash, cik),
    ]);
  }

  if (!batch.length) {
    log(`${key}: 0 SC 13D/G lines found (scanned ${scanned} form.idx entries)`);
    db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, 0);
    return 0;
  }

  const inserted = insertMany(batch);
  db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, inserted);
  log(`${key}: ${inserted} inserted (${batch.length} SC 13D/G found, ${scanned} total form lines scanned)`);
  return inserted;
}

async function runHistoricalBackfill() {
  const now  = new Date();
  const endYr = now.getUTCFullYear();
  const endQ  = Math.ceil((now.getUTCMonth() + 1) / 3);
  let total   = 0;

  log(`Historical backfill: 2004 Q1 through ${endYr} Q${endQ}`);

  for (let yr = 2004; yr <= endYr; yr++) {
    const maxQ = (yr === endYr) ? endQ : 4;
    for (let q = 1; q <= maxQ; q++) {
      total += await fetchQuarterIndex(yr, q);
      await new Promise(r => setTimeout(r, 250)); // polite pause between requests
    }
  }

  log(`Historical backfill complete: ${total} total records inserted`);

  // Enrich tickers for recent (last 2 years) rows that have no ticker yet
  setTimeout(() => enrichRecentTickers().catch(e => log(`Enrichment error: ${e.message}`)), 3000);
}

// ── Ticker enrichment for recent metadata-only rows ───────────────────────
// Fetches the EDGAR filing index page for blank-ticker rows filed in the
// last 2 years to resolve issuer ticker and company name.
// The index page lists both the filer (13D/G reporter) and the subject company.
async function enrichRecentTickers() {
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - 2);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const rows = db.prepare(`
    SELECT id, accession, url
    FROM sc13_transactions
    WHERE (ticker IS NULL OR ticker = '')
      AND filed_date >= ?
    ORDER BY filed_date DESC
    LIMIT 3000
  `).all(cutoffStr);

  if (!rows.length) { log('Ticker enrichment: nothing to enrich'); return; }
  log(`Ticker enrichment: ${rows.length} recent rows to resolve`);

  const updateStmt = db.prepare(`
    UPDATE sc13_transactions SET ticker=?, company=? WHERE id=?
  `);

  let enriched = 0;
  for (const row of rows) {
    try {
      const { status, body } = await get(row.url, 15000);
      if (status !== 200) continue;
      const text = body.toString('utf8');

      // EDGAR index page contains the subject company name and ticker
      // Look for patterns like "(Subject)" rows or "Issuer" labels
      // Common patterns in the HTML index:
      //   <td>COMPANY NAME</td><td>(TICKER)</td>
      //   Issuer: COMPANY NAME (TICKER)
      //   Subject Company: COMPANY NAME (TICKER)
      const patterns = [
        /subject\s+company[^:]*:[^<]*<[^>]+>([^<(]+)\(([A-Z]{1,6})\)/i,
        /issuer[^:]*:[^<]*<[^>]+>([^<(]+)\(([A-Z]{1,6})\)/i,
        /<td[^>]*>([^<]+)<\/td>\s*<td[^>]*>\(([A-Z]{1,6})\)<\/td>/i,
        /([A-Z][^(]{3,60})\s+\(([A-Z]{1,6})\)\s*<\/a>/,
      ];
      let ticker = '', company = '';
      for (const re of patterns) {
        const m = text.match(re);
        if (m) {
          company = m[1].trim().replace(/\s+/g, ' ');
          ticker  = m[2].trim().toUpperCase();
          if (ticker.match(/^[A-Z]{1,6}$/) && !['THE','INC','LLC','LTD','CO','CORP'].includes(ticker)) break;
          ticker = ''; company = '';
        }
      }

      if (ticker) {
        updateStmt.run(ticker, company, row.id);
        enriched++;
      }
    } catch(e) {}
  }

  log(`Ticker enrichment: ${enriched}/${rows.length} rows resolved`);
}

// ── RECENT: Atom feed (last N days) ──────────────────────────────────────
// browse-edgar Atom feed is the same source used by daily-worker.js for Form 4s.
// It covers the last few weeks and is the most reliable near-real-time source.
async function searchAtom13(formType, sinceDate) {
  const filings   = [];
  const seen      = new Set();
  const typeParam = encodeURIComponent(formType);

  for (let start = 0; start < 2000; start += 40) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=${typeParam}&dateb=&owner=include&count=40&start=${start}&output=atom`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`Atom ${formType} HTTP ${status} at start=${start}`); break; }

      const text    = body.toString('utf8');
      const entries = text.split('<entry>').slice(1);
      if (!entries.length) break;

      let oldestOnPage = '';
      for (const entry of entries) {
        const dateM = entry.match(/<updated>(\d{4}-\d{2}-\d{2})/);
        const filed = dateM ? dateM[1] : new Date().toISOString().slice(0, 10);
        const linkM = entry.match(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/([\d]{18})\//);
        if (!linkM) continue;

        const cik    = linkM[1];
        const accRaw = linkM[2];
        if (accRaw.length !== 18) continue;
        const accDash = `${accRaw.slice(0,10)}-${accRaw.slice(10,12)}-${accRaw.slice(12)}`;

        // Extract filer, company, ticker from Atom title
        // Typical format: "SC 13D - FILER NAME - COMPANY NAME (TICKER)"
        const titleM    = entry.match(/<title[^>]*>([^<]+)<\/title>/i);
        const titleText = titleM
          ? titleM[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'')
          : '';
        const tickerM   = titleText.match(/\(([A-Z]{1,6})\)/);
        const tickerHint  = tickerM ? tickerM[1] : '';
        const titleParts  = titleText.split(/\s+-\s+/);
        // titleParts[0] = form type, [1] = filer, [2+] = company
        const filerHint   = titleParts.length >= 2 ? titleParts[1].trim() : '';
        const companyHint = titleParts.length >= 3
          ? titleParts[titleParts.length - 1].replace(/\([^)]*\)/g, '').trim()
          : '';

        if (!seen.has(accDash)) {
          seen.add(accDash);
          filings.push({
            accession:   accDash,
            cik,
            filedDate:   filed,
            formType,
            tickerHint,
            companyHint,
            filerHint,
            secUrl: edgarIndexUrl(accDash, cik),
          });
        }
        if (!oldestOnPage || filed < oldestOnPage) oldestOnPage = filed;
      }

      if (oldestOnPage && oldestOnPage < sinceDate) break;
      if (entries.length < 40) break;
    } catch(e) {
      log(`Atom ${formType} error at start=${start}: ${e.message}`);
      break;
    }
  }

  return filings.filter(f => f.filedDate >= sinceDate);
}

async function runRecentBackfill(daysBack) {
  const since = new Date();
  since.setDate(since.getDate() - daysBack);
  const sinceStr = since.toISOString().slice(0, 10);
  const today    = new Date().toISOString().slice(0, 10);

  log(`Recent SC 13D/G Atom: ${sinceStr} to ${today}`);

  const allFilings = [];
  const seen       = new Set();
  const formTypes  = ['SC 13D', 'SC 13G', 'SC 13D/A', 'SC 13G/A'];
  const results    = await Promise.allSettled(formTypes.map(ft => searchAtom13(ft, sinceStr)));

  for (const r of results) {
    if (r.status !== 'fulfilled') continue;
    for (const f of r.value) {
      if (!seen.has(f.accession)) { seen.add(f.accession); allFilings.push(f); }
    }
  }

  if (!allFilings.length) { log('Recent SC 13D/G: 0 filings found'); return 0; }
  log(`Recent SC 13D/G: ${allFilings.length} filings found`);

  const batch = allFilings.map(f => [
    f.tickerHint  || '',
    f.companyHint || '',
    f.filerHint   || '',
    f.formType,
    f.filedDate,
    f.filedDate,
    null, null, null,
    f.accession,
    f.secUrl,
  ]);

  const inserted = insertMany(batch);
  log(`Recent SC 13D/G: ${inserted} new records inserted`);
  return inserted;
}

// ── MAIN ──────────────────────────────────────────────────────────────────
const mode = process.argv[2] || 'poll';

async function main() {
  log(`=== sc13-worker v2 start (mode=${mode}) ===`);

  // Always run recent Atom feed first — fast, covers last 90 days
  await runRecentBackfill(90);

  if (mode === 'historical') {
    // Manual one-time full historical load
    await runHistoricalBackfill();
    db.close();
    process.exit(0);
  }

  // Poll mode: historical backfill runs in background (non-blocking)
  // sc13_quarter_log tracks completed quarters so restarts pick up where they left off
  log('Starting historical backfill in background (quarters already done will be skipped)...');
  runHistoricalBackfill().catch(e => log(`Historical backfill error: ${e.message}`));

  // Re-check for new filings every 4 hours
  log('SC 13D/G worker: polling every 4 hours');
  setInterval(() => runRecentBackfill(2).catch(e => log(`Poll error: ${e.message}`)), 4 * 60 * 60 * 1000);
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
