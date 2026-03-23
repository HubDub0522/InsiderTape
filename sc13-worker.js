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
  let debugPrinted = 0;

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

    // Debug: print first 3 data lines to confirm format in logs
    if (debugPrinted < 3) {
      log(`  ${key} sample: "${line.slice(0, 120)}"`);
      debugPrinted++;
    }

    // Date filed — EDGAR uses ISO (YYYY-MM-DD) in newer files,
    // MM/DD/YYYY in older quarterly files. Handle both.
    let filedDate = '';
    const isoMatch = line.match(/(\d{4}-\d{2}-\d{2})/);
    const mdyMatch = line.match(/(\d{2})\/(\d{2})\/(\d{4})/);
    if (isoMatch) {
      filedDate = isoMatch[1];
    } else if (mdyMatch) {
      filedDate = `${mdyMatch[3]}-${mdyMatch[1]}-${mdyMatch[2]}`;
    } else { log(`  ${key} no date: "${line.slice(0,80)}"`); continue; }
    if (filedDate < '2000-01-01' || filedDate > '2040-01-01') continue;

    // Filename column: edgar/data/{CIK}/{accession}.txt
    const fnM = line.match(/edgar\/data\/(\d+)\/([\d-]+)\.txt/i);
    if (!fnM) { log(`  ${key} no filename: "${line.slice(0,80)}"`); continue; }
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
    log(`${key}: 0 inserted (scanned ${scanned} matching, ${lines.length} total lines, pastHeader=${pastHeader})`);
    db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, 0);
    return 0;
  }

  const inserted = insertMany(batch);
  db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, inserted);
  log(`${key}: ${inserted} inserted (${scanned} SC 13D/G matched, ${lines.length} total lines)`);
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
// Uses the EDGAR company submissions API (data.sec.gov/submissions/CIK.json)
// to resolve the subject company ticker from the issuer CIK.
//
// The issuer CIK is extracted from the filing's primary document — specifically
// the sc13-index.htm page which contains the subject company CIK in the
// format: <span class="companyName">COMPANY NAME (0001234567) (Subject)</span>
// We then hit data.sec.gov/submissions/CIK{10digits}.json to get the ticker.
//
// For SC 13D/G the filer CIK (first 10 digits of accession) is the INVESTOR.
// The SUBJECT company CIK appears on the index page as "(Subject)".
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

  // Cache CIK→ticker lookups to avoid re-fetching for the same issuer
  const cikTickerCache = {};

  async function lookupTickerByCik(cik) {
    if (cikTickerCache[cik] !== undefined) return cikTickerCache[cik];
    try {
      const padded = cik.padStart(10, '0');
      const { status, body } = await get(`https://data.sec.gov/submissions/CIK${padded}.json`, 10000);
      if (status !== 200) { cikTickerCache[cik] = null; return null; }
      const data = JSON.parse(body.toString('utf8'));
      const ticker  = (data.tickers?.[0] || '').toUpperCase().trim();
      const company = (data.name || '').trim();
      const result  = ticker && ticker.match(/^[A-Z]{1,6}$/) ? { ticker, company } : null;
      cikTickerCache[cik] = result;
      return result;
    } catch(e) { cikTickerCache[cik] = null; return null; }
  }

  async function getSubjectCik(accession, indexUrl) {
    try {
      const { status, body } = await get(indexUrl, 15000);
      if (status !== 200) return null;
      const html = body.toString('utf8');
      // EDGAR index page format:
      // <span class="companyName">COMPANY NAME (0001234567) (Subject)</span>
      const subjectM = html.match(/companyName[^>]*>([^<]+)\((\d{7,10})\)\s*\(Subject\)/i);
      if (subjectM) return subjectM[2].replace(/^0+/, '');

      // Older EDGAR format: plain text "Subject Company:" row
      const subjectAlt = html.match(/subject\s+company[^:]*:.*?CIK[^:]*:\s*(\d{7,10})/is);
      if (subjectAlt) return subjectAlt[1].replace(/^0+/, '');

      return null;
    } catch(e) { return null; }
  }

  let enriched = 0;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    try {
      const subjectCik = await getSubjectCik(row.accession, row.url);
      if (!subjectCik) continue;

      const result = await lookupTickerByCik(subjectCik);
      if (result) {
        updateStmt.run(result.ticker, result.company, row.id);
        enriched++;
      }
    } catch(e) {}
    // Log progress every 200 rows
    if ((i + 1) % 200 === 0) log(`Ticker enrichment: ${i+1}/${rows.length} processed, ${enriched} resolved so far`);
  }

  log(`Ticker enrichment: ${enriched}/${rows.length} rows resolved`);
}

// ── RECENT: EFTS search (last N days) ────────────────────────────────────
// Uses EDGAR full-text search — same API the daily-worker uses for Form 4s.
// Key fix: EFTS requires %20 for spaces in form types (not +).
// "SC 13D" must be encoded as "SC%2013D" not "SC+13D".
async function runRecentBackfill(daysBack) {
  const since = new Date();
  since.setDate(since.getDate() - daysBack);
  const sinceStr = since.toISOString().slice(0, 10);
  const today    = new Date().toISOString().slice(0, 10);

  log(`Recent SC 13D/G EFTS: ${sinceStr} to ${today}`);

  // %20 = space, %2F = slash — EFTS stores form types exactly as filed
  const formTypes = 'SC%2013D,SC%2013G,SC%2013D%2FA,SC%2013G%2FA';
  const allFilings = [];
  const seen       = new Set();

  for (let from = 0; from < 10000; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=${formTypes}&dateRange=custom&startdt=${sinceStr}&enddt=${today}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS SC13 HTTP ${status} at from=${from}`); break; }
      const data = JSON.parse(body.toString('utf8'));
      const hits = data.hits?.hits || [];
      if (!hits.length) break;

      for (const h of hits) {
        const raw     = h._id || '';
        const colonAt = raw.indexOf(':');
        const accDash = colonAt >= 0 ? raw.slice(0, colonAt) : raw.replace(/\//g, '-');
        if (!accDash.match(/^\d{10}-\d{2}-\d{6}$/)) continue;

        const fd       = h._source?.file_date?.slice(0,10) || today;
        const ciks     = h._source?.ciks || [];
        const formType = (h._source?.form_type || 'SC 13D').trim();
        const cik      = (ciks[0] || '').toString() || accDash.slice(0,10).replace(/^0+/,'');

        // EFTS entity_name = filer (the investor making the disclosure)
        // display_names contains subject company as "TICKER (Company Name)"
        const displayNames = h._source?.display_names || [];
        let tickerHint = '', companyHint = '', filerHint = h._source?.entity_name || '';
        for (const dn of displayNames) {
          const m = dn.match(/^([A-Z]{1,6})\s*\((.+)\)/);
          if (m) { tickerHint = m[1]; companyHint = m[2].trim(); break; }
        }

        if (!seen.has(accDash)) {
          seen.add(accDash);
          allFilings.push({
            accession: accDash, cik, filedDate: fd, formType,
            tickerHint, companyHint, filerHint,
            secUrl: edgarIndexUrl(accDash, cik),
          });
        }
      }
      log(`EFTS SC13: from=${from}, ${hits.length} hits, ${allFilings.length} total`);
      if (hits.length < 100) break;
    } catch(e) { log(`EFTS SC13 error: ${e.message}`); break; }
  }

  if (!allFilings.length) { log('Recent SC 13D/G: 0 filings found'); return 0; }
  log(`Recent SC 13D/G: ${allFilings.length} filings found`);

  const batch = allFilings.map(f => [
    f.tickerHint || '', f.companyHint || '', f.filerHint || '',
    f.formType, f.filedDate, f.filedDate,
    null, null, null, f.accession, f.secUrl,
  ]);

  const inserted = insertMany(batch);
  log(`Recent SC 13D/G: ${inserted} new records inserted`);
  return inserted;
}


// ── MAIN ──────────────────────────────────────────────────────────────────
// Args from server.js: node sc13-worker.js {daysBack} {mode}
// e.g.  node sc13-worker.js 90 poll
const _daysBackArg = parseInt(process.argv[2] || '90');
const mode         = process.argv[3] || process.argv[2] || 'poll';
// If only one arg and it's not a number, treat it as mode
const daysBackInit = isNaN(_daysBackArg) ? 90 : _daysBackArg;

async function main() {
  log(`=== sc13-worker v2 start (mode=${mode}, daysBack=${daysBackInit}) ===`);

  // Re-sync any quarters that completed with suspiciously few rows —
  // this catches cases where a previous form.idx fetch timed out mid-stream.
  // A real quarter should have at least 500 rows; anything under 100 is suspect.
  try {
    const thinQuarters = db.prepare(`
      SELECT quarter FROM sc13_quarter_log
      WHERE rows < 100 AND quarter >= '2024Q1'
    `).all();
    if (thinQuarters.length) {
      log(`Re-syncing ${thinQuarters.length} thin quarters: ${thinQuarters.map(r=>r.quarter).join(', ')}`);
      for (const { quarter } of thinQuarters) {
        db.prepare('DELETE FROM sc13_quarter_log WHERE quarter=?').run(quarter);
      }
    }
  } catch(e) {}

  // Small pause on startup — daily-worker is already hitting SEC at boot,
  // give it a few seconds headroom to avoid rate limiting the Atom feed
  await new Promise(r => setTimeout(r, 4000));

  // Always run recent Atom feed first — fast, covers last N days
  await runRecentBackfill(daysBackInit);

  if (mode === 'historical') {
    await runHistoricalBackfill();
    db.close();
    process.exit(0);
  }

  // Poll mode: historical backfill runs in background (non-blocking)
  // sc13_quarter_log tracks completed quarters so restarts skip already-done ones
  log('Starting historical backfill in background (completed quarters skipped)...');
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
