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
    url          TEXT,
    subject_cik  TEXT
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

// Migrate existing DB — add subject_cik column if it doesn't exist yet
// Must run BEFORE preparing the INSERT statement that references it
try { db.exec(`ALTER TABLE sc13_transactions ADD COLUMN subject_cik TEXT`); } catch(_) {}

// One-time migration: clear old single-CIK subject_cik values (stored as just the filer CIK).
// The new format stores all CIKs comma-separated so enrichment can find the subject.
// Old values that don't contain a comma are likely just the filer CIK (no ticker).
// Reset them so the EFTS backfill re-populates with the correct multi-CIK format.
try {
  const cleared = db.prepare(`
    UPDATE sc13_transactions SET subject_cik = NULL
    WHERE subject_cik IS NOT NULL
      AND subject_cik NOT LIKE '%,%'
      AND LENGTH(subject_cik) > 0
  `).run();
  if (cleared.changes > 0) log(`Cleared ${cleared.changes} stale single-CIK subject_cik values`);
} catch(e) {}

const insertSc13 = db.prepare(`
  INSERT OR IGNORE INTO sc13_transactions
    (ticker, company, filer, filing_type, filed_date, period_date,
     pct_owned, shares_owned, shares_delta, accession, url, subject_cik)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
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

  // Detect non-index files: EDGAR sometimes returns a readme/metadata file
  // instead of the actual form.idx for recent or in-progress quarters.
  // Real form.idx starts with a header like "Form Type  Company Name..."
  // Fake files start with "Description:", "Last Data Re", or similar.
  const firstLines = lines.slice(0, 5).join(' ');
  if (/Description:|Last Data Re|Comments:|This file/i.test(firstLines) &&
      !/Form Type/i.test(firstLines)) {
    log(`${key}: EDGAR returned non-index file (readme/metadata), skipping`);
    log(`${key}: first line: "${lines[0]?.slice(0,80)}"`);
    db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, -1);
    return 0;
  }

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
      log(`  ${key} sample [${line.length}]: "${line.slice(0, 200)}"`);
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

    // Filename: edgar/data/{CIK}/{accession}.txt
    // Try with .txt extension first (most common), then without (some newer files)
    const fnM = line.match(/edgar\/data\/(\d+)\/(\d[\d-]{14,19})\.txt/i)
             || line.match(/edgar\/data\/(\d+)\/(\d{10}-\d{2}-\d{6})/);
    if (!fnM) { log(`  ${key} no filename [len=${line.length}]: "${line.slice(0,180)}"`); continue; }
    const cik   = fnM[1];
    const parts = fnM[2].replace(/\.txt$/i,'').split('-');
    if (parts.length !== 3) continue;
    const accDash = `${parts[0].padStart(10,'0')}-${parts[1]}-${parts[2]}`;

    // Filer name: between col 12 and the CIK column
    // Try padded CIK first, then raw CIK (format varies across quarters)
    const cikPadded = cik.padStart(10, '0');
    let cikPos      = line.indexOf(cikPadded);
    if (cikPos < 0)  cikPos = line.indexOf(' ' + cik + ' ');
    if (cikPos < 0)  cikPos = line.indexOf(' ' + cik + '\t');
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
      null, // subject_cik
    ]);
  }

  if (!batch.length) {
    // Diagnostic: log sample of form types found to see what's actually in the file
    const formTypeSample = {};
    let sampleCount = 0;
    for (const l of lines) {
      if (!pastHeader || l.length < 20) continue;
      const ft = l.slice(0, 12).trim();
      if (ft && !formTypeSample[ft]) {
        formTypeSample[ft] = 0;
        sampleCount++;
        if (sampleCount >= 20) break;
      }
      if (ft) formTypeSample[ft]++;
    }
    log(`${key}: 0 inserted (scanned ${scanned} matching, ${lines.length} total lines, pastHeader=${pastHeader})`);
    log(`${key}: form types in file: ${Object.keys(formTypeSample).slice(0,15).join(', ')}`);
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

  // EDGAR only reliably serves form.idx for completed quarters up to ~6 months ago.
  // For recent quarters (2024Q4 and later), form.idx may return a readme/metadata
  // file instead of the actual filing index. Recent data is handled by EFTS instead.
  const FORM_IDX_CUTOFF_YEAR = 2024;
  const FORM_IDX_CUTOFF_Q    = 3; // stop at 2024Q3 inclusive

  log(`Historical backfill: 2004 Q1 through ${FORM_IDX_CUTOFF_YEAR} Q${FORM_IDX_CUTOFF_Q} (EFTS handles recent quarters)`);

  for (let yr = 2004; yr <= FORM_IDX_CUTOFF_YEAR; yr++) {
    const maxQ = (yr === FORM_IDX_CUTOFF_YEAR) ? FORM_IDX_CUTOFF_Q : 4;
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
  // Phase 1: fast-path for rows that already have subject_cik — just hit data.sec.gov
  const fastRows = db.prepare(`
    SELECT id, subject_cik
    FROM sc13_transactions
    WHERE (ticker IS NULL OR ticker = '')
      AND subject_cik IS NOT NULL AND subject_cik != ''
    ORDER BY filed_date DESC
    LIMIT 2000
  `).all();

  if (fastRows.length > 0) {
    log(`Ticker enrichment fast-path: ${fastRows.length} rows with subject_cik to resolve`);
    // Log first 3 subject_cik values to verify format
    fastRows.slice(0, 3).forEach((r, i) => log(`  sample subject_cik[${i}]: ${JSON.stringify(r.subject_cik)}`));
    const updateFast = db.prepare(`UPDATE sc13_transactions SET ticker=?, company=? WHERE id=?`);
    let fastEnriched = 0;
    for (let i = 0; i < fastRows.length; i++) {
      try {
        const result = await lookupTickerByCik(fastRows[i].subject_cik);
        if (result) { updateFast.run(result.ticker, result.company, fastRows[i].id); fastEnriched++; }
      } catch(e) {}
      if ((i + 1) % 100 === 0) log(`Fast enrichment: ${i+1}/${fastRows.length}, ${fastEnriched} resolved`);
    }
    log(`Ticker enrichment fast-path: ${fastEnriched}/${fastRows.length} resolved`);
  }

  // Phase 2: slow-path for rows without subject_cik — fetch filing index page
  const rows = db.prepare(`
    SELECT id, accession, url, subject_cik
    FROM sc13_transactions
    WHERE (ticker IS NULL OR ticker = '')
      AND (subject_cik IS NULL OR subject_cik = '')
    ORDER BY filed_date DESC
    LIMIT 200
  `).all();

  if (!rows.length) { log('Ticker enrichment: nothing to enrich'); return; }
  log(`Ticker enrichment: ${rows.length} recent rows to resolve`);

  const updateStmt = db.prepare(`
    UPDATE sc13_transactions SET ticker=?, company=? WHERE id=?
  `);

  // Cache CIK→ticker lookups to avoid re-fetching for the same issuer
  const cikTickerCache = {};

  let _lookupCount = 0;
  async function lookupTickerByCik(cikOrList) {
    if (!cikOrList) return null;
    const cikList = cikOrList.toString().split(',').map(c => c.trim()).filter(Boolean);
    const isFirst = _lookupCount++ === 0;
    if (isFirst) log(`lookupTickerByCik first call: cikList=${JSON.stringify(cikList)}`);
    for (const cik of cikList) {
      if (cikTickerCache[cik] !== undefined) {
        if (isFirst) log(`  CIK ${cik}: cache hit = ${JSON.stringify(cikTickerCache[cik])}`);
        if (cikTickerCache[cik]) return cikTickerCache[cik];
        continue;
      }
      try {
        const padded = cik.replace(/^0+/, '').padStart(10, '0');
        const url = `https://data.sec.gov/submissions/CIK${padded}.json`;
        if (isFirst) log(`  fetching: ${url}`);
        const { status, body } = await get(url, 10000);
        if (isFirst) log(`  HTTP status: ${status}`);
        if (status !== 200) { cikTickerCache[cik] = null; continue; }
        const data   = JSON.parse(body.toString('utf8'));
        const ticker  = (data.tickers?.[0] || '').toUpperCase().trim();
        const company = (data.name || '').trim();
        if (isFirst) log(`  parsed: ticker=${ticker||'none'}, name=${company}`);
        const result = ticker && ticker.match(/^[A-Z]{1,6}$/) ? { ticker, company } : null;
        cikTickerCache[cik] = result;
        if (result) return result;
      } catch(e) {
        cikTickerCache[cik] = null;
        if (isFirst) log(`  exception: ${e.message}`);
      }
    }
    return null;
  }

  async function getSubjectCik(accession, indexUrl) {
    try {
      const { status, body } = await get(indexUrl, 15000);
      if (status !== 200) return null;
      const html = body.toString('utf8');

      // EDGAR index page format (modern):
      // <span class="companyName">COMPANY NAME (0001234567) (Subject)</span>
      const subjectM = html.match(/companyName[^>]*>([^<]+)\((\d{7,10})\)\s*\(Subject\)/i);
      if (subjectM) return subjectM[2].replace(/^0+/, '');

      // Modern EDGAR index also has this pattern in the header table:
      // <td class="companyInfo"><a href="...">COMPANY NAME</a><br>(0001234567) (Subject)</td>
      const subjectM2 = html.match(/\((\d{7,10})\)\s*\(Subject\)/i);
      if (subjectM2) return subjectM2[1].replace(/^0+/, '');

      // Older EDGAR format
      const subjectAlt = html.match(/subject\s+company[^:]*:.*?CIK[^:]*:\s*(\d{7,10})/is);
      if (subjectAlt) return subjectAlt[1].replace(/^0+/, '');

      // Last resort: look for any 10-digit CIK that appears near "subject" anywhere
      const subjectLoose = html.match(/subject[^\n<]{0,100}(\d{10})/i);
      if (subjectLoose) return subjectLoose[1].replace(/^0+/, '');

      return null;
    } catch(e) { return null; }
  }

  let enriched = 0;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    try {
      let subjectCik = row.subject_cik;
      if (!subjectCik) {
        subjectCik = await getSubjectCik(row.accession, row.url);
      }
      if (!subjectCik) continue;

      const result = await lookupTickerByCik(subjectCik);
      if (result) {
        updateStmt.run(result.ticker, result.company, row.id);
        enriched++;
      }
    } catch(e) {}
    // Log progress every 200 rows
    if ((i + 1) % 50 === 0) log(`Ticker enrichment: ${i+1}/${rows.length} processed, ${enriched} resolved so far`);
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
      // Retry once on 500 — EDGAR EFTS occasionally returns transient 500s
      let resp = await get(url, 30000);
      if (resp.status === 500) {
        log(`EFTS SC13 HTTP 500 at from=${from}, retrying in 10s...`);
        await new Promise(r => setTimeout(r, 10000));
        resp = await get(url, 30000);
      }
      const { status, body } = resp;
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

        // For SC 13D/G: EFTS ciks[] contains both filer CIKs and subject company CIK.
        // We can't reliably identify the subject from position alone since filing
        // agents, investors, and subjects all appear.
        // Store ALL ciks so enrichment can try each one to find the one with a ticker.
        // The subject company will have a stock ticker; the investor/filer won't.
        const subjectCik = ciks.length > 0 ? ciks.join(',') : null;

        const displayNames = h._source?.display_names || [];
        let tickerHint = '', companyHint = '', filerHint = h._source?.entity_name || '';
        // Try to get company name from display_names (first non-filer entry)
        for (const dn of displayNames) {
          if (!filerHint || !dn.toUpperCase().includes(filerHint.slice(0,8).toUpperCase())) {
            companyHint = dn.trim();
            break;
          }
        }

        if (!seen.has(accDash)) {
          seen.add(accDash);
          allFilings.push({
            accession: accDash, cik, filedDate: fd, formType,
            tickerHint, companyHint, filerHint, subjectCik,
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
    null, null, null, f.accession, f.secUrl, f.subjectCik || null,
  ]);

  const inserted = insertMany(batch);
  log(`Recent SC 13D/G: ${inserted} new records inserted`);

  // Backfill subject_cik on existing rows that didn't get it on insert
  // (rows inserted before this column existed, or via OR IGNORE on duplicates)
  const updateCik = db.prepare(`
    UPDATE sc13_transactions SET subject_cik=?, filer=?
    WHERE accession=? AND (subject_cik IS NULL OR subject_cik='')
  `);
  const updateMany = db.transaction(items => {
    let n = 0;
    for (const f of items) {
      if (!f.subjectCik && !f.filerHint) continue;
      n += updateCik.run(f.subjectCik || null, f.filerHint || null, f.accession).changes;
    }
    return n;
  });
  const updated = updateMany(allFilings);
  if (updated > 0) log(`Recent SC 13D/G: ${updated} rows backfilled with subject_cik/filer`);

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
      WHERE rows >= 0 AND rows < 100 AND quarter >= '2023Q1' AND quarter <= '2024Q3'
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
  // On first run, extend EFTS lookback to 18 months to cover the gap
  // between the form.idx cutoff (2024Q3) and the present.
  // On subsequent polls the 2-day window is sufficient.
  const eftsLookback = Math.max(daysBackInit, 540);
  await runRecentBackfill(eftsLookback);

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
  setInterval(() => {
    runRecentBackfill(2)
      .then(() => enrichRecentTickers())
      .catch(e => log(`Poll error: ${e.message}`));
  }, 4 * 60 * 60 * 1000);
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
