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
db.pragma('busy_timeout = 15000'); // wait up to 15s if DB locked by another worker
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
      AND subject_cik != 'SKIP'
      AND LENGTH(subject_cik) > 0
      AND (filer IS NOT NULL AND filer != '')
  `).run();
  if (cleared.changes > 0) log(`Cleared ${cleared.changes} stale single-CIK subject_cik values`);
} catch(e) {}

// Permanently mark rows that have had subject_cik set for 14+ days with no ticker resolved
// These are definitively unresolvable (private funds, non-public entities, defunct companies)
try {
  const skipped = db.prepare(`
    UPDATE sc13_transactions SET subject_cik = 'SKIP'
    WHERE (ticker IS NULL OR ticker = '')
      AND subject_cik IS NOT NULL
      AND subject_cik != ''
      AND subject_cik != 'SKIP'
      AND filed_date < date('now', '-14 days')
  `).run();
  if (skipped.changes > 0) log(`Permanently skipped ${skipped.changes} aged unresolvable rows`);
} catch(e) {}

const insertSc13 = db.prepare(`
  INSERT OR IGNORE INTO sc13_transactions
    (ticker, company, filer, filing_type, filed_date, period_date,
     pct_owned, shares_owned, shares_delta, accession, url, subject_cik)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
`);
function insertMany(rows) {
  // Insert in chunks of 200 rows — sc13-worker is async so we can yield between chunks
  // (called with await insertManyAsync in the parsing function)
  const insertChunk = db.transaction(chunk => {
    let n = 0;
    for (const r of chunk) n += insertSc13.run(...r).changes;
    return n;
  });
  let n = 0;
  const CHUNK = 200;
  for (let i = 0; i < rows.length; i += CHUNK) {
    n += insertChunk(rows.slice(i, i + CHUNK));
  }
  return n;
}

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
  // Legacy form type names (pre-2024Q4)
  'SC 13D', 'SC 13G', 'SC 13D/A', 'SC 13G/A',
  'SC13D',  'SC13G',  'SC13D/A',  'SC13G/A',
  // Current form type names (EDGAR renamed these in 2024Q4+)
  'SCHEDULE 13D', 'SCHEDULE 13G', 'SCHEDULE 13D/A', 'SCHEDULE 13G/A',
]);

// ── Streaming line processor — processes HTTP response line by line ──────────
// Never holds more than one line + a small buffer in memory.
// onLine(line) called for each line. Returns { status, lineCount }.
function getLines(url, ms, onLine) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, url).href;
        return getLines(next, ms, onLine).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return resolve({ status: res.statusCode, lineCount: 0 });
      }
      let buf = '';
      let lineCount = 0;
      res.on('data', chunk => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n')) >= 0) {
          onLine(buf.slice(0, idx));
          buf = buf.slice(idx + 1);
          lineCount++;
        }
      });
      res.on('end', () => {
        if (buf.length) { onLine(buf); lineCount++; }
        resolve({ status: 200, lineCount });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

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

  // ── Streaming parse — never loads full 50MB file into memory ──
  // processFile() streams line-by-line, returns { scanned, totalInserted, lineCount }
  async function processFile(fileUrl, isCompIdx) {
    let pastHeader = false;
    const batch = [];
    let scanned = 0;
    let totalIns = 0;
    let lineCount = 0;
    let debugPrinted = 0;
    let firstLines = [];

    const { status, lineCount: lc } = await getLines(fileUrl, 90000, line => {
      lineCount++;
      if (firstLines.length < 6) firstLines.push(line);

      if (!pastHeader) {
        if (/^-{5,}/.test(line.trim())) pastHeader = true;
        return;
      }
      if (line.length < 40) return;

      const formType = isCompIdx ? line.slice(62, 74).trim() : line.slice(0, 12).trim();
      if (!SC13_TYPES.has(formType)) return;
      scanned++;

      if (debugPrinted < 3) {
        log(`  ${key} sample [${line.length}]: "${line.slice(0, 200)}"`);
        debugPrinted++;
      }

      const isoMatch = line.match(/(\d{4}-\d{2}-\d{2})/);
      const mdyMatch = line.match(/(\d{2})\/(\d{2})\/(\d{4})/);
      let filedDate = '';
      if (isoMatch)      filedDate = isoMatch[1];
      else if (mdyMatch) filedDate = `${mdyMatch[3]}-${mdyMatch[1]}-${mdyMatch[2]}`;
      else return;
      if (filedDate < '2000-01-01' || filedDate > '2040-01-01') return;

      const fnM = line.match(/edgar\/data\/(\d+)\/(\d[\d-]{14,19})\.txt/i)
               || line.match(/edgar\/data\/(\d+)\/(\d{10}-\d{2}-\d{6})/);
      if (!fnM) return;
      const cik   = fnM[1];
      const parts = fnM[2].replace(/\.txt$/i,'').split('-');
      if (parts.length !== 3) return;
      const accDash = `${parts[0].padStart(10,'0')}-${parts[1]}-${parts[2]}`;

      let filer = '', subjectCompanyName = '';
      if (isCompIdx) {
        subjectCompanyName = line.slice(0, 62).trim();
      } else {
        const cikPadded = cik.padStart(10,'0');
        let cikPos = line.indexOf(cikPadded);
        if (cikPos < 0) cikPos = line.indexOf(' ' + cik + ' ');
        if (cikPos < 0) cikPos = line.indexOf(' ' + cik + '\t');
        filer = cikPos > 12 ? line.slice(12, cikPos).replace(/\s{2,}/g,' ').trim() : '';
      }

      const subjectCik = isCompIdx ? cik.padStart(10,'0') : null;
      batch.push(['', subjectCompanyName, filer, formType, filedDate, filedDate,
        null, null, null, accDash, edgarIndexUrl(accDash, cik), subjectCik]);

      if (batch.length >= 500) {
        totalIns += insertMany(batch);
        batch.length = 0;
      }
    });

    if (batch.length > 0) totalIns += insertMany(batch);
    return { status, scanned, totalInserted: totalIns, lineCount, firstLines };
  }

  // Try form.idx first
  let result;
  try {
    result = await processFile(url, false);
  } catch(e) { log(`${key}: fetch error — ${e.message}`); return 0; }

  if (result.status !== 200) { log(`${key}: HTTP ${result.status} — skipping`); return 0; }

  // Check if form.idx returned a readme (non-index file)
  const firstMeaningful = result.firstLines.find(l => l.trim().length > 10) || '';
  const isReadme = /^(Description|Last Data Received|Comments|This file contains)/i.test(firstMeaningful.trim())
                || (result.firstLines.join(' ').includes('Description:') && !firstMeaningful.includes('Form Type'));

  if (isReadme) {
    log(`${key}: form.idx is readme — trying company.idx fallback...`);
    const url2 = `https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${q}/company.idx`;
    try {
      result = await processFile(url2, true);
      if (result.status !== 200) {
        db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, -1);
        return 0;
      }
      // Check company.idx is also not a readme
      const fl2 = result.firstLines.join(' ');
      if (/Description:|Last Data Re|Comments:/i.test(fl2) && !/Company Name/i.test(fl2)) {
        log(`${key}: company.idx also readme — skipping quarter`);
        db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, -1);
        return 0;
      }
      log(`${key}: using company.idx`);
    } catch(e) {
      db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, -1);
      return 0;
    }
  }

  if (result.scanned === 0) {
    log(`${key}: 0 inserted (scanned 0 matching, ${result.lineCount} total lines)`);
    db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, 0);
    return 0;
  }

  db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)').run(key, result.totalInserted);
  log(`${key}: ${result.totalInserted} inserted (${result.scanned} SC 13D/G matched, ${result.lineCount} total lines)`);
  return result.totalInserted;
}

async function runHistoricalBackfill() {
  const now  = new Date();
  const endYr = now.getUTCFullYear();
  const endQ  = Math.ceil((now.getUTCMonth() + 1) / 3);
  let total   = 0;

  // EDGAR only reliably serves form.idx for completed quarters up to ~6 months ago.
  // Extend backfill through all completed quarters up to (but not including) current.
  // The current quarter's form.idx is refreshed separately by runRecentBackfill().
  const _now2 = new Date();
  const curYear = _now2.getFullYear();
  const curQ    = Math.floor(_now2.getMonth() / 3) + 1;
  // Last completed quarter = one before current
  const FORM_IDX_CUTOFF_YEAR = curQ === 1 ? curYear - 1 : curYear;
  const FORM_IDX_CUTOFF_Q    = curQ === 1 ? 4 : curQ - 1;

  // One-time migration: clear log entries for quarters that were previously skipped
  // due to the old EFTS cutoff (2024Q4 through last completed quarter).
  // These quarters are now handled by form.idx and need to be fetched.
  const skippedQuarters = [];
  for (let yr = 2024; yr <= FORM_IDX_CUTOFF_YEAR; yr++) {
    const startQ = yr === 2024 ? 4 : 1;
    const endQ   = yr === FORM_IDX_CUTOFF_YEAR ? FORM_IDX_CUTOFF_Q : 4;
    for (let q = startQ; q <= endQ; q++) {
      const key = `${yr}Q${q}`;
      const existing = db.prepare('SELECT rows FROM sc13_quarter_log WHERE quarter=?').get(key);
      // Clear if: never attempted (!existing), rows=0 (skipped), rows=-1 (readme/failed)
      // Don't clear quarters that were properly fetched (rows > 0)
      if (!existing || existing.rows <= 0) {
        db.prepare('DELETE FROM sc13_quarter_log WHERE quarter=?').run(key);
        skippedQuarters.push(key);
      }
    }
  }
  if (skippedQuarters.length) log(`Clearing ${skippedQuarters.length} previously-skipped quarters: ${skippedQuarters.join(', ')}`);

  // Migration moved to main()

  log(`Historical backfill: 2004 Q1 through ${FORM_IDX_CUTOFF_YEAR} Q${FORM_IDX_CUTOFF_Q}`);

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

// Targeted backfill — only fetch specific missing quarters (e.g. ['2009Q2', '2015Q4'])
async function runHistoricalBackfillForQuarters(quarterKeys) {
  if (!quarterKeys.length) return;
  let total = 0;
  for (const key of quarterKeys) {
    const yr = parseInt(key.slice(0, 4));
    const q  = parseInt(key.slice(5));
    log(`Fetching missing quarter: ${key}`);
    total += await fetchQuarterIndex(yr, q);
    await new Promise(r => setTimeout(r, 500));
  }
  log(`Missing quarter backfill complete: ${total} records inserted for ${quarterKeys.join(', ')}`);
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
  // Migration: extract subject_cik from URL only for company.idx rows
  // (identified by having company name set but blank filer — company.idx format)
  // form.idx rows have the FILER's CIK in the URL, not the subject company's CIK
  const urlMigration = db.prepare(`
    UPDATE sc13_transactions
    SET subject_cik = CAST(
      SUBSTR(url, INSTR(url, '/data/') + 6,
        INSTR(SUBSTR(url, INSTR(url, '/data/') + 6), '/') - 1
      ) AS TEXT
    )
    WHERE (subject_cik IS NULL OR subject_cik = '')
      AND url IS NOT NULL AND url LIKE '%/data/%/%'
      AND (ticker IS NULL OR ticker = '')
      AND company IS NOT NULL AND company != ''
      AND (filer IS NULL OR filer = '')
      AND subject_cik != 'SKIP'
  `).run();
  if (urlMigration.changes > 0) log(`Subject CIK migration: ${urlMigration.changes} rows updated from URL`);

  // Shared cache and lookup function — defined first so both phases can use it
  const cikTickerCache = {};
  let _lookupLogCount = 0;
  async function lookupTickerByCik(cikOrList) {
    if (!cikOrList) return null;
    const cikList = cikOrList.toString().split(',').map(c => c.trim()).filter(Boolean);
    for (const cik of cikList) {
      if (cikTickerCache[cik] !== undefined) {
        if (cikTickerCache[cik]) return cikTickerCache[cik];
        continue;
      }
      try {
        const padded = cik.replace(/^0+/, '').padStart(10, '0');
        const { status, body } = await get(`https://data.sec.gov/submissions/CIK${padded}.json`, 10000);
        if (status !== 200) { cikTickerCache[cik] = null; continue; }
        const data   = JSON.parse(body.toString('utf8'));
        const ticker  = (data.tickers?.[0] || '').toUpperCase().trim();
        const company = (data.name || '').trim();
        const result  = ticker && ticker.match(/^[A-Z]{1,7}(-[A-Z]{1,2})?$/) ? { ticker, company } : null;
        cikTickerCache[cik] = result;

        if (result) return result;
        // Mark non-public entities (funds, individuals) so we skip them next run
        if (!ticker && company) {
          const isNonPublic = /LLC|LP|L\.P\.|Fund|Partners|Capital|Management|Corp\.|Inc\.|Trust|Holding|Group|Associates|Advisors|& Co|Individual|Person/i.test(company)
            || /^[A-Z][a-z]+ [A-Z][a-z]+$/.test(company.trim()); // looks like "First Last" person name
          if (isNonPublic) {
            // cache as permanently null — don't re-query this CIK
            cikTickerCache[cik] = null;
          }
        }
      } catch(e) { cikTickerCache[cik] = null; }
    }
    return null;
  }

  // Phase 1: fast-path — rows with subject_cik already set, just hit data.sec.gov
  const fastRows = db.prepare(`
    SELECT id, subject_cik FROM sc13_transactions
    WHERE (ticker IS NULL OR ticker = '')
      AND subject_cik IS NOT NULL AND subject_cik != ''
      AND subject_cik != 'SKIP'
    ORDER BY filed_date DESC LIMIT 500
  `).all();

  if (fastRows.length > 0) {
    log(`Ticker enrichment fast-path: ${fastRows.length} rows with subject_cik`);
    fastRows.slice(0, 3).forEach((r, i) => log(`  sample[${i}]: ${r.subject_cik}`));
    const upd = db.prepare(`UPDATE sc13_transactions SET ticker=?, company=? WHERE id=?`);
    let n = 0;
    for (let i = 0; i < fastRows.length; i++) {
      try {
        const result = await lookupTickerByCik(fastRows[i].subject_cik);
        if (result) {
          // Also try to set filer name if currently blank
          // The filer is the investor entity — NOT the subject company
          const row = db.prepare('SELECT filer FROM sc13_transactions WHERE id=?').get(fastRows[i].id);
          if (!row?.filer) {
            // Filer CIK is the first CIK in subject_cik that did NOT resolve to a ticker
            const cikList = (fastRows[i].subject_cik||'').split(',').map(c=>c.trim()).filter(Boolean);
            let filerName = '';
            for (const cik of cikList) {
              const padded = cik.replace(/^0+/,'').padStart(10,'0');
              try {
                const {status,body} = await get(`https://data.sec.gov/submissions/CIK${padded}.json`,8000);
                if (status===200) {
                  const d = JSON.parse(body.toString('utf8'));
                  const t = (d.tickers?.[0]||'').toUpperCase().trim();
                  if (!t || !t.match(/^[A-Z]{1,6}$/)) {
                    filerName = (d.name||'').trim(); // no ticker = it's the investor/filer
                    break;
                  }
                }
              } catch(e) {}
            }
            upd.run(result.ticker, result.company, fastRows[i].id);
            if (filerName) db.prepare("UPDATE sc13_transactions SET filer=? WHERE id=? AND (filer IS NULL OR filer='')").run(filerName, fastRows[i].id);
          } else {
            upd.run(result.ticker, result.company, fastRows[i].id);
          }
          n++;
        }
      } catch(e) {}
      if ((i + 1) % 100 === 0) log(`Fast enrichment: ${i+1}/${fastRows.length}, ${n} resolved`);
      if ((i + 1) % 50 === 0) await new Promise(r => setTimeout(r, 5)); // yield to event loop
    }
    log(`Ticker enrichment fast-path: ${n}/${fastRows.length} resolved`);

  // If resolution rate is very low, clear subject_cik on unresolved rows
  // so Phase 2 (index page scraping) gets a chance to find them
  if (fastRows.length > 0 && n === 0) {
    const cleared = db.prepare(`
      UPDATE sc13_transactions SET subject_cik = 'SKIP'
      WHERE (ticker IS NULL OR ticker = '')
        AND subject_cik IS NOT NULL AND subject_cik != '' AND subject_cik != 'SKIP'
        AND (filer IS NULL OR filer = '')
      LIMIT 500
    `).run();
    if (cleared.changes > 0) log(`Marked ${cleared.changes} unresolvable rows as SKIP — Phase 2 will handle via index page`);
  }
  }

  // Phase 2: slow-path — rows without subject_cik, fetch the filing index page
  const rows = db.prepare(`
    SELECT id, accession, url FROM sc13_transactions
    WHERE (ticker IS NULL OR ticker = '')
      AND (subject_cik IS NULL OR subject_cik = '' OR subject_cik = 'SKIP')
    ORDER BY filed_date DESC LIMIT 100
  `).all();

  if (!rows.length) { log('Ticker enrichment phase 2: nothing to enrich'); return; }
  log(`Ticker enrichment phase 2: ${rows.length} rows to resolve via index page`);

  async function getSubjectCik(accession, indexUrl) {
    try {
      if (!indexUrl) return null;
      const { status, body } = await get(indexUrl, 15000);
      if (status !== 200) return null;
      const html = body.toString('utf8');
      // EDGAR index page format: company name + "(Subject)" + CIK= in following href
      // e.g. >ATI Physical Therapy, Inc. (Subject) ...CIK=0001815849&amp;action=
      const m = html.match(/\(Subject\)[\s\S]{0,300}?CIK=0*(\d{4,10})/i)
             || html.match(/\((\d{7,10})\)\s*\(Subject\)/i); // fallback: old format
      return m ? m[1].replace(/^0+/, '') : null;
    } catch(e) { return null; }
  }

  const upd2 = db.prepare(`UPDATE sc13_transactions SET ticker=?, company=? WHERE id=?`);
  let enriched = 0;
  for (let i = 0; i < rows.length; i++) {
    try {
      const subjectCik = await getSubjectCik(rows[i].accession, rows[i].url);
      if (!subjectCik) continue;
      const result = await lookupTickerByCik(subjectCik);
      if (result) { upd2.run(result.ticker, result.company, rows[i].id); enriched++; }
    } catch(e) {}
    if ((i + 1) % 25 === 0) log(`Phase 2 enrichment: ${i+1}/${rows.length}, ${enriched} resolved`);
  }
  log(`Ticker enrichment phase 2: ${enriched}/${rows.length} resolved`);

  // Phase 3: resolve blank filer names from EDGAR filing index
  const blankFilerRows = db.prepare(`
    SELECT id, accession, url FROM sc13_transactions
    WHERE (filer IS NULL OR filer = '') AND url IS NOT NULL
    ORDER BY filed_date DESC LIMIT 200
  `).all();

  if (blankFilerRows.length > 0) {
    log(`Filer name resolution: ${blankFilerRows.length} rows with blank filer`);
    const updFiler = db.prepare(`UPDATE sc13_transactions SET filer=? WHERE id=?`);
    let filerResolved = 0;
    for (const row of blankFilerRows) {
      try {
        const { status, body } = await get(row.url, 10000);
        if (status !== 200) continue;
        const html = body.toString('utf8');
        // EDGAR index page lists filer as: "Filed by: ENTITY NAME (CIK: ...)" 
        // or in the header table as the first company listed
        const m = html.match(/company[^<]*name[^<]*<[^>]+>([^<]{3,80})<\/[^>]+>/i)
               || html.match(/<b>([^<]{5,80})<\/b>\s*\(Filer\)/i)
               || html.match(/filer\s*[:\-]\s*([A-Z][^<\r\n]{4,60})/i);
        if (m) {
          const name = m[1].replace(/&amp;/g,'&').replace(/\s+/g,' ').trim();
          if (name.length > 3 && !name.match(/^(SEC|EDGAR|Filing|Index)/i)) {
            updFiler.run(name, row.id);
            filerResolved++;
          }
        }
      } catch(e) {}
      await new Promise(r => setTimeout(r, 150)); // polite pause
    }
    log(`Filer name resolution: ${filerResolved}/${blankFilerRows.length} resolved`);
  }
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

  // The form.idx for the CURRENT quarter is updated daily by EDGAR.
  // Re-fetch it on every poll run to pick up new filings.
  // Previous quarters are already fully backfilled and cached.
  const now = new Date();
  const curYear = now.getFullYear();
  const curQ    = Math.floor(now.getMonth() / 3) + 1;

  // Also fetch the previous quarter in case we missed any late-filed entries
  const quarters = [];
  quarters.push({ year: curYear, q: curQ });
  // Add previous quarter
  if (curQ === 1) quarters.push({ year: curYear - 1, q: 4 });
  else            quarters.push({ year: curYear,     q: curQ - 1 });

  let totalInserted = 0;
  for (const { year, q } of quarters) {
    const key = `${year}Q${q}`;
    log(`Refreshing ${key} form.idx for recent SC 13D/G filings...`);

    // Temporarily remove from log so fetchFormIdx re-fetches it
    const prev = db.prepare('SELECT rows FROM sc13_quarter_log WHERE quarter=?').get(key);
    db.prepare('DELETE FROM sc13_quarter_log WHERE quarter=?').run(key);

    const inserted = await fetchQuarterIndex(year, q);
    totalInserted += inserted;

    // Restore log entry with updated count
    if (prev) {
      db.prepare('INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES (?,?)')
        .run(key, (prev.rows || 0) + inserted);
    }
  }

  if (totalInserted > 0) log(`Recent SC 13D/G: ${totalInserted} new records from form.idx refresh`);
  else log(`Recent SC 13D/G: 0 new records (form.idx current)`);
  return totalInserted;
}


async function main() {
  const daysBackInit = parseInt(process.argv[2] || '90');
  const mode         = process.argv[3] || 'poll';
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

  // On first-ever run: do 18-month EFTS backfill to cover the gap since form.idx cutoff.
  // On subsequent boots: just fetch the last 2 days (new filings only).
  // Track completion with a marker in sc13_quarter_log.
  const eftsInitDone = db.prepare("SELECT 1 FROM sc13_quarter_log WHERE quarter='EFTS_INIT'").get();

  // Check if EFTS backfill covered 2024Q4-present (after form.idx cutoff ~Sep 2024)
  // form.idx only goes to 2024Q3, so anything after Oct 2024 must come from EFTS.
  // If we have fewer than 50 rows after Oct 2024, the EFTS backfill was incomplete.
  let needsRefetch = !eftsInitDone;
  if (eftsInitDone) {
    const postFormIdxCount = db.prepare(`
      SELECT COUNT(*) AS n FROM sc13_transactions
      WHERE filed_date >= '2024-10-01'
    `).get().n;
    if (postFormIdxCount < 500) {
      log(`SC 13D/G backfill incomplete: only ${postFormIdxCount} rows after 2024-10-01 — re-running via atom feed`);
      db.prepare("DELETE FROM sc13_quarter_log WHERE quarter='EFTS_INIT'").run();
      needsRefetch = true;
    }
  }

  if (needsRefetch) {
    log('Fetching 18-month EFTS backfill to cover 2024Q4-present...');
    const eftsLookback = Math.max(daysBackInit, 540);
    const n = await runRecentBackfill(eftsLookback);
    db.prepare("INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES ('EFTS_INIT',?)").run(n);
    log(`EFTS initial backfill complete (${n} records). Future boots will only fetch recent 5 days.`);
  } else {
    // Normal boot: get new filings from last 5 days to catch any gaps
    await runRecentBackfill(5);
  }

  if (mode === 'historical') {
    await runHistoricalBackfill();
    db.close();
    process.exit(0);
  }

  // Poll mode: run historical backfill only if not yet complete.
  // With 80 quarters all marked done, skip entirely — don't waste boot time iterating.
  // Build the full expected set of quarter keys
  // Migration: re-fetch 2024Q4+ with SCHEDULE 13D/G type names (EDGAR renamed in 2024Q4)
  const migrationDone = db.prepare("SELECT 1 FROM sc13_quarter_log WHERE quarter='SCHEDULE_13_MIGRATION'").get();
  if (!migrationDone) {
    log('Migration: clearing 2024Q4+ quarters for SCHEDULE 13D/G re-fetch...');
    const _now3 = new Date();
    const _curY = _now3.getFullYear();
    const _curQ = Math.floor(_now3.getMonth() / 3) + 1;
    const _cutY = _curQ === 1 ? _curY - 1 : _curY;
    const _cutQ = _curQ === 1 ? 4 : _curQ - 1;
    for (let yr = 2024; yr <= _cutY; yr++) {
      const startQ = yr === 2024 ? 4 : 1;
      const endQ   = yr === _cutY ? _cutQ : 4;
      for (let q = startQ; q <= endQ; q++) {
        db.prepare("DELETE FROM sc13_quarter_log WHERE quarter=?").run(`${yr}Q${q}`);
        log(`  Cleared ${yr}Q${q} for re-fetch`);
      }
    }
    db.prepare("INSERT OR REPLACE INTO sc13_quarter_log (quarter,rows) VALUES ('SCHEDULE_13_MIGRATION',1)").run();
    log('Migration complete');
  }

  const _n = new Date();
  const _fy = _n.getFullYear(), _fq = Math.floor(_n.getMonth()/3)+1;
  const _cutY2 = _fq===1?_fy-1:_fy, _cutQ2 = _fq===1?4:_fq-1;
  const expectedQuarters = new Set();
  for (let yr = 2004; yr <= _cutY2; yr++) {
    const maxQ = yr === _cutY2 ? _cutQ2 : 4;
    for (let q = 1; q <= maxQ; q++) expectedQuarters.add(`${yr}Q${q}`);
  }
  const doneSet = new Set(
    db.prepare(`SELECT quarter FROM sc13_quarter_log WHERE quarter >= '2004Q1' AND quarter <= '${_cutY2}Q${_cutQ2}'`)
      .all().map(r => r.quarter)
  );
  const missing = [...expectedQuarters].filter(k => !doneSet.has(k));

  if (missing.length === 0) {
    log(`Historical backfill: all ${expectedQuarters.size} quarters complete. Skipping.`);
  } else {
    log(`Historical backfill: ${doneSet.size}/${expectedQuarters.size} done. Missing: ${missing.join(', ')}`);
    runHistoricalBackfillForQuarters(missing).catch(e => log(`Historical backfill error: ${e.message}`));
  }

  // Always run enrichment on startup to resolve tickers for any unenriched rows
  log('SC 13D/G worker: running enrichment then scheduling polls...');
  enrichRecentTickers().catch(e => log(`Enrichment error: ${e.message}`));

  // ── SC 13D/G scheduled polls ────────────────────────────────────
  // 4:30pm ET = 20:30 UTC — catch filings made during/after market hours
  // 3:30am ET = 07:30 UTC — overnight sweep + enrichment run
  // No continuous 4-hour polling — lean on schedule instead

  function scheduleSc13At(utcHour, utcMin, label) {
    function msToNext() {
      const now = new Date();
      const target = new Date(now);
      target.setUTCHours(utcHour, utcMin, 0, 0);
      if (target <= now) target.setUTCDate(target.getUTCDate() + 1);
      return target - now;
    }
    function run() {
      const ms = msToNext();
      log(`Next SC 13D/G ${label} in ${Math.round(ms/60000)}min`);
      setTimeout(async () => {
        log(`SC 13D/G ${label} starting...`);
        await runRecentBackfill(5).catch(e => log(`${label} backfill error: ${e.message}`));
        await enrichRecentTickers().catch(e => log(`${label} enrichment error: ${e.message}`));
        run(); // reschedule
      }, ms);
    }
    run();
  }

  scheduleSc13At(20, 30, '4:30pm ET poll');   // 20:30 UTC = 4:30pm ET
  scheduleSc13At(7,  30, '3:30am ET sweep');  // 07:30 UTC = 3:30am ET
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
