'use strict';

// sc13-worker.js — Ingests Schedule 13D and 13G filings from EDGAR EFTS.
// Runs as a long-lived process alongside daily-worker.js.
// Writes into sc13_transactions table in the same SQLite DB.
//
// Why a separate table from trades:
//   - 13D/G filings report aggregate ownership snapshots, not per-share open-market trades
//   - The filer is typically an entity (fund, firm), not a named officer/director
//   - No meaningful qty/price/type fields — what matters is pct_owned and shares_delta
//   - Mixing into the trades table would corrupt screener queries and UNIQUE constraints
//
// Poll schedule:
//   - Startup: backfill 90 days
//   - Every 4 hours: check last 2 days (13D/G have 10-day filing window, no need for 2-min RSS)

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

// ── Schema ─────────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS sc13_transactions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT NOT NULL,
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
  );
  CREATE INDEX IF NOT EXISTS idx_sc13_ticker     ON sc13_transactions(ticker);
  CREATE INDEX IF NOT EXISTS idx_sc13_filed_date ON sc13_transactions(filed_date DESC);
  CREATE INDEX IF NOT EXISTS idx_sc13_filer      ON sc13_transactions(filer);
`);

const insertSc13 = db.prepare(`
  INSERT OR IGNORE INTO sc13_transactions
    (ticker, company, filer, filing_type, filed_date, period_date,
     pct_owned, shares_owned, shares_delta, accession, url)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
`);

// ── Logging ────────────────────────────────────────────────────────────────
function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11,19)}] ${msg}\n`); }

// ── Rate-limited HTTPS GET (max 8 req/sec, respects SEC limits) ────────────
const _reqTimes = [];
async function get(url, ms = 25000, _hops = 0) {
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
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function parseDate(s) {
  if (!s) return null;
  const d = s.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  const yr = parseInt(d.slice(0, 4), 10);
  if (yr < 2000 || yr > 2040) return null;
  return d;
}

// ── Parse a 13D/G filing document for ownership details ──────────────────
// Primary targets: pct_owned (e.g. "9.2%"), shares_owned (aggregate shares)
// Works on both XML structured data and the more common HTML/text documents
function parseSc13Doc(body, accession, filedDate, filingType) {
  // Normalise — strip HTML tags for plain-text matching
  const text = body.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

  // ── Ticker: try CUSIP number table or <issuer> block first ──────────────
  let ticker = '';
  let company = '';
  let filer = '';

  // SEC structured XML: <nameOfIssuer>, <issuerCusip>, <nameOfReportingPerson>
  const issuerM = body.match(/<nameOfIssuer[^>]*>\s*([^<]+?)\s*<\/nameOfIssuer>/i);
  if (issuerM) company = issuerM[1].trim();

  const reporterM = body.match(/<nameOfReportingPerson[^>]*>\s*([^<]+?)\s*<\/nameOfReportingPerson>/i)
                 || body.match(/<reportingOwnerName[^>]*>\s*([^<]+?)\s*<\/reportingOwnerName>/i);
  if (reporterM) filer = reporterM[1].trim();

  // Ticker from XML
  const tickerM = body.match(/<issuerTradingSymbol[^>]*>\s*([A-Z]{1,6})\s*<\/issuerTradingSymbol>/i)
               || body.match(/TRADING SYMBOL[^:]*:\s*([A-Z]{1,6})/i)
               || text.match(/\bCUSIP\b[^:]*:\s*[0-9A-Z]+\s+.*?([A-Z]{2,6})\b/i);
  if (tickerM) ticker = tickerM[1].trim().toUpperCase();

  // Fallback: extract from the accession index if we still don't have it
  // (caller will enrich from the EFTS hit metadata)

  // ── Percent owned ────────────────────────────────────────────────────────
  // Look for patterns like "9.2%", "9.2 percent", in proximity to "aggregate"
  // or "percent of class" language
  let pctOwned = null;
  const pctPatterns = [
    /aggregate\s+(?:percentage|percent(?:age)?)\s+of\s+class[^0-9]*([0-9]+\.?[0-9]*)\s*%/i,
    /percent(?:age)?\s+of\s+class[^0-9]*([0-9]+\.?[0-9]*)\s*%/i,
    /([0-9]+\.?[0-9]*)\s*%\s+of\s+(?:the\s+)?(?:outstanding|common|total)/i,
    /beneficially\s+own[^0-9]*([0-9]+\.?[0-9]*)\s*%/i,
    /<percentOfClass[^>]*>\s*([0-9]+\.?[0-9]*)\s*<\/percentOfClass>/i,
    /row\s*(?:13|11)[^0-9]*([0-9]+\.?[0-9]*)\s*%/i,
  ];
  for (const re of pctPatterns) {
    const m = text.match(re) || body.match(re);
    if (m) {
      const v = parseFloat(m[1]);
      if (v > 0 && v <= 100) { pctOwned = v; break; }
    }
  }

  // ── Shares owned (aggregate) ─────────────────────────────────────────────
  let sharesOwned = null;
  const sharesPatterns = [
    /<aggregateAmountBeneficiallyOwned[^>]*>\s*([\d,]+)\s*<\/aggregateAmountBeneficiallyOwned>/i,
    /aggregate\s+amount[^0-9]+([\d,]{4,})/i,
    /row\s*(?:11|9)[^0-9]+([\d,]{5,})/i,
    /beneficially\s+own[^0-9]+([\d,]{4,})\s+shares/i,
    /(\d[\d,]{4,})\s+shares[^.]*beneficially/i,
  ];
  for (const re of sharesPatterns) {
    const m = text.match(re) || body.match(re);
    if (m) {
      const v = parseInt(m[1].replace(/,/g, ''), 10);
      if (v > 0 && v < 5_000_000_000) { sharesOwned = v; break; }
    }
  }

  // ── Filer name fallback from HTML title / header ─────────────────────────
  if (!filer) {
    const titleM = body.match(/<title[^>]*>([^<]+)<\/title>/i);
    if (titleM) {
      // Title often looks like "SC 13D - Pale Fire Capital SE - PHR"
      const parts = titleM[1].split(/[-–]/);
      if (parts.length >= 2) filer = parts[1].trim();
    }
  }

  return { ticker, company, filer, pctOwned, sharesOwned };
}

// ── Fetch and parse a single 13D/G filing ─────────────────────────────────
async function fetchSc13Filing(accession, filedDate, filingType, ciks, tickerHint, companyHint) {
  const acc = accession.replace(/-/g, '');
  const allCiks = [...new Set((ciks || []).map(k => parseInt(k, 10).toString()))];
  if (!allCiks.length) allCiks.push(acc.slice(0, 10).replace(/^0+/, '') || '0');

  const edgarBase = 'https://www.sec.gov/Archives/edgar/data';
  const url = `${edgarBase}/${allCiks[0]}/${acc}/${accession}-index.json`;

  let primaryDocUrl = null;
  let primaryDocBody = null;

  // Try JSON index to find the primary document
  try {
    const { status, body } = await get(url);
    if (status === 200) {
      const idx = JSON.parse(body);
      // Prefer .xml or .htm primary doc
      const docs = idx.documents || [];
      const xmlDoc  = docs.find(d => d.document?.match(/\.xml$/i) && d.type?.match(/13[DG]/i));
      const htmDoc  = docs.find(d => d.document?.match(/\.(htm|html)$/i) && d.type?.match(/13[DG]/i));
      const anyXml  = docs.find(d => d.document?.match(/\.xml$/i));
      const anyHtm  = docs.find(d => d.document?.match(/\.(htm|html)$/i));
      const chosen  = xmlDoc || htmDoc || anyXml || anyHtm;
      if (chosen) {
        primaryDocUrl = `${edgarBase}/${allCiks[0]}/${acc}/${chosen.document}`;
      }
    }
  } catch(e) {}

  // Fallback: try known filename patterns
  if (!primaryDocUrl) {
    for (const cik of allCiks) {
      for (const name of [`${accession}.xml`, 'sc13d.xml', 'sc13g.xml', 'sc13da.xml', 'sc13ga.xml']) {
        try {
          const { status } = await get(`${edgarBase}/${cik}/${acc}/${name}`, 5000);
          if (status === 200) {
            primaryDocUrl = `${edgarBase}/${cik}/${acc}/${name}`;
            break;
          }
        } catch(e) {}
      }
      if (primaryDocUrl) break;
    }
  }

  if (!primaryDocUrl) return null;

  try {
    const { status, body } = await get(primaryDocUrl, 20000);
    if (status !== 200) return null;
    primaryDocBody = body;
  } catch(e) { return null; }

  const parsed = parseSc13Doc(primaryDocBody, accession, filedDate, filingType);

  // Use hints from EFTS metadata if parsing didn't find them
  const ticker  = (parsed.ticker || tickerHint || '').toUpperCase().trim();
  const company = parsed.company || companyHint || '';
  const filer   = parsed.filer || '';

  if (!ticker || !filer) return null;

  // Compute shares_delta vs previous filing for same filer+ticker
  let sharesDelta = null;
  if (parsed.sharesOwned !== null) {
    try {
      const prev = db.prepare(`
        SELECT shares_owned FROM sc13_transactions
        WHERE ticker = ? AND filer = ? AND filed_date < ?
        ORDER BY filed_date DESC LIMIT 1
      `).get(ticker, filer, filedDate);
      if (prev && prev.shares_owned !== null) {
        sharesDelta = parsed.sharesOwned - prev.shares_owned;
      }
    } catch(e) {}
  }

  return {
    ticker,
    company,
    filer,
    filing_type: filingType,
    filed_date:  filedDate,
    period_date: filedDate, // 13D/G don't have a separate transaction date
    pct_owned:   parsed.pctOwned,
    shares_owned: parsed.sharesOwned,
    shares_delta: sharesDelta,
    accession,
    url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=&type=${encodeURIComponent(filingType)}&dateb=&owner=include&count=10&search_text=&accession=${accession}`,
  };
}

// ── Search EFTS for 13D/G filings in date range ───────────────────────────
async function searchEFTS13(startDate, endDate) {
  const filings = [];
  const formTypes = 'SC+13D,SC+13G,SC+13D%2FA,SC+13G%2FA';

  for (let from = 0; from < 5000; from += 100) {
    const url = `https://efts.sec.gov/LATEST/search-index?forms=${formTypes}&dateRange=custom&startdt=${startDate}&enddt=${endDate}&from=${from}&size=100`;
    try {
      const { status, body } = await get(url, 30000);
      if (status !== 200) { log(`EFTS 13D/G HTTP ${status}`); break; }
      const data = JSON.parse(body);
      const hits = data.hits?.hits || [];
      if (!hits.length) break;

      for (const h of hits) {
        const raw      = h._id || '';
        const colonAt  = raw.indexOf(':');
        const accDash  = colonAt >= 0 ? raw.slice(0, colonAt) : raw.replace(/\//g, '-');
        if (!accDash.match(/^\d{10}-\d{2}-\d{6}$/)) continue;

        const fd         = parseDate(h._source?.file_date) || endDate;
        const ciks       = h._source?.ciks || [];
        const formType   = (h._source?.form_type || 'SC 13D').trim();
        // EFTS sometimes includes ticker/company in entity_name or display_names
        const entityName = h._source?.entity_name || '';
        const displayNames = h._source?.display_names || [];
        // Try to extract ticker from display_names — format: "TICKER (Company Name)"
        let tickerHint = '';
        let companyHint = entityName;
        for (const dn of displayNames) {
          const m = dn.match(/^([A-Z]{1,6})\s*\(/);
          if (m) { tickerHint = m[1]; break; }
        }

        filings.push({ accession: accDash, ciks, filedDate: fd, formType, tickerHint, companyHint });
      }

      if (hits.length < 100) break;
    } catch(e) { log(`EFTS 13D/G error: ${e.message}`); break; }
  }

  log(`EFTS 13D/G: ${filings.length} filings for ${startDate} to ${endDate}`);
  return filings;
}

// ── Process a batch of filings ────────────────────────────────────────────
async function processBatch(filings, label) {
  if (!filings.length) return 0;
  log(`${label}: processing ${filings.length} filings`);

  let inserted = 0;
  let failed   = 0;
  const CONCURRENCY = 4; // gentler than Form 4 worker — these docs are larger

  for (let i = 0; i < filings.length; i += CONCURRENCY) {
    const batch   = filings.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(f => fetchSc13Filing(f.accession, f.filedDate, f.formType, f.ciks, f.tickerHint, f.companyHint))
    );

    for (const r of results) {
      if (r.status === 'fulfilled' && r.value) {
        const t = r.value;
        try {
          const changes = insertSc13.run(
            t.ticker, t.company, t.filer, t.filing_type,
            t.filed_date, t.period_date,
            t.pct_owned, t.shares_owned, t.shares_delta,
            t.accession, t.url
          );
          if (changes.changes > 0) inserted++;
        } catch(e) {}
      } else if (r.status === 'rejected') {
        failed++;
      }
    }

    if ((i + CONCURRENCY) % 40 === 0) {
      log(`  ${i + CONCURRENCY}/${filings.length} — inserted:${inserted} failed:${failed}`);
    }
  }

  log(`${label} done — inserted:${inserted} failed:${failed} from ${filings.length} filings`);
  return inserted;
}

// ── Backfill N days ───────────────────────────────────────────────────────
async function runBackfill(daysBack) {
  const today = new Date().toISOString().slice(0, 10);
  const start = new Date();
  start.setDate(start.getDate() - daysBack);
  const startDate = start.toISOString().slice(0, 10);

  log(`SC 13D/G backfill: ${startDate} to ${today}`);
  try {
    const filings  = await searchEFTS13(startDate, today);
    const inserted = await processBatch(filings, 'SC13 Backfill');
    log(`SC 13D/G backfill complete: ${inserted} records inserted`);
  } catch(e) {
    log(`SC 13D/G backfill error: ${e.message}\n${e.stack}`);
  }
}

// ── MAIN ──────────────────────────────────────────────────────────────────
const daysBack = parseInt(process.argv[2] || '90');
const mode     = process.argv[3] || 'poll';

async function main() {
  log(`=== sc13-worker start (mode=${mode}, daysBack=${daysBack}) ===`);

  // Initial backfill
  await runBackfill(daysBack);

  if (mode === 'backfill') {
    db.close();
    process.exit(0);
  }

  // Continuous mode: re-check every 4 hours
  log('SC 13D/G worker: polling every 4 hours');
  setInterval(() => runBackfill(2), 4 * 60 * 60 * 1000);
}

main().catch(e => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  db.close();
  process.exit(1);
});
