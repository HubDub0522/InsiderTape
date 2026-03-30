'use strict';
// congress-worker.js — House PTR ingestion from disclosures-clerk.house.gov

const https    = require('https');
const http     = require('http');
const zlib     = require('zlib');
const path     = require('path');
const fs       = require('fs');
const Database = require('better-sqlite3');

// ── DB ────────────────────────────────────────────────────────────────────────
const DATA_DIR = (() => {
  const e = process.env.DB_PATH;
  if (e) return path.dirname(e);
  for (const d of ['/var/data', path.join(__dirname, 'data')]) {
    try { fs.mkdirSync(d, { recursive: true }); const p = path.join(d,'.probe'); fs.writeFileSync(p,'1'); fs.unlinkSync(p); return d; } catch(_){}
  }
  return path.join(__dirname, 'data');
})();
const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, 'trades.db');
console.log(`[congress] DB: ${DB_PATH}`);

let db;
try { db = new Database(DB_PATH); }
catch(e) { console.error(`[congress] DB open failed: ${e.message}`); process.exit(1); }
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 10000');

// ── Schema ────────────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS gov_trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber          TEXT NOT NULL,
    member           TEXT NOT NULL,
    ticker           TEXT,
    asset_description TEXT,
    transaction_type TEXT,
    transaction_date TEXT,
    disclosure_date  TEXT,
    amount_range     TEXT,
    owner            TEXT,
    filing_url       TEXT,
    doc_id           TEXT,
    UNIQUE(chamber, doc_id, ticker, transaction_date, transaction_type)
  )
`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_ticker ON gov_trades(ticker)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_td     ON gov_trades(transaction_date DESC)`);
try { db.exec(`ALTER TABLE gov_trades ADD COLUMN doc_id TEXT`); } catch(_) {}
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_gov_docid ON gov_trades(doc_id)`); } catch(_) {}

const seenDocs = new Set(
  db.prepare("SELECT DISTINCT doc_id FROM gov_trades WHERE doc_id IS NOT NULL").all().map(r => r.doc_id)
);
console.log(`[congress] Already processed ${seenDocs.size} doc IDs`);

// ── Insert ────────────────────────────────────────────────────────────────────
const ins = db.prepare(`
  INSERT OR IGNORE INTO gov_trades
    (chamber,member,ticker,asset_description,transaction_type,
     transaction_date,disclosure_date,amount_range,owner,filing_url,doc_id)
  VALUES
    (@chamber,@member,@ticker,@asset_description,@transaction_type,
     @transaction_date,@disclosure_date,@amount_range,@owner,@filing_url,@doc_id)
`);
const insertMany = db.transaction(rows => {
  let n = 0;
  for (const r of rows) n += ins.run(r).changes;
  return n;
});

// ── HTTP ──────────────────────────────────────────────────────────────────────
function get(url, ms = 60000, hops = 0) {
  if (hops > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      timeout: ms,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const loc = res.headers.location;
        return get(loc.startsWith('http') ? loc : new URL(loc, url).href, ms, hops+1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── PDF text extractor ────────────────────────────────────────────────────────
function extractPdfText(buf) {
  const text = [];
  let pos = 0;
  while (pos < buf.length - 10) {
    const streamIdx = buf.indexOf('stream', pos);
    if (streamIdx < 0) break;
    const dictStart = Math.max(0, streamIdx - 500);
    const dict = buf.slice(dictStart, streamIdx).toString('latin1');
    const isFlateDecode = dict.includes('FlateDecode') || dict.includes('/Fl\n') || dict.includes('/Fl ') || dict.includes('/Fl>');
    let dataStart = streamIdx + 6;
    if (buf[dataStart] === 13) dataStart++;
    if (buf[dataStart] === 10) dataStart++;
    const endIdx = buf.indexOf('endstream', dataStart);
    if (endIdx < 0 || endIdx <= dataStart) { pos = streamIdx + 7; continue; }

    if (isFlateDecode) {
      const compressed = buf.slice(dataStart, endIdx);
      let decompressed = null;
      // Try all inflate variants
      const errors = [];
      for (const fn of [zlib.inflateSync, zlib.inflateRawSync, zlib.unzipSync]) {
        try { decompressed = fn(compressed).toString('latin1'); break; }
        catch(e) { errors.push(fn.name+':'+e.message.slice(0,30)); }
      }
      // Retry trimming trailing bytes
      if (!decompressed) {
        for (let trim = 1; trim <= 8 && !decompressed; trim++) {
          for (const fn of [zlib.inflateSync, zlib.inflateRawSync]) {
            try { decompressed = fn(compressed.slice(0, compressed.length - trim)).toString('latin1'); break; } catch(_) {}
          }
        }
      }
      if (!decompressed && errors.length && text.length === 0) {
        // Log first stream failure only
        const firstBytes = compressed.slice(0,8).toString('hex');
        process.stderr.write('[pdf-dbg] compress firstBytes:'+firstBytes+' size:'+compressed.length+' errors:'+errors.join('|')+'\n');
      }
      if (decompressed) {
        const tjRe = /\(([^)]*)\)\s*Tj/g;
        let m;
        while ((m = tjRe.exec(decompressed)) !== null) text.push(m[1].replace(/\\[nrt]/g,' '));
        const TJRe = /\[([^\]]*)\]\s*TJ/g;
        while ((m = TJRe.exec(decompressed)) !== null) {
          const strRe = /\(([^)]*)\)/g; let sm;
          while ((sm = strRe.exec(m[1])) !== null) text.push(sm[1].replace(/\\[nrt]/g,' '));
        }
      }
    }
    pos = endIdx + 9;
  }
  return text.join(' ').replace(/\s+/g, ' ').trim();
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function normDate(raw = '') {
  if (!raw || raw === '--') return null;
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0,10);
  return null;
}
function normType(raw = '') {
  const t = raw.trim().toLowerCase();
  if (t.includes('purchase') || t === 'buy' || t === 'p') return 'P';
  if (t.includes('sale') || t.includes('sell') || t === 's') return 'S';
  return null;
}

// ── Parse PTR text (works for both HTML and PDF-extracted text) ───────────────
function parseHousePTR(rawText, member, docId, filingUrl) {
  const rows = [];
  const text = rawText.replace(/<[^>]+>/g,' ').replace(/&amp;/g,'&').replace(/\s+/g,' ');

  // Disclosure date from signature
  const signedMatch = text.match(/Digitally Signed[^,]*,\s*(\d{1,2}\/\d{1,2}\/\d{4})/);
  const disclosureDate = signedMatch ? normDate(signedMatch[1]) : null;

  const tickerRe = /([^(]{3,80})\(([A-Z]{1,5})\)(?:\s*\[(?:ST|OP|DC|CS|MF|ET|PS|AS)\])?/g;
  const SKIP = new Set(['EST','LLC','INC','ETF','THE','FOR','AND','ARE','NOT','USA','SEC','ACT','GPO','PDF']);
  let m;
  while ((m = tickerRe.exec(text)) !== null) {
    const ticker = m[2];
    if (SKIP.has(ticker)) continue;

    const after = text.slice(m.index + m[0].length, m.index + m[0].length + 400);
    const typeM  = after.match(/\b(Purchase|Sale|Exchange|P|S)\b/i);
    const txType = typeM ? normType(typeM[1]) : null;
    if (!txType) continue;

    const afterType = after.slice(after.indexOf(typeM[0]) + typeM[0].length);
    const dateM    = afterType.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    const txDate   = dateM ? normDate(dateM[1]) : null;
    const amtM     = after.match(/(\$[\d,]+\s*[-–]\s*\$[\d,]+|\$[\d,]+\+?)/);
    const amtRange = amtM ? amtM[1].replace(/\s+/g,' ').slice(0,50) : null;
    const ownerM   = after.match(/\b(Self|Spouse|Joint|Dependent)\b/i);

    rows.push({
      chamber:'H', member, ticker,
      asset_description: m[1].trim().slice(0,200),
      transaction_type: txType,
      transaction_date: txDate,
      disclosure_date: disclosureDate,
      amount_range: amtRange,
      owner: ownerM ? ownerM[1].slice(0,20) : 'Self',
      filing_url: filingUrl,
      doc_id: docId,
    });
  }
  return rows;
}

// ── Fetch one year's ZIP ──────────────────────────────────────────────────────
async function fetchYearZip(year) {
  const zipUrl = `https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${year}FD.ZIP`;
  console.log(`[congress] Fetching House ZIP for ${year}...`);

  let xmlBody;
  try {
    const { status, body } = await get(zipUrl, 90000);
    if (status !== 200) throw new Error(`ZIP returned HTTP ${status}`);

    // Parse ZIP natively
    let offset = 0, found = false;
    while (offset < body.length - 4) {
      if (body.readUInt32LE(offset) !== 0x04034b50) { offset++; continue; }
      const compression = body.readUInt16LE(offset + 8);
      const compSize    = body.readUInt32LE(offset + 18);
      const fnameLen    = body.readUInt16LE(offset + 26);
      const extraLen    = body.readUInt16LE(offset + 28);
      const fname       = body.slice(offset + 30, offset + 30 + fnameLen).toString();
      const dataStart   = offset + 30 + fnameLen + extraLen;
      if (fname.toLowerCase().endsWith('.xml')) {
        const compressed = body.slice(dataStart, dataStart + compSize);
        xmlBody = compression === 8
          ? zlib.inflateRawSync(compressed).toString('utf8')
          : compressed.toString('utf8');
        found = true;
        break;
      }
      offset = dataStart + compSize;
    }
    if (!found) throw new Error('No XML found in ZIP');
  } catch(e) {
    console.warn(`[congress] House ${year} ZIP failed: ${e.message}`);
    return 0;
  }

  console.log('[congress] XML preview:', xmlBody.slice(0, 250));

  // Parse XML
  const getTag = (xml, tag) => { const m = xml.match(new RegExp(`<${tag}>([^<]*)</${tag}>`,'i')); return m ? m[1].trim() : ''; };
  const rowRe  = /<(?:Row|Member)>([\s\S]*?)<\/(?:Row|Member)>/gi;
  const PTR_TYPES = new Set(['ptr','periodic transaction report','periodic','p t r','p']);
  const newFilings = [];
  let rm;
  while ((rm = rowRe.exec(xmlBody)) !== null) {
    const row = rm[1];
    const ft  = getTag(row, 'FilingType').toLowerCase();
    if (ft && !PTR_TYPES.has(ft)) continue;
    const docId = getTag(row, 'DocID') || getTag(row, 'DocumentID');
    if (!docId || seenDocs.has(docId)) continue;
    const first = getTag(row, 'First') || getTag(row, 'FirstName');
    const last  = getTag(row, 'Last')  || getTag(row, 'LastName');
    newFilings.push({ docId, member: `${first} ${last}`.trim() || 'Unknown', year });
  }
  console.log(`[congress] House ${year}: ${newFilings.length} new PTR filings`);

  let totalInserted = 0;
  for (const f of newFilings) {
    const pdfUrl = `https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${f.year}/${f.docId}.pdf`;
    try {
      await sleep(500);
      const { status, body } = await get(pdfUrl, 20000);
      if (status !== 200) continue;

      let bodyStr;
      const isPDF = body[0] === 0x25 && body[1] === 0x50 && body[2] === 0x44 && body[3] === 0x46;
      if (isPDF) {
        bodyStr = extractPdfText(body);
        if (!bodyStr.trim()) {
          // Diagnostic on failure
          const streamCount = (body.toString('latin1').match(/\bstream\b/g)||[]).length;
          const hasFD = body.indexOf('FlateDecode') >= 0;
          console.warn(`[congress] ${f.docId}: PDF extraction failed (streams:${streamCount} FlateDecode:${hasFD} size:${body.length})`);
          seenDocs.add(f.docId);
          continue;
        }
      } else {
        bodyStr = body.toString('utf8');
      }

      const rows = parseHousePTR(bodyStr, f.member, f.docId, pdfUrl);
      if (rows.length) {
        const n = insertMany(rows);
        if (n) console.log(`[congress] ${f.member} (${f.docId}): ${n} trades`);
        totalInserted += n;
      }
      seenDocs.add(f.docId);
    } catch(e) {
      console.warn(`[congress] PDF ${f.docId} error: ${e.message}`);
    }
  }
  return totalInserted;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function fetchHouse() {
  const currentYear = new Date().getFullYear();
  const startYear   = parseInt(process.env.CONGRESS_START_YEAR || String(currentYear));
  let total = 0;
  for (let y = startYear; y <= currentYear; y++) {
    try { total += await fetchYearZip(y); }
    catch(e) { console.warn(`[congress] Year ${y} failed: ${e.message}`); }
  }
  return total;
}

(async () => {
  try {
    const n = await fetchHouse();
    console.log(`[congress] Done. Total new trades: ${n}`);
    db.close();
    process.exit(0);
  } catch(e) {
    console.error('[congress] FATAL:', e.message);
    process.exit(1);
  }
})();
