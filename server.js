'use strict';

// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Server
//
//  Data source: SEC Insider Transactions Data Sets (quarterly ZIPs)
//  https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
//
//  Each quarterly ZIP contains pre-parsed TSV files:
//    SUBMISSION.tsv        — filing metadata (ticker, company, date)
//    REPORTINGOWNER.tsv    — insider name & title
//    NONDERIV_TRANS.tsv    — stock transactions (buys/sells)
//    DERIV_TRANS.tsv       — option/derivative transactions
//
//  We download the last 4 quarters on startup, join the tables,
//  store everything in memory, and serve from there.
//  No XML parsing. No per-request API calls. No rate limits.
//
//  Price data: FMP (confirmed working, free tier)
// ─────────────────────────────────────────────────────────────

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const http     = require('http');
const fs       = require('fs');
const path     = require('path');
const zlib     = require('zlib');
const readline = require('readline');

const app  = express();
const PORT = process.env.PORT || 3000;
const FMP  = 'OJfv9bPVEMrnwPX7noNpJLZCFLLFTmlu';

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── TRADE STORE ─────────────────────────────────────────────
let trades      = [];   // all normalised trades, newest-first
let lastBuilt   = null;
let buildStatus = 'idle'; // idle | running | done | error
let buildLog    = [];

function log(msg) {
  const line = `[${new Date().toISOString().slice(11,19)}] ${msg}`;
  console.log(line);
  buildLog.push(line);
  if (buildLog.length > 100) buildLog.shift();
}

// ─── HTTP HELPERS ─────────────────────────────────────────────
function get(url, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com' },
      timeout: timeoutMs,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location, timeoutMs).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url.slice(0,60)}`)); });
  });
}

// Download a ZIP and extract named files into a map of filename→lines[]
async function fetchZipTSVs(url, wantedFiles) {
  log(`Downloading ${url}`);
  const { status, body } = await get(url, 120000);
  if (status !== 200) throw new Error(`HTTP ${status} for ${url}`);
  log(`Downloaded ${Math.round(body.length/1024)}KB`);

  // Parse ZIP manually (no external deps)
  // Find each local file header and extract
  const result = {};
  let offset = 0;
  const buf = body;

  while (offset < buf.length - 4) {
    // Local file header signature: PK\x03\x04
    if (buf[offset] !== 0x50 || buf[offset+1] !== 0x4B ||
        buf[offset+2] !== 0x03 || buf[offset+3] !== 0x04) {
      offset++;
      continue;
    }

    const compression  = buf.readUInt16LE(offset + 8);
    const compSize     = buf.readUInt32LE(offset + 18);
    const uncompSize   = buf.readUInt32LE(offset + 22);
    const fnameLen     = buf.readUInt16LE(offset + 26);
    const extraLen     = buf.readUInt16LE(offset + 28);
    const fname        = buf.slice(offset + 30, offset + 30 + fnameLen).toString('utf8');
    const dataStart    = offset + 30 + fnameLen + extraLen;
    const dataEnd      = dataStart + compSize;

    const basename = fname.split('/').pop().toUpperCase();
    if (wantedFiles.some(w => basename.startsWith(w))) {
      log(`  Extracting ${fname} (${Math.round(compSize/1024)}KB compressed)`);
      try {
        let raw;
        if (compression === 0) {
          raw = buf.slice(dataStart, dataEnd);
        } else if (compression === 8) {
          raw = zlib.inflateRawSync(buf.slice(dataStart, dataEnd));
        } else {
          log(`  Unknown compression ${compression} for ${fname}, skipping`);
          offset = dataEnd;
          continue;
        }
        const text  = raw.toString('utf8');
        const lines = text.split('\n');
        result[basename] = lines;
        log(`  ${basename}: ${lines.length} lines`);
      } catch(e) {
        log(`  Error extracting ${fname}: ${e.message}`);
      }
    }

    offset = dataEnd;
  }

  return result;
}

// Parse TSV lines into array of objects using first line as headers
function parseTSV(lines) {
  if (!lines || lines.length < 2) return [];
  const headers = lines[0].split('\t').map(h => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    if (cols.length < 2) continue;
    const row = {};
    headers.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    rows.push(row);
  }
  return rows;
}

// Current + last 3 quarters
function getRecentQuarters() {
  const quarters = [];
  const now = new Date();
  let year = now.getFullYear();
  let q    = Math.ceil((now.getMonth() + 1) / 3);

  // The current quarter's file might not exist yet (published at quarter end)
  // Start from previous quarter to be safe
  for (let i = 0; i < 5; i++) {
    q--;
    if (q < 1) { q = 4; year--; }
    quarters.push({ year, q });
    if (quarters.length >= 4) break;
  }
  return quarters;
}

// ─── BUILD TRADE STORE ────────────────────────────────────────
async function buildTradeStore() {
  if (buildStatus === 'running') return;
  buildStatus = 'running';
  buildLog    = [];
  log('=== Starting trade store build ===');

  try {
    const quarters = getRecentQuarters();
    log(`Will fetch quarters: ${quarters.map(q=>`${q.year}Q${q.q}`).join(', ')}`);

    const allTrades = [];

    for (const { year, q } of quarters) {
      const url = `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${year}q${q}_form345.zip`;
      try {
        const files = await fetchZipTSVs(url, ['SUBMISSION', 'REPORTINGOWNER', 'NONDERIV_TRANS', 'DERIV_TRANS']);

        const submissions = parseTSV(files['SUBMISSION.TSV'] || files['SUBMISSION.TXT']);
        const owners      = parseTSV(files['REPORTINGOWNER.TSV'] || files['REPORTINGOWNER.TXT']);
        const ndTrans     = parseTSV(files['NONDERIV_TRANS.TSV'] || files['NONDERIV_TRANS.TXT']);
        const dTrans      = parseTSV(files['DERIV_TRANS.TSV'] || files['DERIV_TRANS.TXT']);

        log(`${year}Q${q}: ${submissions.length} submissions, ${owners.length} owners, ${ndTrans.length} ND trans, ${dTrans.length} D trans`);

        // Build lookup maps
        // submission: ACCESSION_NUMBER → { ticker, company, filingDate }
        const subMap = {};
        for (const s of submissions) {
          const acc = s.ACCESSION_NUMBER || s.accession_number || '';
          if (!acc) continue;
          subMap[acc] = {
            ticker:  (s.ISSUERTRADINGSYMBOL  || s.issuerTradingSymbol  || '').trim().toUpperCase(),
            company: (s.ISSUERNAME           || s.issuerName           || '').trim(),
            filed:   (s.FILEDATE             || s.filingDate           || s.PERIOD_OF_REPORT || '').trim(),
            period:  (s.PERIOD_OF_REPORT     || s.periodOfReport       || '').trim(),
          };
        }

        // owner: ACCESSION_NUMBER → { name, title }
        const ownerMap = {};
        for (const o of owners) {
          const acc = o.ACCESSION_NUMBER || o.accession_number || '';
          if (!acc) continue;
          if (!ownerMap[acc]) {
            ownerMap[acc] = {
              name:  (o.RPTOWNERNAME    || o.reportingOwnerName || o.rptOwnerName || '').trim(),
              title: (o.OFFICERTITLE    || o.officerTitle       || o.RPTOWNERRELATIONSHIP || '').trim(),
            };
          }
        }

        // Process non-derivative transactions
        for (const t of ndTrans) {
          const acc  = t.ACCESSION_NUMBER || t.accession_number || '';
          const sub  = subMap[acc];
          const own  = ownerMap[acc];
          if (!sub || !sub.ticker) continue;

          const qty   = Math.abs(parseFloat(t.TRANS_SHARES || t.transShares || '0') || 0);
          const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE || t.transPricePerShare || '0') || 0);
          const date  = (t.TRANS_DATE || t.transactionDate || sub.period || sub.filed || '').slice(0,10);
          if (!date) continue;

          allTrades.push({
            ticker:  sub.ticker,
            company: sub.company,
            insider: own?.name  || '',
            title:   own?.title || '',
            trade:   date,
            filing:  sub.filed.slice(0,10),
            type:    (t.TRANS_CODE || t.transactionCode || '?').trim(),
            qty:     Math.round(qty),
            price:   +price.toFixed(2),
            value:   Math.round(qty * price),
            owned:   Math.round(Math.abs(parseFloat(t.SHRSOWNFOLLOWINGTRANS || t.sharesOwnedFollowingTransaction || '0') || 0)),
          });
        }

        // Process derivative transactions
        for (const t of dTrans) {
          const acc  = t.ACCESSION_NUMBER || t.accession_number || '';
          const sub  = subMap[acc];
          const own  = ownerMap[acc];
          if (!sub || !sub.ticker) continue;

          const qty   = Math.abs(parseFloat(t.TRANS_SHARES || t.transShares || t.UNDERLYING_SHARES || '0') || 0);
          const price = Math.abs(parseFloat(t.TRANS_PRICEPERSHARE || t.exercisePrice || t.EXERCISE_PRICE || '0') || 0);
          const date  = (t.TRANS_DATE || t.transactionDate || sub.period || sub.filed || '').slice(0,10);
          if (!date) continue;

          allTrades.push({
            ticker:  sub.ticker,
            company: sub.company,
            insider: own?.name  || '',
            title:   own?.title || '',
            trade:   date,
            filing:  sub.filed.slice(0,10),
            type:    (t.TRANS_CODE || t.transactionCode || '?').trim(),
            qty:     Math.round(qty),
            price:   +price.toFixed(2),
            value:   Math.round(qty * price),
            owned:   0,
          });
        }

        log(`${year}Q${q}: running total ${allTrades.length} trades`);
      } catch(e) {
        log(`${year}Q${q} FAILED: ${e.message}`);
      }
    }

    // Deduplicate & sort
    const seen   = new Set();
    const unique = allTrades
      .filter(t => t.ticker && t.trade && t.type)
      .sort((a,b) => b.trade.localeCompare(a.trade))
      .filter(t => {
        const k = `${t.ticker}|${t.insider}|${t.trade}|${t.type}|${t.qty}`;
        if (seen.has(k)) return false;
        seen.add(k); return true;
      });

    trades      = unique;
    lastBuilt   = new Date();
    buildStatus = 'done';
    log(`=== Build complete: ${unique.length} unique trades ===`);

  } catch(e) {
    buildStatus = 'error';
    log(`=== Build ERROR: ${e.message} ===`);
  }
}

// ─── PRICE CACHE ──────────────────────────────────────────────
const priceCache = new Map();
function getPriceCache(k)      { const c = priceCache.get(k); return c && Date.now() < c.exp ? c.val : null; }
function setPriceCache(k,v,ms) { priceCache.set(k, { val:v, exp: Date.now()+ms }); }

// ─── ROUTES ───────────────────────────────────────────────────

// SCREENER — latest 500 trades
app.get('/api/screener', (req, res) => {
  if (buildStatus === 'running' && !trades.length)
    return res.json({ building: true, message: 'Loading SEC data, check back in ~60 seconds', trades: [] });
  res.json(trades.slice(0, 500));
});

// TICKER — all trades for a symbol
app.get('/api/ticker', (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  res.json(trades.filter(t => t.ticker === symbol));
});

// INSIDER — all trades for a person
app.get('/api/insider', (req, res) => {
  const name = (req.query.name || '').trim().toUpperCase();
  if (!name) return res.status(400).json({ error: 'name required' });
  res.json(trades.filter(t => (t.insider||'').toUpperCase().includes(name)));
});

// PRICE — FMP EOD
app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  const cached = getPriceCache(symbol);
  if (cached) return res.json(cached);

  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${encodeURIComponent(symbol)}&apikey=${FMP}`
    );
    if (status !== 200) return res.status(502).json({ error: `FMP HTTP ${status}` });

    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    if (!raw.length) return res.status(404).json({ error: `No price data for ${symbol}` });

    const bars = raw.slice().reverse()
      .map(d => ({ time: d.date, open: +(+d.open).toFixed(2), high: +(+d.high).toFixed(2), low: +(+d.low).toFixed(2), close: +(+d.close).toFixed(2), volume: d.volume||0 }))
      .filter(d => d.close > 0 && d.time);

    setPriceCache(symbol, bars, 60*60*1000);
    res.json(bars);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// STATUS — build progress
app.get('/api/status', (req, res) => res.json({
  status:    buildStatus,
  trades:    trades.length,
  lastBuilt: lastBuilt?.toISOString() || null,
  log:       buildLog.slice(-20),
}));

// REBUILD — force refresh
app.get('/api/rebuild', (req, res) => {
  res.json({ started: true });
  buildTradeStore();
});

// DIAG
app.get('/api/diag', async (req, res) => {
  const out = {
    store: { status: buildStatus, trades: trades.length, lastBuilt: lastBuilt?.toISOString(), log: buildLog.slice(-10) }
  };
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body.toString());
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    out.price  = { ok: status===200 && raw.length>0, bars: raw.length };
  } catch(e) { out.price = { ok:false, error: e.message }; }
  res.json(out);
});

app.get('/api/health', (req, res) =>
  res.json({ ok:true, trades: trades.length, status: buildStatus }));

// ─── STARTUP ──────────────────────────────────────────────────
// Kick off build immediately
buildTradeStore();

// Rebuild every 24h
setInterval(() => buildTradeStore(), 24 * 60 * 60 * 1000);

app.listen(PORT, () => log(`InsiderTape on port ${PORT}`));
