'use strict';

// ─────────────────────────────────────────────────────────────
//  INSIDERTAPE — Server v4
//
//  Architecture:
//  • Trades stored in memory (rebuilt daily from SEC EFTS + XML)
//  • Price data from FMP (1hr cache)
//  • Screener serves from in-memory trade store
//  • Ticker lookups filter in-memory store — zero per-request SEC calls
//
//  Daily sync runs at startup then every 24h at 9pm ET (after SEC closes)
//  EFTS search gives us last 30 days of Form 4s in one call
//  Each filing's XML is fetched once and parsed, then discarded
// ─────────────────────────────────────────────────────────────

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const http    = require('http');
const fs      = require('fs');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;
const FMP  = 'OJfv9bPVEMrnwPX7noNpJLZCFLLFTmlu';
const DATA_FILE = path.join(__dirname, 'data', 'trades.json');

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── ensure data dir exists ───────────────────────────────────
fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });

// ─────────────────────────────────────────────────────────────
//  IN-MEMORY TRADE STORE
// ─────────────────────────────────────────────────────────────
let tradeStore = [];          // all parsed trades, sorted newest-first
let storeLastBuilt = null;    // Date of last successful build
let buildInProgress = false;

// Load from disk on startup if available
function loadFromDisk() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      tradeStore     = raw.trades || [];
      storeLastBuilt = raw.builtAt ? new Date(raw.builtAt) : null;
      console.log(`Loaded ${tradeStore.length} trades from disk (built ${storeLastBuilt?.toISOString()})`);
    }
  } catch(e) {
    console.error('Could not load trades from disk:', e.message);
  }
}

function saveToDisk(trades) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ builtAt: new Date().toISOString(), trades }, null, 0));
    console.log(`Saved ${trades.length} trades to disk`);
  } catch(e) {
    console.error('Could not save trades:', e.message);
  }
}

// ─────────────────────────────────────────────────────────────
//  HTTP HELPER
// ─────────────────────────────────────────────────────────────
function get(url, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent':      'InsiderTape/1.0 admin@insidertape.com',
        'Accept':          'application/json, text/html, */*',
        'Accept-Encoding': 'identity',
      },
      timeout: timeoutMs,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location, timeoutMs).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url.slice(0,80)}`)); });
  });
}

// ─────────────────────────────────────────────────────────────
//  PRICE CACHE
// ─────────────────────────────────────────────────────────────
const _priceCache = new Map();
function fromPriceCache(k)      { const c = _priceCache.get(k); return c && Date.now() < c.exp ? c.val : null; }
function toPriceCache(k, v, ms) { _priceCache.set(k, { val: v, exp: Date.now() + ms }); }

// ─────────────────────────────────────────────────────────────
//  XML PARSER — Form 4
// ─────────────────────────────────────────────────────────────
function xmlGet(xml, tag) {
  // Pattern 1: <tag><value>X</value></tag>
  let m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*<value>\\s*([^<]+?)\\s*</value>', 'is'));
  if (m?.[1]?.trim()) return m[1].trim();
  // Pattern 2: <tag>X</tag>
  m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*([^<\\s][^<]*?)\\s*</' + tag + '>', 'i'));
  if (m?.[1]?.trim()) return m[1].trim();
  return '';
}

function parseForm4(xml, fallbackTicker) {
  const insider = xmlGet(xml, 'rptOwnerName');
  const title   = xmlGet(xml, 'officerTitle');
  const ticker  = xmlGet(xml, 'issuerTradingSymbol') || fallbackTicker || '';
  const company = xmlGet(xml, 'issuerName');
  const filed   = xmlGet(xml, 'periodOfReport');
  if (!ticker) return [];

  const trades = [];
  function parseBlock(block, isDeriv) {
    const code  = xmlGet(block, 'transactionCode') || (isDeriv ? 'A' : 'P');
    const date  = xmlGet(block, 'transactionDate') || filed || '';
    if (!date) return;
    const shares = Math.abs(parseFloat(xmlGet(block, 'transactionShares') ||
                   (isDeriv ? xmlGet(block, 'underlyingSecurityShares') : '') || '0') || 0);
    const price  = Math.abs(parseFloat(xmlGet(block, 'transactionPricePerShare') ||
                   (isDeriv ? xmlGet(block, 'exercisePrice') : '') || '0') || 0);
    const owned  = Math.abs(parseFloat(xmlGet(block, 'sharesOwnedFollowingTransaction') || '0') || 0);
    trades.push({
      ticker, company, insider, title,
      trade:  date.slice(0,10),
      filing: (filed || date).slice(0,10),
      type:   code,
      qty:    Math.round(shares),
      price:  +price.toFixed(2),
      value:  Math.round(shares * price),
      owned:  Math.round(owned),
    });
  }

  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  while ((m = ndRe.exec(xml)) !== null) parseBlock(m[1], false);
  while ((m = dRe.exec(xml))  !== null) parseBlock(m[1], true);
  return trades;
}

// ─────────────────────────────────────────────────────────────
//  FETCH ONE FILING
// ─────────────────────────────────────────────────────────────
async function fetchFiling(accession, primaryDoc) {
  const clean  = accession.replace(/-/g,'').padStart(18,'0');
  const dashed = clean.replace(/^(\d{10})(\d{2})(\d{6})$/, '$1-$2-$3');
  const filerCIK = parseInt(clean.slice(0,10), 10);
  const base = `https://www.sec.gov/Archives/edgar/data/${filerCIK}/${clean}/`;

  // Strategy 1: direct primaryDoc
  if (primaryDoc) {
    try {
      const { status, body } = await get(base + primaryDoc, 15000);
      if (status === 200 && body.includes('<ownershipDocument>'))
        return body;
    } catch(e) {}
  }

  // Strategy 2: index.htm → find XML filename
  try {
    const { status, body } = await get(`${base}${dashed}-index.htm`, 15000);
    if (status === 200 && !body.startsWith('<!DOCTYPE')) {
      const m = body.match(/href="([^"]+\.xml)"/i);
      if (m) {
        const fn = m[1].split('/').pop();
        const { status: xs, body: xml } = await get(base + fn, 15000);
        if (xs === 200 && xml.includes('<ownershipDocument>')) return xml;
      }
    }
  } catch(e) {}

  return null;
}

// ─────────────────────────────────────────────────────────────
//  BUILD TRADE STORE from EFTS
//  Called on startup and daily
// ─────────────────────────────────────────────────────────────
async function buildTradeStore() {
  if (buildInProgress) return;
  buildInProgress = true;
  console.log('Building trade store from SEC EFTS...');

  try {
    // Fetch last 30 days from EFTS — gives us all Form 4s
    const end   = new Date().toISOString().split('T')[0];
    const start = new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
    const eftsUrl = `https://efts.sec.gov/LATEST/search-index?forms=4&dateRange=custom&startdt=${start}&enddt=${end}&_source=period_of_report,file_date,entity_name,period_of_report&from=0&size=200`;

    const { status, body } = await get(eftsUrl, 30000);
    if (status !== 200) throw new Error(`EFTS HTTP ${status}`);

    const hits = JSON.parse(body)?.hits?.hits || [];
    console.log(`EFTS: ${hits.length} filings to process`);

    // Process in batches of 8
    const allTrades = [];
    const BATCH = 8;
    for (let i = 0; i < hits.length; i += BATCH) {
      const batch = hits.slice(i, i + BATCH);
      await Promise.allSettled(batch.map(async hit => {
        try {
          // Extract accession from _id: "0001234567:26:000004:filename.xml"
          const parts  = (hit._id || '').split(':');
          const acc    = parts.slice(0,3).join('-');  // "0001234567-26-000004"
          const primary = parts[3] || '';
          if (!acc || acc === '--') return;

          const xml = await fetchFiling(acc, primary);
          if (!xml) return;

          const trades = parseForm4(xml, '');
          allTrades.push(...trades);
        } catch(e) { /* skip bad filing */ }
      }));
      if (i % 40 === 0) console.log(`  Processed ${Math.min(i+BATCH, hits.length)}/${hits.length}, ${allTrades.length} trades so far`);
    }

    // Also fetch screener data from FMP (works for free) to supplement
    try {
      const [r0, r1] = await Promise.all([
        get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=100&apikey=${FMP}`),
        get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=1&limit=100&apikey=${FMP}`),
        get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=2&limit=100&apikey=${FMP}`),
      ]);
      [r0, r1].forEach(r => {
        if (r.status !== 200) return;
        try {
          JSON.parse(r.body).forEach(t => {
            const qty   = Math.abs(t.securitiesTransacted || 0);
            const price = Math.abs(t.price || 0);
            const typeMap = {'P-Purchase':'P','S-Sale':'S','S-Sale+OE':'S','A-Award':'A','M-Exempt':'M','G-Gift':'G','F-InKind':'F','D-Return':'D','C-Conversion':'C','X-InTheMoney':'X'};
            allTrades.push({
              ticker:  (t.symbol||'').toUpperCase(),
              company:  t.companyName||'',
              insider:  t.reportingName||'',
              title:    t.typeOfOwner||'',
              trade:   (t.transactionDate||'').slice(0,10),
              filing:  (t.filingDate||t.transactionDate||'').slice(0,10),
              type:    typeMap[t.transactionType]||t.transactionType||'?',
              qty, price: +price.toFixed(2), value: Math.round(qty*price),
              owned: Math.abs(t.securitiesOwned||0),
            });
          });
        } catch(e) {}
      });
      console.log(`FMP supplement added`);
    } catch(e) { console.log('FMP supplement skipped:', e.message); }

    // Deduplicate
    const seen = new Set();
    const unique = allTrades
      .filter(t => t.ticker && t.trade)
      .sort((a,b) => b.trade.localeCompare(a.trade))
      .filter(t => {
        const k = `${t.ticker}|${t.insider}|${t.trade}|${t.type}|${t.qty}`;
        if (seen.has(k)) return false;
        seen.add(k); return true;
      });

    tradeStore     = unique;
    storeLastBuilt = new Date();
    saveToDisk(unique);
    console.log(`Trade store built: ${unique.length} unique trades`);

  } catch(e) {
    console.error('buildTradeStore error:', e.message);
  } finally {
    buildInProgress = false;
  }
}

// ─────────────────────────────────────────────────────────────
//  ROUTES
// ─────────────────────────────────────────────────────────────

// SCREENER — serve from store, most recent 200
app.get('/api/screener', (req, res) => {
  const trades = tradeStore.slice(0, 500);
  res.json(trades);
  // Trigger rebuild if store is stale (>23hr) and nobody else is building
  if (!buildInProgress && (!storeLastBuilt || Date.now() - storeLastBuilt > 23*3600000)) {
    buildTradeStore();
  }
});

// TICKER — filter store by symbol
app.get('/api/ticker', (req, res) => {
  const symbol = (req.query.symbol||'').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  const trades = tradeStore.filter(t => t.ticker === symbol);
  res.json(trades);
});

// INSIDER — filter store by name
app.get('/api/insider', (req, res) => {
  const name = (req.query.name||'').trim().toUpperCase();
  if (!name) return res.status(400).json({ error: 'name required' });
  const trades = tradeStore.filter(t => (t.insider||'').toUpperCase().includes(name));
  res.json(trades);
});

// PRICE — FMP, 1hr cache
app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol||'').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  try {
    const cached = fromPriceCache(symbol);
    if (cached) return res.json(cached);

    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${symbol}&apikey=${FMP}`
    );
    if (status !== 200) return res.status(502).json({ error: `FMP HTTP ${status}` });

    const data = JSON.parse(body);
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    if (!raw.length) return res.status(404).json({ error: `No price data for ${symbol}` });

    const bars = raw.slice().reverse().map(d => ({
      time:   d.date,
      open:   +(+d.open ).toFixed(2),
      high:   +(+d.high ).toFixed(2),
      low:    +(+d.low  ).toFixed(2),
      close:  +(+d.close).toFixed(2),
      volume: d.volume || 0,
    })).filter(d => d.close > 0 && d.time);

    toPriceCache(symbol, bars, 60*60*1000);
    res.json(bars);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// DIAG
app.get('/api/diag', async (req, res) => {
  const out = {
    store: {
      trades:    tradeStore.length,
      lastBuilt: storeLastBuilt?.toISOString() || 'never',
      building:  buildInProgress,
      sample:    tradeStore[0] || null,
      tickers:   [...new Set(tradeStore.map(t=>t.ticker))].slice(0,20),
    }
  };

  // Test FMP price
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body);
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    out.price = { ok: status===200 && raw.length>0, bars: raw.length, latest: raw[0] };
  } catch(e) { out.price = { ok:false, error: e.message }; }

  // Test FMP screener
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=3&apikey=${FMP}`);
    const d = JSON.parse(body);
    out.fmp_screener = { ok: status===200 && Array.isArray(d), count: d?.length };
  } catch(e) { out.fmp_screener = { ok:false, error: e.message }; }

  res.json(out);
});

// FORCE REBUILD (admin)
app.get('/api/rebuild', (req, res) => {
  res.json({ started: true, wasBuilding: buildInProgress });
  if (!buildInProgress) buildTradeStore();
});

app.get('/api/health', (req, res) =>
  res.json({ ok:true, trades: tradeStore.length, lastBuilt: storeLastBuilt?.toISOString() }));

// ─────────────────────────────────────────────────────────────
//  STARTUP
// ─────────────────────────────────────────────────────────────
loadFromDisk();

// Build on startup if data is missing or stale
const staleMs = 23 * 60 * 60 * 1000;
if (!tradeStore.length || !storeLastBuilt || (Date.now() - storeLastBuilt) > staleMs) {
  console.log('Store empty or stale — building now...');
  buildTradeStore();
}

// Daily rebuild at 9pm ET (01:00 UTC)
function scheduleNextBuild() {
  const now  = new Date();
  const next = new Date(now);
  next.setUTCHours(1, 0, 0, 0);
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
  const ms = next - now;
  console.log(`Next trade store rebuild in ${Math.round(ms/3600000)}h`);
  setTimeout(() => { buildTradeStore().then(scheduleNextBuild); }, ms);
}
scheduleNextBuild();

app.listen(PORT, () => console.log(`InsiderTape on port ${PORT}`));
