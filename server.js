'use strict';

const express = require('express');
const cors    = require('cors');
const https   = require('https');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;
const FMP  = 'OJfv9bPVEMrnwPX7noNpJLZCFLLFTmlu';

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── HTTP GET ─────────────────────────────────────────────────
function get(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent':      'InsiderTape/1.0 admin@insidertape.com',
        'Accept':          'application/json, */*',
        'Accept-Encoding': 'identity',
      },
      timeout: 25000,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode,
        body:   Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function safeParse(body) {
  try { return JSON.parse(body); } catch(e) { return null; }
}

// ─── CACHE ────────────────────────────────────────────────────
const cache = new Map();
function getCache(k)      { const c = cache.get(k); return c && Date.now() < c.exp ? c.val : null; }
function setCache(k,v,ms) { cache.set(k, { val: v, exp: Date.now() + ms }); }

// ─── NORMALISE FMP TRADE ──────────────────────────────────────
const TYPE = {
  'P-Purchase':'P','S-Sale':'S','S-Sale+OE':'S',
  'A-Award':'A','M-Exempt':'M','G-Gift':'G',
  'F-InKind':'F','D-Return':'D','C-Conversion':'C',
  'X-InTheMoney':'X','J-Other':'J',
};
function norm(t) {
  const qty   = Math.abs(t.securitiesTransacted || 0);
  const price = Math.abs(t.price || 0);
  return {
    ticker:  (t.symbol        || '').toUpperCase(),
    company:  t.companyName   || '',
    insider:  t.reportingName || '',
    title:    t.typeOfOwner   || '',
    trade:   (t.transactionDate || '').slice(0,10),
    filing:  (t.filingDate || t.transactionDate || '').slice(0,10),
    type:    TYPE[t.transactionType] || (t.transactionType||'').charAt(0) || '?',
    qty,
    price:   +price.toFixed(2),
    value:   Math.round(qty * price),
    owned:   Math.abs(t.securitiesOwned || 0),
  };
}

// ─── SCREENER ─────────────────────────────────────────────────
// Uses FMP latest insider trading — confirmed working, free tier
app.get('/api/screener', async (req, res) => {
  const cached = getCache('screener');
  if (cached) { console.log('Screener: cache hit'); return res.json(cached); }

  try {
    // 3 pages × 100 = up to 300 recent trades
    const results = await Promise.all([0,1,2].map(p =>
      get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=${p}&limit=100&apikey=${FMP}`)
        .catch(e => ({ status: 0, body: '[]', error: e.message }))
    ));

    const raw = results.flatMap(r => {
      if (r.status !== 200) { console.log('FMP page error:', r.status, r.body?.slice(0,100)); return []; }
      const d = safeParse(r.body);
      return Array.isArray(d) ? d : [];
    });

    if (!raw.length) {
      const errBody = results[0]?.body?.slice(0,300);
      return res.status(502).json({ error: 'FMP returned no trades', fmp_response: errBody });
    }

    const trades = raw
      .map(norm)
      .filter(t => t.ticker && t.trade)
      .sort((a,b) => b.trade.localeCompare(a.trade));

    console.log(`Screener: ${trades.length} trades`);
    setCache('screener', trades, 15 * 60 * 1000); // 15 min
    res.json(trades);

  } catch(e) {
    console.error('Screener error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── TICKER ───────────────────────────────────────────────────
// Sources (in order):
//  1. Filter already-loaded screener data (instant, no API call)
//  2. SEC EFTS full-text search for this symbol (free, no rate issues)
//  3. XML parse each filing result
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  const cached = getCache('ticker:' + symbol);
  if (cached) return res.json(cached);

  const trades = [];
  const seen   = new Set();

  function addTrade(t) {
    if (!t?.trade || !t?.ticker) return;
    const k = `${t.insider}|${t.trade}|${t.type}|${t.qty}`;
    if (seen.has(k)) return;
    seen.add(k);
    trades.push(t);
  }

  // Source 1: filter screener cache (covers last ~2 weeks, instant)
  const screenerCache = getCache('screener');
  if (screenerCache) {
    screenerCache.filter(t => t.ticker === symbol).forEach(addTrade);
    console.log(`Ticker ${symbol}: ${trades.length} from screener cache`);
  }

  // Source 2: SEC EFTS — search for this symbol, last 6 months
  try {
    const end   = new Date().toISOString().split('T')[0];
    const start = new Date(Date.now() - 180 * 86400000).toISOString().split('T')[0];
    const url   = `https://efts.sec.gov/LATEST/search-index?q="${encodeURIComponent(symbol)}"&forms=4&dateRange=custom&startdt=${start}&enddt=${end}&from=0&size=40`;
    const { status, body } = await get(url);

    if (status === 200) {
      const hits = safeParse(body)?.hits?.hits || [];
      console.log(`Ticker ${symbol}: ${hits.length} hits from EFTS`);

      // Fetch XMLs in batches of 5
      const BATCH = 5;
      for (let i = 0; i < hits.length; i += BATCH) {
        const batch = hits.slice(i, i + BATCH);
        await Promise.allSettled(batch.map(async hit => {
          try {
            const parts  = (hit._id || '').split(':');
            const acc    = parts.slice(0,3).join('-');
            const primary = parts[3] || '';
            if (!acc || acc === '--') return;

            const clean  = acc.replace(/-/g,'').padStart(18,'0');
            const dashed = clean.replace(/^(\d{10})(\d{2})(\d{6})$/, '$1-$2-$3');
            const filerCIK = parseInt(clean.slice(0,10), 10);
            const base   = `https://www.sec.gov/Archives/edgar/data/${filerCIK}/${clean}/`;

            let xml = null;

            // Try primaryDoc first
            if (primary) {
              const r = await get(base + primary, 12000).catch(() => null);
              if (r?.status === 200 && r.body.includes('<ownershipDocument>')) xml = r.body;
            }

            // Fallback: index.htm
            if (!xml) {
              const r = await get(`${base}${dashed}-index.htm`, 12000).catch(() => null);
              if (r?.status === 200 && !r.body.startsWith('<!')) {
                const m = r.body.match(/href="([^"]+\.xml)"/i);
                if (m) {
                  const fn = m[1].split('/').pop();
                  const r2 = await get(base + fn, 12000).catch(() => null);
                  if (r2?.status === 200) xml = r2.body;
                }
              }
            }

            if (!xml) return;
            parseForm4(xml, symbol).forEach(t => addTrade(t));
          } catch(e) { /* skip */ }
        }));
      }
    }
  } catch(e) {
    console.error(`Ticker ${symbol} EFTS error:`, e.message);
  }

  trades.sort((a,b) => b.trade.localeCompare(a.trade));
  console.log(`Ticker ${symbol}: ${trades.length} total trades`);
  setCache('ticker:' + symbol, trades, 30 * 60 * 1000);
  res.json(trades);
});

// ─── XML PARSER ───────────────────────────────────────────────
function xmlGet(xml, tag) {
  let m = xml.match(new RegExp('<' + tag + '[^>]*>\\s*<value>\\s*([^<]+?)\\s*</value>', 'is'));
  if (m?.[1]?.trim()) return m[1].trim();
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

  const out = [];
  function parseBlock(block, isDeriv) {
    const code  = xmlGet(block, 'transactionCode') || (isDeriv ? 'A' : 'P');
    const date  = xmlGet(block, 'transactionDate') || filed || '';
    if (!date) return;
    const shares = Math.abs(parseFloat(xmlGet(block,'transactionShares') || (isDeriv ? xmlGet(block,'underlyingSecurityShares') : '') || '0') || 0);
    const price  = Math.abs(parseFloat(xmlGet(block,'transactionPricePerShare') || (isDeriv ? xmlGet(block,'exercisePrice') : '') || '0') || 0);
    const owned  = Math.abs(parseFloat(xmlGet(block,'sharesOwnedFollowingTransaction') || '0') || 0);
    out.push({ ticker, company, insider, title, trade: date.slice(0,10), filing: (filed||date).slice(0,10), type: code, qty: Math.round(shares), price: +price.toFixed(2), value: Math.round(shares*price), owned: Math.round(owned) });
  }
  let m;
  const ndRe = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
  const dRe  = /<derivativeTransaction>([\s\S]*?)<\/derivativeTransaction>/g;
  while ((m = ndRe.exec(xml)) !== null) parseBlock(m[1], false);
  while ((m = dRe.exec(xml))  !== null) parseBlock(m[1], true);
  return out;
}

// ─── INSIDER ──────────────────────────────────────────────────
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.status(400).json({ error: 'name required' });

  const cached = getCache('insider:' + name);
  if (cached) return res.json(cached);

  try {
    const results = await Promise.all([0,1,2].map(p =>
      get(`https://financialmodelingprep.com/stable/insider-trading/reporting-name?name=${encodeURIComponent(name)}&page=${p}&limit=100&apikey=${FMP}`)
        .catch(() => ({ status: 0, body: '[]' }))
    ));

    const raw = results.flatMap(r => {
      if (r.status !== 200) return [];
      const d = safeParse(r.body);
      return Array.isArray(d) ? d : [];
    });

    const trades = raw.map(norm).filter(t => t.trade)
      .sort((a,b) => b.trade.localeCompare(a.trade));

    setCache('insider:' + name, trades, 60 * 60 * 1000);
    res.json(trades);

  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── PRICE ────────────────────────────────────────────────────
app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  const cached = getCache('price:' + symbol);
  if (cached) return res.json(cached);

  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${encodeURIComponent(symbol)}&apikey=${FMP}`
    );

    if (status !== 200)
      return res.status(502).json({ error: `FMP price HTTP ${status}`, detail: body.slice(0,200) });

    const data = safeParse(body);
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    if (!raw.length) return res.status(404).json({ error: `No price data for ${symbol}` });

    const bars = raw.slice().reverse()
      .map(d => ({
        time:   d.date,
        open:   +(+d.open ).toFixed(2),
        high:   +(+d.high ).toFixed(2),
        low:    +(+d.low  ).toFixed(2),
        close:  +(+d.close).toFixed(2),
        volume: d.volume || 0,
      }))
      .filter(d => d.close > 0 && d.time);

    console.log(`Price ${symbol}: ${bars.length} bars`);
    setCache('price:' + symbol, bars, 60 * 60 * 1000);
    res.json(bars);

  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── DIAG ─────────────────────────────────────────────────────
app.get('/api/diag', async (req, res) => {
  const out = { time: new Date().toISOString() };

  // Test screener endpoint
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=3&apikey=${FMP}`
    );
    const d = safeParse(body);
    out.screener = {
      ok:     status === 200 && Array.isArray(d) && d.length > 0,
      status, count: Array.isArray(d) ? d.length : 0,
      raw:    body.slice(0, 300),
    };
  } catch(e) { out.screener = { ok: false, error: e.message }; }

  // Test ticker endpoint (returns 402 on free tier — expected)
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/insider-trading/search?symbol=AAPL&page=0&limit=3&apikey=${FMP}`
    );
    const d = safeParse(body);
    out.ticker = {
      ok:     status === 200 && Array.isArray(d) && d.length > 0,
      status, count: Array.isArray(d) ? d.length : 0,
      raw:    body.slice(0, 300),
    };
  } catch(e) { out.ticker = { ok: false, error: e.message }; }

  // Test price
  try {
    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`
    );
    const data = safeParse(body);
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    out.price = { ok: status === 200 && raw.length > 0, status, bars: raw.length, latest: raw[0] };
  } catch(e) { out.price = { ok: false, error: e.message }; }

  res.json(out);
});

app.get('/api/health', (req, res) =>
  res.json({ ok: true, time: new Date().toISOString() }));

app.listen(PORT, () => console.log(`InsiderTape on port ${PORT}`));
