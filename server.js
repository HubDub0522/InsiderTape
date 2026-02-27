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

function get(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InsiderTape/1.0 admin@insidertape.com', 'Accept': 'application/json' },
      timeout: 25000,
    }, res => {
      if ([301,302,303].includes(res.statusCode) && res.headers.location)
        return get(res.headers.location).then(resolve).catch(reject);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

const _cache = new Map();
function fromCache(k)      { const c = _cache.get(k); return c && Date.now() < c.exp ? c.val : null; }
function toCache(k, v, ms) { _cache.set(k, { val: v, exp: Date.now() + ms }); }

function norm(t) {
  const typeMap = {
    'P-Purchase':'P','S-Sale':'S','S-Sale+OE':'S','A-Award':'A',
    'M-Exempt':'M','G-Gift':'G','F-InKind':'F','D-Return':'D',
    'C-Conversion':'C','X-InTheMoney':'X','J-Other':'J',
  };
  const qty   = Math.abs(t.securitiesTransacted || 0);
  const price = Math.abs(t.price || 0);
  return {
    ticker:  (t.symbol       || '').toUpperCase(),
    company:  t.companyName  || '',
    insider:  t.reportingName|| '',
    title:    t.typeOfOwner  || '',
    trade:   (t.transactionDate || '').slice(0,10),
    filing:  (t.filingDate   || t.transactionDate || '').slice(0,10),
    type:     typeMap[t.transactionType] || t.transactionType || '?',
    qty,
    price:   +price.toFixed(2),
    value:   Math.round(qty * price),
    owned:   Math.abs(t.securitiesOwned || 0),
  };
}

function safeParse(body) {
  try { return JSON.parse(body); } catch(e) { return []; }
}

// SCREENER
app.get('/api/screener', async (req, res) => {
  try {
    const cached = fromCache('screener');
    if (cached) return res.json(cached);

    const [r0, r1] = await Promise.all([
      get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=100&apikey=${FMP}`),
      get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=1&limit=100&apikey=${FMP}`),
    ]);

    const raw = [
      ...(r0.status===200 ? safeParse(r0.body) : []),
      ...(r1.status===200 ? safeParse(r1.body) : []),
    ];

    if (!Array.isArray(raw) || !raw.length)
      return res.status(502).json({ error: `FMP error: ${r0.status} / body: ${r0.body.slice(0,200)}` });

    const result = raw.map(norm).filter(t => t.ticker && t.trade)
      .sort((a,b) => b.trade.localeCompare(a.trade));

    console.log(`Screener: ${result.length} trades`);
    toCache('screener', result, 15*60*1000);
    res.json(result);
  } catch(e) {
    console.error('Screener:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// TICKER
app.get('/api/ticker', async (req, res) => {
  const symbol = (req.query.symbol||'').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  try {
    const cached = fromCache('ticker:'+symbol);
    if (cached) return res.json(cached);

    const pages = await Promise.all([0,1,2,3,4].map(p =>
      get(`https://financialmodelingprep.com/stable/insider-trading/search?symbol=${symbol}&page=${p}&limit=100&apikey=${FMP}`)
        .catch(() => ({status:0, body:'[]'}))
    ));

    const raw = pages.flatMap(r => r.status===200 ? safeParse(r.body) : []);
    const seen = new Set();
    const result = raw.map(norm).filter(t => t.trade)
      .sort((a,b) => b.trade.localeCompare(a.trade))
      .filter(t => { const k=`${t.insider}|${t.trade}|${t.type}|${t.qty}`; if(seen.has(k))return false; seen.add(k); return true; });

    console.log(`Ticker ${symbol}: ${result.length} trades`);
    toCache('ticker:'+symbol, result, 30*60*1000);
    res.json(result);
  } catch(e) {
    console.error(`Ticker ${symbol}:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// INSIDER
app.get('/api/insider', async (req, res) => {
  const name = (req.query.name||'').trim();
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const cached = fromCache('insider:'+name.toLowerCase());
    if (cached) return res.json(cached);

    const pages = await Promise.all([0,1,2].map(p =>
      get(`https://financialmodelingprep.com/stable/insider-trading/reporting-name?name=${encodeURIComponent(name)}&page=${p}&limit=100&apikey=${FMP}`)
        .catch(() => ({status:0, body:'[]'}))
    ));

    const raw = pages.flatMap(r => r.status===200 ? safeParse(r.body) : []);
    const result = raw.map(norm).filter(t => t.trade).sort((a,b) => b.trade.localeCompare(a.trade));

    console.log(`Insider "${name}": ${result.length} trades`);
    toCache('insider:'+name.toLowerCase(), result, 60*60*1000);
    res.json(result);
  } catch(e) {
    console.error(`Insider "${name}":`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// PRICE
app.get('/api/price', async (req, res) => {
  const symbol = (req.query.symbol||'').toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  try {
    const cached = fromCache('price:'+symbol);
    if (cached) return res.json(cached);

    const { status, body } = await get(
      `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${symbol}&apikey=${FMP}`
    );
    if (status !== 200) return res.status(502).json({ error: `FMP price HTTP ${status}`, body: body.slice(0,200) });

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

    console.log(`Price ${symbol}: ${bars.length} bars`);
    toCache('price:'+symbol, bars, 60*60*1000);
    res.json(bars);
  } catch(e) {
    console.error(`Price ${symbol}:`, e.message);
    res.status(500).json({ error: e.message });
  }
});

// DIAG
app.get('/api/diag', async (req, res) => {
  const out = {};
  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=3&apikey=${FMP}`);
    const d = safeParse(body);
    out.screener = { ok: status===200 && Array.isArray(d) && d.length>0, status, count: d.length, sample: d[0] };
  } catch(e) { out.screener = { ok:false, error:e.message }; }

  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/insider-trading/search?symbol=AAPL&page=0&limit=3&apikey=${FMP}`);
    const d = safeParse(body);
    out.ticker = { ok: status===200 && Array.isArray(d) && d.length>0, status, count: d.length, sample: d[0] };
  } catch(e) { out.ticker = { ok:false, error:e.message }; }

  try {
    const { status, body } = await get(`https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey=${FMP}`);
    const data = JSON.parse(body);
    const raw  = Array.isArray(data) ? data : (data?.historical || []);
    out.price = { ok: status===200 && raw.length>0, status, bars: raw.length, latest: raw[0] };
  } catch(e) { out.price = { ok:false, error:e.message }; }

  res.json(out);
});

app.get('/api/health', (req, res) => res.json({ ok:true, source:'FMP', time: new Date().toISOString() }));

app.listen(PORT, () => console.log(`InsiderTape on port ${PORT}`));
