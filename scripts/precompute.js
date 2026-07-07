'use strict';

// precompute.js — runs after daily-worker in GitHub Actions
// Pre-computes stock-lists and stores in computed_cache so Vercel
// can serve them instantly without heavy SQL queries on every request.

const https  = require('https');
const { createClient } = require('@libsql/client');

const TURSO_URL   = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;
if (!TURSO_URL) { console.error('TURSO_DATABASE_URL not set'); process.exit(1); }

const client = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN || undefined });

function log(msg) { process.stdout.write(`[${new Date().toISOString().slice(11, 19)}] ${msg}\n`); }

async function dbQuery(sql, args = []) {
  const r = await client.execute({ sql, args });
  return r.rows.map(row => Object.fromEntries(r.columns.map((c, i) => [c, row[i] ?? null])));
}
async function dbRun(sql, args = []) {
  return client.execute({ sql, args });
}

// Timing Alpha (0–100): blends 30d magnitude (diminishing past ~15%), 30d win rate,
// and 90d durability (penalizes reversals), shrinking small samples toward neutral.
// Must stay identical to computeTimingAlpha() in server.js.
function computeTimingAlpha(avgRet30, avgRet90, win30Rate, tradeCount) {
  if (avgRet30 === null || avgRet30 === undefined) return null;
  const mag  = avgRet30 >= 0 ? 38 * (1 - Math.exp(-avgRet30 / 10)) : Math.max(-30, avgRet30 * 1.2);
  const cons = (win30Rate !== null && win30Rate !== undefined) ? (win30Rate - 50) * 0.5 : 0;
  const dur  = Math.max(-22, Math.min(14, (avgRet90 || 0) * 0.35));
  let raw = 45 + mag * 0.7 + cons * 0.5 + dur;
  raw = 45 + (raw - 45) * Math.min(1, (tradeCount || 0) / 12);
  return Math.round(Math.max(0, Math.min(100, raw)));
}

async function computeStockLists() {
  log('Computing stock-lists...');
  const [mostActive, hotBuys, clusterBuys, freshBuys, heavySells] = await Promise.all([
    dbQuery(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS insiders,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sells,
               SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
               SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
               MAX(trade_date) AS latest_date
             FROM trades WHERE trade_date >= date('now','-14 days') AND trade_date <= date('now')
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
             GROUP BY ticker HAVING (buys >= 1 OR sells >= 1) AND (buy_val >= 1000 OR sell_val >= 1000)
             ORDER BY (buy_val + sell_val) DESC LIMIT 24`),
    dbQuery(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS buyers,
               COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buys,
               SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val
             FROM trades WHERE trade_date >= date('now','-30 days') AND trade_date <= date('now')
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL') AND TRIM(type)='P'
             GROUP BY ticker HAVING buyers >= 1 AND buy_val >= 50000
             ORDER BY buy_val DESC LIMIT 16`),
    dbQuery(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS buyer_count,
               COUNT(*) AS trade_count, SUM(COALESCE(value,0)) AS total_val, MAX(trade_date) AS latest
             FROM trades WHERE trade_date >= date('now','-14 days') AND trade_date <= date('now')
               AND TRIM(type)='P' AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
             GROUP BY ticker HAVING buyer_count >= 3
             ORDER BY buyer_count DESC, total_val DESC LIMIT 12`),
    dbQuery(`SELECT ticker, MAX(company) AS company, MAX(insider) AS insider,
               MAX(value) AS val, MAX(trade_date) AS date
             FROM trades WHERE filing_date >= date('now','-2 days') AND TRIM(type)='P'
               AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
               AND COALESCE(value,0) >= 25000
             GROUP BY ticker ORDER BY val DESC LIMIT 16`),
    dbQuery(`SELECT ticker, MAX(company) AS company,
               COUNT(DISTINCT insider) AS seller_count,
               SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val
             FROM trades WHERE trade_date >= date('now','-30 days') AND trade_date <= date('now')
               AND TRIM(type) IN ('S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
               AND COALESCE(company,'') NOT IN ('','N/A','NA','None','NULL')
             GROUP BY ticker HAVING seller_count >= 2 AND sell_val >= 500000
             ORDER BY sell_val DESC LIMIT 12`),
  ]);

  const payload = { hotBuys, clusterBuys, freshBuys, heavySells, mostActive };
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('stock-lists', ?, ?)`,
    [JSON.stringify(payload), Date.now()]
  );
  log(`stock-lists cached: ${mostActive.length} mostActive, ${hotBuys.length} hotBuys, ${clusterBuys.length} clusters`);
}

async function ensureComputedCacheTable() {
  await client.execute(`CREATE TABLE IF NOT EXISTS computed_cache (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    computed_at INTEGER NOT NULL
  )`);
}

async function computeFirstBuys() {
  log('Computing first-buys...');
  // Match the Radar tile's request (365-day gap, 90-day lookback)
  const lookbackDays = 90, minGapDays = 365, limit = 100;
  const rows = await dbQuery(`
    WITH recent_buys AS (
      SELECT DISTINCT insider, ticker FROM trades
      WHERE TRIM(type)='P' AND trade_date >= date('now','-${lookbackDays} days') AND trade_date <= date('now')
        AND insider IS NOT NULL AND ticker IS NOT NULL
    ),
    latest AS (
      SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title,
             MAX(t.trade_date) AS latest_trade, MAX(t.filing_date) AS latest_filing,
             MAX(t.price) AS latest_price, MAX(t.qty) AS latest_qty,
             MAX(t.value) AS latest_value, MAX(t.owned) AS latest_owned
      FROM trades t JOIN recent_buys rb ON t.insider=rb.insider AND t.ticker=rb.ticker
      WHERE TRIM(t.type)='P' AND t.trade_date >= date('now','-${lookbackDays} days') AND t.trade_date <= date('now')
      GROUP BY t.insider, t.ticker
    ),
    prev AS (
      SELECT t.insider, t.ticker, MAX(t.trade_date) AS prev_trade, MAX(t.owned) AS prev_owned
      FROM trades t JOIN recent_buys rb ON t.insider=rb.insider AND t.ticker=rb.ticker
      WHERE TRIM(t.type)='P' AND t.trade_date < date('now','-${lookbackDays} days')
      GROUP BY t.insider, t.ticker
    )
    SELECT l.ticker, l.company, l.insider, l.title,
           l.latest_trade, l.latest_filing, l.latest_price, l.latest_qty, l.latest_value, l.latest_owned,
           p.prev_trade, p.prev_owned,
           CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
    FROM latest l JOIN prev p ON l.insider=p.insider AND l.ticker=p.ticker
    WHERE CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= ${minGapDays}
    ORDER BY gap_days DESC LIMIT ${limit}
  `);
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('firstbuys', ?, ?)`,
    [JSON.stringify(rows), Date.now()]
  );
  log(`first-buys cached: ${rows.length} results`);
}

async function computeProximity() {
  log('Computing proximity...');
  const today = new Date().toISOString().slice(0, 10);
  const since = new Date(Date.now() - 180 * 86400000).toISOString().slice(0, 10);

  const rows = await dbQuery(`
    SELECT t.ticker, t.company, t.insider, t.title,
           t.trade_date AS buyDate, t.value AS buyVal, t.filing_date
    FROM trades t
    WHERE TRIM(t.type)='P'
      AND t.trade_date >= '${since}' AND t.trade_date <= '${today}'
      AND t.ticker GLOB '[A-Z]*' AND LENGTH(t.ticker) BETWEEN 1 AND 6
      AND t.value > 0
    ORDER BY t.trade_date DESC LIMIT 400
  `);

  // Simple proximity: estimate earnings ~45 days after quarter end
  function estimateNextEarnings(buyDate) {
    const d = new Date(buyDate + 'T12:00:00Z');
    const yr = d.getUTCFullYear();
    const qEnds = [
      new Date(Date.UTC(yr, 2, 31)), new Date(Date.UTC(yr, 5, 30)),
      new Date(Date.UTC(yr, 8, 30)), new Date(Date.UTC(yr, 11, 31)),
      new Date(Date.UTC(yr+1, 2, 31)),
    ];
    const nextQEnd = qEnds.find(e => e > d);
    if (!nextQEnd) return null;
    const est = new Date(nextQEnd);
    est.setUTCDate(est.getUTCDate() + 45);
    return est.toISOString().slice(0, 10);
  }

  const results = [], seen = new Set();
  for (const row of rows) {
    const key = `${row.ticker}|${row.insider}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const estDate = estimateNextEarnings(row.buyDate);
    if (!estDate) continue;
    const daysTo = Math.round((new Date(estDate + 'T12:00:00Z') - new Date()) / 86400000);
    if (daysTo < 0 || daysTo > 180) continue;
    const isCsuite = /\b(CEO|CFO|COO|CTO|President|Chairman)\b/i.test(row.title || '');
    let score = daysTo <= 7 ? 40 : daysTo <= 14 ? 28 : daysTo <= 30 ? 18 : 10;
    score += 10; // quarterly event
    if (isCsuite) score += 8;
    if ((row.buyVal || 0) >= 5000000) score += 12;
    else if ((row.buyVal || 0) >= 1000000) score += 8;
    else if ((row.buyVal || 0) >= 500000) score += 5;
    score = Math.min(100, score);
    results.push({
      ticker: row.ticker, company: row.company || row.ticker,
      insider: row.insider || '—', title: row.title || '—',
      buyDate: row.buyDate, buyVal: row.buyVal || 0, buyValue: row.buyVal || 0,
      nextEvent: { date: estDate, type: 'QUARTERLY', label: 'Est. Earnings', predicted: true, confirmed: false, daysToFromToday: daysTo },
      daysTo, score, isAbnormal: false, repeatPattern: false,
    });
  }
  results.sort((a, b) => b.score - a.score);
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('proximity', ?, ?)`,
    [JSON.stringify(results), Date.now()]
  );
  log(`proximity cached: ${results.length} results`);
}

async function computeMonitorSentiment() {
  log('Computing monitor-sentiment...');
  const now = new Date(), etOff = -5;
  const etNow = new Date(now.getTime() + etOff * 3600000);
  const dow = etNow.getUTCDay();
  const lastTrade = new Date(etNow);
  if (dow === 0) lastTrade.setUTCDate(etNow.getUTCDate() - 2);
  else if (dow === 6) lastTrade.setUTCDate(etNow.getUTCDate() - 1);
  const todayStr = lastTrade.toISOString().slice(0, 10);
  const tradeDow = lastTrade.getUTCDay();
  const weekStart = new Date(lastTrade);
  weekStart.setUTCDate(lastTrade.getUTCDate() - (tradeDow === 0 ? 6 : tradeDow - 1));
  const weekStr    = weekStart.toISOString().slice(0, 10);
  const monthStart = new Date(lastTrade); monthStart.setUTCDate(lastTrade.getUTCDate() - 30);
  const monthStr   = monthStart.toISOString().slice(0, 10);
  const qStartMonth = Math.floor(lastTrade.getUTCMonth() / 3) * 3;
  const quarterStr = `${lastTrade.getUTCFullYear()}-${String(qStartMonth + 1).padStart(2, '0')}-01`;

  async function windowStats(cutStr, endStr) {
    const rows = await dbQuery(`
      SELECT COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
             COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count,
             COALESCE(SUM(CASE WHEN TRIM(type)='P' THEN value ELSE 0 END), 0) AS buy_val,
             COALESCE(SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN value ELSE 0 END), 0) AS sell_val,
             COUNT(DISTINCT CASE WHEN TRIM(type)='P' THEN insider END) AS unique_buyers,
             COUNT(DISTINCT CASE WHEN TRIM(type) IN ('S','S-') THEN insider END) AS unique_sellers
      FROM trades WHERE trade_date >= ? AND trade_date <= ?
        AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND COALESCE(value,0) > 0
    `, [cutStr, endStr]);
    return rows[0] || {};
  }

  const result = {
    today:   { cutStr: todayStr,   ...(await windowStats(todayStr,   todayStr))   },
    week:    { cutStr: weekStr,    ...(await windowStats(weekStr,    todayStr))   },
    month:   { cutStr: monthStr,   ...(await windowStats(monthStr,   todayStr))   },
    quarter: { cutStr: quarterStr, ...(await windowStats(quarterStr, todayStr))   },
  };
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('monitor-sentiment', ?, ?)`,
    [JSON.stringify(result), Date.now()]
  );
  log('monitor-sentiment cached');
}

async function computeScreener90() {
  log('Computing screener-90d...');
  const rows = await dbQuery(`
    SELECT ticker, MAX(company) AS company, insider, MAX(title) AS title,
           trade_date AS trade, MAX(filing_date) AS filing,
           TRIM(type) AS type, MAX(qty) AS qty, MAX(price) AS price,
           MAX(value) AS value, MAX(owned) AS owned, MAX(accession) AS accession
    FROM trades
    WHERE trade_date >= date('now','-90 days') AND trade_date <= date('now')
      AND TRIM(type) IN ('P','S','S-')
      AND ticker NOT IN ('N/A','NA','NONE','NULL','--','-','.')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 10
    GROUP BY ticker, insider, trade_date, type
    ORDER BY trade_date DESC LIMIT 20000
  `);
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('screener-90d', ?, ?)`,
    [JSON.stringify(rows), Date.now()]
  );
  log(`screener-90d cached: ${rows.length} rows`);
}

async function computeFirstBuysMonitor() {
  log('Computing firstbuys-monitor...');
  const lookbackDays = 92, minGapDays = 730, limit = 100;
  const rows = await dbQuery(`
    WITH recent_buys AS (
      SELECT DISTINCT insider, ticker FROM trades
      WHERE TRIM(type)='P' AND trade_date >= date('now','-${lookbackDays} days') AND trade_date <= date('now')
        AND insider IS NOT NULL AND ticker IS NOT NULL
    ),
    latest AS (
      SELECT t.ticker, MAX(t.company) AS company, t.insider, MAX(t.title) AS title,
             MAX(t.trade_date) AS latest_trade, MAX(t.filing_date) AS latest_filing,
             MAX(t.price) AS latest_price, MAX(t.qty) AS latest_qty,
             MAX(t.value) AS latest_value, MAX(t.owned) AS latest_owned
      FROM trades t JOIN recent_buys rb ON t.insider=rb.insider AND t.ticker=rb.ticker
      WHERE TRIM(t.type)='P' AND t.trade_date >= date('now','-${lookbackDays} days') AND t.trade_date <= date('now')
      GROUP BY t.insider, t.ticker
    ),
    prev AS (
      SELECT t.insider, t.ticker, MAX(t.trade_date) AS prev_trade, MAX(t.owned) AS prev_owned
      FROM trades t JOIN recent_buys rb ON t.insider=rb.insider AND t.ticker=rb.ticker
      WHERE TRIM(t.type)='P' AND t.trade_date < date('now','-${lookbackDays} days')
      GROUP BY t.insider, t.ticker
    )
    SELECT l.ticker, l.company, l.insider, l.title,
           l.latest_trade, l.latest_filing, l.latest_price, l.latest_qty, l.latest_value, l.latest_owned,
           p.prev_trade, p.prev_owned,
           CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) AS gap_days
    FROM latest l JOIN prev p ON l.insider=p.insider AND l.ticker=p.ticker
    WHERE CAST(julianday(l.latest_trade) - julianday(p.prev_trade) AS INTEGER) >= ${minGapDays}
    ORDER BY gap_days DESC LIMIT ${limit}
  `);
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('firstbuys-monitor', ?, ?)`,
    [JSON.stringify(rows), Date.now()]
  );
  log(`firstbuys-monitor cached: ${rows.length} results`);
}

// Remove DRIP / director-plan clusters: 3+ distinct insiders buying the SAME
// ticker on the SAME day at the EXACT same price, each a small buy (<$5,000).
// These are coded 'P' with no footnote, so text-based filters can't catch them,
// but the identical-price + same-day + multi-insider signature is a routine plan,
// not open-market conviction. Real cluster buys have varied fill prices.
async function cleanupPlanClusters() {
  log('Cleaning up DRIP/director-plan clusters...');
  const removed = await dbRun(`
    DELETE FROM trades
    WHERE id IN (
      SELECT t.id FROM trades t
      WHERE TRIM(t.type) = 'P' AND t.price > 0 AND COALESCE(t.value,0) < 5000
        AND EXISTS (
          SELECT 1 FROM trades t2
          WHERE t2.ticker = t.ticker AND t2.trade_date = t.trade_date AND t2.price = t.price
            AND TRIM(t2.type) = 'P' AND COALESCE(t2.value,0) < 5000
          GROUP BY t2.ticker, t2.trade_date, t2.price
          HAVING COUNT(DISTINCT t2.insider) >= 3
        )
    )
  `);
  log(`Plan-cluster cleanup: removed ${removed.rowsAffected} trades`);
}

// Pre-compute the Insider Sentiment index (heavy 120-month aggregation + S&P 500)
async function computeInsiderSentiment() {
  log('Computing insider-sentiment...');
  const months = 120;
  const rows = await dbQuery(`
    SELECT strftime('%Y-%m', trade_date) AS month,
           strftime('%Y-%m', trade_date) || '-01' AS month_date,
           SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
           SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
           COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
           COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count
    FROM trades
    WHERE trade_date >= date('now', '-' || ? || ' months') AND trade_date <= date('now')
      AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND COALESCE(value, 0) >= 10000
    GROUP BY month HAVING buy_val + sell_val > 0 ORDER BY month ASC
  `, [months]);

  const monthly = rows.map(r => ({
    date: r.month_date, buyPct: r.buy_val / (r.buy_val + r.sell_val),
    buyVal: r.buy_val, sellVal: r.sell_val, buyCount: r.buy_count, sellCount: r.sell_count,
  }));
  const smoothed = monthly.map((m, i) => {
    const sl = monthly.slice(Math.max(0, i - 2), i + 1);
    return { ...m, smoothedBuyPct: sl.reduce((s, x) => s + x.buyPct, 0) / sl.length };
  });
  const vals = smoothed.map(m => m.smoothedBuyPct).sort((a, b) => a - b);
  const p = n => vals[Math.floor(vals.length * n)] || 0;
  const thresholds = { p10: p(0.10), p25: p(0.25), median: p(0.50), p75: p(0.75), p90: p(0.90) };

  const endTs = Math.floor(Date.now() / 1000), startTs = endTs - months * 31 * 86400;
  let spxData = [];
  try {
    const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1mo&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    if (resp.ok) {
      const d = await resp.json();
      const r = d?.chart?.result?.[0];
      if (r?.timestamp) {
        const q = r.indicators.quote[0];
        spxData = r.timestamp.map((t, i) => ({ date: new Date(t * 1000).toISOString().slice(0, 7) + '-01', close: q.close?.[i] || null })).filter(x => x.close);
      }
    }
  } catch(_) {}

  const result = { insider: smoothed, spx: spxData, thresholds };
  await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-sentiment', ?, ?)`, [JSON.stringify(result), Date.now()]);
  log(`insider-sentiment cached: ${smoothed.length} months, ${spxData.length} S&P points`);
}

// Pre-warm the price cache for the most-active tickers so their charts load instantly
async function prewarmPrices() {
  log('Pre-warming price cache...');
  await client.execute(`CREATE TABLE IF NOT EXISTS price_cache (symbol TEXT PRIMARY KEY, bars_json TEXT NOT NULL, fetched_at INTEGER NOT NULL)`);
  const rows = await dbQuery(`
    SELECT ticker FROM trades
    WHERE TRIM(type) IN ('P','S','S-') AND trade_date >= date('now','-365 days')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
    GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 150
  `);
  const endTs = Math.floor(Date.now() / 1000), startTs = endTs - 1830 * 86400; // ~5 years
  let warmed = 0;
  for (const { ticker } of rows) {
    try {
      const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!resp.ok) continue;
      const d = await resp.json();
      const r = d?.chart?.result?.[0];
      if (!r?.timestamp) continue;
      const q = r.indicators.quote[0];
      const bars = r.timestamp.map((t, i) => ({ time: new Date(t * 1000).toISOString().slice(0, 10), open: q.open?.[i] || 0, high: q.high?.[i] || 0, low: q.low?.[i] || 0, close: q.close?.[i] || 0, volume: q.volume?.[i] || 0 })).filter(b => b.close > 0);
      if (bars.length < 2) continue;
      await dbRun(`INSERT OR REPLACE INTO price_cache (symbol, bars_json, fetched_at) VALUES (?,?,?)`, [ticker, JSON.stringify(bars), Date.now()]);
      warmed++;
    } catch(_) {}
    await new Promise(r => setTimeout(r, 120)); // gentle on Yahoo
  }
  log(`Price cache pre-warmed: ${warmed}/${rows.length} tickers`);
}

// Keep the table to ~5 years so scans stay small and Turso reads stay low.
async function prune5yr() {
  const r = await dbRun("DELETE FROM trades WHERE trade_date < date('now','-1830 days')");
  if (r.rowsAffected) log(`Pruned ${r.rowsAffected} trades older than 5 years`);
}

// Pre-score the insider leaderboard (Top Insider Scores + Best Timing Insiders).
// This replaces the client's slow per-insider scoring loop (which was timing out).
async function computeInsiderLeaderboard() {
  log('Computing insider leaderboard...');
  const candidates = await dbQuery(`
    SELECT insider AS name, MAX(title) AS title, COUNT(*) AS total_buys
    FROM trades WHERE insider IS NOT NULL AND TRIM(type)='P' AND price > 0
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND insider IN (
        SELECT DISTINCT insider FROM trades
        WHERE TRIM(type)='P' AND price > 0 AND COALESCE(value,0) >= 10000
          AND trade_date >= date('now','-90 days')
      )
    GROUP BY insider HAVING total_buys >= 4 ORDER BY MAX(value) DESC LIMIT 80
  `);
  if (!candidates.length) {
    await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-leaderboard', ?, ?)`, [JSON.stringify({ accuracy: [], timing: [] }), Date.now()]);
    log('insider-leaderboard: no candidates'); return;
  }

  const endTs = Math.floor(Date.now() / 1000), startTs = endTs - 3 * 365 * 86400; // 3yr of bars for forward returns
  const priceCache = {};
  async function getBars(ticker) {
    if (priceCache[ticker] !== undefined) return priceCache[ticker];
    try {
      const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (resp.ok) {
        const d = await resp.json(); const r = d?.chart?.result?.[0];
        if (r?.timestamp) {
          const q = r.indicators.quote[0];
          priceCache[ticker] = r.timestamp.map((t, i) => ({ time: new Date(t * 1000).toISOString().slice(0, 10), close: q.close?.[i] || 0 })).filter(b => b.close > 0);
          await new Promise(rr => setTimeout(rr, 100));
          return priceCache[ticker];
        }
      }
    } catch(_) {}
    priceCache[ticker] = null;
    await new Promise(rr => setTimeout(rr, 100));
    return null;
  }

  const CAP = 100, cap = r => Math.max(-CAP, Math.min(CAP, r));
  const today = new Date().toISOString().slice(0, 10);
  const addDays = (ds, n) => { const d = new Date(ds + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };

  const accuracy = [], timing = [];
  for (const c of candidates) {
    const rows = await dbQuery(`
      SELECT ticker, trade_date AS trade, COALESCE(price,0) AS price
      FROM trades WHERE UPPER(insider) LIKE UPPER(?) AND TRIM(type)='P' AND price > 0
      ORDER BY trade_date DESC LIMIT 500`, [c.name]);
    if (rows.length < 4) continue;
    const tickers = [...new Set(rows.map(r => r.ticker))];
    for (const t of tickers) await getBars(t);

    const scored = rows.map(t => {
      const bars = priceCache[t.ticker];
      if (!bars || !bars.length) return null;
      const buyDate = t.trade.slice(0, 10);
      const buyPrice = t.price || bars.find(b => b.time >= buyDate)?.close || 0;
      if (!buyPrice) return null;
      if (today < addDays(buyDate, 90)) return null;
      const barMap = {}; bars.forEach(b => { barMap[b.time] = b.close; });
      const priceOn = ds => { for (let d = 0; d <= 5; d++) { const s = addDays(ds, d); if (barMap[s] != null) return barMap[s]; } return null; };
      const p30 = priceOn(addDays(buyDate, 30)), p90 = priceOn(addDays(buyDate, 90));
      return { ret30: p30 ? cap((p30 - buyPrice) / buyPrice * 100) : null, ret90: p90 ? cap((p90 - buyPrice) / buyPrice * 100) : null };
    }).filter(Boolean);

    const completed = scored.filter(s => s.ret90 !== null);
    if (completed.length < 4) continue;
    const rets90 = completed.map(s => s.ret90), rets30 = completed.filter(s => s.ret30 !== null).map(s => s.ret30);
    const winRate = Math.round(rets90.filter(r => r > 0).length / rets90.length * 100);
    const avgRet90 = +(rets90.reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
    const avgRet30 = rets30.length ? +(rets30.reduce((a, b) => a + b, 0) / rets30.length).toFixed(1) : null;
    const avgMag = +(rets90.map(Math.abs).reduce((a, b) => a + b, 0) / rets90.length).toFixed(1);
    const median = [...rets90].sort((a, b) => a - b)[Math.floor(rets90.length / 2)];
    const consist = Math.round(Math.min(100, Math.max(0, (median / Math.max(avgMag, 1) + 1) * 50)));
    const timingAvg30 = rets30.length ? rets30.reduce((a, b) => a + b, 0) / rets30.length : 0;
    const timingBonus = Math.round(Math.min(20, Math.max(0, (timingAvg30 + 8) / 16 * 20)));
    const baseScore = winRate * 0.40 + Math.min(35, Math.max(0, avgRet90 / 20 * 35)) + consist * 0.15 + Math.min(10, completed.length * 1.2);
    const accuracyScore = Math.round(Math.min(100, Math.max(0, baseScore * 0.80 + timingBonus)));
    const tier = accuracyScore >= 75 ? 'ELITE' : accuracyScore >= 55 ? 'STRONG' : accuracyScore >= 35 ? 'AVERAGE' : 'WEAK';
    const win30Rate = rets30.length ? Math.round(rets30.filter(r => r > 0).length / rets30.length * 100) : null;
    const timingAlpha = computeTimingAlpha(avgRet30, avgRet90, win30Rate, completed.length);
    const tickers3 = tickers.slice(0, 3).join(', ');

    if (accuracyScore >= 35) accuracy.push({ name: c.name, title: c.title || '', accuracyScore, tier, winRate, avgRet90, avgRet30, tradeCount: completed.length, tickers: tickers3 });
    if (avgRet30 !== null) {
      const verdict = avgRet30 >= 8 ? 'Buys trigger immediate upward moves'
                    : avgRet30 >= 3 ? 'Above-average short-term reaction'
                    : avgRet30 >= 0 ? 'Mixed short-term price reaction'
                    : 'Buys often followed by weakness';
      timing.push({ name: c.name, title: c.title || '', timingAlpha, avgRet30, avgRet90, win30Rate, verdict, tradeCount: completed.length, tickers: tickers3 });
    }
  }
  accuracy.sort((a, b) => b.accuracyScore - a.accuracyScore);
  timing.sort((a, b) => b.timingAlpha - a.timingAlpha);
  await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-leaderboard', ?, ?)`, [JSON.stringify({ accuracy, timing }), Date.now()]);
  log(`insider-leaderboard cached: ${accuracy.length} accuracy, ${timing.length} timing`);
}

async function main() {
  log('=== precompute start ===');
  await ensureComputedCacheTable();

  // The heavy full-history aggregates (5-year sentiment, deep first-buy scans) barely
  // change intraday and are the biggest Turso-read consumers, so run them only once
  // per day (the morning run). Everything else runs every ingestion.
  const heavyRun = process.env.FORCE_FULL === '1' || new Date().getUTCHours() <= 14;
  log(`Mode: ${heavyRun ? 'FULL (heavy aggregates included)' : 'LIGHT (recent-data only)'}`);

  // Cleanup + prune first so caches reflect filtered, in-range data
  await cleanupPlanClusters();
  await prune5yr();

  // Light, recent-data caches — every run
  await Promise.all([
    computeStockLists(),
    computeFirstBuysMonitor(),
    computeProximity(),
    computeMonitorSentiment(),
    computeScreener90(),
  ]);

  // Heavy full-history caches — once per day
  if (heavyRun) {
    await computeInsiderSentiment();
    await computeFirstBuys();
    await computeInsiderLeaderboard();
  }

  // Price pre-warm runs last (longest — many external Yahoo calls, no Turso reads)
  await prewarmPrices();
  log('=== precompute done ===');
}

main().catch(e => { log('FATAL: ' + e.message); process.exit(1); });
