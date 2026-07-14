'use strict';

// precompute.js - runs after daily-worker in GitHub Actions
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

// ── Yahoo earnings-calendar access ────────────────────────────────────────────
// The chart API used for prices is unauthenticated, but earnings dates live behind
// quoteSummary, which requires a crumb + session cookie. Resolve them once per run.
const YF_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';
let _yfAuth; // undefined = not tried, null = failed, {crumb,cookie} = ok
async function getYahooAuth() {
  if (_yfAuth !== undefined) return _yfAuth;
  try {
    const readCookies = (r) => (typeof r.headers.getSetCookie === 'function' ? r.headers.getSetCookie() : [])
      .map(c => c.split(';')[0]).join('; ');
    let cookie = readCookies(await fetch('https://fc.yahoo.com', { headers: { 'User-Agent': YF_UA } }));
    if (!cookie) cookie = readCookies(await fetch('https://finance.yahoo.com', { headers: { 'User-Agent': YF_UA } }));
    const r2 = await fetch('https://query2.finance.yahoo.com/v1/test/getcrumb', {
      headers: { 'User-Agent': YF_UA, 'Cookie': cookie },
    });
    const crumb = (await r2.text()).trim();
    if (crumb && crumb.length < 40 && !crumb.includes('<')) {
      _yfAuth = { crumb, cookie };
      log('yahoo auth ok (earnings crumb acquired)');
      return _yfAuth;
    }
    log('yahoo auth: unexpected crumb response');
  } catch (e) { log('yahoo auth failed: ' + e.message); }
  _yfAuth = null;
  return _yfAuth;
}

// Returns { next:'YYYY-MM-DD'|null, past:['YYYY-MM-DD',...] } or null on failure.
// `past` holds fiscal quarter-END dates (announcements land ~40 days later).
async function fetchEarningsFromYahoo(ticker) {
  const auth = await getYahooAuth();
  if (!auth) return null;
  try {
    const url = `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(ticker)}`
      + `?modules=calendarEvents,earningsHistory&crumb=${encodeURIComponent(auth.crumb)}`;
    const resp = await fetch(url, { headers: { 'User-Agent': YF_UA, 'Cookie': auth.cookie } });
    if (!resp.ok) return null;
    const d = await resp.json();
    const res = d?.quoteSummary?.result?.[0];
    if (!res) return null;
    const todayStr = new Date().toISOString().slice(0, 10);
    const dates = (res.calendarEvents?.earnings?.earningsDate || [])
      .map(e => e?.raw).filter(Boolean)
      .map(ts => new Date(ts * 1000).toISOString().slice(0, 10)).sort();
    const next = dates.find(d => d >= todayStr) || dates[dates.length - 1] || null;
    const past = (res.earningsHistory?.history || []).map(h => h.quarter?.fmt).filter(Boolean);
    return { next, past };
  } catch (e) { return null; }
}

async function ensureEarningsCacheTable() {
  await dbRun(`CREATE TABLE IF NOT EXISTS earnings_cache (
    ticker TEXT PRIMARY KEY, next_date TEXT, past_json TEXT, fetched_at INTEGER
  )`);
}

// Add ~40 days to a fiscal quarter-end so it approximates the announcement date.
function quarterEndToAnnounce(qEndStr) {
  const d = new Date(qEndStr + 'T12:00:00Z');
  d.setUTCDate(d.getUTCDate() + 40);
  return d.toISOString().slice(0, 10);
}

async function computeProximity() {
  log('Computing proximity...');
  await ensureEarningsCacheTable();
  const now = Date.now();
  const todayStr = new Date().toISOString().slice(0, 10);
  const since = new Date(now - 180 * 86400000).toISOString().slice(0, 10);

  const rows = await dbQuery(`
    SELECT t.ticker, t.company, t.insider, t.title,
           t.trade_date AS buyDate, t.value AS buyVal
    FROM trades t
    WHERE TRIM(t.type)='P'
      AND t.trade_date >= '${since}' AND t.trade_date <= '${todayStr}'
      AND t.ticker GLOB '[A-Z]*' AND LENGTH(t.ticker) BETWEEN 1 AND 6
      AND t.value > 0
    ORDER BY t.trade_date DESC LIMIT 400
  `);

  // Most recent buy per insider+ticker → candidate signals
  const candidates = [], seen = new Set();
  for (const row of rows) {
    const key = `${row.ticker}|${row.insider}`;
    if (seen.has(key)) continue;
    seen.add(key);
    candidates.push(row);
  }
  const tickers = [...new Set(candidates.map(c => c.ticker))];

  // Full buy history for these tickers (for repeat-pattern detection)
  const buyHistory = {}; // 'ticker|insider' -> [buyDate,...]
  if (tickers.length) {
    const ph = tickers.map(() => '?').join(',');
    const histRows = await dbQuery(
      `SELECT ticker, insider, trade_date AS buyDate FROM trades
       WHERE TRIM(type)='P' AND value > 0 AND ticker IN (${ph})`, tickers);
    for (const h of histRows) {
      const k = `${h.ticker}|${h.insider}`;
      (buyHistory[k] || (buyHistory[k] = [])).push(h.buyDate);
    }
  }

  // Earnings dates per ticker (Yahoo, cached 7 days). Only stale/new tickers hit Yahoo.
  const EARN_TTL = 7 * 86400000;
  const earningsByTicker = {};
  let fetched = 0;
  for (const tk of tickers) {
    let cached = (await dbQuery(
      'SELECT next_date, past_json, fetched_at FROM earnings_cache WHERE ticker = ?', [tk]))[0];
    if (!cached || (now - (cached.fetched_at || 0)) > EARN_TTL) {
      const e = await fetchEarningsFromYahoo(tk);
      if (e) {
        await dbRun('INSERT OR REPLACE INTO earnings_cache (ticker,next_date,past_json,fetched_at) VALUES (?,?,?,?)',
          [tk, e.next || null, JSON.stringify(e.past || []), now]);
        cached = { next_date: e.next, past_json: JSON.stringify(e.past || []) };
      } else if (!cached) {
        await dbRun('INSERT OR REPLACE INTO earnings_cache (ticker,next_date,past_json,fetched_at) VALUES (?,?,?,?)',
          [tk, null, '[]', now]);
        cached = { next_date: null, past_json: '[]' };
      }
      fetched++;
      await new Promise(r => setTimeout(r, 150)); // gentle on Yahoo
    }
    let past = []; try { past = JSON.parse(cached.past_json || '[]'); } catch (_) {}
    earningsByTicker[tk] = { next: cached.next_date || null, past };
  }
  log(`proximity: earnings for ${tickers.length} tickers (${fetched} fetched, rest cached)`);

  // Fallback: estimate next earnings ~45 days after the next quarter end
  function estimateNextEarnings(fromStr) {
    const d = new Date(fromStr + 'T12:00:00Z');
    const yr = d.getUTCFullYear();
    const qEnds = [Date.UTC(yr,2,31), Date.UTC(yr,5,30), Date.UTC(yr,8,30), Date.UTC(yr,11,31), Date.UTC(yr+1,2,31)]
      .map(ms => new Date(ms));
    const nextQEnd = qEnds.find(e => e > d);
    if (!nextQEnd) return null;
    const est = new Date(nextQEnd); est.setUTCDate(est.getUTCDate() + 45);
    return est.toISOString().slice(0, 10);
  }

  const results = [];
  for (const c of candidates) {
    const earn = earningsByTicker[c.ticker] || { next: null, past: [] };
    const confirmed = !!earn.next && earn.next >= todayStr;
    const nextDate = confirmed ? earn.next : estimateNextEarnings(todayStr);
    if (!nextDate) continue;
    const daysTo = Math.round((new Date(nextDate + 'T12:00:00Z') - now) / 86400000);
    if (daysTo < 0 || daysTo > 180) continue;

    const isCsuite   = /\b(CEO|CFO|COO|CTO|President|Chair(man)?)\b/i.test(c.title || '');
    const isDirector = /\bdirector\b/i.test(c.title || '');
    const insiderRole = isCsuite || isDirector;

    // Repeat pattern: 2+ prior buys by this insider landed shortly before an earnings
    // event. Uses real past quarters (shifted to announce dates) plus estimated windows.
    const priorBuys = (buyHistory[`${c.ticker}|${c.insider}`] || []).filter(d => d < c.buyDate);
    const knownEvents = earn.past.map(quarterEndToAnnounce);
    priorBuys.forEach(b => { const e = estimateNextEarnings(b); if (e) knownEvents.push(e); });
    let priorHits = 0;
    for (const b of priorBuys) {
      const bt = new Date(b + 'T12:00:00Z').getTime();
      if (knownEvents.some(ev => { const diff = (new Date(ev + 'T12:00:00Z').getTime() - bt) / 86400000; return diff >= 0 && diff <= 30; }))
        priorHits++;
    }
    const repeatPattern = priorHits >= 2;

    // Abnormal: near a CONFIRMED earnings date - 21d for anyone, 45d for an insider role.
    const isAbnormal = confirmed && (daysTo <= 21 || (insiderRole && daysTo <= 45));

    let score = daysTo <= 7 ? 40 : daysTo <= 14 ? 30 : daysTo <= 21 ? 22 : daysTo <= 45 ? 14 : 8;
    if (confirmed) score += 10;
    if (isCsuite)  score += 8;
    if ((c.buyVal||0) >= 5000000) score += 12;
    else if ((c.buyVal||0) >= 1000000) score += 8;
    else if ((c.buyVal||0) >= 500000) score += 5;
    if (repeatPattern) score += 12;
    if (isAbnormal)    score += 8;
    score = Math.min(100, score);

    results.push({
      ticker: c.ticker, company: c.company || c.ticker,
      insider: c.insider || '-', title: c.title || '-',
      buyDate: c.buyDate, buyVal: c.buyVal || 0, buyValue: c.buyVal || 0,
      nextEvent: { date: nextDate, type: 'EARNINGS', label: 'Earnings', predicted: !confirmed, confirmed, daysToFromToday: daysTo },
      daysTo, score, isAbnormal, repeatPattern,
    });
  }
  results.sort((a, b) => b.score - a.score);
  await dbRun(
    `INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('proximity', ?, ?)`,
    [JSON.stringify(results), Date.now()]
  );
  log(`proximity cached: ${results.length} results (${results.filter(r=>r.nextEvent.confirmed).length} confirmed, ${results.filter(r=>r.isAbnormal).length} abnormal, ${results.filter(r=>r.repeatPattern).length} repeat)`);
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

// Remove "purchases" filed at a price far below the stock's actual trading range
// on that date - warrant/option exercises and subscription/placement events that
// come through as open-market "P" buys (e.g. an insider "buying" $BORR at $1.66
// while it traded $4.25-$4.44 that day). These are impossible on the open market
// and pollute every buy-based signal. We can only verify tickers that have cached
// price bars; others are left untouched. Scoped to the last 180 days to cover the
// signal windows and avoid split-adjustment false positives on old trades.
async function cleanupNonOpenMarket() {
  log('Cleaning non-open-market buys (price far below trading range)...');
  const rows = await dbQuery(`
    SELECT id, ticker, trade_date, price FROM trades
    WHERE TRIM(type)='P' AND COALESCE(price,0) > 0
      AND trade_date >= date('now','-180 days')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
  `);
  const byTicker = {};
  rows.forEach(r => { (byTicker[r.ticker] || (byTicker[r.ticker] = [])).push(r); });
  const tickers = Object.keys(byTicker);

  // Batch-load the cached price bars for these tickers
  const lowByTicker = {}; // ticker -> { 'YYYY-MM-DD': low }
  for (let i = 0; i < tickers.length; i += 50) {
    const chunk = tickers.slice(i, i + 50);
    const cacheRows = await dbQuery(
      `SELECT symbol, bars_json FROM price_cache WHERE symbol IN (${chunk.map(() => '?').join(',')})`, chunk);
    for (const cr of cacheRows) {
      try {
        const map = {};
        for (const b of JSON.parse(cr.bars_json)) map[b.time] = b.low;
        lowByTicker[cr.symbol] = map;
      } catch (_) {}
    }
  }

  const shift = (d, n) => { const x = new Date(d + 'T12:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
  const toDelete = [];
  for (const [ticker, trs] of Object.entries(byTicker)) {
    const lowMap = lowByTicker[ticker];
    if (!lowMap) continue; // no cached bars - can't verify, leave it
    for (const t of trs) {
      const d = (t.trade_date || '').slice(0, 10);
      let low = lowMap[d];
      // Exact date may be a weekend/holiday/gap - check the nearest few days
      for (let i = 1; i <= 3 && low == null; i++) low = lowMap[shift(d, -i)] ?? lowMap[shift(d, i)];
      if (low != null && low > 0 && t.price < low * 0.7) toDelete.push(t.id);
    }
  }

  let removed = 0;
  for (let i = 0; i < toDelete.length; i += 200) {
    const batch = toDelete.slice(i, i + 200);
    const res = await dbRun(`DELETE FROM trades WHERE id IN (${batch.map(() => '?').join(',')})`, batch);
    removed += res.rowsAffected || batch.length;
  }
  log(`Non-open-market cleanup: checked ${rows.length} buys across ${tickers.length} tickers, removed ${removed}`);
}

// Pre-compute the Insider Sentiment index (heavy 120-month aggregation + S&P 500)
async function computeInsiderSentiment() {
  // Historical monthly sentiment is frozen - only the current month (and a buffer
  // for late-filed Form 4s) changes. So we keep the cached history and re-aggregate
  // only the last ~95 days, then merge. Full 120-month scan runs once, to bootstrap.
  let prev = null;
  try {
    const row = (await dbQuery("SELECT value_json FROM computed_cache WHERE key = 'insider-sentiment'"))[0];
    if (row) prev = JSON.parse(row.value_json);
  } catch (_) {}
  const haveHistory = prev && Array.isArray(prev.insider) && prev.insider.length > 6;
  log(`Computing insider-sentiment (${haveHistory ? 'incremental ~95d scan' : 'bootstrap full scan'})...`);

  const scanClause = haveHistory ? `trade_date >= date('now','-95 days')` : `trade_date >= date('now','-120 months')`;
  const rows = await dbQuery(`
    SELECT strftime('%Y-%m', trade_date) AS month,
           strftime('%Y-%m', trade_date) || '-01' AS month_date,
           SUM(CASE WHEN TRIM(type)='P' THEN COALESCE(value,0) ELSE 0 END) AS buy_val,
           SUM(CASE WHEN TRIM(type) IN ('S','S-') THEN COALESCE(value,0) ELSE 0 END) AS sell_val,
           COUNT(CASE WHEN TRIM(type)='P' THEN 1 END) AS buy_count,
           COUNT(CASE WHEN TRIM(type) IN ('S','S-') THEN 1 END) AS sell_count
    FROM trades
    WHERE ${scanClause} AND trade_date <= date('now')
      AND TRIM(type) IN ('P','S','S-') AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND COALESCE(value, 0) >= 10000
    GROUP BY month HAVING buy_val + sell_val > 0 ORDER BY month ASC
  `);
  const fresh = rows.map(r => ({
    date: r.month_date, buyPct: r.buy_val / (r.buy_val + r.sell_val),
    buyVal: r.buy_val, sellVal: r.sell_val, buyCount: r.buy_count, sellCount: r.sell_count,
  }));

  // Merge fresh recent months over the cached history (fresh wins for overlaps),
  // then keep the most recent 120 months.
  const byDate = {};
  if (haveHistory) for (const m of prev.insider) byDate[m.date] = { date: m.date, buyPct: m.buyPct, buyVal: m.buyVal, sellVal: m.sellVal, buyCount: m.buyCount, sellCount: m.sellCount };
  for (const m of fresh) byDate[m.date] = m;
  const monthly = Object.values(byDate).sort((a, b) => (a.date < b.date ? -1 : 1)).slice(-120);

  // Smoothing (3-month trailing) + percentile thresholds, all in memory (no DB reads)
  const smoothed = monthly.map((m, i) => {
    const sl = monthly.slice(Math.max(0, i - 2), i + 1);
    return { ...m, smoothedBuyPct: sl.reduce((s, x) => s + x.buyPct, 0) / sl.length };
  });
  const vals = smoothed.map(m => m.smoothedBuyPct).sort((a, b) => a - b);
  const p = n => vals[Math.floor(vals.length * n)] || 0;
  const thresholds = { p10: p(0.10), p25: p(0.25), median: p(0.50), p75: p(0.75), p90: p(0.90) };

  // S&P: reuse cached history, refresh only the recent months (small Yahoo call).
  let spxData = (haveHistory && Array.isArray(prev.spx)) ? prev.spx.slice() : [];
  try {
    const endTs = Math.floor(Date.now() / 1000);
    const startTs = haveHistory ? endTs - 120 * 86400 : endTs - 120 * 31 * 86400;
    const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1mo&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    if (resp.ok) {
      const d = await resp.json();
      const r = d?.chart?.result?.[0];
      if (r?.timestamp) {
        const q = r.indicators.quote[0];
        const freshSpx = r.timestamp.map((t, i) => ({ date: new Date(t * 1000).toISOString().slice(0, 7) + '-01', close: q.close?.[i] || null })).filter(x => x.close);
        const spxByDate = {};
        for (const s of spxData) spxByDate[s.date] = s;
        for (const s of freshSpx) spxByDate[s.date] = s;
        spxData = Object.values(spxByDate).sort((a, b) => (a.date < b.date ? -1 : 1)).slice(-120);
      }
    }
  } catch (_) {}

  const result = { insider: smoothed, spx: spxData, thresholds };
  await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-sentiment', ?, ?)`, [JSON.stringify(result), Date.now()]);
  log(`insider-sentiment cached: ${smoothed.length} months, ${spxData.length} S&P points`);
}

// One-time: clear the old 1-year price cache so everything refetches at 5 years.
// Guarded by a marker so it only runs once, not every cycle.
async function migratePriceCacheTo5yr() {
  await client.execute(`CREATE TABLE IF NOT EXISTS price_cache (symbol TEXT PRIMARY KEY, bars_json TEXT NOT NULL, fetched_at INTEGER NOT NULL)`);
  const marker = await dbQuery("SELECT 1 AS n FROM computed_cache WHERE key = 'price_5yr_migrated'");
  if (marker.length) return;
  await dbRun("DELETE FROM price_cache");
  await dbRun("INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('price_5yr_migrated', '1', ?)", [Date.now()]);
  log('Cleared price cache for one-time 5-year migration');
}

// Pre-warm the price cache for the most-active tickers so their charts load instantly
async function prewarmPrices() {
  log('Pre-warming price cache...');
  await client.execute(`CREATE TABLE IF NOT EXISTS price_cache (symbol TEXT PRIMARY KEY, bars_json TEXT NOT NULL, fetched_at INTEGER NOT NULL)`);
  // Cover (a) recent open-market buy tickers first - these power the Radar
  // price-context tiles (Buying at the Lows / Recent Winners) and are often small
  // caps outside the top-by-volume set - then (b) generally active tickers.
  const rows = await dbQuery(`
    SELECT ticker,
           MAX(CASE WHEN TRIM(type)='P' AND trade_date >= date('now','-35 days')
                     AND COALESCE(value,0) >= 50000 THEN 1 ELSE 0 END) AS recent_buy
    FROM trades
    WHERE ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6
      AND ( (TRIM(type)='P' AND trade_date >= date('now','-35 days') AND COALESCE(value,0) >= 50000)
            OR (TRIM(type) IN ('P','S','S-') AND trade_date >= date('now','-365 days')) )
    GROUP BY ticker
    ORDER BY recent_buy DESC, COUNT(*) DESC
    LIMIT 500
  `);
  // Skip tickers already cached in the last ~20h so the larger list stays cheap on
  // Yahoo (first run warms everything, later runs only refresh stale entries).
  const fresh = new Set((await dbQuery(
    `SELECT symbol FROM price_cache WHERE fetched_at >= ?`, [Date.now() - 20 * 3600000]
  )).map(r => r.symbol));
  const endTs = Math.floor(Date.now() / 1000), startTs = endTs - 1830 * 86400; // ~5 years
  let warmed = 0;
  for (const { ticker } of rows) {
    if (fresh.has(ticker)) continue;
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
  log(`Price cache pre-warmed: ${warmed} fetched, ${fresh.size} already fresh, ${rows.length} candidates`);
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
  // Candidates = top recent buyers (last 90 days) by their largest recent buy.
  // Restricting to a 90-day window uses idx_trades_ttype_date and stays small,
  // instead of grouping all 5 years of P trades with an IN subquery (which scanned
  // ~1M rows and stalled). The >=4 total-buys filter is applied per candidate below.
  const candidates = await dbQuery(`
    SELECT insider AS name, MAX(title) AS title, MAX(COALESCE(value,0)) AS mv
    FROM trades
    WHERE TRIM(type)='P' AND price > 0 AND COALESCE(value,0) >= 10000
      AND trade_date >= date('now','-90 days')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6 AND insider IS NOT NULL
    GROUP BY insider ORDER BY mv DESC LIMIT 120
  `);
  if (!candidates.length) {
    await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-leaderboard', ?, ?)`, [JSON.stringify({ accuracy: [], timing: [] }), Date.now()]);
    log('insider-leaderboard: no candidates'); return;
  }

  const CAP = 100, cap = r => Math.max(-CAP, Math.min(CAP, r));
  const today = new Date().toISOString().slice(0, 10);
  const addDays = (ds, n) => { const d = new Date(ds + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };

  // Pass 1: pull each candidate's buys (indexed exact match) and collect tickers.
  const perCand = [];
  const allTickers = new Set();
  for (const c of candidates) {
    // c.name is the exact stored insider string, so match the plain (indexed)
    // insider column directly - no UPPER(), guaranteed to use idx_insider.
    const rows = await dbQuery(`
      SELECT ticker, trade_date AS trade, COALESCE(price,0) AS price
      FROM trades WHERE insider = ? AND TRIM(type)='P' AND price > 0
      ORDER BY trade_date DESC LIMIT 500`, [c.name]);
    if (rows.length < 4) continue;
    perCand.push({ c, rows });
    rows.forEach(r => allTickers.add(r.ticker));
  }

  // Batch-load daily bars from the prewarmed price_cache table (one query per 100
  // tickers) instead of 300+ single-row round-trips - that was the bottleneck.
  // Only tickers with cached bars get scored.
  const priceCache = {};
  const tickerArr = [...allTickers];
  for (let i = 0; i < tickerArr.length; i += 100) {
    const chunk = tickerArr.slice(i, i + 100);
    const cacheRows = await dbQuery(`SELECT symbol, bars_json FROM price_cache WHERE symbol IN (${chunk.map(() => '?').join(',')})`, chunk);
    for (const cr of cacheRows) {
      try { priceCache[cr.symbol] = JSON.parse(cr.bars_json).filter(b => b.close > 0).map(b => ({ time: b.time, close: b.close })); } catch(_) {}
    }
  }

  const accuracy = [], timing = [];
  for (const { c, rows } of perCand) {
    const tickers = [...new Set(rows.map(r => r.ticker))];
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

// Data study: measure how stocks performed AFTER open-market insider buys, using
// cached daily price bars. Aggregate forward returns at 1M/3M/6M/12M vs the S&P
// 500, plus cuts by insider role and buy size. Runs at most weekly (heavy scan).
// Public/evergreen analysis - no premium signals.
async function computeInsiderStudy() {
  // Recompute if the cached study is missing, an older structure/version, or >25
  // days old (this version fetches prices market-wide, so it runs ~monthly).
  const STUDY_VERSION = 4;
  try {
    const ex = (await dbQuery("SELECT computed_at, value_json FROM computed_cache WHERE key='insider-study'"))[0];
    if (ex && process.env.FORCE_FULL !== '1') {
      let fresh = false;
      try { const j = JSON.parse(ex.value_json); fresh = j.version === STUDY_VERSION && (Date.now() - ex.computed_at < 25 * 24 * 3600000); } catch(_) {}
      if (fresh) { log('insider-study fresh, skip'); return; }
    }
  } catch(_) {}
  log('Computing insider-study (market-wide signal sweep vs Russell 2000 + S&P 500)...');

  // Benchmarks: Russell 2000 (fairer for the small/mid-cap insider universe) AND
  // the S&P 500 - we report excess/beat vs both.
  const rutClose = {}, spxClose = {};
  const endTs = Math.floor(Date.now() / 1000), startTs = endTs - 1830 * 86400;
  async function loadBench(sym, into) {
    try {
      const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const d = await resp.json(); const r = d?.chart?.result?.[0];
      if (r?.timestamp) { const q = r.indicators.quote[0]; r.timestamp.forEach((t, i) => { const c = q.close?.[i]; if (c > 0) into[new Date(t * 1000).toISOString().slice(0, 10)] = c; }); }
    } catch(_) {}
  }
  await loadBench('%5ERUT', rutClose);
  await loadBench('%5EGSPC', spxClose);
  const shiftDate = (ymd, n) => { const x = new Date(ymd + 'T12:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
  const onOrBefore = (map, date) => { for (let i = 0; i <= 6; i++) { const dd = shiftDate(date, -i); if (map[dd] != null) return map[dd]; } return null; };

  // Preload whatever price bars we already have cached.
  const barsByTicker = {};
  try {
    const priceRows = await dbQuery('SELECT symbol, bars_json FROM price_cache');
    for (const pr of priceRows) {
      try { const arr = JSON.parse(pr.bars_json).filter(b => b.close > 0).map(b => ({ t: b.time, c: b.close })); arr.sort((a, b) => a.t < b.t ? -1 : 1); if (arr.length > 30) barsByTicker[pr.symbol] = arr; } catch(_) {}
    }
  } catch(_) {}

  // Every open-market buy over 5 years, market-wide, old enough for a forward
  // window. Title + value drive the scenario tags. (~1M rows read; runs ~monthly.)
  const buys = await dbQuery(`
    SELECT ticker, insider, trade_date AS d, MAX(title) AS title, MAX(COALESCE(value,0)) AS value
    FROM trades
    WHERE TRIM(type)='P' AND COALESCE(value,0) >= 10000
      AND trade_date <= date('now','-30 days') AND trade_date >= date('now','-1826 days')
      AND ticker GLOB '[A-Z]*' AND LENGTH(ticker) BETWEEN 1 AND 6 AND insider IS NOT NULL AND insider != ''
    GROUP BY ticker, insider, trade_date`);
  const roleOf = t => {
    const u = (t || '').toUpperCase();
    if ((u.includes('CEO') || u.includes('CHIEF EXECUTIVE')) && !u.includes('DEPUTY')) return 'ceo';
    if (u.includes('CFO') || u.includes('CHIEF FINANCIAL')) return 'cfo';
    if (u.includes('COO') || u.includes('CHIEF OPERATING')) return 'coo';
    if (u.includes('PRESIDENT') && !u.includes('VICE PRESIDENT')) return 'president';
    if (u.includes('DIRECTOR') && !u.includes('MANAGING DIRECTOR')) return 'director';
    if (u.includes('10%') || u.includes('TEN PERCENT')) return 'tenpct';
    return 'other';
  };
  const byTicker = {}, byInsider = {};
  for (const b of buys) {
    const d = (b.d || '').slice(0, 10);
    (byTicker[b.ticker] || (byTicker[b.ticker] = [])).push({ d, insider: b.insider, role: roleOf(b.title), value: +b.value || 0 });
    (byInsider[b.insider] || (byInsider[b.insider] = [])).push(d);
  }
  for (const k in byInsider) byInsider[k].sort();

  const CLUSTER_WIN = 30, COOLDOWN = 90, DAY = 86400000, FIRSTBUY_GAP = 1095, LOW_LOOKBACK = 252, NEAR_LOW = 0.10;
  // Deduped cluster waves per ticker, tagged with distinct-insider size.
  const clusterEventsByTicker = {};
  for (const ticker of Object.keys(byTicker)) {
    const list = byTicker[ticker].sort((a, b) => a.d < b.d ? -1 : 1);
    let last = null; const evs = [];
    for (let i = 0; i < list.length; i++) {
      const end = new Date(list[i].d + 'T12:00:00Z').getTime();
      const bound = end - CLUSTER_WIN * DAY;
      const seen = new Set();
      for (let j = i; j >= 0; j--) { const dj = new Date(list[j].d + 'T12:00:00Z').getTime(); if (dj < bound) break; seen.add(list[j].insider); }
      if (seen.size >= 2 && (last === null || end - last > COOLDOWN * DAY)) { evs.push({ d: list[i].d, size: seen.size }); last = end; }
    }
    if (evs.length) clusterEventsByTicker[ticker] = evs;
  }

  // Price universe: all tickers with buys, most-active first, capped. Fetch the
  // ones we do not already have cached (Yahoo, throttled, in memory - not persisted).
  const allTk = Object.keys(byTicker).sort((a, b) => byTicker[b].length - byTicker[a].length);
  const toFetch = allTk.filter(t => !barsByTicker[t]);
  let tried = 0, gotBars = 0;
  for (const ticker of toFetch) {
    if (tried >= 5000) break; // safety cap
    tried++;
    try {
      const resp = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&period1=${startTs}&period2=${endTs}`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (resp.ok) {
        const d = await resp.json(); const r = d?.chart?.result?.[0];
        if (r?.timestamp) {
          const q = r.indicators.quote[0];
          const arr = r.timestamp.map((t, i) => ({ t: new Date(t * 1000).toISOString().slice(0, 10), c: q.close?.[i] || 0 })).filter(b => b.c > 0);
          if (arr.length > 30) { arr.sort((a, b) => a.t < b.t ? -1 : 1); barsByTicker[ticker] = arr; gotBars++; }
        }
      }
    } catch(_) {}
    await new Promise(r => setTimeout(r, 180));
  }
  log(`insider-study: ${allTk.length} tickers with buys, fetched ${gotBars}/${toFetch.length} new price series (had ${allTk.length - toFetch.length} cached)`);

  const WIN = { '1M': 21, '3M': 63, '6M': 126, '12M': 252 };
  const today = new Date().toISOString().slice(0, 10);
  const midDate = shiftDate(today, -912); // ~2.5y ago - first-half/second-half split for robustness
  // Measure one entry once: returns per-window { ret, rx (Russell excess), sx (S&P excess) }.
  function measure(bars, entryDate) {
    const ei = bars.findIndex(x => x.t >= entryDate);
    if (ei < 0) return null;
    if (new Date(bars[ei].t) - new Date(entryDate) > 6 * DAY) return null;
    const entry = bars[ei].c; if (!(entry > 0)) return null;
    const rE = onOrBefore(rutClose, bars[ei].t), sE = onOrBefore(spxClose, bars[ei].t);
    const out = {}; let any = false;
    for (const [k, n] of Object.entries(WIN)) {
      const fi = ei + n; if (fi >= bars.length) continue;
      const fwd = bars[fi].c; if (!(fwd > 0)) continue;
      const ret = fwd / entry - 1; if (ret > 4 || ret < -0.95) continue;
      let rx = null, sx = null;
      if (rE) { const rF = onOrBefore(rutClose, bars[fi].t); if (rF) rx = ret - (rF / rE - 1); }
      if (sE) { const sF = onOrBefore(spxClose, bars[fi].t); if (sF) sx = ret - (sF / sE - 1); }
      out[k] = { ret, rx, sx }; any = true;
    }
    return any ? out : null;
  }
  function isNearLow(bars, entryDate) {
    const ei = bars.findIndex(x => x.t >= entryDate); if (ei < 0) return false;
    let lo = Infinity; for (let i = Math.max(0, ei - LOW_LOOKBACK); i <= ei; i++) if (bars[i].c < lo) lo = bars[i].c;
    return lo < Infinity && bars[ei].c <= lo * (1 + NEAR_LOW);
  }

  const mkWin = () => Object.fromEntries(Object.keys(WIN).map(k => [k, { ret: [], pos: 0, n: 0, rut: { exc: [], beat: 0, excN: 0 }, spx: { exc: [], beat: 0, excN: 0 } }]));
  const scen = {};
  const acc = name => scen[name] || (scen[name] = { w: mkWin(), h1: [], h2: [] });
  function tally(name, out, entryDate) {
    const a = acc(name);
    for (const [k, o] of Object.entries(out)) {
      const w = a.w[k]; w.ret.push(o.ret); w.n++; if (o.ret > 0) w.pos++;
      if (o.rx != null) { w.rut.exc.push(o.rx); w.rut.excN++; if (o.rx > 0) w.rut.beat++; }
      if (o.sx != null) { w.spx.exc.push(o.sx); w.spx.excN++; if (o.sx > 0) w.spx.beat++; }
    }
    if (out['6M']) (entryDate < midDate ? a.h1 : a.h2).push(out['6M'].ret);
  }

  let minD = '9999', maxD = '0';
  for (const ticker of Object.keys(byTicker)) {
    const bars = barsByTicker[ticker]; if (!bars) continue;
    for (const b of byTicker[ticker]) {
      const out = measure(bars, b.d); if (!out) continue;
      if (b.d < minD) minD = b.d; if (b.d > maxD) maxD = b.d;
      tally('all', out, b.d);
      tally('role_' + b.role, out, b.d);
      tally(b.value >= 1e6 ? 'size_1m_plus' : b.value >= 1e5 ? 'size_100k_1m' : 'size_under_100k', out, b.d);
      const nl = isNearLow(bars, b.d); if (nl) tally('near_52w_low', out, b.d);
      let prior = null; const hist = byInsider[b.insider];
      if (hist) for (let i = hist.length - 1; i >= 0; i--) { if (hist[i] < b.d) { prior = hist[i]; break; } }
      const fb = prior && (new Date(b.d) - new Date(prior)) / DAY >= FIRSTBUY_GAP;
      if (fb) tally('first_buy_3y', out, b.d);
      if (b.role === 'ceo') { if (nl) tally('ceo_near_low', out, b.d); if (fb) tally('ceo_first_buy', out, b.d); }
    }
    if (clusterEventsByTicker[ticker]) {
      for (const ev of clusterEventsByTicker[ticker]) {
        const out = measure(bars, ev.d); if (!out) continue;
        if (ev.size >= 2) tally('cluster_2plus', out, ev.d);
        if (ev.size >= 3) tally('cluster_3plus', out, ev.d);
        if (ev.size >= 4) tally('cluster_4plus', out, ev.d);
      }
    }
  }

  const median = arr => { if (!arr.length) return 0; const s = [...arr].sort((x, y) => x - y); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
  const mean = arr => arr.length ? arr.reduce((x, y) => x + y, 0) / arr.length : 0;
  const pct = x => Math.round(x * 1000) / 10;
  const benchStat = b => ({ medianExcess: pct(median(b.exc)), pctBeat: b.excN ? pct(b.beat / b.excN) : 0 });
  const winStat = w => ({ n: w.n, medianRet: pct(median(w.ret)), meanRet: pct(mean(w.ret)), pctPositive: w.n ? pct(w.pos / w.n) : 0, rut: benchStat(w.rut), spx: benchStat(w.spx) });
  const scenarios = {};
  for (const [name, a] of Object.entries(scen)) {
    scenarios[name] = {
      windows: Object.fromEntries(Object.keys(WIN).map(k => [k, winStat(a.w[k])])),
      robust6M: { h1Median: pct(median(a.h1)), h2Median: pct(median(a.h2)), h1n: a.h1.length, h2n: a.h2.length },
    };
  }

  const result = {
    version: STUDY_VERSION,
    benchmarks: ['Russell 2000', 'S&P 500'],
    generated: today,
    sample: { from: minD, to: maxD, midDate, minValue: 10000, tickersPriced: allTk.filter(t => barsByTicker[t]).length, tickersTotal: allTk.length },
    scenarios,
  };
  await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('insider-study', ?, ?)`, [JSON.stringify(result), Date.now()]);
  const top = Object.entries(scenarios).map(([n, x]) => [n, x.windows['6M'].rut.pctBeat, x.windows['6M'].n]).sort((a, b) => b[1] - a[1]).slice(0, 5);
  log(`insider-study cached v${STUDY_VERSION}: ${Object.keys(scenarios).length} scenarios, ${result.sample.tickersPriced}/${result.sample.tickersTotal} tickers priced. Top 6M %-beat-Russell: ${top.map(t => `${t[0]} ${t[1]}%(${t[2]})`).join(', ')}`);
}

// Precompute the sitemap's ticker + insider lists so the /sitemap.xml route reads
// one small cached row instead of running two 5-year GROUP BY scans (~1M rows read
// each) on request, which blew the request timeout. Refreshes at most weekly.
async function computeSitemapLists() {
  try {
    const ex = (await dbQuery("SELECT computed_at FROM computed_cache WHERE key='sitemap-lists'"))[0];
    if (ex && Date.now() - ex.computed_at < 6 * 24 * 3600000 && process.env.FORCE_FULL !== '1') { log('sitemap-lists fresh, skip'); return; }
  } catch(_) {}
  log('Computing sitemap-lists...');
  const tickers = (await dbQuery("SELECT ticker FROM trades WHERE ticker GLOB '[A-Z]*' AND trade_date >= date('now','-1825 days') GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 800")).map(r => r.ticker);
  const insiders = (await dbQuery("SELECT insider, COUNT(*) AS n FROM trades WHERE insider IS NOT NULL AND insider != '' AND TRIM(type) IN ('P','S','S-') AND trade_date >= date('now','-1825 days') GROUP BY insider ORDER BY n DESC LIMIT 600")).map(r => r.insider);
  await dbRun(`INSERT OR REPLACE INTO computed_cache (key, value_json, computed_at) VALUES ('sitemap-lists', ?, ?)`, [JSON.stringify({ tickers, insiders }), Date.now()]);
  log(`sitemap-lists cached: ${tickers.length} tickers, ${insiders.length} insiders`);
}

async function main() {
  log('=== precompute start ===');
  await ensureComputedCacheTable();
  // Build the case-insensitive insider index here (long-running env) - it is too
  // large to reliably build during a serverless cold boot, which left insider
  // lookups full-scanning the table and timing out.
  try { await dbRun('CREATE INDEX IF NOT EXISTS idx_insider_upper ON trades(UPPER(insider))'); log('idx_insider_upper ensured'); } catch(e) { log('idx_insider_upper error: ' + e.message); }

  // The heavy full-history aggregates (5-year sentiment, deep first-buy scans) barely
  // change intraday and are the biggest Turso-read consumers, so run them only once
  // per day (the morning run). Everything else runs every ingestion.
  const heavyRun = process.env.FORCE_FULL === '1' || new Date().getUTCHours() <= 14;
  const dow = new Date().getUTCDay(); // 0=Sun .. 6=Sat
  // Weekly = Monday's heavy pass (for the full-scan hygiene DELETEs).
  const weeklyRun = process.env.FORCE_FULL === '1' || (heavyRun && dow === 1);
  log(`Mode: ${heavyRun ? 'FULL' : 'LIGHT'}${weeklyRun ? ' +weekly' : ''}`);

  // Prune always (cheap, uses the trade_date index). The read-heavy cleanups scan
  // trades + price bars, so run them only on the once/day heavy pass.
  await prune5yr();
  if (heavyRun) {
    await cleanupPlanClusters();
    await cleanupNonOpenMarket();
  }
  // Data-hygiene safety nets (bad dates, non-P/S types, implausible values). Each
  // full-scans the table, so run weekly - they rarely delete anything. Moved here
  // from daily-worker, where they ran on every ingestion.
  if (weeklyRun) {
    await dbRun(`DELETE FROM trades WHERE trade_date < '2000-01-01' OR trade_date > '2030-12-31'`).catch(() => {});
    await dbRun(`DELETE FROM trades WHERE TRIM(type) NOT IN ('P','S','S-')`).catch(() => {});
    await dbRun(`DELETE FROM trades WHERE value > 5000000000 OR price > 1500000 OR qty > 500000000`).catch(() => {});
  }

  // Light, recent-data caches - every run
  await Promise.all([
    computeStockLists(),
    computeFirstBuysMonitor(),
    computeProximity(),
    computeMonitorSentiment(),
    computeScreener90(),
  ]);

  // Self-gating caches (sitemap ~weekly, insider-study ~monthly). Run these
  // BEFORE the heavy block - and regardless of light/heavy mode - so the slower
  // leaderboard can never block the study, and a run does not depend on FORCE_FULL.
  await computeSitemapLists().catch(e => log('sitemap-lists error: ' + e.message));
  await computeInsiderStudy().catch(e => log('insider-study error: ' + e.message));

  // Heavy caches - once per day. Sentiment is now incremental (~95d scan), so it
  // is cheap to run daily.
  if (heavyRun) {
    await computeInsiderSentiment();
    await computeFirstBuys();
    await computeInsiderLeaderboard();
  }

  // Price pre-warm runs last (longest - many external Yahoo calls, no Turso reads)
  await migratePriceCacheTo5yr();
  await prewarmPrices();
  log('=== precompute done ===');
}

main().catch(e => { log('FATAL: ' + e.message); process.exit(1); });
