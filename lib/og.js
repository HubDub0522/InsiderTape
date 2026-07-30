'use strict';

// Dynamic Open Graph / Twitter share-card renderer.
// satori (HTML/flex -> SVG, text baked to vector paths) + @resvg/resvg-wasm
// (SVG -> PNG). Pure JS/WASM, so no native binaries and no fonts needed at
// rasterization time. Everything lazy-loads once per warm serverless instance.

const fs = require('fs');
const path = require('path');

const ASSETS = path.join(__dirname, '..', 'assets');

let _init = null;
function _load() {
  if (_init) return _init;
  _init = (async () => {
    const satori = (await import('satori')).default;
    const { Resvg, initWasm } = await import('@resvg/resvg-wasm');
    await initWasm(fs.readFileSync(path.join(ASSETS, 'resvg.wasm')));
    const fonts = [400, 600, 700, 800].map(w => ({
      name: 'Inter', weight: w, style: 'normal',
      data: fs.readFileSync(path.join(ASSETS, 'fonts', `inter-${w}.woff`)),
    }));
    return { satori, Resvg, fonts };
  })();
  return _init;
}

// Element helper: satori needs display:flex on any div with >1 child, so default it.
const el = (type, style, children) => ({
  type,
  props: { style: (type === 'div' && !('display' in (style || {}))) ? { display: 'flex', ...style } : (style || {}), children },
});

// spec: { eyebrow, title, titleSize, titleColor, subtitle, stat:{value,label,color},
//         badge:{text,color,bg} }
function buildCard(spec) {
  const kids = [];
  if (spec.badge) {
    kids.push(el('div', {
      alignSelf: 'flex-start', fontSize: '22px', fontWeight: 700,
      color: spec.badge.color || '#12905f', background: spec.badge.bg || 'rgba(18,144,95,0.12)',
      padding: '6px 16px', borderRadius: '8px', marginBottom: '22px', letterSpacing: '1px',
    }, spec.badge.text));
  }
  kids.push(el('div', {
    fontSize: (spec.titleSize || 82) + 'px', fontWeight: 800,
    color: spec.titleColor || '#1a2030', lineHeight: '1.04', letterSpacing: '-1px',
  }, spec.title));
  if (spec.subtitle) {
    kids.push(el('div', { fontSize: '34px', color: '#3a4555', marginTop: '18px' }, spec.subtitle));
  }
  if (spec.stat && spec.stat.value != null) {
    kids.push(el('div', { alignItems: 'baseline', marginTop: '30px' }, [
      el('div', { fontSize: '60px', fontWeight: 800, color: spec.stat.color || '#12905f' }, String(spec.stat.value)),
      ...(spec.stat.label ? [el('div', { fontSize: '28px', color: '#6e7a8a', marginLeft: '16px' }, spec.stat.label)] : []),
    ]));
  }
  return el('div', {
    width: '1200px', height: '630px', flexDirection: 'column',
    background: '#ffffff', padding: '54px 64px', fontFamily: 'Inter', position: 'relative',
  }, [
    el('div', { position: 'absolute', top: '0', left: '0', right: '0', height: '12px', background: '#0a6f88' }, []),
    el('div', { alignItems: 'center', justifyContent: 'space-between' }, [
      el('div', { fontSize: '30px', fontWeight: 800, letterSpacing: '2px' }, [
        el('span', { color: '#1a2030' }, 'INSIDER'),
        el('span', { color: '#0a6f88' }, 'TAPE'),
      ]),
      el('div', { fontSize: '22px', color: '#6e7a8a', fontWeight: 600, letterSpacing: '1px' }, spec.eyebrow || ''),
    ]),
    el('div', { flexDirection: 'column', flexGrow: 1, justifyContent: 'center' }, kids),
    el('div', { justifyContent: 'space-between', alignItems: 'center', borderTop: '1px solid #e2e6ea', paddingTop: '22px' }, [
      el('div', { fontSize: '26px', color: '#0a6f88', fontWeight: 700 }, 'insidertape.com'),
      el('div', { fontSize: '22px', color: '#6e7a8a' }, 'Live SEC Form 4 insider trading'),
    ]),
  ]);
}

// Dark "product screenshot" card: a stylized candlestick chart with green buy /
// red sell markers and an insider-pressure histogram, plus a headline + CTA.
// Built from deterministic data so the card is stable across renders. All
// drawing is absolutely-positioned divs (no fonts needed); text is baked by satori.
function buildChartCard(spec) {
  const W = 1200, H = 630, bg = '#0a0f1a';
  const cx0 = 52, cw = 1096;                 // chart x-range
  const pTop = 104, pBot = 344;              // price panel
  const prTop = 362, prBot = 414, prMid = (prTop + prBot) / 2; // pressure panel
  const N = 46;
  let seed = 8123; const rnd = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
  const cl = []; for (let i = 0; i < N; i++) { const trend = i * 0.62; const dip = -10 * Math.exp(-Math.pow((i - 24) / 7, 2)); cl.push(66 + trend + dip + (rnd() - 0.5) * 4.2); }
  const op = cl.map((c, i) => i ? cl[i - 1] : c - 1);
  const hi = cl.map((c, i) => Math.max(op[i], c) + rnd() * 2 + 0.5);
  const lo = cl.map((c, i) => Math.min(op[i], c) - rnd() * 2 - 0.5);
  const mn = Math.min(...lo), mx = Math.max(...hi), rng = (mx - mn) || 1;
  const yP = v => pTop + (1 - (v - mn) / rng) * (pBot - pTop);
  const xC = i => cx0 + (i + 0.5) * (cw / N);
  const bw = Math.max(7, cw / N * 0.6);
  const nodes = [];
  for (let i = 0; i < N; i++) {
    const up = cl[i] >= op[i], col = up ? '#2fd24f' : '#ff453a', x = xC(i);
    nodes.push(el('div', { position: 'absolute', left: (x - 1) + 'px', top: yP(hi[i]) + 'px', width: '2px', height: Math.max(1, yP(lo[i]) - yP(hi[i])) + 'px', background: col, opacity: 0.85 }, []));
    const byT = yP(Math.max(op[i], cl[i])), byB = yP(Math.min(op[i], cl[i]));
    nodes.push(el('div', { position: 'absolute', left: (x - bw / 2) + 'px', top: byT + 'px', width: bw + 'px', height: Math.max(2, byB - byT) + 'px', background: col, borderRadius: '1px' }, []));
  }
  const buys = [3, 23, 25, 40], sells = [15, 34, 43];
  const marker = (i, color, fill) => { const r = 22, x = xC(i), y = yP(cl[i]); nodes.push(el('div', { position: 'absolute', left: (x - r) + 'px', top: (y - r) + 'px', width: (2 * r) + 'px', height: (2 * r) + 'px', borderRadius: r + 'px', background: fill, border: '3px solid ' + color }, [])); };
  buys.forEach(i => marker(i, '#2fd24f', 'rgba(47,210,79,0.16)'));
  sells.forEach(i => marker(i, '#ff453a', 'rgba(255,69,58,0.16)'));
  nodes.push(el('div', { position: 'absolute', left: cx0 + 'px', top: prMid + 'px', width: cw + 'px', height: '1px', background: '#22304a' }, []));
  for (let i = 0; i < N; i++) {
    const isBuy = buys.some(b => Math.abs(b - i) <= 1) || i > N - 8;
    const isSell = sells.some(s => Math.abs(s - i) <= 1) || (i > 10 && i < 20);
    let up = isBuy && !isSell, h = 6 + rnd() * 10;
    if (!isBuy && !isSell) { h = 4 + rnd() * 6; up = rnd() > 0.5; }
    nodes.push(el('div', { position: 'absolute', left: (xC(i) - bw / 2) + 'px', top: (up ? prMid - h : prMid) + 'px', width: bw + 'px', height: h + 'px', background: up ? '#2fd24f' : '#ff453a', opacity: 0.8, borderRadius: '1px' }, []));
  }
  const overlay = [
    el('div', { position: 'absolute', left: '48px', top: '34px', alignItems: 'center', justifyContent: 'space-between', width: (W - 96) + 'px' }, [
      el('div', { fontSize: '30px', fontWeight: 800, letterSpacing: '2px' }, [el('span', { color: '#eaf1f8' }, 'INSIDER'), el('span', { color: '#2aa9c9' }, 'TAPE')]),
      el('div', { alignItems: 'center' }, [
        el('div', { width: '16px', height: '16px', borderRadius: '8px', background: '#2fd24f', marginRight: '8px' }, []),
        el('div', { fontSize: '22px', color: '#9fb2c6', marginRight: '22px' }, 'Buys'),
        el('div', { width: '16px', height: '16px', borderRadius: '8px', background: '#ff453a', marginRight: '8px' }, []),
        el('div', { fontSize: '22px', color: '#9fb2c6' }, 'Sells'),
      ]),
    ]),
    el('div', { position: 'absolute', left: '48px', top: '442px', flexDirection: 'column', width: (W - 96) + 'px' }, [
      el('div', { fontSize: '50px', fontWeight: 800, color: '#ffffff', letterSpacing: '-1px', lineHeight: '1.05' }, spec.headline || 'Every insider trade. Plotted on the chart.'),
      el('div', { fontSize: '25px', color: '#9fb2c6', marginTop: '12px' }, spec.sub || 'Real-time SEC Form 4 buys and sells, the moment they file.'),
      el('div', { alignItems: 'center', marginTop: '20px' }, [
        el('div', { fontSize: '24px', fontWeight: 700, color: '#06131d', background: '#2fd24f', padding: '12px 26px', borderRadius: '10px' }, spec.cta || 'Start a free 7-day trial'),
        el('div', { fontSize: '24px', color: '#2aa9c9', fontWeight: 700, marginLeft: '22px' }, 'insidertape.com'),
      ]),
    ]),
  ];
  return el('div', { width: W + 'px', height: H + 'px', background: bg, position: 'relative', fontFamily: 'Inter' }, [...nodes, ...overlay]);
}

async function renderOgPng(spec) {
  const { satori, Resvg, fonts } = await _load();
  const tree = (spec && spec.variant === 'chart') ? buildChartCard(spec) : buildCard(spec);
  const svg = await satori(tree, { width: 1200, height: 630, fonts });
  return Buffer.from(new Resvg(svg, { fitTo: { mode: 'width', value: 1200 } }).render().asPng());
}

module.exports = { renderOgPng };
