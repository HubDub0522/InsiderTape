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

async function renderOgPng(spec) {
  const { satori, Resvg, fonts } = await _load();
  const svg = await satori(buildCard(spec), { width: 1200, height: 630, fonts });
  return Buffer.from(new Resvg(svg, { fitTo: { mode: 'width', value: 1200 } }).render().asPng());
}

module.exports = { renderOgPng };
