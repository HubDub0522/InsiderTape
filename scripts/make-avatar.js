'use strict';
// One-off: render a high-contrast radar avatar PNG for the X profile.
const fs = require('fs');
const path = require('path');

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="800" height="800" viewBox="0 0 800 800">
  <defs>
    <radialGradient id="bg" cx="40%" cy="36%" r="80%">
      <stop offset="0%" stop-color="#14293f"/>
      <stop offset="55%" stop-color="#0b1929"/>
      <stop offset="100%" stop-color="#05101c"/>
    </radialGradient>
  </defs>
  <rect width="800" height="800" fill="url(#bg)"/>
  <circle cx="400" cy="400" r="386" fill="none" stroke="#2ee6ff" stroke-width="7" stroke-opacity="0.95"/>
  <circle cx="400" cy="400" r="300" fill="none" stroke="#3fc3e0" stroke-width="3.5" stroke-opacity="0.55"/>
  <circle cx="400" cy="400" r="210" fill="none" stroke="#3fc3e0" stroke-width="3.5" stroke-opacity="0.6"/>
  <circle cx="400" cy="400" r="120" fill="none" stroke="#3fc3e0" stroke-width="3.5" stroke-opacity="0.65"/>
  <line x1="20" y1="400" x2="780" y2="400" stroke="#3fc3e0" stroke-width="2.5" stroke-opacity="0.22"/>
  <line x1="400" y1="20" x2="400" y2="780" stroke="#3fc3e0" stroke-width="2.5" stroke-opacity="0.22"/>
  <path d="M400 400 L669 131 L774 334 Z" fill="#2ee6ff" fill-opacity="0.20"/>
  <line x1="400" y1="400" x2="669" y2="131" stroke="#5cf0ff" stroke-width="6" stroke-linecap="round" stroke-opacity="0.95"/>
  <circle cx="300" cy="300" r="36" fill="#2fd24f" fill-opacity="0.20"/>
  <circle cx="300" cy="300" r="16" fill="#43ff70"/>
  <circle cx="515" cy="475" r="30" fill="#2fd24f" fill-opacity="0.18"/>
  <circle cx="515" cy="475" r="13" fill="#43ff70"/>
  <circle cx="475" cy="250" r="27" fill="#ff453a" fill-opacity="0.18"/>
  <circle cx="475" cy="250" r="12" fill="#ff6155"/>
  <circle cx="400" cy="400" r="26" fill="#2ee6ff" fill-opacity="0.22"/>
  <circle cx="400" cy="400" r="12" fill="#8af5ff"/>
</svg>`;

(async () => {
  const { Resvg, initWasm } = await import('@resvg/resvg-wasm');
  await initWasm(fs.readFileSync(path.join(__dirname, '..', 'assets', 'resvg.wasm')));
  const png = new Resvg(svg, { fitTo: { mode: 'width', value: 800 } }).render().asPng();
  const out = path.join(__dirname, '..', 'marketing', 'x-avatar-v2.png');
  fs.writeFileSync(out, png);
  console.log('wrote', out, png.length, 'bytes');
})();
