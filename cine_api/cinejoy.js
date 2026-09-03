// cinejoy.js — resolve stream links from cinejoy.to via api.shegu.st (attestation + scrypt POW)
// Usage:
//   node cinejoy.js movie <tmdbId> [server]
//   node cinejoy.js tv <tmdbId> <season> <episode> [server]
// Also usable as a module: const { resolve } = require('./cinejoy.js');

// ---------------------------------------------------------------- browser stubs
// The cinejoy chunk is a browser bundle; give it the few globals it touches.
const _store = {};
globalThis.localStorage = {
    getItem: k => (k in _store ? _store[k] : null),
    setItem: (k, v) => { _store[k] = String(v); },
    removeItem: k => { delete _store[k]; },
};
globalThis.window = globalThis;
Object.defineProperty(globalThis, 'navigator', {
    value: { userAgent: 'Node', language: 'en', onLine: true, hardwareConcurrency: 8, maxTouchPoints: 0, vendor: '', platform: 'MacIntel', cookieEnabled: true, userAgentData: undefined, serviceWorker: undefined },
    configurable: true
});
globalThis.document = {
    createElement: () => ({ style: {}, setAttribute() {}, appendChild() {}, addEventListener() {}, getContext: () => null, width: 0, height: 0 }),
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
    cookie: '',
    currentScript: { src: '' },
    readyState: 'complete',
    documentElement: { style: {} },
    hidden: false,
    visibilityState: 'visible',
};
Object.defineProperty(globalThis, 'location', { value: new URL('https://cinejoy.to/'), configurable: true });
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = fn => setTimeout(fn, 16);
globalThis.cancelAnimationFrame = clearTimeout;
globalThis.requestIdleCallback = fn => setTimeout(fn, 1);
globalThis.devicePixelRatio = 2;
globalThis.innerWidth = 1280;
globalThis.innerHeight = 800;
globalThis.screen = { width: 1280, height: 800, availWidth: 1280, availHeight: 800, colorDepth: 24 };
globalThis.XMLHttpRequest = class { open() {} send() {} setRequestHeader() {} addEventListener() {} };
globalThis.WebSocket = class { constructor() { this.readyState = 3; } send() {} close() {} addEventListener() {} };
globalThis.EventSource = class { constructor() { this.readyState = 2; } addEventListener() {} close() {} };
globalThis.Image = class { set src(v) {} };
globalThis.Audio = class { play() {} };
globalThis.BroadcastChannel = class { postMessage() {} close() {} addEventListener() {} };
globalThis.CustomEvent = class extends Event { constructor(t, o) { super(t); } };
globalThis.CanvasRenderingContext2D = function () {};
globalThis.HTMLCanvasElement = function () {};
globalThis.HTMLElement = function () {};
globalThis.OffscreenCanvas = class { getContext() { return null; } };
globalThis.addEventListener = () => {};
globalThis.removeEventListener = () => {};

const REFERER = 'https://cinejoy.to/';

let chunkPromise = null;
function loadChunk() {
    if (!chunkPromise) {
        chunkPromise = import('./cinejoy-lib/CGdZuRE1.js').then(m => m);
    }
    return chunkPromise;
}

// Resolve a title to its master playlist URL.
//   type: 'movie' | 'tv'
//   ctx: { tmdbId, imdbId?, year?, title?, season?, episode? }
// Returns { url, sourceType, captions, provider } or throws.
async function resolve(type, ctx, onProgress, preferredServer) {
    const chunk = await loadChunk();
    const de = chunk.s; // de is the exported resolver ('s' in the export map)
    const cb = onProgress || (() => {});
    let winner = null;
    const preferred = preferredServer ? String(preferredServer).trim() : '';
    const res = await de(
        type === 'tv' ? 'tv' : 'movie',
        {
            tmdbId: String(ctx.tmdbId || ctx.tmdbId),
            imdbId: ctx.imdbId || ctx.imdbId,
            year: ctx.year,
            title: ctx.title,
            season: ctx.season ? String(ctx.season) : undefined,
            episode: ctx.episode ? String(ctx.episode) : undefined,
        },
        (p) => {
            if (p && p.status && p.status !== 'checking') winner = p.provider;
            cb(p);
        },
        preferred || undefined
    );
    if (!res || !res.result || !res.result.url) {
        throw new Error('No source found on cinejoy');
    }
    return {
        url: res.result.url,
        sourceType: res.result.sourceType || 'hls',
        captions: res.result.captions || [],
        provider: winner || (res.providers && res.providers[0] ? res.providers[0].name : 'cinejoy'),
        providers: res.providers || [],
    };
}

// Sniff the stream URL: HLS master playlist vs direct MP4 (needs cinejoy referer)
async function sniff(url) {
    try {
        const { impersonatedRequest } = require('../browserfetch.js');
        const res = await impersonatedRequest(url, {
            headers: {
                'Referer': REFERER,
                'Origin': 'https://cinejoy.to',
                'Accept': '*/*'
            },
            timeoutMs: 5000
        });
        const buf = res.body;
        const head = buf.subarray(0, 64).toString('latin1');
        if (head.startsWith('#EXTM3U')) {
            let maxRes = 0;
            const re = /RESOLUTION=\d{3,4}x(\d{3,4})/g;
            let m;
            const txt = buf.toString('latin1');
            while ((m = re.exec(txt))) maxRes = Math.max(maxRes, parseInt(m[1]));
            return { kind: 'hls', maxRes };
        }
        if (head.includes('ftyp')) return { kind: 'mp4', maxRes: 0 };
        return { kind: 'unknown', maxRes: 0 };
    } catch { return { kind: 'unknown', maxRes: 0 }; }
}

async function main() {
    const [type, id, season, episode, wantServer] = process.argv.slice(2);
    if (!type || !id) {
        console.log('Usage: node cinejoy.js movie <tmdbId> [server]');
        console.log('       node cinejoy.js tv <tmdbId> <season> <episode> [server]');
        process.exit(1);
    }
    const ctx = { tmdbId: id };
    if (type === 'tv') {
        ctx.season = season || '1';
        ctx.episode = episode || '1';
    }
    try {
        process.stdout.write('Resolving via cinejoy... ');
        const res = await resolve(type, ctx, (p) => {
            if (p && p.status) process.stdout.write(`\n  [${p.provider}] ${p.status}`);
        });
        process.stdout.write('\n');
        console.log(`\n✅ ${id} — ${res.provider} (${res.sourceType})`);
        const kind = await sniff(res.url);
        console.log(`\n📺 Stream (${kind.kind}${kind.maxRes ? ', up to ' + kind.maxRes + 'p' : ''}):\n${res.url}`);
        if (res.captions && res.captions.length) {
            console.log(`\n💬 Captions (${res.captions.length}):`);
            for (const c of res.captions.slice(0, 10)) console.log(`  [${c.language || '?'}] ${c.label || c.url}`);
        }
    } catch (e) {
        console.error('\n❌', e.message);
        process.exit(1);
    }
    process.exit(0); // the cinejoy chunk leaves a timer running; force-exit in CLI mode
}

if (require.main === module) main();

module.exports = { resolve, sniff, REFERER };