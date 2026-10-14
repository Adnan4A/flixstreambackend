const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { webcrypto } = require('crypto');
global.crypto = webcrypto;

// Lordflix/Solstice needs browser Origin. Do not stamp that Origin onto cinejoy/shegu fetches.
const __fetch = global.fetch;
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
// Cinejoy moved api.shegu.st -> api.wing.st (shegu origin is CF 502).
// Oracle/PHX egress was IP-banned by shegu; keep optional WARP SOCKS as fallback.
const CINEJOY_API_HOST = 'api.wing.st';
const SHEGU_SOCKS = process.env.SHEGU_SOCKS || process.env.ALL_PROXY || 'socks5h://127.0.0.1:40000';

function rewriteCinejoyApiUrl(href) {
    if (!href) return href;
    return String(href).replace(/api\.shegu\.st/gi, CINEJOY_API_HOST);
}

function isCinejoyApiUrl(href) {
    return /api\.(shegu\.st|wing\.st)/i.test(href || '');
}

function mergeHeaders(base, incoming) {
    const out = { ...base };
    if (!incoming) return out;
    if (typeof incoming.forEach === 'function') {
        incoming.forEach((value, key) => { out[key] = value; });
        return out;
    }
    return { ...out, ...incoming };
}

function findCurlBinary() {
    const env = process.env.CURL_IMPERSONATE;
    const names = ['curl_chrome145', 'curl_chrome142', 'curl-impersonate', 'curl'];
    const dirs = (process.env.PATH || '').split(':').filter(Boolean);
    if (env && fs.existsSync(env)) return env;
    for (const dir of dirs) {
        for (const name of names) {
            const p = path.join(dir, name);
            if (fs.existsSync(p)) return p;
        }
    }
    return 'curl';
}

async function bodyToBuffer(body) {
    if (body == null) return null;
    if (Buffer.isBuffer(body)) return body;
    if (body instanceof ArrayBuffer) return Buffer.from(body);
    if (ArrayBuffer.isView(body)) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
    if (typeof body === 'string') return Buffer.from(body);
    if (typeof body === 'object' && typeof body.getReader === 'function') {
        const chunks = [];
        const reader = body.getReader();
        for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(Buffer.from(value));
        }
        return Buffer.concat(chunks);
    }
    return Buffer.from(String(body));
}

/** Fetch via SOCKS (WARP) — used only for api.shegu.st which bans the VPS IP. */
function fetchViaSocks(url, opts = {}) {
    const href = rewriteCinejoyApiUrl(typeof url === 'string' ? url : (url && (url.url || url.href)) || '');
    const method = (opts.method || 'GET').toUpperCase();
    const headers = mergeHeaders({
        'User-Agent': BROWSER_UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://cinejoy.pk',
        'Referer': 'https://cinejoy.pk/',
    }, opts.headers);
    // System curl is enough here — shegu blocks by IP, not JA3. Prefer it for SOCKS stability.
    const bin = fs.existsSync('/usr/bin/curl') ? '/usr/bin/curl' : findCurlBinary();
    const hdrFile = path.join(require('os').tmpdir(), 'shegu-hdr-' + process.pid + '-' + Date.now() + '.txt');
    const args = [
        '-sS', '-L', '--max-time', '30',
        '-x', SHEGU_SOCKS,
        '-X', method,
        '-D', hdrFile,
        '-o', '-',
        '--compressed',
    ];
    for (const [k, v] of Object.entries(headers)) {
        if (v == null || k.toLowerCase() === 'content-length') continue;
        args.push('-H', `${k}: ${v}`);
    }
    args.push(href);

    return (async () => {
        const bodyBuf = await bodyToBuffer(opts.body);
        if (bodyBuf && bodyBuf.length) {
            args.splice(args.length - 1, 0, '--data-binary', '@-');
        }
        const child = spawn(bin, args, { stdio: ['pipe', 'pipe', 'pipe'] });
        const chunks = [];
        const errChunks = [];
        child.stdout.on('data', (d) => chunks.push(d));
        child.stderr.on('data', (d) => errChunks.push(d));
        if (bodyBuf && bodyBuf.length) child.stdin.end(bodyBuf);
        else child.stdin.end();

        const code = await new Promise((resolve, reject) => {
            child.on('error', reject);
            child.on('close', resolve);
        });
        const bodyPart = Buffer.concat(chunks);
        let headerText = '';
        try {
            headerText = fs.readFileSync(hdrFile, 'utf8');
        } catch (_) {}
        try { fs.unlinkSync(hdrFile); } catch (_) {}

        if (code !== 0 && !bodyPart.length && !headerText) {
            const err = Buffer.concat(errChunks).toString('utf8').trim();
            throw new Error('shegu socks fetch failed: ' + (err || ('exit ' + code)));
        }
        // After redirects, -D may contain multiple HTTP responses — use the last.
        const blocks = headerText.split(/\r?\n\r?\n/).filter((b) => /^HTTP\/\d/i.test(b));
        const last = blocks[blocks.length - 1] || headerText;
        const statusLine = (last.split(/\r?\n/)[0] || '');
        const statusMatch = statusLine.match(/HTTP\/\d(?:\.\d)?\s+(\d+)/i);
        const status = statusMatch ? parseInt(statusMatch[1], 10) : (code === 0 ? 200 : 502);
        const hdrs = new Headers();
        for (const line of last.split(/\r?\n/).slice(1)) {
            const c = line.indexOf(':');
            if (c > 0) hdrs.set(line.slice(0, c).trim(), line.slice(c + 1).trim());
        }
        return new Response(bodyPart, { status, headers: hdrs });
    })();
}

global.fetch = async (url, opts = {}) => {
    let href = typeof url === 'string' ? url : (url && (url.url || url.href)) || '';
    if (isCinejoyApiUrl(href)) {
        href = rewriteCinejoyApiUrl(href);
        url = href;
        // Prefer direct to api.wing.st. SOCKS only if direct is banned/unreachable —
        // a down WARP proxy used to black-hole every cinejoy resolve.
        try {
            const direct = await __fetch(url, {
                ...opts,
                headers: mergeHeaders({
                    'User-Agent': BROWSER_UA,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://cinejoy.pk',
                    'Referer': 'https://cinejoy.pk/',
                }, opts.headers),
            });
            if (direct.status !== 403 && direct.status !== 451 && direct.status !== 502) {
                return direct;
            }
            console.error('cinejoy api direct', direct.status, 'trying socks');
        } catch (e) {
            console.error('cinejoy api direct fail', e.message);
        }
        if (SHEGU_SOCKS) {
            try { return await fetchViaSocks(url, opts); } catch (e) {
                console.error('cinejoy api socks fail', e.message);
            }
        }
        return __fetch(url, opts);
    }
    const lordflix = /lordflix/i.test(href);
    const base = {
        'User-Agent': BROWSER_UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(lordflix ? { Origin: 'https://lordflix.org', Referer: 'https://lordflix.org/' } : {}),
    };
    return __fetch(url, { ...opts, headers: mergeHeaders(base, opts.headers) });
};

// Load Solstice crypto engine
eval(fs.readFileSync(path.join(__dirname, 'fullCrypto.js'), 'utf8') + `
    global._L5 = L5;
    global._ze = ze;
    global._Ue = Ue;
    global._Wa = Wa;
`);

const BASE = 'https://hongkong.lordflix.club';
const CDN_HOST = 'thx.earthcleaner.cc';
const DRAGON_NAMES = { Lisbon: 'Vhagar', Solara: 'Caraxes', Athens: 'Syrax', Joy: 'Meleys', Castle: 'Sunfyre', Sakura: 'Vermithor', Canaias: 'Seasmoke' };
const SOURCE_BY_SERVER = Object.fromEntries(
    Object.entries(DRAGON_NAMES).flatMap(([src, dragon]) => [
        [src.toLowerCase(), src],
        [dragon.toLowerCase(), src],
    ])
);
function preferredSourceName(raw) {
    if (typeof raw !== 'string') return '';
    const t = raw.trim();
    if (!t || /^auto$/i.test(t)) return '';
    return SOURCE_BY_SERVER[t.toLowerCase()] || t;
}

// Lisbon (Vhagar) is the provider that still returns a real HLS host.
// Nebula's bright67 host is a redirect-loop honeypot. Athens and Solara
// often have no mirror and fall through to that honeypot, so try Lisbon first.
const CLEAN_SOURCES = ['Lisbon', 'Athens', 'Nebula', 'Castle', 'Joy', 'Sakura', 'Canaias', 'Solara'];
const LAST_RESORT_SOURCES = [];
const ALL_SOURCES = [...CLEAN_SOURCES, ...LAST_RESORT_SOURCES];

function isDirtyCdnHost(host) {
    const h = String(host || '').toLowerCase();
    if (!h) return false;
    if (h === 'info.movieboxnoob.cc' || h.startsWith('info.moviebox')) return true;
    if (h.includes('bright67.online') || h.includes('dontscrape')) return true;
    return false;
}

function streamHostAcceptable(url, allowDirty) {
    try {
        const host = new URL(url).hostname;
        if (isHostBlocked(host)) return false;
        if (!allowDirty && isDirtyCdnHost(host)) return false;
        return true;
    } catch {
        return false;
    }
}

function normalizeExcludeList(raw) {
    const out = new Set();
    const list = Array.isArray(raw) ? raw : (typeof raw === 'string' && raw ? [raw] : []);
    for (const item of list) {
        const mapped = preferredSourceName(item) || String(item || '').trim();
        if (mapped) out.add(mapped.toLowerCase());
        const dragon = DRAGON_NAMES[mapped];
        if (dragon) out.add(dragon.toLowerCase());
        out.add(String(item || '').trim().toLowerCase());
    }
    out.delete('');
    return out;
}

/**
 * Try every available server until one returns a real playable m3u8 (or mp4).
 * Clean CDN hosts first; dirty hosts only after clean options are exhausted.
 * `exclude` skips providers the player already failed on.
 */
async function resolveCinejoyPreferClean(cj, mediaType, ctx, preferred, exclude) {
    const excluded = normalizeExcludeList(exclude);
    const order = [];
    const push = (s) => {
        if (!s) return;
        const key = String(s).trim();
        if (!key) return;
        if (excluded.has(key.toLowerCase())) return;
        if (excluded.has((DRAGON_NAMES[key] || '').toLowerCase())) return;
        if (order.some((x) => x.toLowerCase() === key.toLowerCase())) return;
        order.push(key);
    };

    push(preferred);
    for (const s of CLEAN_SOURCES) push(s);
    for (const s of LAST_RESORT_SOURCES) push(s);

    let lastErr = null;
    let sawWarming = false;
    let lastProviders = [];
    const triedUrls = new Set();

    async function trySource(src, allowDirty) {
        const info = await cj.resolve(mediaType, ctx, undefined, src);
        if (Array.isArray(info && info.providers)) {
            lastProviders = info.providers;
            for (const p of info.providers) {
                if (p && p.status === 'ok') push(p.name);
            }
        }
        const providerName = info && info.provider ? String(info.provider) : '';
        if (providerName && (
            excluded.has(providerName.toLowerCase()) ||
            excluded.has((DRAGON_NAMES[providerName] || '').toLowerCase())
        )) {
            console.error('cinejoy skip', src, 'provider excluded', providerName);
            return null;
        }
        if (!info || !looksLikeStreamUrl(info.url)) {
            console.error('cinejoy skip', src, 'no https stream url');
            return null;
        }
        // Dirty hosts are deferred on pass 1 — do NOT burn the URL so pass 2 can retry.
        if (!streamHostAcceptable(info.url, allowDirty)) {
            const host = (() => { try { return new URL(info.url).hostname; } catch { return '?'; } })();
            if (!allowDirty && isDirtyCdnHost(host)) {
                console.error('cinejoy skip', src, 'dirty host deferred', host);
                return null;
            }
            console.error('cinejoy skip', src, 'blocked/unacceptable host', host);
            triedUrls.add(info.url);
            return null;
        }
        if (triedUrls.has(info.url)) return null;
        triedUrls.add(info.url);

        // Require a live #EXTM3U playlist (or a sniffable MP4) before accepting.
        try {
            const probe = await probePlaylistPlayable(info.url);
            if (probe.ok) {
                console.log('cinejoy accept', src, '->', info.provider, 'm3u8 ok');
                return Object.assign(info, { _playable: { kind: 'hls', maxRes: 0 } });
            }
            if (probe.reason === 'no_playable_rendition') sawWarming = true;
            console.error('cinejoy skip', src, 'playlist not playable', probe.reason || probe.http);
        } catch (e) {
            console.error('cinejoy skip', src, 'playlist probe failed', e.message);
        }

        try {
            const sniff = await cj.sniff(info.url);
            if (sniff && sniff.kind === 'mp4') {
                console.log('cinejoy accept', src, '->', info.provider, 'mp4');
                return Object.assign(info, { _playable: sniff });
            }
        } catch (_) {}
        return null;
    }

    // Pass 1: clean hosts only. Pass 2: allow dirty CDNs so auto never dies on Solara alone.
    for (const allowDirty of [false, true]) {
        for (let i = 0; i < order.length; i++) {
            const src = order[i];
            try {
                const info = await trySource(src, allowDirty);
                if (info) return info;
            } catch (e) {
                lastErr = e;
                console.error('cinejoy try', src + ':', e.message);
            }
        }
        // Providers discovered mid-pass are appended to `order` and picked up by later indexes.
        for (const s of ALL_SOURCES) push(s);
        if (lastProviders.length) {
            for (const p of lastProviders) {
                if (p && p.status === 'ok') push(p.name);
            }
        }
    }

    if (sawWarming) throw new Error('playlist warming: the source is still preparing this title');
    throw lastErr || new Error('No playable m3u8 from any cinejoy server');
}
const PORT = process.env.PORT || 3010;
const TMDB_KEY = process.env.TMDB_API_KEY || '8265bd1679663a7ea12ac168da84d2e8';
const TMDB_IMG = 'https://image.tmdb.org/t/p/w500';
const metaCache = new Map();
const META_TTL = 6 * 60 * 60 * 1000;
const MAX_BODY = 8 * 1024;
const CLIENT_HEADER = 'x-flix-client';

const ALLOWED_HOSTS = new Set([
    'flixstream.ca',
    'www.flixstream.ca',
    'localhost',
    '127.0.0.1',
]);

function parseOrigin(value) {
    if (!value || typeof value !== 'string') return '';
    try {
        return new URL(value).origin;
    } catch {
        return '';
    }
}

function isAllowedOrigin(origin) {
    if (!origin) return false;
    try {
        const u = new URL(origin);
        if (u.hostname === 'localhost' || u.hostname === '127.0.0.1') {
            return u.protocol === 'http:';
        }
        return u.protocol === 'https:' && ALLOWED_HOSTS.has(u.hostname);
    } catch {
        return false;
    }
}

function hostOrigin(req) {
    const host = req.headers.host;
    if (!host || typeof host !== 'string') return '';
    try {
        const u = new URL(`http://${host}`);
        if (u.hostname === 'localhost' || u.hostname === '127.0.0.1') {
            return u.protocol === 'http:' ? u.origin : '';
        }
        if (ALLOWED_HOSTS.has(u.hostname)) return `https://${u.hostname}`;
    } catch {}
    return '';
}

function hasFlixClient(req) {
    const v = req.headers[CLIENT_HEADER];
    return v === '1' || v === 1;
}

function requestOrigin(req) {
    const origin = req.headers.origin;
    if (origin && isAllowedOrigin(origin)) return origin;
    const refererOrigin = parseOrigin(req.headers.referer);
    if (refererOrigin && isAllowedOrigin(refererOrigin)) return refererOrigin;
    // Player fetch/HLS xhr may omit Origin+Referer (security headers). Trust client marker.
    if (hasFlixClient(req)) {
        const fromHost = hostOrigin(req);
        if (fromHost) return fromHost;
    }
    return '';
}

function clientIp(req) {
    const cf = req.headers['cf-connecting-ip'];
    if (typeof cf === 'string' && cf) return cf.split(',')[0].trim();
    return req.socket.remoteAddress || 'unknown';
}

const rateBuckets = new Map();
setInterval(() => {
    const cutoff = Date.now() - 120000;
    for (const [k, b] of rateBuckets) {
        if (b.start < cutoff) rateBuckets.delete(k);
    }
}, 60000).unref();

function tooMany(ip, key, max, windowMs) {
    const id = ip + ':' + key;
    const now = Date.now();
    let b = rateBuckets.get(id);
    if (!b || now - b.start > windowMs) {
        b = { start: now, n: 0 };
        rateBuckets.set(id, b);
    }
    b.n += 1;
    return b.n > max;
}

function isPrivateHostname(hostname) {
    const h = String(hostname || '').toLowerCase().replace(/^\[|\]$/g, '');
    if (!h || h === 'localhost' || h.endsWith('.localhost') || h === '::1' || h === '0.0.0.0') return true;
    if (h === 'metadata.google.internal' || h.endsWith('.internal')) return true;
    if (/^127\./.test(h) || /^10\./.test(h) || /^192\.168\./.test(h) || /^169\.254\./.test(h)) return true;
    const m = h.match(/^172\.(\d+)\./);
    if (m && Number(m[1]) >= 16 && Number(m[1]) <= 31) return true;
    return false;
}

function publicHttpUrl(raw) {
    let u;
    try { u = new URL(String(raw || '')); } catch { return null; }
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
    if (u.username || u.password) return null;
    if (isPrivateHostname(u.hostname)) return null;
    return u;
}

// Rewrite an m3u8 body: direct=1 → absolute CDN URLs (browser fetches segments),
// otherwise → proxied /api/cjproxy URLs (server relays everything).
function rewritePlaylist(text, targetUrl, direct) {
    const baseUrl = targetUrl.href.slice(0, targetUrl.href.lastIndexOf('/') + 1);
    const toAbs = (u) => {
        if (!u || u.startsWith('#') || /^data:/i.test(u)) return u;
        return /^https?:\/\//i.test(u) ? u : new URL(u, baseUrl).href;
    };
    const toProxy = (u) => '/api/cjproxy?url=' + encodeURIComponent(toAbs(u));
    const rewriteLine = (line, mapper) => line.startsWith('#') ? line : mapper(line);
    if (direct) {
        return text
            .replace(/^(\S+)$/gm, line => rewriteLine(line, toAbs))
            .replace(/URI="([^"]+)"/g, (m, u) => 'URI="' + toAbs(u) + '"');
    }
    return text
        .replace(/^(\S+)$/gm, line => rewriteLine(line, toProxy))
        .replace(/URI="([^"]+)"/g, (m, u) => 'URI="' + toProxy(u) + '"');
}

function applySecurityHeaders(res, opts = {}) {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    if (!opts.allowFrame) res.setHeader('X-Frame-Options', 'DENY');
    // no-referrer breaks same-origin /api/* auth fallback for the embedded player.
    res.setHeader('Referrer-Policy', opts.playerPage ? 'strict-origin-when-cross-origin' : 'no-referrer');
    res.setHeader('X-Powered-By', '');
}

function serveStaticFile(res, filePath) {
    const ext = path.extname(filePath).toLowerCase();
    const types = {
        '.png': 'image/png',
        '.ico': 'image/x-icon',
        '.svg': 'image/svg+xml',
        '.webp': 'image/webp',
    };
    fs.readFile(filePath, (err, buf) => {
        if (err) { hide(res, 404); return; }
        res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
        res.end(buf);
    });
}

function applyCors(res, origin) {
    if (!origin) return;
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range, X-Flix-Client');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range');
    res.setHeader('Access-Control-Max-Age', '600');
}

function hide(res, status, reason) {
    const code = status || 404;
    if (code === 404) {
        void notifyTelegram('API resolving failed.\nHTTP 404' + (reason ? '\n' + reason : ''));
    }
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end('{}');
}

const telegram = {
    botToken: process.env.TELEGRAM_BOT_TOKEN || '8591460817:AAFfvWMhzzdVSyQNQ-yTz_gh8JRpilaWYUY',
    chatId: process.env.TELEGRAM_CHAT_ID || '8254382347',
};
let lastTelegramAt = 0;
const TELEGRAM_COOLDOWN_MS = 5 * 60 * 1000;

async function notifyTelegram(text) {
    const now = Date.now();
    if (now - lastTelegramAt < TELEGRAM_COOLDOWN_MS) return;
    lastTelegramAt = now;
    const token = telegram.botToken;
    const chatId = telegram.chatId;
    if (!token || !chatId) return;
    try {
        await __fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: String(text).slice(0, 1000) }),
            signal: AbortSignal.timeout(8000),
        });
    } catch (e) {
        console.error('telegram:', e.message);
    }
}

function cinejoyFailStatus(err) {
    const msg = String(err && err.message || err || '');
    if (/\b451\b/.test(msg) || /\b403\b/.test(msg)) return 451;
    return 500;
}

// 550 (Fight Club) is currently pinned to a dead transcoder node and 502s.
// Probe a few titles so one bad asset does not mark the whole API down.
const HEALTH_TMDBS = [155, 27205, 603, 680, 550];
const HEALTH_TTL_OK = 90 * 1000;
const HEALTH_TTL_FAIL = 45 * 1000;
const CINEJOY_CDN_HEADERS = {
    Referer: 'https://cinejoy.pk/',
    Origin: 'https://cinejoy.pk',
    Accept: '*/*',
};
const renditionCache = new Map();
const RENDITION_TTL_MS = 3 * 60 * 1000;


function firstPlaylistUris(playlistText, playlistUrl, limit) {
    const out = [];
    for (const line of String(playlistText || '').split(/\r?\n/)) {
        const s = line.trim();
        if (!s || s.startsWith('#')) continue;
        try {
            out.push(/^https?:\/\//i.test(s) ? s : new URL(s, playlistUrl).href);
        } catch {
            continue;
        }
        if (out.length >= limit) break;
    }
    return out;
}

function headOk(url, maxSec = 8) {
    return new Promise((resolve) => {
        const child = spawn('/usr/bin/curl', [
            '-sS', '-I', '-o', '/dev/null', '-w', '%{http_code}',
            '--max-time', String(maxSec),
            '-H', 'Origin: https://cinejoy.pk',
            '-H', 'Referer: https://cinejoy.pk/',
            url,
        ], { stdio: ['ignore', 'pipe', 'ignore'] });
        let out = '';
        const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) {} resolve(false); }, (maxSec + 1) * 1000);
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.on('error', () => { clearTimeout(timer); resolve(false); });
        child.on('close', () => {
            clearTimeout(timer);
            const code = parseInt(out.trim(), 10) || 0;
            resolve(code >= 200 && code < 300);
        });
    });
}

// Hosts that answer a browser at flixstream.ca (CORS allowed). Others only accept the
// cinejoy.pk Origin, so their segments must go through /api/cjproxy.
const corsHostCache = new Map();
function hostAllowsFlixOrigin(url) {
    let host;
    try { host = new URL(url).hostname; } catch (e) { return Promise.resolve(false); }
    const cached = corsHostCache.get(host);
    if (cached && Date.now() - cached.at < (cached.ok ? 30 * 60 * 1000 : 3 * 60 * 1000)) return Promise.resolve(cached.ok);
    return new Promise((resolve) => {
        const child = spawn('/usr/bin/curl', [
            '-sS', '-D', '-', '-o', '/dev/null', '--max-time', '8',
            '-X', 'GET', '-H', 'Range: bytes=0-0',
            '-H', 'Origin: https://flixstream.ca',
            url,
        ], { stdio: ['ignore', 'pipe', 'ignore'] });
        let out = '';
        const done = (ok, cache) => {
            clearTimeout(timer);
            if (cache) corsHostCache.set(host, { ok, at: Date.now() });
            resolve(ok);
        };
        const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) {} done(false, false); }, 9000);
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.on('error', () => done(false, false));
        child.on('close', () => {
            const status = parseInt((out.match(/^HTTP\/[\d.]+\s+(\d{3})/m) || [])[1], 10) || 0;
            const acao = /^access-control-allow-origin:\s*(\*|https:\/\/flixstream\.ca)/mi.test(out);
            // 502/5xx are the CDN's flaky segments, not a CORS verdict; do not cache those.
            if (status >= 500 || status === 0) return done(false, false);
            done(status >= 200 && status < 300 && acao, true);
        });
    });
}

async function segmentsDirectOk(playlistText, playlistUrl) {
    const lines = String(playlistText || '').split('\n').map((l) => l.trim());
    const first = lines.find((l) => l && !l.startsWith('#'));
    const mapUri = (String(playlistText).match(/URI="([^"]+)"/) || [])[1];
    const pick = first || mapUri;
    if (!pick) return false;
    let abs;
    try { abs = new URL(pick, playlistUrl.href).href; } catch (e) { return false; }
    return hostAllowsFlixOrigin(abs);
}

/** A rendition is playable when its playlist and first segment answer 2xx. */
async function renditionPlayable(mediaPlaylistUrl) {
    const cached = renditionCache.get(mediaPlaylistUrl);
    if (cached && Date.now() - cached.at < cached.ttl) return cached.ok;
    const { impersonatedRequest } = require('./browserfetch.js');
    let ok = false;
    try {
        let up = null;
        try {
            up = await impersonatedRequest(mediaPlaylistUrl, { headers: CINEJOY_CDN_HEADERS, timeoutMs: 8000 });
        } catch (e) { up = null; }
        // Cold titles can take 20s+ to answer the first time, then are instant.
        // A timeout is not a verdict, so wait once more before giving up.
        if (!up || !up.status) {
            up = await impersonatedRequest(mediaPlaylistUrl, { headers: CINEJOY_CDN_HEADERS, timeoutMs: 32000 });
        }
        const text = up && up.body ? up.body.toString('latin1') : '';
        if (up && up.status >= 200 && up.status < 300 && text.startsWith('#EXTM3U')) {
            // The first segment is often the one the transcoder never wrote.
            // Keep the rendition if any of the first few segments answers 2xx.
            const segs = firstPlaylistUris(text, mediaPlaylistUrl, 4);
            const checks = await Promise.all(segs.map((seg) => headOk(seg, 14)));
            ok = checks.some(Boolean);
        }
    } catch (e) {
        ok = false;
    }
    renditionCache.set(mediaPlaylistUrl, { ok, at: Date.now(), ttl: ok ? RENDITION_TTL_MS : 15000 });
    return ok;
}

/**
 * Master playlists currently advertise renditions whose segments 502
 * ("origin unavailable"). Drop those so the player does not start on a dead level.
 * Returns { text, ok, dropped }.
 */
async function keepPlayableRenditions(text, masterUrl) {
    if (!/#EXT-X-STREAM-INF:/i.test(text)) {
        return { text, ok: String(text || '').startsWith('#EXTM3U'), dropped: 0 };
    }
    const abs = (u) => {
        try { return /^https?:\/\//i.test(u) ? u : new URL(u, masterUrl).href; }
        catch { return u; }
    };
    const lines = String(text).split(/\r?\n/);
    const jobs = [];
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        // Video renditions already carry mp4a. The separate audio playlist 502s
        // and hls.js will not start the video while that track is required.
        if (/^#EXT-X-MEDIA:/i.test(line) && /TYPE=AUDIO/i.test(line)) {
            jobs.push({ kind: 'media', line: i, url: '' });
            continue;
        }
        const media = /^#EXT-X-MEDIA:/i.test(line) ? line.match(/URI="([^"]+)"/i) : null;
        if (media) jobs.push({ kind: 'media', line: i, url: abs(media[1]) });
        if (/^#EXT-X-STREAM-INF:/i.test(line)) {
            const next = lines[i + 1] ? lines[i + 1].trim() : '';
            if (next && !next.startsWith('#')) jobs.push({ kind: 'var', inf: i, urlLine: i + 1, url: abs(next) });
        }
    }
    const results = await Promise.all(jobs.map(async (job) => ({
        job,
        ok: job.url ? await renditionPlayable(job.url) : false,
    })));
    const dropLine = new Set();
    let videos = 0;
    let videosOk = 0;
    for (const { job, ok } of results) {
        if (job.kind === 'var') {
            videos += 1;
            if (ok) videosOk += 1;
            else {
                dropLine.add(job.inf);
                dropLine.add(job.urlLine);
            }
        } else if (!ok) {
            dropLine.add(job.line);
        }
    }
    if (!videosOk) return { text: '', ok: false, dropped: videos };
    const kept = lines.filter((_, i) => !dropLine.has(i));
    const hasMedia = kept.some((l) => /^#EXT-X-MEDIA:/i.test(l));
    const outLines = hasMedia ? kept : kept.map((l) => l.replace(/,AUDIO="[^"]*"/ig, ''));
    if (videos - videosOk > 0) {
        console.error('cinejoy dropped dead renditions', videos - videosOk, 'of', videos, masterUrl);
    }
    return { text: outLines.join('\n'), ok: true, dropped: videos - videosOk };
}
// status is only "ok" when resolve + live playlist fetch both succeed (#EXTM3U, not 451).
let healthCache = {
    status: null,
    checkedAt: 0,
    provider: null,
    host: null,
    http: null,
    playlistOk: false,
    reason: null,
};
let healthInFlight = null;

// Per-title result cache for /api/cinejoy (resolve + sniff + subs are expensive).
const cinejoyCache = new Map();
const CINEJOY_CACHE_TTL = 3 * 60 * 60 * 1000;
const SUBTITLE_LIST_URL = 'https://subs.wing.st/subtitles';

function subtitleRank(sub) {
    const display = String((sub && sub.display) || '').toLowerCase();
    let score = 0;
    if (sub && sub.type === 'vtt') score += 5;
    if (sub && sub.type === 'srt') score += 2;
    if (/\b(forced|signs|songs?)\b/.test(display)) score -= 80;
    if (/\b(sdh|hearing)\b/.test(display) || /englishhi|\.hi\b/.test(display)) score -= 15;
    return score;
}

function pickSubtitles(list) {
    const groups = new Map();
    for (const sub of Array.isArray(list) ? list : []) {
        if (!sub || typeof sub.url !== 'string' || !/^https:\/\//i.test(sub.url)) continue;
        const lang = String(sub.language || 'und').toLowerCase();
        if (!groups.has(lang)) groups.set(lang, []);
        groups.get(lang).push(sub);
    }
    const order = [...groups.keys()].sort((a, b) => (a === 'en' ? -1 : b === 'en' ? 1 : a.localeCompare(b)));
    const out = [];
    for (const lang of order) {
        const ranked = groups.get(lang).slice().sort((a, b) => subtitleRank(b) - subtitleRank(a));
        const cap = lang === 'en' ? 8 : 2;
        for (const sub of ranked.slice(0, cap)) {
            out.push({
                language: sub.language || lang,
                url: sub.url.replace(/subtitles\.shegu\.st/gi, 'subs.wing.st'),
                type: sub.type || 'srt',
                display: String(sub.display || sub.language || 'Subtitle').slice(0, 180)
            });
        }
    }
    return out;
}

function mergeSubtitles(captions, wingList) {
    const extra = [];
    for (const cap of Array.isArray(captions) ? captions : []) {
        if (!cap) continue;
        const url = cap.url || cap.file || '';
        if (typeof url !== 'string' || !/^https?:\/\//i.test(url)) continue;
        extra.push({
            language: cap.language || cap.lang || 'und',
            url,
            type: cap.type || cap.format || 'vtt',
            display: cap.display || cap.label || cap.language || 'Subtitle'
        });
    }
    return pickSubtitles(extra.concat(Array.isArray(wingList) ? wingList : []));
}

async function fetchWingSubtitles(isTv, tmdb, season, episode) {
    const id = parseInt(tmdb, 10);
    if (!Number.isInteger(id) || id < 1) return [];
    const q = new URLSearchParams({ type: isTv ? 'tv' : 'movie', tmdb: String(id) });
    if (isTv) {
        q.set('season', String(Number.isInteger(parseInt(season, 10)) && parseInt(season, 10) > 0 ? parseInt(season, 10) : 1));
        q.set('episode', String(Number.isInteger(parseInt(episode, 10)) && parseInt(episode, 10) > 0 ? parseInt(episode, 10) : 1));
    }
    try {
        const r = await fetch(SUBTITLE_LIST_URL + '?' + q.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Origin': 'https://cinejoy.pk',
                'Referer': 'https://cinejoy.pk/'
            },
            signal: AbortSignal.timeout(8000)
        });
        if (!r.ok) return [];
        const data = await r.json();
        return Array.isArray(data) ? data : ((data && data.subtitles) || []);
    } catch (e) {
        console.error('subtitle list error:', e.message);
        return [];
    }
}

// CDN-side block cooldown: after a 451/403 we stop touching that host for a while.
const blockedHosts = new Map();
const BLOCK_COOLDOWN_MS = 30 * 60 * 1000;

function hostBlockedUntil(host) {
    return blockedHosts.get(host) || 0;
}

function isHostBlocked(host) {
    return hostBlockedUntil(host) > Date.now();
}

function markHostBlocked(host) {
    if (!host) return;
    blockedHosts.set(host, Date.now() + BLOCK_COOLDOWN_MS);
    console.error('CDN block on ' + host + ' — cooling down ' + (BLOCK_COOLDOWN_MS / 60000) + 'min');
}

function looksLikeStreamUrl(url) {
    return typeof url === 'string' && /^https?:\/\//i.test(url) && url.length > 12;
}

function healthPayload() {
    return {
        status: healthCache.status || 'fail',
        playlistOk: !!healthCache.playlistOk,
        provider: healthCache.provider || null,
        host: healthCache.host || null,
        http: healthCache.http,
        reason: healthCache.reason || null,
        tmdb: healthCache.tmdb || null,
        checkedAt: healthCache.checkedAt || null,
    };
}

const warmingUrls = new Map();
/**
 * Some titles answer with a master playlist but the CDN has not built the renditions yet
 * (first rendition fetch takes 20s+, first segments longer). Poke them in the background so a
 * retry a few seconds later finds a warm cache, the same thing cinejoy.pk's own player does by waiting.
 */
function warmUpMaster(masterText, masterUrl) {
    const last = warmingUrls.get(masterUrl);
    if (last && Date.now() - last < 120000) return;
    warmingUrls.set(masterUrl, Date.now());
    if (warmingUrls.size > 200) warmingUrls.delete(warmingUrls.keys().next().value);
    const { impersonatedRequest } = require('./browserfetch.js');
    const uris = [];
    for (const line of String(masterText).split(/\r?\n/)) {
        const t = line.trim();
        if (t && !t.startsWith('#')) {
            try { uris.push(new URL(t, masterUrl).href); } catch (e) {}
        }
    }
    // Lowest rendition first: that is what a player starts on.
    uris.slice(0, 2).forEach((u) => {
        (async () => {
            try {
                const up = await impersonatedRequest(u, { headers: CINEJOY_CDN_HEADERS, timeoutMs: 90000 });
                const text = up && up.body ? up.body.toString('latin1') : '';
                if (up && up.status >= 200 && up.status < 300 && text.startsWith('#EXTM3U')) {
                    await Promise.all(firstPlaylistUris(text, u, 3).map((seg) => headOk(seg, 60)));
                    renditionCache.delete(u);
                }
            } catch (e) { /* best effort */ }
        })();
    });
}

/** Fetch master playlist and require a real #EXTM3U body (not 451/403/empty). */
async function probePlaylistPlayable(streamUrl) {
    const { impersonatedRequest } = require('./browserfetch.js');
    const up = await Promise.race([
        impersonatedRequest(streamUrl, {
            headers: { Referer: 'https://cinejoy.pk/', Origin: 'https://cinejoy.pk' },
            timeoutMs: 15000,
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('playlist probe timeout')), 16000)),
    ]);
    const status = up && up.status;
    const head = up && up.body ? up.body.subarray(0, 16).toString('latin1') : '';
    if (status === 451 || status === 403) {
        try { markHostBlocked(new URL(streamUrl).hostname); } catch {}
        return { ok: false, http: status, reason: 'cdn_blocked_' + status };
    }
    if (status < 200 || status >= 300) {
        const hint = up && up.body ? up.body.subarray(0, 60).toString('latin1').trim() : '';
        const reason = /transcoding on node|origin unavailable/i.test(hint)
            ? hint.slice(0, 60)
            : ('playlist_http_' + status);
        return { ok: false, http: status, reason };
    }
    if (!head.startsWith('#EXTM3U')) {
        return { ok: false, http: status, reason: 'not_m3u8' };
    }
    const text = up.body.toString('latin1');
    if (/#EXT-X-STREAM-INF:/i.test(text)) {
        const filtered = await keepPlayableRenditions(text, streamUrl);
        if (!filtered.ok) {
            warmUpMaster(text, streamUrl);
            return { ok: false, http: status, reason: 'no_playable_rendition' };
        }
    }
    return { ok: true, http: status, reason: null };
}

async function checkStreamHealth() {
    const now = Date.now();
    const ttl = healthCache.status === 'ok' ? HEALTH_TTL_OK : HEALTH_TTL_FAIL;
    if (healthCache.status && now - healthCache.checkedAt < ttl) return healthCache.status;
    if (healthInFlight) return healthInFlight;
    healthInFlight = (async () => {
        try {
            const cj = require('./cine_api/cinejoy.js');
            const deadline = Date.now() + 45000;
            let lastReason = 'No playable m3u8 from any cinejoy server';
            for (const tmdbId of HEALTH_TMDBS) {
                if (Date.now() > deadline) break;
                try {
                    const info = await resolveCinejoyPreferClean(cj, 'movie', { tmdbId }, '');
                    if (!looksLikeStreamUrl(info && info.url)) {
                        lastReason = 'no_stream_url';
                        continue;
                    }
                    const probe = await probePlaylistPlayable(info.url);
                    if (!probe.ok) {
                        lastReason = (probe.reason || 'playlist_not_playable') + ' tmdb=' + tmdbId;
                        console.error('health skip tmdb', tmdbId, probe.reason || probe.http);
                        continue;
                    }
                    let host = null;
                    try { host = new URL(info.url).hostname; } catch {}
                    healthCache = {
                        status: 'ok',
                        checkedAt: Date.now(),
                        provider: info.provider || null,
                        host,
                        http: probe.http,
                        playlistOk: true,
                        reason: null,
                        tmdb: tmdbId,
                    };
                    return 'ok';
                } catch (e) {
                    lastReason = String(e.message || 'unknown').slice(0, 100);
                    console.error('health skip tmdb', tmdbId, lastReason);
                }
            }
            healthCache = {
                status: 'fail', checkedAt: Date.now(), provider: null, host: null,
                http: null, playlistOk: false, reason: String(lastReason).slice(0, 120),
            };
            void notifyTelegram('API resolving failed.\nHealth probe: ' + String(lastReason).slice(0, 200));
            return 'fail';
        } catch (e) {
            console.error('health:', e.message);
            healthCache = {
                status: 'fail', checkedAt: Date.now(), provider: null, host: null,
                http: null, playlistOk: false, reason: String(e.message || 'unknown').slice(0, 120),
            };
            void notifyTelegram('API resolving failed.\nHealth probe: ' + String(e.message || 'unknown').slice(0, 200));
            return 'fail';
        } finally {
            healthInFlight = null;
        }
    })();
    return healthInFlight;
}

setInterval(() => { void checkStreamHealth(); }, 120000).unref();

function readBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', chunk => {
            body += chunk;
            if (body.length > MAX_BODY) {
                req.destroy();
                reject(new Error('too large'));
            }
        });
        req.on('end', () => resolve(body));
        req.on('error', reject);
    });
}

// Resolve a single TMDB: returns {playlist, error?}
async function resolveStream(provider, tmdbId) {
    try {
        const attest = await _Wa(BASE);
        const encPath = await _ze(_L5.encode(`/${provider}/movie?tmdb=${tmdbId}`));
        const res = await fetch(`${BASE}/${encPath}`, {
            headers: { 'X-Attest': attest }
        });
        const hex = (await res.text()).trim();
        const dec = await _Ue(hex);
        const raw = JSON.parse(new TextDecoder().decode(dec));
        if (raw.stream) {
            raw.stream = raw.stream.map(s => {
                const original = s.playlist || '';
                const primary = original.replace(/https:\/\/[^.]+\.horseapples\.cc/, 'https://' + CDN_HOST);
                return { ...s, playlist: primary, fallbackPlaylist: original !== primary ? original : '' };
            });
        }
        return raw;
    } catch (e) {
        return { error: e.message };
    }
}

function formatRuntime(mins) {
    const n = parseInt(mins, 10);
    if (!n || n < 1) return '';
    const h = Math.floor(n / 60);
    const m = n % 60;
    if (h && m) return `${h}h ${m}m`;
    if (h) return `${h}h`;
    return `${m}m`;
}

function pickLogo(logos = []) {
    const ranked = logos.filter(l => l && l.file_path).sort((a, b) => {
        const score = (l) => {
            let s = l.width || 0;
            if (l.iso_639_1 === 'en') s += 200000;
            else if (!l.iso_639_1) s += 80000;
            const p = l.file_path || '';
            if (p.endsWith('.png') || p.endsWith('.svg')) s += 4000;
            return s;
        };
        return score(b) - score(a);
    });
    return ranked[0] ? TMDB_IMG + ranked[0].file_path : '';
}

function pickCert(type, data) {
    if (type === 'tv') {
        const rows = data.content_ratings?.results || [];
        const us = rows.find(r => r.iso_3166_1 === 'US' && r.rating);
        const any = rows.find(r => r.rating);
        return (us || any)?.rating || '';
    }
    const rows = data.release_dates?.results || [];
    const us = rows.find(r => r.iso_3166_1 === 'US');
    const dates = (us?.release_dates || rows.flatMap(r => r.release_dates || []));
    const withCert = dates.filter(d => d.certification);
    const theatrical = withCert.find(d => d.type === 3);
    return (theatrical || withCert[0])?.certification || '';
}

async function fetchTmdbMeta(type, id, seasonNum) {
    const kind = type === 'tv' ? 'tv' : 'movie';
    const sn = kind === 'tv' ? (parseInt(seasonNum, 10) || 0) : 0;
    const cacheKey = sn > 0 ? `${kind}:${id}:s${sn}` : `${kind}:${id}`;
    const cached = metaCache.get(cacheKey);
    if (cached && Date.now() < cached.expires) return cached.data;

    const extra = kind === 'tv' ? 'images,content_ratings' : 'images,release_dates';
    const url = `https://api.themoviedb.org/3/${kind}/${id}?api_key=${TMDB_KEY}&append_to_response=${extra}&include_image_language=en,null`;
    const r = await __fetch(url, { headers: { Accept: 'application/json' } });
    if (!r.ok) throw new Error('TMDB ' + r.status);
    const d = await r.json();

    const title = d.title || d.name || '';
    const date = d.release_date || d.first_air_date || '';
    const runtime = d.runtime || (d.episode_run_time && d.episode_run_time[0]) || d.last_episode_to_air?.runtime || 0;
    const data = {
        title,
        overview: d.overview || '',
        year: date ? parseInt(date.slice(0, 4), 10) : null,
        runtime: runtime || null,
        runtimeLabel: formatRuntime(runtime),
        rating: typeof d.vote_average === 'number' ? Math.round(d.vote_average * 10) / 10 : null,
        certification: pickCert(kind, d),
        logo: pickLogo(d.images?.logos || []),
        posterPath: d.poster_path || ''
    };

    if (kind === 'tv') {
        data.seasons = (d.seasons || [])
            .filter(s => s && s.season_number > 0)
            .map(s => ({
                season_number: s.season_number,
                name: s.name || ('Season ' + s.season_number),
                episode_count: s.episode_count || 0
            }));
        const useSeason = sn > 0 ? sn : (data.seasons[0] && data.seasons[0].season_number) || 1;
        try {
            const sr = await __fetch(
                `https://api.themoviedb.org/3/tv/${id}/season/${useSeason}?api_key=${TMDB_KEY}`,
                { headers: { Accept: 'application/json' } }
            );
            if (sr.ok) {
                const sd = await sr.json();
                data.seasonName = sd.name || ('Season ' + useSeason);
                data.episodes = (sd.episodes || []).map(e => ({
                    n: e.episode_number,
                    name: e.name || ('Episode ' + e.episode_number),
                    overview: e.overview || '',
                    still: e.still_path ? ('https://image.tmdb.org/t/p/w300' + e.still_path) : '',
                    runtime: e.runtime || null
                }));
            }
        } catch (e) {
            console.error('season meta:', e.message);
        }
        if (!data.episodes) data.episodes = [];
    }

    metaCache.set(cacheKey, { data, expires: Date.now() + META_TTL });
    return data;
}

const server = http.createServer(async (req, res) => {
    applySecurityHeaders(res);

    let url;
    try {
        url = new URL(req.url, `http://localhost:${PORT}`);
    } catch {
        hide(res, 400);
        return;
    }
    const pathname = url.pathname;
    const origin = requestOrigin(req);
    applyCors(res, origin);

    if (req.method === 'OPTIONS') {
        if (!origin) { hide(res, 404, 'Missing Origin'); return; }
        res.writeHead(204);
        res.end();
        return;
    }

    // Health: only "ok" when resolve + live #EXTM3U playlist fetch succeed (not URL-only).
    if (
        pathname === '/' ||
        pathname === '/health' ||
        pathname === '/api/health' ||
        pathname === '/status' ||
        pathname === '/robots.txt'
    ) {
        if (pathname === '/robots.txt') {
            res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('User-agent: *\nDisallow: /\n');
            return;
        }
        // Local player: /?tmdb=123 serves the cinejoy player HTML.
        if (pathname === '/' && url.searchParams.has('tmdb')) {
            applySecurityHeaders(res, { playerPage: true, allowFrame: true });
            fs.readFile(path.join(__dirname, 'cinejoy.html'), (err, buf) => {
                if (err) { res.writeHead(404); res.end('Not Found'); return; }
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                res.end(buf);
            });
            return;
        }
        const status = await checkStreamHealth();
        const body = healthPayload();
        res.writeHead(status === 'ok' ? 200 : 503, {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
        });
        res.end(JSON.stringify(body));
        return;
    }

    if (pathname === '/cinejoy.html') {
        applySecurityHeaders(res, { playerPage: true, allowFrame: true });
        fs.readFile(path.join(__dirname, 'cinejoy.html'), (err, buf) => {
            if (err) { res.writeHead(404); res.end('Not Found'); return; }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(buf);
        });
        return;
    }

    if (pathname === '/logo.png' || pathname === '/favicon.ico') {
        const file = pathname === '/favicon.ico'
            ? path.join(__dirname, 'favicon.ico')
            : path.join(__dirname, 'logo.png');
        serveStaticFile(res, file);
        return;
    }

    if (!pathname.startsWith('/api/')) {
        hide(res, 404, pathname);
        return;
    }

    if (!origin) {
        hide(res, 404, 'Missing Origin ' + pathname);
        return;
    }

    const ip = clientIp(req);
    const isProxy = pathname === '/api/cjproxy' || pathname === '/api/proxy' || pathname === '/api/subtitle' || pathname === '/api/srt2vtt';
    if (tooMany(ip, isProxy ? 'media' : 'api', isProxy ? 800 : 40, 60000)) {
        res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': '60' });
        res.end('{}');
        return;
    }

    if (pathname === '/api/meta') {
        const tmdb = parseInt(url.searchParams.get('tmdb'), 10);
        const type = url.searchParams.get('type') === 'tv' ? 'tv' : 'movie';
        const season = parseInt(url.searchParams.get('season'), 10);
        if (!Number.isInteger(tmdb) || tmdb < 1) { hide(res, 400); return; }
        try {
            const data = await fetchTmdbMeta(type, tmdb, type === 'tv' ? season : 0);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        } catch (e) {
            console.error('meta error:', e.message);
            hide(res, 502);
        }
        return;
    }

    if (pathname === '/api/resolve') {
        if (req.method !== 'POST') { hide(res, 404); return; }
        try {
            const parsed = JSON.parse(await readBody(req));
            const tmdb = parseInt(parsed && parsed.tmdb, 10);
            if (!Number.isInteger(tmdb) || tmdb < 1) { hide(res, 400); return; }
            const data = await resolveStream(typeof parsed.provider === 'string' ? parsed.provider : 'Solstice', tmdb);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        } catch (e) {
            console.error('resolve error:', e.message);
            hide(res, 500);
        }
        return;
    }

    if (pathname === '/api/cinejoy') {
        if (req.method !== 'POST') { hide(res, 404); return; }
        try {
            const parsed = JSON.parse(await readBody(req));
            const tmdb = parseInt(parsed && parsed.tmdb, 10);
            if (!Number.isInteger(tmdb) || tmdb < 1) { hide(res, 400); return; }
            const isTv = parsed.type === 'tv';
            const season = parseInt(parsed.season, 10);
            const episode = parseInt(parsed.episode, 10);
            const preferred = preferredSourceName(parsed && parsed.server);
            const exclude = Array.isArray(parsed && parsed.exclude) ? parsed.exclude : [];
            const excludeKey = exclude.map((s) => preferredSourceName(s) || String(s || '').trim().toLowerCase()).filter(Boolean).sort().join(',');
            const key = (isTv ? 'tv' : 'movie') + ':' + tmdb +
                (isTv ? ':' + (Number.isInteger(season) && season > 0 ? season : 1) + ':' + (Number.isInteger(episode) && episode > 0 ? episode : 1) : '') +
                ':' + (preferred || 'auto') +
                (excludeKey ? ':ex:' + excludeKey : '');
            const cached = cinejoyCache.get(key);
            if (cached && cached.at > Date.now() - CINEJOY_CACHE_TTL) {
                res.writeHead(200, { 'Content-Type': 'application/json', 'X-Cache': 'hit' });
                res.end(JSON.stringify(cached.data));
                return;
            }
            const cj = require('./cine_api/cinejoy.js');
            const resolveCtx = {
                tmdbId: tmdb,
                season: Number.isInteger(season) && season > 0 ? season : undefined,
                episode: Number.isInteger(episode) && episode > 0 ? episode : undefined,
                title: typeof parsed.title === 'string' ? parsed.title.slice(0, 200) : undefined,
                imdbId: typeof parsed.imdb === 'string' ? parsed.imdb.slice(0, 32) : undefined,
                year: parseInt(parsed.year, 10) || undefined
            };
            const subP = fetchWingSubtitles(isTv, tmdb, season, episode);
            // Auto: walk every server until a live m3u8 is confirmed. Never return a dead URL.
            const mediaKind = isTv ? 'tv' : 'movie';
            let info;
            try {
                info = await resolveCinejoyPreferClean(cj, mediaKind, resolveCtx, preferred || '', exclude);
            } catch (firstErr) {
                // Excluding a server only helps if another one works. Without an exclusion
                // the client already retries, so fail fast instead of doubling the wait.
                if (!normalizeExcludeList(exclude).size) throw firstErr;
                console.error('cinejoy retry without exclusions after', String(firstErr && firstErr.message || firstErr).slice(0, 120));
                info = await resolveCinejoyPreferClean(cj, mediaKind, resolveCtx, preferred || '', []);
            }
            const subRes = await subP;
            if (!info || !looksLikeStreamUrl(info.url)) throw new Error('Resolve returned no stream URL');
            let sniff = info._playable && info._playable.kind
                ? { kind: info._playable.kind, maxRes: info._playable.maxRes || 0 }
                : { kind: 'unknown', maxRes: 0 };
            if (sniff.kind === 'unknown' && !isHostBlocked(new URL(info.url).hostname)) {
                sniff = await cj.sniff(info.url);
            }
            if (sniff.kind === 'hls' && !sniff.maxRes) {
                try {
                    const detail = await cj.sniff(info.url);
                    if (detail && detail.maxRes) sniff.maxRes = detail.maxRes;
                } catch (_) {}
            }
            // Master stays on /api/cjproxy so dead renditions can be removed.
            // Variant and segment URLs inside it are rewritten to the CDN (direct),
            // which sends Access-Control-Allow-Origin: * — same path cinejoy.pk uses.
            const playlist = '/api/cjproxy?url=' + encodeURIComponent(info.url);
            const activeInternal = info.provider;
            const data = {
                stream: [{
                    playlist,
                    kind: sniff.kind === 'mp4' ? 'mp4' : 'hls',
                    qualities: sniff.maxRes ? [`${sniff.maxRes}p`] : [],
                    rank: sniff.maxRes || 0,
                    source: 'cinejoy'
                }],
                alternatives: [],
                subtitles: mergeSubtitles(info.captions, subRes),
                meta: { title: typeof parsed.title === 'string' ? parsed.title.slice(0, 200) : String(tmdb) },
                provider: DRAGON_NAMES[activeInternal] || activeInternal,
                providerInternal: activeInternal,
                servers: (info.providers || []).map(p => ({
                    name: DRAGON_NAMES[p.name] || p.name,
                    internal: p.name,
                    status: p.status,
                    active: p.name === activeInternal
                }))
            };
            cinejoyCache.set(key, { at: Date.now(), data });
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        } catch (e) {
            console.error('cinejoy error:', e.message);
            const status = cinejoyFailStatus(e);
            void notifyTelegram('API resolving failed.\nHTTP ' + status + '\n' + String(e.message || '').slice(0, 200));
            res.writeHead(status, { 'Content-Type': 'application/json' });
            const reason = String(e.message || '');
            const code = /No source found/i.test(reason) ? 'no_source'
            : /playlist warming/i.test(reason) ? 'warming'
                : /No playable|not playable|playlist probe/i.test(reason) ? 'no_playable_playlist'
                : status === 451 ? 'blocked' : 'upstream';
            res.end(JSON.stringify({ error: 'Unavailable', status: 'fail', code }));
        }
        return;
    }

    // API: fetch subtitles from arigold
    if (pathname === '/api/subtitles') {
        const tmdb = url.searchParams.get('tmdb');
        if (!tmdb) { res.writeHead(400); res.end('Missing tmdb'); return; }
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
        try {
            const subtitles = await fetchWingSubtitles(false, tmdb, 1, 1);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ subtitles }));
        } catch (e) {
            console.error('subtitles error:', e.message);
            hide(res, 502);
        }
        return;
    }

    // API: convert SRT to VTT
    if (pathname === '/api/srt2vtt') {
        const targetUrl = publicHttpUrl(url.searchParams.get('url'));
        if (!targetUrl) { hide(res, 400); return; }
        const target = targetUrl.href;
        try {
            const srtRes = await fetch(target, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Referer': 'https://www.opensubtitles.org/'
                }
            });
            const srt = await srtRes.text();
            let vtt = 'WEBVTT\n\n';
            vtt += srt
                .replace(/\r\n/g, '\n')
                .replace(/(\d{2}):(\d{2}):(\d{2}),(\d{3})/g, '$1:$2:$3.$4');
            res.writeHead(200, { 'Content-Type': 'text/vtt; charset=utf-8' });
            res.end(vtt);
        } catch (e) {
            console.error('srt2vtt error:', e.message);
            hide(res, 502);
        }
        return;
    }

    // API: fetch subtitle content by URL, return as VTT with correct MIME type
    if (pathname === '/api/subtitle') {
        const rawTarget = String(url.searchParams.get('url') || '').replace(/subtitles\.shegu\.st/gi, 'subs.wing.st');
        const targetUrl = publicHttpUrl(rawTarget);
        if (!targetUrl) { hide(res, 400); return; }
        const target = targetUrl.href;
        try {
            const subRes = await fetch(target, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Referer': 'https://cinejoy.pk/',
                    'Origin': 'https://cinejoy.pk',
                    'Accept': 'text/plain,text/vtt,*/*'
                },
                signal: AbortSignal.timeout(12000),
                redirect: 'follow'
            });
            if (!subRes.ok) {
                res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
                res.end('');
                return;
            }
            let content = await subRes.text();
            if (content.length > 2000000) content = content.slice(0, 2000000);
            content = content.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
            if (/^\s*</.test(content) || /cloudflare|error code:/i.test(content.slice(0, 240))) {
                res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
                res.end('');
                return;
            }
            // Convert SRT to VTT if needed
            if (!content.startsWith('WEBVTT')) {
                content = 'WEBVTT\n\n' + content.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2');
            }
            res.writeHead(200, {
                'Content-Type': 'text/vtt; charset=utf-8',
                'Cache-Control': 'public, max-age=3600'
            });
            res.end(content);
        } catch (e) {
            console.error('subtitle error:', e.message);
            res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('');
        }
        return;
    }

    // Proxy: cinejoy streams (spoofs cinejoy.pk origin, rewrites m3u8, passes Range)
    // ?direct=1 rewrites playlist URLs to absolute CDN URLs so the BROWSER fetches
    // segments directly (server touches the CDN only for the master playlist).
    if (pathname === '/api/cjproxy') {
        const targetUrl = publicHttpUrl(url.searchParams.get('url'));
        if (!targetUrl) { hide(res, 400); return; }
        const corsHdrs = {
            'Access-Control-Allow-Origin': '*',
            'Vary': 'Origin',
            'Access-Control-Allow-Headers': 'Content-Type, Range, X-Flix-Client',
            'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        };
        // 502/504 from this route is replaced by Cloudflare and the CORS headers
        // are stripped, so hls.js reports a fake network error while loading a playlist.
        const playlistFail = (status) => {
            const code = status === 502 || status === 503 || status === 504 ? 404 : status;
            res.writeHead(code, corsHdrs);
            res.end();
        };
        if (req.method === 'OPTIONS') {
            res.writeHead(204, corsHdrs);
            res.end();
            return;
        }
        const target = targetUrl.href;
        const ply = /\.m3u8(?:$|\?)/i.test(target);

        // Playlist fetch: Chrome-impersonating, fully buffered (master/media playlists
        // are small). Everything else keeps the streaming relay below.
        if (ply) {
            const { impersonatedRequest } = require('./browserfetch.js');
            let up;
            for (let attempt = 0; attempt < 3; attempt++) {
                try {
                    up = await impersonatedRequest(target, { headers: { 'Referer': 'https://cinejoy.pk/' }, timeoutMs: 20000 });
                } catch (e) {
                    console.error('cjproxy playlist fetch:', e.message);
                    up = null;
                }
                const body = up && up.body ? up.body.toString('latin1') : '';
                if (up && up.status >= 200 && up.status < 300 && body.startsWith('#EXTM3U')) break;
                up = null;
            }
            if (!up) { playlistFail(404); return; }
            if (up.status === 451 || up.status === 403) markHostBlocked(targetUrl.hostname);
            if (!up.body.length) { playlistFail(404); return; }
            const head = up.body.subarray(0, 16).toString('latin1');
            if (head.startsWith('#EXTM3U')) {
                let playlistText = up.body.toString('latin1');
                const isMaster = /#EXT-X-STREAM-INF:/i.test(playlistText);
                if (isMaster) {
                    const filtered = await keepPlayableRenditions(playlistText, target);
                    if (!filtered.ok) {
                        console.error('cjproxy master has no playable rendition', target);
                        playlistFail(404);
                        return;
                    }
                    playlistText = filtered.text;
                }
                // Masters stay on /api/cjproxy (the CDN playlist itself often 502s
                // in the browser). Segment URLs go direct only when the CDN allows a
                // flixstream.ca origin (like cinejoy.pk does); otherwise they are proxied.
                const rewritten = rewritePlaylist(playlistText, targetUrl, !isMaster && await segmentsDirectOk(playlistText, targetUrl));
                res.writeHead(up.status, Object.assign({
                    'Content-Type': 'application/vnd.apple.mpegurl',
                    'Cache-Control': 'no-store'
                }, corsHdrs));
                res.end(rewritten);
                return;
            }
            // Playlist URL but not an m3u8 (rare): serve buffered with type sniffing.
            let ctype = 'application/octet-stream';
            if (head.includes('ftyp') || up.body.subarray(4, 8).toString('latin1') === 'moof') ctype = 'video/mp4';
            else if (up.body[0] === 0x47) ctype = 'video/mp2t';
            res.writeHead(up.status, Object.assign({ 'Content-Type': ctype, 'Cache-Control': 'no-store' }, corsHdrs));
            res.end(up.body);
            return;
        }

        const hdrs = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://cinejoy.pk',
            'Referer': 'https://cinejoy.pk/'
        };
        if (req.headers.range) hdrs['Range'] = req.headers.range;
        try {
            // Upstream segments sometimes hang or 502 once and then work. A short first
            // attempt plus one retry keeps playback moving instead of burning 20s per gap.
            let proxyRes = null;
            for (let attempt = 0; attempt < 2; attempt++) {
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), attempt === 0 ? 7000 : 12000);
                try {
                    proxyRes = await fetch(target, { headers: hdrs, signal: ctrl.signal });
                    clearTimeout(timer);
                    if (proxyRes.ok || proxyRes.status === 206 || proxyRes.status < 500) {
                        setTimeout(() => ctrl.abort(), 30000).unref();
                        break;
                    }
                    try { await proxyRes.body.cancel(); } catch (_) {}
                } catch (e) {
                    clearTimeout(timer);
                    proxyRes = null;
                }
            }
            if (!proxyRes) { res.writeHead(404, corsHdrs); res.end(); return; }
            if (proxyRes.status === 451 || proxyRes.status === 403) {
                markHostBlocked(targetUrl.hostname);
            }
            if (!proxyRes.ok && proxyRes.status !== 206) {
                // Cloudflare replaces origin 502 pages and strips CORS, which makes
                // hls.js report a fake CORS failure. A missing transcoder segment
                // ("origin unavailable") is a gap, so answer 404 with our CORS headers.
                let status = proxyRes.status;
                if (status === 502 || status === 503 || status === 504) status = 404;
                res.writeHead(status, corsHdrs);
                res.end();
                return;
            }
            const reader = proxyRes.body.getReader();
            const { value: first, done } = await reader.read();
            if (done || !first) { res.writeHead(502, corsHdrs); res.end(); return; }
            const firstBuf = Buffer.from(first);
            const head = firstBuf.subarray(0, 16).toString('latin1');

            if (head.startsWith('#EXTM3U')) {
                let text = firstBuf.toString('latin1');
                for (;;) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    text += Buffer.from(value).toString('latin1');
                }
                const isMaster = /#EXT-X-STREAM-INF:/i.test(text);
                const rewritten = rewritePlaylist(text, targetUrl, !isMaster && await segmentsDirectOk(text, targetUrl));
                res.writeHead(proxyRes.status, Object.assign({
                    'Content-Type': 'application/vnd.apple.mpegurl',
                    'Cache-Control': 'no-store'
                }, corsHdrs));
                res.end(rewritten);
                return;
            }

            let ctype = proxyRes.headers.get('content-type') || 'application/octet-stream';
            if (head.includes('ftyp') || firstBuf.subarray(4, 8).toString('latin1') === 'moof') ctype = 'video/mp4';
            else if (head.charCodeAt(0) === 0x47) ctype = 'video/mp2t';
            const outHdrs = Object.assign({
                'Content-Type': ctype,
                'Accept-Ranges': proxyRes.headers.get('accept-ranges') || 'bytes',
                'Cache-Control': 'no-store'
            }, corsHdrs);
            for (const h of ['content-length', 'content-range']) {
                const v = proxyRes.headers.get(h);
                if (v) outHdrs[h === 'content-length' ? 'Content-Length' : 'Content-Range'] = v;
            }
            res.writeHead(proxyRes.status, outHdrs);
            res.write(first);
            const { Readable } = require('stream');
            const stream = Readable.fromWeb(new ReadableStream({
                async start(controller) {
                    for (;;) {
                        const { value, done } = await reader.read();
                        if (done) { controller.close(); break; }
                        controller.enqueue(value);
                    }
                }
            }));
            stream.on('error', () => {});
            res.on('error', () => stream.destroy());
            stream.pipe(res);
        } catch (e) {
            console.error('cjproxy error:', e.message);
            hide(res, 502);
        }
        return;
    }

    if (pathname === '/api/proxy') {
        const targetUrl = publicHttpUrl(url.searchParams.get('url'));
        if (!targetUrl) { hide(res, 400); return; }
        const target = targetUrl.href;
        try {
            const proxyRes = await fetch(target, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://lordflix.org/',
                    'Origin': 'https://lordflix.org'
                }
            });
            for (const [k, v] of proxyRes.headers.entries()) {
                if (k === 'content-encoding' || k === 'content-length' || k === 'transfer-encoding' || k === 'access-control-allow-origin') continue;
                res.setHeader(k, v);
            }
            if (target.includes('.m3u8') && proxyRes.ok) {
                const text = await proxyRes.text();
                const baseUrl = target.slice(0, target.lastIndexOf('/') + 1);
                const rewritten = text.replace(/^(\S+\.(?:m3u8|ts))$/gm, (_, p) => {
                    const abs = p.startsWith('http') ? p : new URL(p, baseUrl).href;
                    return '/api/proxy?url=' + encodeURIComponent(abs);
                });
                res.writeHead(proxyRes.status, { 'Content-Type': 'application/vnd.apple.mpegurl' });
                res.end(rewritten);
            } else {
                res.writeHead(proxyRes.status);
                const reader = proxyRes.body.getReader();
                const pump = () => reader.read().then(({ done, value }) => {
                    if (done) { res.end(); return; }
                    res.write(value);
                    pump();
                }).catch(() => res.end());
                pump();
            }
        } catch (e) {
            console.error('proxy error:', e.message);
            hide(res, 502);
        }
        return;
    }

    hide(res, 404);
});

server.listen(PORT, '127.0.0.1', () => {
    console.log(`flixstream-backend listening on http://127.0.0.1:${PORT}`);
});
