const http = require('http');
const fs = require('fs');
const path = require('path');
const { webcrypto } = require('crypto');
global.crypto = webcrypto;

// Lordflix/Solstice needs browser Origin. Do not stamp that Origin onto cinejoy/shegu fetches.
const __fetch = global.fetch;
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
function mergeHeaders(base, incoming) {
    const out = { ...base };
    if (!incoming) return out;
    if (typeof incoming.forEach === 'function') {
        incoming.forEach((value, key) => { out[key] = value; });
        return out;
    }
    return { ...out, ...incoming };
}
global.fetch = async (url, opts = {}) => {
    const href = typeof url === 'string' ? url : (url && (url.url || url.href)) || '';
    // NOTE: do NOT route api.shegu.st through browserfetch here — resolve POSTs a body
    // and impersonatedRequest is playlist-oriented (no body). Native fetch works for shegu;
    // Chrome impersonation stays on /api/cjproxy playlist fetches only.
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

// Prefer clean CDNs (lol.movieboxnoob / shegu) before Lisbon's flagged info.* host.
const CLEAN_SOURCES = ['Solara', 'Castle', 'Joy'];
const LAST_RESORT_SOURCES = ['Lisbon'];

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

/** Sequential Solara → Castle → Joy → Lisbon. One hit per step; stop on first clean URL. */
async function resolveCinejoyPreferClean(cj, mediaType, ctx, preferred) {
    const order = [];
    const push = (s) => { if (s && !order.includes(s)) order.push(s); };
    push(preferred);
    for (const s of CLEAN_SOURCES) push(s);
    for (const s of LAST_RESORT_SOURCES) push(s);

    let lastErr = null;
    for (const src of order) {
        const allowDirty = LAST_RESORT_SOURCES.includes(src);
        try {
            const info = await cj.resolve(mediaType, ctx, undefined, src);
            if (!info || !looksLikeStreamUrl(info.url)) continue;
            if (!streamHostAcceptable(info.url, allowDirty)) {
                console.error('cinejoy skip', src, 'unacceptable host', (() => { try { return new URL(info.url).hostname; } catch { return '?'; } })());
                continue;
            }
            return info;
        } catch (e) {
            lastErr = e;
            console.error('cinejoy try', src + ':', e.message);
        }
    }
    throw lastErr || new Error('Resolve returned no stream URL');
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

function requestOrigin(req) {
    const origin = req.headers.origin;
    if (origin && isAllowedOrigin(origin)) return origin;
    const refererOrigin = parseOrigin(req.headers.referer);
    if (refererOrigin && isAllowedOrigin(refererOrigin)) return refererOrigin;
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

function applySecurityHeaders(res) {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('X-Powered-By', '');
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

const HEALTH_TMDB = 550;
const HEALTH_TTL_OK = 90 * 1000;
const HEALTH_TTL_FAIL = 45 * 1000;
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
        checkedAt: healthCache.checkedAt || null,
    };
}

/** Fetch master playlist and require a real #EXTM3U body (not 451/403/empty). */
async function probePlaylistPlayable(streamUrl) {
    const { impersonatedRequest } = require('./browserfetch.js');
    const up = await Promise.race([
        impersonatedRequest(streamUrl, {
            headers: { Referer: 'https://cinejoy.to/', Origin: 'https://cinejoy.to' },
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
        return { ok: false, http: status, reason: 'playlist_http_' + status };
    }
    if (!head.startsWith('#EXTM3U')) {
        return { ok: false, http: status, reason: 'not_m3u8' };
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
            const info = await Promise.race([
                resolveCinejoyPreferClean(cj, 'movie', { tmdbId: HEALTH_TMDB }, ''),
                new Promise((_, reject) => setTimeout(() => reject(new Error('resolve timeout')), 20000)),
            ]);
            if (!looksLikeStreamUrl(info && info.url)) {
                healthCache = {
                    status: 'fail', checkedAt: Date.now(), provider: info && info.provider || null,
                    host: null, http: null, playlistOk: false, reason: 'no_stream_url',
                };
                void notifyTelegram('API resolving failed.\nHealth probe: no HLS URL');
                return 'fail';
            }
            let host = null;
            try { host = new URL(info.url).hostname; } catch {}
            // URL alone is NOT enough — Lisbon used to resolve while CDN returned 451.
            const probe = await probePlaylistPlayable(info.url);
            healthCache = {
                status: probe.ok ? 'ok' : 'fail',
                checkedAt: Date.now(),
                provider: info.provider || null,
                host,
                http: probe.http,
                playlistOk: !!probe.ok,
                reason: probe.reason,
            };
            if (!probe.ok) {
                void notifyTelegram(
                    'API resolving failed.\nHealth probe: playlist not playable\n' +
                    'provider=' + (info.provider || '?') + ' host=' + (host || '?') +
                    ' http=' + probe.http + ' reason=' + probe.reason
                );
            }
            return healthCache.status;
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
        fs.readFile(path.join(__dirname, 'cinejoy.html'), (err, buf) => {
            if (err) { res.writeHead(404); res.end('Not Found'); return; }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(buf);
        });
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
            const key = (isTv ? 'tv' : 'movie') + ':' + tmdb +
                (isTv ? ':' + (Number.isInteger(season) && season > 0 ? season : 1) + ':' + (Number.isInteger(episode) && episode > 0 ? episode : 1) : '') +
                ':' + (preferred || 'auto');
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
            const subP = fetch(`https://subtitles.shegu.st/subtitles?type=${isTv ? 'tv' : 'movie'}&tmdb=${tmdb}` +
                (isTv ? `&season=${Number.isInteger(season) && season > 0 ? season : 1}&episode=${Number.isInteger(episode) && episode > 0 ? episode : 1}` : ''), {
                    headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36' },
                    signal: AbortSignal.timeout(8000)
                })
                .then(r => r.json())
                .catch(() => ({ subtitles: [] }));
            // Prefer Solara/Castle/Joy (clean CDNs); Lisbon only as last resort.
            // Sequential — never fan-out — so we don't burst api.shegu.st.
            const info = await resolveCinejoyPreferClean(cj, isTv ? 'tv' : 'movie', resolveCtx, preferred || '');
            const subRes = await subP;
            if (!info || !looksLikeStreamUrl(info.url)) throw new Error('Resolve returned no stream URL');
            // Sniff only when the CDN host is not in cooldown; otherwise report unknown.
            let sniff = { kind: 'unknown', maxRes: 0 };
            if (!isHostBlocked(new URL(info.url).hostname)) {
                sniff = await cj.sniff(info.url);
            }
            // Keep playlist URLs on /api/cjproxy (no direct=1). CDN hosts 403 browser
            // Origin / lack CORS, so HLS.js must load variants+segments via our proxy.
            const playlist = '/api/cjproxy?url=' + encodeURIComponent(info.url);
            const data = {
                stream: [{
                    playlist,
                    kind: sniff.kind,
                    qualities: sniff.maxRes ? [`${sniff.maxRes}p`] : [],
                    rank: sniff.maxRes || 0,
                    source: 'cinejoy'
                }],
                alternatives: [],
                subtitles: [...(info.captions || []), ...(subRes.subtitles || [])],
                meta: { title: typeof parsed.title === 'string' ? parsed.title.slice(0, 200) : String(tmdb) },
                provider: DRAGON_NAMES[info.provider] || info.provider,
                servers: (info.providers || []).map(p => ({ name: DRAGON_NAMES[p.name] || p.name, status: p.status, active: p.name === info.provider }))
            };
            cinejoyCache.set(key, { at: Date.now(), data });
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
        } catch (e) {
            console.error('cinejoy error:', e.message);
            const status = cinejoyFailStatus(e);
            void notifyTelegram('API resolving failed.\nHTTP ' + status + '\n' + String(e.message || '').slice(0, 200));
            res.writeHead(status, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Unavailable', status: 'fail' }));
        }
        return;
    }

    // API: fetch subtitles from arigold
    if (pathname === '/api/subtitles') {
        const tmdb = url.searchParams.get('tmdb');
        if (!tmdb) { res.writeHead(400); res.end('Missing tmdb'); return; }
        if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
        try {
            const subRes = await fetch(`https://subtitles.arigold.ru/subtitles?type=movie&tmdb=${tmdb}`, {
                headers: { 'Origin': 'https://subtitles.arigold.ru', 'Referer': 'https://subtitles.arigold.ru/' }
            });
            const data = await subRes.json();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
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
        const targetUrl = publicHttpUrl(url.searchParams.get('url'));
        if (!targetUrl) { hide(res, 400); return; }
        const target = targetUrl.href;
        try {
            const subRes = await fetch(target, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Referer': 'https://www.opensubtitles.org/',
                    'Accept': '*/*'
                }
            });
            let content = await subRes.text();
            // Convert SRT to VTT if needed
            content = content.replace(/\r\n/g, '\n');
            if (!content.startsWith('WEBVTT')) {
                content = 'WEBVTT\n\n' + content.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2');
            }
            res.writeHead(200, { 'Content-Type': 'text/vtt; charset=utf-8' });
            res.end(content);
        } catch (e) {
            console.error('subtitle error:', e.message);
            hide(res, 502);
        }
        return;
    }

    // Proxy: cinejoy streams (spoofs cinejoy.to origin, rewrites m3u8, passes Range)
    // ?direct=1 rewrites playlist URLs to absolute CDN URLs so the BROWSER fetches
    // segments directly (server touches the CDN only for the master playlist).
    if (pathname === '/api/cjproxy') {
        const targetUrl = publicHttpUrl(url.searchParams.get('url'));
        if (!targetUrl) { hide(res, 400); return; }
        const direct = url.searchParams.get('direct') === '1';
        const corsHdrs = { 'Access-Control-Allow-Origin': '*', 'Vary': 'Origin', 'Access-Control-Allow-Headers': 'Range' };
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
            try {
                up = await impersonatedRequest(target, { headers: { 'Referer': 'https://cinejoy.to/' }, timeoutMs: 20000 });
            } catch (e) {
                console.error('cjproxy playlist fetch:', e.message);
                hide(res, 502);
                return;
            }
            if (up.status === 451 || up.status === 403) markHostBlocked(targetUrl.hostname);
            if (!up.body.length) { res.writeHead(502, corsHdrs); res.end(); return; }
            const head = up.body.subarray(0, 16).toString('latin1');
            if (head.startsWith('#EXTM3U')) {
                const rewritten = rewritePlaylist(up.body.toString('latin1'), targetUrl, direct);
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
            'Origin': 'https://cinejoy.to',
            'Referer': 'https://cinejoy.to/'
        };
        if (req.headers.range) hdrs['Range'] = req.headers.range;
        try {
            const proxyRes = await fetch(target, { headers: hdrs, signal: AbortSignal.timeout(20000) });
            if (proxyRes.status === 451 || proxyRes.status === 403) {
                markHostBlocked(targetUrl.hostname);
            }
            if (!proxyRes.ok && proxyRes.status !== 206) {
                res.writeHead(proxyRes.status, corsHdrs);
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
                const rewritten = rewritePlaylist(text, targetUrl, direct);
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
