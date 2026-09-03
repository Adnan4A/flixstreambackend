const http = require('http');
const fs = require('fs');
const path = require('path');
const { webcrypto } = require('crypto');
global.crypto = webcrypto;

// Add browser headers to all outgoing requests (needed by _Wa -> _Ee under the hood)
const __fetch = global.fetch;
global.fetch = async (url, opts = {}) => {
    opts.headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://lordflix.org',
        'Referer': 'https://lordflix.org/',
        ...(opts.headers || {})
    };
    return __fetch(url, opts);
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
const VIDLOVE_API = 'https://api.shows.st';
const VIDLOVE_SOURCES = ['moviebox', 'vidapi', 'ipcloud', 'tcloud', 'vixsrc', '1embed', 'xpass', 'vidrift', 'lookmovie', 'vidnest'];
const PORT = process.env.PORT || 3010;
const HOST = process.env.HOST || '0.0.0.0';

// MIME types
const MIME = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
};

function serveFile(res, filePath) {
    const ext = path.extname(filePath);
    fs.readFile(filePath, (err, data) => {
        if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
        }
        res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
        res.end(data);
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

// Per-title cache: remembers which vidlove source works, so repeat loads skip the full probe
const sourceCache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // cached source is only a hint; the URL is re-fetched each time

// Resolve from 67movies.nl's vidlove backend (no crypto, plain JSON)
// Returns the same contract as resolveStream: {stream: [{playlist}], subtitles}
async function resolveVidlove(type, tmdbId, season, episode, wantSource) {
    const qs = type === 'tv'
        ? `tv?id=${tmdbId}&season=${season}&episode=${episode}&mode=json`
        : `movie?id=${tmdbId}&mode=json`;
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Referer': 'https://player.vidlove.cc/',
        'Origin': 'https://player.vidlove.cc',
        'Accept': '*/*'
    };
    const cacheKey = `${type}:${tmdbId}:${season}:${episode}`;

    // Fast path: a source known to work for this title. One request instead of ten.
    const cached = sourceCache.get(cacheKey);
    if (cached && Date.now() < cached.expires && !wantSource) {
        const hit = await probeVidloveSource(type, tmdbId, season, episode, cached.source, headers, qs);
        if (hit) {
            sourceCache.set(cacheKey, { source: cached.source, expires: Date.now() + CACHE_TTL });
            return {
                stream: [{ playlist: hit.playlist, kind: hit.kind, qualities: hit.qualities, rank: hit.rank }],
                alternatives: [],
                subtitles: hit.subtitles,
                meta: { title: hit.title }
            };
        }
        sourceCache.delete(cacheKey); // stale -> full fallback below
    }

    // Cold path: probe all sources in parallel. Speed is king: return the first
    // success the instant it lands (no grace window). Any better-quality source
    // that finishes before we respond is shipped as `alternatives` so the player
    // can upgrade itself. The true best source is cached in the background.
    const targets = wantSource ? [wantSource] : VIDLOVE_SOURCES;
    let firstResult = null;
    let best = null;
    const completed = [];
    const all = targets.map(src => probeVidloveSource(type, tmdbId, season, episode, src, headers, qs)
        .catch(() => null)
        .then(r => {
            if (!r) return r;
            completed.push(r);
            if (!best || r.rank > best.rank) best = r;
            if (!firstResult) firstResult = r;
            return r;
        }));
    const firstSuccess = new Promise(res => {
        let pending = all.length;
        all.forEach(p => p.then(r => { if (r) res(r); else if (--pending === 0) res(null); }));
    });
    const first = await firstSuccess;
    if (!first) return { error: 'No source available on vidlove' };

    // Upgrade hint: probes that beat the first-returned source on quality
    const alternatives = completed
        .filter(r => r !== firstResult && r.rank > firstResult.rank)
        .sort((a, b) => b.rank - a.rank)
        .map(a => ({ playlist: a.playlist, kind: a.kind, qualities: a.qualities, rank: a.rank, source: a.source }));

    // Background: once every probe settles, remember the best source for next load
    if (!wantSource) {
        Promise.all(all).then(() => {
            if (best) sourceCache.set(cacheKey, { source: best.source, expires: Date.now() + CACHE_TTL });
        }).catch(() => {});
    }
    return {
        stream: [{ playlist: firstResult.playlist, kind: firstResult.kind, qualities: firstResult.qualities, rank: firstResult.rank }],
        alternatives,
        subtitles: firstResult.subtitles,
        meta: { title: firstResult.title }
    };
}

async function probeVidloveSource(type, tmdbId, season, episode, source, headers, qs) {
    const res = await fetch(`${VIDLOVE_API}/${qs}&sources=${source}`, { headers, signal: AbortSignal.timeout(2000) });
    if (!res.ok) return null;
    const data = await res.json();
    const s = data.source;
    if (!s || !s.url) return null;
    const sniff = (s.type || '').includes('mp4')
        ? { kind: 'mp4', maxRes: qualityRank(s.qualities) }
        : await sniffStream(s.url, headers);
    const rank = Math.max(sniff.maxRes, qualityRank(s.qualities));
    return {
        rank,
        source,
        kind: sniff.kind,
        playlist: s.url,
        qualities: s.qualities || [],
        subtitles: (data.subtitles || []).map(sub => ({
            display: sub.label || sub.language,
            url: sub.file,
            language: sub.language || 'en',
            source: 'vidlove',
            isHearingImpaired: /hi\b|hearing/i.test(sub.label || '')
        })),
        title: data.meta?.title || data.meta?.name || type === 'tv' ? `${tmdbId} S${season}E${episode}` : tmdbId
    };
}

// Extract the best declared quality (1080p / 4K / 2160p ...) from a qualities array
function qualityRank(qualities) {
    if (!Array.isArray(qualities)) return 0;
    let max = 0;
    for (const q of qualities) {
        const s = String(q).toLowerCase();
        const m = s.match(/(\d{3,4})\s*p/i);
        let h = m ? parseInt(m[1]) : 0;
        if (!h && (s.includes('4k') || s.includes('2160'))) h = 2160;
        max = Math.max(max, h);
    }
    return max;
}

// Fetch the first bytes of a URL to detect HLS playlist vs direct MP4
// For HLS, also reports the max RESOLUTION found in the master playlist
async function sniffStream(url, headers) {
    try {
        const res = await fetch(url, { headers, signal: AbortSignal.timeout(2000) });
        let buf = Buffer.alloc(0);
        const reader = res.body.getReader();
        for (let i = 0; i < 4; i++) {
            const { value, done } = await reader.read();
            if (done || !value) break;
            buf = Buffer.concat([buf, Buffer.from(value)]);
            if (buf.length >= 65536) break;
        }
        await reader.cancel();
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

const server = http.createServer(async (req, res) => {
    // CORS
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    // Parse URL and body
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = url.pathname;

    // API: resolve a movie
    if (pathname === '/api/resolve') {
        if (req.method !== 'POST') { res.writeHead(405); res.end('POST only'); return; }
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { tmdb, provider } = JSON.parse(body);
                const data = await resolveStream(provider || 'Solstice', parseInt(tmdb) || 550);
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(data));
            } catch (e) {
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // API: resolve from vidlove (67movies backend)
    if (pathname === '/api/vidlove') {
        if (req.method !== 'POST') { res.writeHead(405); res.end('POST only'); return; }
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { tmdb, type, season, episode, source } = JSON.parse(body);
                const data = await resolveVidlove(
                    type === 'tv' ? 'tv' : 'movie',
                    parseInt(tmdb) || 550,
                    parseInt(season) || 1,
                    parseInt(episode) || 1,
                    source
                );
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(data));
            } catch (e) {
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
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
            res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(JSON.stringify(data));
        } catch (e) {
            res.writeHead(502);
            res.end(JSON.stringify({ error: e.message }));
        }
        return;
    }

    // API: convert SRT to VTT
    if (pathname === '/api/srt2vtt') {
        const target = url.searchParams.get('url');
        if (!target) { res.writeHead(400); res.end('Missing url'); return; }
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
            res.writeHead(200, { 'Content-Type': 'text/vtt; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
            res.end(vtt);
        } catch (e) {
            res.writeHead(502);
            res.end(JSON.stringify({ error: e.message }));
        }
        return;
    }

    // API: fetch subtitle content by URL, return as VTT with correct MIME type
    if (pathname === '/api/subtitle') {
        const target = url.searchParams.get('url');
        if (!target) { res.writeHead(400); res.end('Missing url'); return; }
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
            res.writeHead(200, { 'Content-Type': 'text/vtt; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
            res.end(content);
        } catch (e) {
            res.writeHead(502);
            res.end(JSON.stringify({ error: e.message }));
        }
        return;
    }

    // Proxy: vidlove streams (spoofs player.vidlove.cc origin, rewrites m3u8, passes Range)
    if (pathname === '/api/vlproxy') {
        const target = url.searchParams.get('url');
        if (!target) { res.writeHead(400); res.end('Missing url'); return; }
        const hdrs = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://player.vidlove.cc',
            'Referer': 'https://player.vidlove.cc/'
        };
        if (req.headers.range) hdrs['Range'] = req.headers.range;
        try {
            const proxyRes = await fetch(target, { headers: hdrs });
            if (!proxyRes.ok && proxyRes.status !== 206) {
                res.writeHead(proxyRes.status);
                res.end();
                return;
            }
            // Sniff the first chunk to classify the payload
            const reader = proxyRes.body.getReader();
            const { value: first, done } = await reader.read();
            if (done || !first) { res.writeHead(502); res.end(); return; }
            const firstBuf = Buffer.from(first);
            const head = firstBuf.subarray(0, 16).toString('latin1');

            if (head.startsWith('#EXTM3U')) {
                // HLS playlist: read the rest, rewrite all absolute URL lines through the proxy
                let text = firstBuf.toString('latin1');
                for (;;) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    text += Buffer.from(value).toString('latin1');
                }
                const rewritten = text.replace(/^https?:\/\/\S+$/gm, line =>
                    '/api/vlproxy?url=' + encodeURIComponent(line));
                res.writeHead(proxyRes.status, {
                    'Content-Type': 'application/vnd.apple.mpegurl',
                    'Cache-Control': 'no-store',
                    'Access-Control-Allow-Origin': '*'
                });
                res.end(rewritten);
                return;
            }

            // Binary: stream through with sniffed type and range passthrough
            let ctype = proxyRes.headers.get('content-type') || 'application/octet-stream';
            if (head.includes('ftyp')) ctype = 'video/mp4';
            else if (head.charCodeAt(0) === 0x47) ctype = 'video/mp2t';
            const outHdrs = {
                'Content-Type': ctype,
                'Accept-Ranges': proxyRes.headers.get('accept-ranges') || 'bytes',
                'Cache-Control': 'no-store',
                'Access-Control-Allow-Origin': '*'
            };
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
            console.error('vlproxy error:', e.message);
            res.writeHead(502);
            res.end(JSON.stringify({ error: e.message }));
        }
        return;
    }

    // Proxy: fetch HLS content from horseapples.cc with proper headers
    if (pathname === '/api/proxy') {
        const target = url.searchParams.get('url');
        if (!target) { res.writeHead(400); res.end('Missing url'); return; }
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
                if (k === 'content-encoding' || k === 'content-length' || k === 'transfer-encoding') continue;
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
            res.writeHead(502);
            res.end(JSON.stringify({ error: e.message }));
        }
        return;
    }

    // Root: API info (this build is a backend-only deployment)
    if (pathname === '/' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            name: 'flixstream-backend',
            version: '1.2.0',
            description: 'vidlove (67movies) stream resolver + media proxy. Speed-first: first working source returns immediately, better-quality sources reported in `alternatives` so clients can upgrade. 2s caps.',
            endpoints: {
                'POST /api/vidlove': '{"tmdb":550,"type":"movie|tv","season":1,"episode":1,"source":"vidapi"}',
                'GET /api/vlproxy': '/api/vlproxy?url=<encoded playlist>',
                'GET /api/srt2vtt': '/api/srt2vtt?url=<encoded .srt>'
            },
            notes: [
                'Omit `source` to auto-pick; the player upgrades to `alternatives[0]` if it beats stream[0].rank.',
                'Playlist URLs expire after a few minutes; re-resolve per load.'
            ]
        }));
        return;
    }

    // Serve static files
    const filePath = pathname === '/' 
        ? path.join(__dirname, 'player.html')
        : path.join(__dirname, pathname);

    serveFile(res, filePath);
});

server.listen(PORT, HOST, () => {
    console.log(`flixstream-backend (API only) listening on http://${HOST}:${PORT}`);
});
