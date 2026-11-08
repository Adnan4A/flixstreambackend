'use strict';
// Free-proxy pool for hosts that reject datacenter IPs (lightspeed.st).
// The pool is refreshed in the background and every candidate is verified against a
// real playlist from the blocked host before it is used.
const { spawn } = require('child_process');

const PROXIED_HOST = /(^|\.)lightspeed\.st$/i;
const LIST_URLS = [
    'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=6000&country=all&ssl=all&anonymity=all',
    'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
    'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
];
const POOL_SIZE = 24;
const CANDIDATES = 700;
const TEST_CONCURRENCY = 80;
const REFRESH_MS = 8 * 60 * 1000;
const CDN_HEADERS = { Origin: 'https://cinejoy.pk', Referer: 'https://cinejoy.pk/' };

const WARP = process.env.WARP_PROXY === '' ? '' : (process.env.WARP_PROXY || 'socks5h://127.0.0.1:40000');
let warpFails = 0;
let warpDownUntil = 0;
let pool = []; // { addr, ms, fails }
let refreshing = null;
let lastRefresh = 0;
let probeProvider = null;

function shouldProxy(url) {
    try { return PROXIED_HOST.test(new URL(url).hostname); } catch (e) { return false; }
}

function curlVia(addr, url, { headers = CDN_HEADERS, timeoutMs = 10000, method = 'GET', maxBytes = 40 * 1024 * 1024, onChild } = {}) {
    return new Promise((resolve) => {
        const args = ['-s', '-m', String(Math.max(1, Math.ceil(timeoutMs / 1000))), '--connect-timeout', '5',
            '-x', /^socks/.test(addr) ? addr : 'http://' + addr, '-X', method, '-w', '\n__CODE__%{http_code}'];
        for (const [k, v] of Object.entries(headers)) args.push('-H', k + ': ' + v);
        args.push(url);
        const child = spawn('/usr/bin/curl', args, { stdio: ['ignore', 'pipe', 'ignore'] });
        if (onChild) onChild(child);
        const chunks = [];
        let size = 0;
        const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) {} }, timeoutMs + 2000);
        child.stdout.on('data', (d) => {
            size += d.length;
            if (size > maxBytes) { try { child.kill('SIGKILL'); } catch (e) {} return; }
            chunks.push(d);
        });
        child.on('error', () => { clearTimeout(timer); resolve({ status: 0, body: Buffer.alloc(0) }); });
        child.on('close', () => {
            clearTimeout(timer);
            const buf = Buffer.concat(chunks);
            const tail = buf.subarray(Math.max(0, buf.length - 24)).toString('latin1');
            const m = tail.match(/\n__CODE__(\d{3})$/);
            if (!m) { resolve({ status: 0, body: Buffer.alloc(0) }); return; }
            const cut = buf.length - ('\n__CODE__' + m[1]).length;
            resolve({ status: parseInt(m[1], 10), body: buf.subarray(0, cut) });
        });
    });
}

async function fetchList(url) {
    try {
        const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
        const t = await r.text();
        return t.split(/\r?\n/).map((s) => s.trim()).filter((s) => /^\d+\.\d+\.\d+\.\d+:\d+$/.test(s));
    } catch (e) { return []; }
}

async function findProbeUrl() {
    if (!probeProvider) probeProvider = () => null;
    return probeProvider();
}

function setProbeProvider(fn) { probeProvider = fn; }

async function refresh() {
    if (refreshing) return refreshing;
    refreshing = (async () => {
        const probe = await findProbeUrl();
        if (!probe) { console.error('proxypool: no probe url'); return; }
        const lists = await Promise.all(LIST_URLS.map(fetchList));
        const seen = new Set(pool.map((p) => p.addr));
        let all = Array.from(new Set([].concat(...lists))).filter((a) => !seen.has(a));
        for (let i = all.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [all[i], all[j]] = [all[j], all[i]];
        }
        all = all.slice(0, CANDIDATES);
        const good = [];
        let idx = 0;
        async function worker() {
            while (idx < all.length && good.length < POOL_SIZE * 3) {
                const addr = all[idx++];
                const t0 = Date.now();
                const r = await curlVia(addr, probe, { timeoutMs: 7000 });
                if (r.status === 200 && r.body.subarray(0, 7).toString('latin1') === '#EXTM3U') {
                    good.push({ addr, ms: Date.now() - t0, fails: 0, master: r.body.toString('latin1') });
                }
            }
        }
        await Promise.all(Array.from({ length: TEST_CONCURRENCY }, worker));
        let segUrl = null;
        try {
            const m = good[0] && good[0].master.split(/\r?\n/).find((l) => l && !l.startsWith('#'));
            if (m) {
                const vurl = new URL(m, probe).href;
                for (const g of good.slice(0, 5)) {
                    const v = await curlVia(g.addr, vurl, { timeoutMs: 8000 });
                    const t = v.body.toString('latin1');
                    const seg = t.split(/\r?\n/).filter((l) => l && !l.startsWith('#'))[1];
                    if (v.status === 200 && seg) { segUrl = new URL(seg, vurl).href; break; }
                }
            }
        } catch (e) {}
        if (segUrl) {
            let k = 0;
            async function speed() {
                while (k < good.length) {
                    const g = good[k++];
                    const t0 = Date.now();
                    const r = await curlVia(g.addr, segUrl, { timeoutMs: 9000 });
                    if (r.status === 200 && r.body.length > 50000) g.bps = r.body.length / Math.max(0.2, (Date.now() - t0) / 1000);
                    else g.bps = 0;
                }
            }
            await Promise.all(Array.from({ length: 12 }, speed));
            for (let i = good.length - 1; i >= 0; i--) if (!(good[i].bps > 120000)) good.splice(i, 1);
        }
        for (const g of good) delete g.master;
        const kept = pool.filter((p) => p.fails < 2);
        pool = kept.concat(good).sort((a, b) => (b.bps || 0) - (a.bps || 0)).slice(0, POOL_SIZE);
        lastRefresh = Date.now();
        console.error('proxypool: refreshed, ' + pool.length + ' working proxies (tested ' + all.length + ')');
    })().catch((e) => console.error('proxypool refresh error:', e.message)).finally(() => { refreshing = null; });
    return refreshing;
}

function ensureFresh() {
    if (refreshing) return;
    if (Date.now() - lastRefresh > REFRESH_MS || (pool.length < 6 && Date.now() - lastRefresh > 20000)) void refresh();
}

async function request(url, { headers, timeoutMs = 12000, method = 'GET' } = {}) {
    const merged0 = Object.assign({}, CDN_HEADERS, headers || {});
    if (WARP && Date.now() > warpDownUntil) {
        for (let i = 0; i < 2; i++) {
            const r = await curlVia(WARP, url, { headers: merged0, timeoutMs: Math.min(timeoutMs, 25000), method });
            const removed = r.status === 404 && r.body.length <= 8 && r.body.toString('latin1').trim() === 'Removed';
            if (r.status !== 0 && !removed && r.status !== 403 && r.status !== 429) { warpFails = 0; return { status: r.status, body: r.body, headers: {} }; }
        }
        if (++warpFails >= 3) { warpDownUntil = Date.now() + 60000; warpFails = 0; }
    }
    if (!pool.length) await refresh();
    else ensureFresh();
    const tried = new Set();
    const running = new Set();
    const merged = Object.assign({}, CDN_HEADERS, headers || {});
    const deadline = Date.now() + timeoutMs + 15000;
    const launch = () => {
        const cands = pool.filter((p) => !tried.has(p.addr));
        if (!cands.length) return null;
        cands.sort((a, b) => (b.bps || 150000) - (a.bps || 150000));
        const pick = cands[Math.floor(Math.random() * Math.min(cands.length, 6))];
        tried.add(pick.addr);
        const handle = { pick, child: null, t0: Date.now() };
        handle.promise = curlVia(pick.addr, url, {
            headers: merged,
            timeoutMs: Math.min(timeoutMs, 20000),
            method,
            onChild: (c) => { handle.child = c; },
        }).then((r) => ({ handle, r }));
        running.add(handle);
        return handle;
    };
    launch();
    while (running.size && Date.now() < deadline) {
        const stagger = new Promise((res) => setTimeout(() => res(null), 2000));
        const done = await Promise.race([stagger, ...Array.from(running).map((h) => h.promise)]);
        if (!done) {
            if (running.size < 4) launch();
            continue;
        }
        const { handle, r } = done;
        running.delete(handle);
        const pick = handle.pick;
        const removed = r.status === 404 && r.body.length <= 8 && r.body.toString('latin1').trim() === 'Removed';
        if (r.status === 0 || removed || r.status === 407 || r.status === 403 || r.status === 429) {
            pick.fails++;
            if (pick.fails >= 2 || removed) pool = pool.filter((p) => p !== pick);
            if (!running.size && !launch()) break;
            continue;
        }
        pick.fails = 0;
        if (r.body.length > 60000) {
            const bps = r.body.length / Math.max(0.2, (Date.now() - handle.t0) / 1000);
            pick.bps = pick.bps ? pick.bps * 0.6 + bps * 0.4 : bps;
        }
        for (const h of running) { try { h.child && h.child.kill('SIGKILL'); } catch (e) {} }
        return { status: r.status, body: r.body, headers: {} };
    }
    for (const h of running) { try { h.child && h.child.kill('SIGKILL'); } catch (e) {} }
    if (Date.now() - lastRefresh > 30000) void refresh();
    return { status: 0, body: Buffer.alloc(0), headers: {} };
}

function stats() { return { size: pool.length, lastRefresh, refreshing: !!refreshing }; }

function viaWarp(url, { headers, timeoutMs = 20000 } = {}) {
    if (!WARP) return Promise.resolve({ status: 0, body: Buffer.alloc(0) });
    return curlVia(WARP, url, { headers: headers || { 'User-Agent': 'Mozilla/5.0' }, timeoutMs, maxBytes: 8 * 1024 * 1024 });
}

module.exports = { shouldProxy, request, refresh, stats, setProbeProvider, viaWarp };
