// browserfetch.js — Chrome-impersonating fetches for CDN-facing requests.
// Uses curl-impersonate (JA3/JA4 Chrome fingerprints) when available; falls
// back to plain undici fetch with full browser headers otherwise.
'use strict';

const { spawn } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

const IMPERSONATE_TARGET = 'chrome145';
const TMP_DIR = os.tmpdir();

const CHROME_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36';

const FALLBACK_HEADERS = {
    'User-Agent': CHROME_UA,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://cinejoy.to',
    'Referer': 'https://cinejoy.to/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-CH-UA': '"Chromium";v="145", "Google Chrome";v="145", "Not_A Brand";v="24"',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"macOS"',
    'Cache-Control': 'no-cache',
};

function detectBinary() {
    const env = process.env.CURL_IMPERSONATE;
    const candidates = [];
    if (env) candidates.push(env);
    const names = ['curl-impersonate', 'curl_chrome145', 'curl_chrome142', 'curl_chrome133a', 'curl_chrome116'];
    for (const dir of (process.env.PATH || '').split(':')) {
        if (!dir) continue;
        for (const name of names) {
            const p = path.join(dir, name);
            if (fs.existsSync(p)) candidates.push(p);
        }
    }
    // Home-dir installs (prebuilt tarball) and Homebrew layouts
    const home = os.homedir();
    for (const name of names) {
        candidates.push(path.join(home, 'bin', 'curl-impersonate', name));
    }
    for (const base of ['/opt/homebrew/opt/curl-impersonate', '/usr/local/opt/curl-impersonate']) {
        candidates.push(path.join(base, 'bin', 'curl_chrome145'), path.join(base, 'libexec/curl/chrome-145/bin/curl'));
    }
    const found = candidates.find(p => p && fs.existsSync(p));
    if (found) console.error('browserfetch: using', found);
    return found || null;
}

let CURL = undefined;
function curlBinary() {
    if (CURL === undefined) CURL = detectBinary();
    return CURL;
}

// curl <= 8.10 style shared lib layouts store libs per-version; not needed here.
function baseArgs(bin) {
    if (path.basename(bin) === 'curl-impersonate') return ['--compressed', '--impersonate', IMPERSONATE_TARGET];
    return ['--compressed']; // wrapper scripts already bake --impersonate
}

// Full-buffer request via curl-impersonate (intended for small bodies: m3u8).
function impersonatedRequest(target, { headers = {}, timeoutMs = 20000, method = 'GET' } = {}) {
    const bin = curlBinary();
    if (!bin) return lowLevelFallback(target, headers, timeoutMs, method);
    return new Promise((resolve, reject) => {
        const outFile = path.join(TMP_DIR, 'cj-body-' + process.pid + '-' + Math.random().toString(36).slice(2, 10) + '.bin');
        const jar = path.join(TMP_DIR, 'cj-jar-' + process.pid + '-' + Math.random().toString(36).slice(2, 10) + '.txt');
        const args = [
            '-sS', '-L', '-o', outFile, '-w', '%{http_code}',
            '--max-time', String(Math.max(1, Math.ceil(timeoutMs / 1000))),
            ...baseArgs(bin),
            '-b', jar, '-c', jar,
        ];
        for (const [k, v] of Object.entries(headers || {})) args.push('-H', `${k}: ${v}`);
        args.push('-X', method, target);
        const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'], env: process.env });
        let stdout = '';
        let stderr = '';
        child.stdout.on('data', c => { stdout += c.toString(); });
        child.stderr.on('data', c => { stderr += c.toString(); });
        const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs + 8000);
        child.on('error', err => { clearTimeout(timer); cleanup(); reject(err); });
        child.on('close', code => {
            clearTimeout(timer);
            if (code !== 0) {
                cleanup();
                console.error('browserfetch curl(' + code + '):', stderr.trim().slice(0, 200));
                lowLevelFallback(target, headers, timeoutMs, method).then(resolve, reject);
                return;
            }
            fs.readFile(outFile, (err, body) => {
                cleanup();
                if (err) { reject(err); return; }
                const status = parseInt(stdout.trim(), 10) || 200;
                resolve({ status, body, headers: {} });
            });
        });
        function cleanup() {
            fs.unlink(outFile, () => {});
            fs.unlink(jar, () => {});
        }
    });
}

// Fallback when curl-impersonate is unavailable: plain fetch + browser headers.
async function lowLevelFallback(target, headers, timeoutMs, method) {
    const res = await fetch(target, {
        method,
        headers: Object.assign({}, FALLBACK_HEADERS, headers),
        signal: AbortSignal.timeout(timeoutMs),
    });
    return { status: res.status, body: Buffer.from(await res.arrayBuffer()), headers: res.headers };
}

module.exports = { impersonatedRequest, curlBinary };