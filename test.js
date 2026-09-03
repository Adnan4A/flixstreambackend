const { webcrypto } = require('crypto');
global.crypto = webcrypto;

const originalFetch = global.fetch;
global.fetch = async (url, options = {}) => {
    options.headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Referer': 'https://lordflix.org/',
        ...options.headers
    };
    return originalFetch(url, options);
};

eval(require('fs').readFileSync(__dirname + '/fullCrypto.js','utf8') + `
    global._L5 = L5;
    global._ze = ze;
    global._Ue = Ue;
    global._Wa = Wa;
`);

const tmdbId = parseInt(process.argv[2]) || 550;
const base = "https://hongkong.lordflix.club";

(async () => {
    console.log(`[1] Solving PoW for TMDB ${tmdbId}...`);
    const attest = await _Wa(base);

    const path = `/Solstice/movie?tmdb=${tmdbId}`;
    console.log(`[2] Encrypting path: ${path}`);
    const encHex = await _ze(_L5.encode(path));

    console.log(`[3] Fetching...`);
    const res = await fetch(`${base}/${encHex}`, { headers: { "X-Attest": attest } });
    const hex = (await res.text()).trim();
    const data = JSON.parse(new TextDecoder().decode(await _Ue(hex)));

    if (data.stream?.[0]?.playlist) {
        console.log(`\n✅ Playlist: ${data.stream[0].playlist}`);
    } else {
        console.log(`\n${JSON.stringify(data, null, 2)}`);
    }
})().catch(e => console.error("❌", e.message));
