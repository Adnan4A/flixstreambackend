// vidlove.js — resolve stream links from 67movies.nl's vidlove backend (no crypto needed)
// Usage:
//   node vidlove.js movie 1564614 [source]
//   node vidlove.js tv 1396 1 1 [source]
// Try all sources in order until one returns a link (or pass a specific one).

const API = 'https://api.shows.st';
const SOURCES = ['moviebox', 'vidapi', 'ipcloud', 'tcloud', 'vixsrc', '1embed', 'xpass', 'vidrift', 'lookmovie', 'vidnest'];
const HDRS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Referer': 'https://player.vidlove.cc/',
    'Origin': 'https://player.vidlove.cc',
    'Accept': '*/*'
};

async function resolve(type, id, season, episode, source) {
    const qs = type === 'tv'
        ? `tv?id=${id}&season=${season}&episode=${episode}&mode=json`
        : `movie?id=${id}&mode=json`;
    const url = `${API}/${qs}&sources=${source}`;
    const res = await fetch(url, { headers: HDRS });
    if (!res.ok) throw new Error(`HTTP ${res.status} from ${source}`);
    return res.json();
}

// Sniff the stream URL: HLS playlist vs direct MP4
async function sniff(url) {
    try {
        const res = await fetch(url, { headers: HDRS });
        const buf = await res.arrayBuffer();
        const head = Buffer.from(buf.slice(0, 64)).toString('latin1');
        if (head.startsWith('#EXTM3U')) return 'hls';
        if (head.includes('ftyp')) return 'mp4';
        return 'unknown';
    } catch { return 'unknown'; }
}

async function main() {
    const [type, id, season, episode, wantSource] = process.argv.slice(2);
    if (!type || !id) {
        console.log('Usage: node vidlove.js movie <tmdbId> [source]');
        console.log('       node vidlove.js tv <tmdbId> <season> <episode> [source]');
        process.exit(1);
    }

    const targets = wantSource ? [wantSource] : SOURCES;
    for (const src of targets) {
        try {
            process.stdout.write(`[${src}] trying... `);
            const data = await resolve(type, id, season, episode, src);
            const s = data.source;
            if (!s || !s.url) { console.log('no source'); continue; }
            console.log('OK');
            console.log(`\n✅ ${data.meta?.title || data.meta?.name || id} — ${src} (${s.label || s.source})`);
            const kind = await sniff(s.url);
            console.log(`\n📺 Stream (${kind}):\n${s.url}`);
            if (s.qualities?.length) {
                console.log(`\n🎚 Qualities:`);
                for (const q of s.qualities) console.log(`  ${q.quality} (${q.codec || q.type}): ${q.url}`);
            }
            if (data.subtitles?.length) {
                console.log(`\n💬 Subtitles (${data.subtitles.length}):`);
                for (const sub of data.subtitles.slice(0, 10)) console.log(`  [${sub.type}] ${sub.label}: ${sub.file}`);
            }
            return;
        } catch (e) {
            console.log(`error: ${e.message}`);
        }
    }
    console.log('\n❌ No source found for this title.');
}

main().catch(e => console.error('❌', e.message));
