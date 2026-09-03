# FlixStream Backend

Standalone Node.js API that resolves movie/TV streams from the vidlove (67movies) backend, proxies media playback, and serves subtitles. Zero dependencies — Node 18+ only.

Use it to add playback to **any existing web app** (e.g. one already using TMDB for metadata): send the TMDB id, get a stream back, and proxy playback through this API.

## Deploy

1. Upload this folder to your VPS (Node 18+ required)
2. Install / start:

```bash
npm start
# or: node server.js
```

The server listens on `PORT` (default `3010`):

```bash
PORT=8080 npm start
```

## Production setup (optional but recommended)

**pm2:**

```bash
npm i -g pm2
pm2 start server.js --name flixstream
pm2 save && pm2 startup
```

**systemd** (`/etc/systemd/system/flixstream.service`):

```ini
[Unit]
Description=FlixStream Backend
After=network.target

[Service]
WorkingDirectory=/opt/flixstream-backend
ExecStart=/usr/bin/node server.js
Environment=PORT=3010
Restart=always

[Install]
WantedBy=multi-user.target
```

**nginx reverse proxy** (disables buffering for video streams):

```nginx
server {
    listen 80;
    server_name api.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:3010;
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## API

### POST /api/vidlove — resolve a stream

Request:

```json
{ "tmdb": 550, "type": "movie" }
```

TV: `{ "tmdb": 1396, "type": "tv", "season": 1, "episode": 1 }`

Optional `source` forces one vidlove backend: `moviebox | vidapi | ipcloud | tcloud | vixsrc | 1embed | xpass | vidrift | lookmovie | vidnest`. Omit it to auto-pick the highest-quality working source.

Response:

```json
{
  "stream": [{
    "playlist": "https://b.ballerina.../api?d=...",
    "kind": "hls",
    "qualities": []
  }],
  "subtitles": [
    { "display": "English", "url": "https://cache.vdrk.site/.../English.vtt", "language": "en", "source": "vidlove", "isHearingImpaired": false }
  ],
  "meta": { "title": "Fight Club" }
}
```

`kind` is `hls` or `mp4`.

### GET /api/vlproxy?url=... — media proxy (REQUIRED for playback)

vidlove streams only work with the `player.vidlove.cc` origin, so **all** playback requests must go through this endpoint. It spoofs the origin, sniffs the content type, rewrites HLS playlists, and supports `Range` for seeking.

- HLS master/variant playlists: returns rewritten `application/vnd.apple.mpegurl` — feed this URL to hls.js, not the raw one
- Media segments: returns `video/mp2t` / `video/mp4` streamed
- `Range: bytes=0-` headers are forwarded (browser seeking works)

### GET /api/subtitle?url=... — subtitles as VTT

Fetches the subtitle URL and returns VTT (converts SRT automatically). Also handles audio-like raw fetches — safe for `<track>` or custom rendering.

### GET /api/srt2vtt?url=... — SRT → VTT conversion

### GET /api/subtitles?tmdb=... — OpenSubtitles lookup (arigold)

Optional; only needed if you want OpenSubtitles as an extra source.

## Integrating with your TMDB web app

```js
// 1. Resolve (fresh on every page load - URLs expire in minutes)
const res = await fetch('https://api.yourdomain.com/api/vidlove', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ tmdb: 550, type: 'movie' })
});
const { stream, subtitles } = await res.json();
const source = stream[0];

// 2. Play HLS through the proxy (never use source.playlist directly)
const API = 'https://api.yourdomain.com';
const proxied = u => API + '/api/vlproxy?url=' + encodeURIComponent(u);

if (source.kind === 'hls') {
  const hls = new Hls();
  hls.loadSource(proxied(source.playlist));   // hls.js resolves rewrites internally
  hls.attachMedia(video);
} else {
  video.src = proxied(source.playlist);       // direct MP4 through the proxy
}

// 3. Subtitles (English preferred - pick it automatically)
const eng = subtitles.find(s => /^english([^a-z]|$)/i.test(s.display));
if (eng) track.src = API + '/api/subtitle?url=' + encodeURIComponent(eng.url);
```

CORS is wide open (`Access-Control-Allow-Origin: *`), so this works from any origin.

## Files

| File | Purpose |
|---|---|
| `server.js` | The API server (entry point) |
| `fullCrypto.js` | Crypto engine for the legacy lordflix resolver (kept for compatibility) |
| `package.json` | Metadata + start script |

Optional: drop `player.html` (and its assets) into this folder and it also serves a complete player at `/`.
