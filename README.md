# Hitster × Tidal 🎵

**Play Hitster with Tidal instead of Spotify.**

Hitster is a music guessing card game where each card has a QR code that normally opens a Spotify track. This project replaces Spotify entirely — scan the card with your phone, and the song streams instantly through your Tidal subscription, right in the browser. No Spotify account needed.

This is an unofficial fan project and is not affiliated with, endorsed by, or connected to Hitster or its creators in any way.

```
Phone camera  →  QR scan (browser JS)
                      ↓
              Flask server (Python)
                      ↓
         CSV lookup (songseeker repo)
                      ↓
       Tidal search / cache lookup
                      ↓
    stream URL  →  <audio> in browser
```

---

## Quick Start

### With Docker (recommended)

The image is built directly from this GitHub repository — no manual cloning needed.

```bash
# 1. Copy docker-compose.yml to your server, then:
docker compose up --build -d

# 2. Open on your desktop to complete the one-time Tidal login
open https://localhost:6001
# Accept the certificate warning: Advanced → Proceed

# 3. Click "Log in to Tidal", authenticate, done.
#    The token is saved to the volume and reused on every restart.

# 4. Open on your phone (same Wi-Fi network):
open https://192.168.x.x:6001
```

**`docker-compose.yml`**
```yaml
services:
  hitster-tidal:
    build:
      context: https://github.com/maahn/hitster-tidal.git
      # Docker clones this repo automatically — no local checkout needed.
      # Pin a release with: ...hitster-tidal.git#v1.0
    container_name: hitster-tidal
    ports:
      - "6001:6001"
    volumes:
      # Persists Tidal token, TLS cert, and Tidal ID caches between restarts.
      # Change the left side to any host path you prefer.
      - /docker/hitster-tidal/data:/data
    environment:
      FLASK_PORT: "6001"
      COUNTDOWN_SEC: "3"
      CACHE_DIR: "/data/cache"
      TOKEN_FILE: "/data/tidal_token.json"
    restart: unless-stopped
```

To update to the latest version:
```bash
docker compose build --no-cache && docker compose up -d
```

### Without Docker (local development)

```bash
pip install flask tidalapi pandas requests pyopenssl cryptography
python app.py
# Data (token, certs, cache) is stored in ./data/ next to app.py
```

---

## How It Works

### Game selection
The app fetches [`playlists.csv`](https://github.com/andygruber/songseeker-hitster-playlists/blob/main/playlists.csv) from the [songseeker-hitster-playlists](https://github.com/andygruber/songseeker-hitster-playlists) community repo at startup. This lists all available Hitster editions (DE, EN, FR, …). Select your edition from the dropdown — the corresponding card CSV is fetched and caching begins automatically.

### QR scanning
Click **Start Scanner** on your phone. The browser accesses the camera directly (no app install). Point it at any Hitster card QR code and the song lookup starts immediately. Supports both normal and inverted QR codes.

### Spoiler-free UI
The song title and artist are **never shown in the browser** — only logged on the server side for the game host to see. The browser shows a countdown, then starts playing. This is a guessing game after all.

### How the Tidal ID cache works

Searching Tidal by text (artist + title) takes ~0.5 s per card and can be unreliable for songs with ambiguous names. To fix this, the app pre-builds a **Tidal ID cache** the first time you load a game edition:

```
data/cache/<edition>-tidal-cache.csv

Card#,TidalID
1,12345678
2,87654321
3,          ← NOT FOUND (falls back to live search at play time)
```

**Build process:**
1. Select a game in the UI
2. The card CSV is fetched from the songseeker repo
3. A background thread works through every card: searches Tidal for `title + artist`, picks the best match, records the Tidal track ID
4. Progress is shown live as a progress bar in the UI
5. Cache is saved to disk — subsequent loads of the same edition are instant

**At play time:**
- Cache hit → `session.track(id)` — fast and reliable
- Cache miss → live Tidal search as fallback

**Rebuilding the cache** (e.g. after a CSV update):
```bash
# Docker
docker exec hitster-tidal rm /data/cache/<edition>-tidal-cache.csv
docker compose restart

# Local
rm data/cache/<edition>-tidal-cache.csv
```

---

## Tidal Login

The app uses Tidal's OAuth device flow. On first run a login URL appears in the browser UI. Click it, log in to Tidal in the new tab, and return — the token is cached to disk and reused automatically on every restart. You only need to log in once.

---

## Certificate Warning

The app generates a self-signed TLS certificate on first run (HTTPS is required for camera access on mobile). Browsers will warn on first visit:

- **Safari:** *Show Details → visit this website → Confirm*
- **Firefox:** *Advanced → Accept the Risk and Continue*
- **Chrome / Android:** *Advanced → Proceed*

You only need to do this once per browser. On iOS, if the camera is still blocked after accepting, go to **Settings → General → About → Certificate Trust Settings** and enable full trust for the certificate.

---

## Configuration

| Variable | Default (Docker) | Default (local) | Description |
|---|---|---|---|
| `FLASK_PORT` | `6001` | `6001` | HTTPS port |
| `COUNTDOWN_SEC` | `3` | `3` | Seconds before playback starts (0 = instant) |
| `CACHE_DIR` | `/data/cache` | `./data/cache` | Tidal ID cache location |
| `TOKEN_FILE` | `/data/tidal_token.json` | `./data/tidal_token.json` | Tidal OAuth token |
| `PLAYLISTS_INDEX_URL` | *(songseeker repo)* | *(songseeker repo)* | Override playlists.csv URL |

---

## Credits

- Card data: [andygruber/songseeker-hitster-playlists](https://github.com/andygruber/songseeker-hitster-playlists)
- Tidal API: [EbbLabs/python-tidal](https://github.com/EbbLabs/python-tidal)
- QR decoding: [cozmo/jsQR](https://github.com/cozmo/jsQR)
- Created with claude.ai