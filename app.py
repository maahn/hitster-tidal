#!/usr/bin/env python3
"""
Hitster × Tidal Player
-----------------------
Scan a Hitster QR code → look up the track via a community CSV →
search/cache on Tidal → stream in-browser via tidalapi.

See README.md for full setup instructions.
"""

import io
import json
import logging
import os
import re
import threading
import time
from pathlib import Path

import pandas as pd
import requests as req
import tidalapi
from flask import Flask, Response, jsonify, render_template_string, request

# ─────────────────────────────────────────────────────────
#  Config  (override via environment variables in Docker)
# ─────────────────────────────────────────────────────────
PLAYLISTS_INDEX_URL = os.environ.get(
    "PLAYLISTS_INDEX_URL",
    "https://raw.githubusercontent.com/andygruber/songseeker-hitster-playlists/main/playlists.csv",
)
RAW_BASE_URL = (
    "https://raw.githubusercontent.com/andygruber/songseeker-hitster-playlists/main/"
)

# Use /data in Docker, ./data locally — override any time via env vars
_IN_DOCKER = Path("/.dockerenv").exists()
_DATA_ROOT = Path("/data") if _IN_DOCKER else Path(__file__).parent / "data"

CACHE_DIR = Path(os.environ.get("CACHE_DIR", str(_DATA_ROOT / "cache")))
TOKEN_FILE = Path(os.environ.get("TOKEN_FILE", str(_DATA_ROOT / "tidal_token.json")))
COUNTDOWN_SEC = int(os.environ.get("COUNTDOWN_SEC", "0"))
FLASK_PORT = int(os.environ.get("FLASK_PORT", "6001"))

# ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
app = Flask(__name__)

# Suppress noisy per-request poll logs
import logging as _logging


class _NoStateFilter(_logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "/api/state" not in msg and "/api/oauth_status" not in msg


_logging.getLogger("werkzeug").addFilter(_NoStateFilter())

# ── Global state ──
state_lock = threading.Lock()
state = {
    "status": "idle",  # idle | searching | countdown | playing | error
    "card_number": None,
    "stream_url": None,
    "tidal_url": None,
    "countdown": 0,
    "error": None,
    # playlist / game selection
    "active_game": None,
    "active_file": None,
    "cache_status": "idle",  # idle | building | done | error
    "cache_progress": 0,
    "cache_total": 0,
    # oauth
    "oauth_url": None,
    "oauth_done": False,
}

_tidal_session = None
_playlists_df = None  # index → (File, Game)
_active_df = None  # loaded card CSV for the active game


# ─────────────────────────────────────────────────────────
#  Tidal OAuth  (non-blocking: URL surfaced to browser)
# ─────────────────────────────────────────────────────────
def get_tidal_session() -> tidalapi.Session:
    global _tidal_session
    if _tidal_session and _tidal_session.check_login():
        return _tidal_session

    session = tidalapi.Session()
    if TOKEN_FILE.exists():
        try:
            td = json.loads(TOKEN_FILE.read_text())
            session.load_oauth_session(
                token_type=td["token_type"],
                access_token=td["access_token"],
                refresh_token=td.get("refresh_token"),
                expiry_time=td.get("expiry_time"),
            )
            if session.check_login():
                log.info("Tidal: restored cached session.")
                _tidal_session = session
                with state_lock:
                    state["oauth_done"] = True
                return session
        except Exception as e:
            log.warning("Tidal restore failed: %s", e)

    # Start device OAuth — surface URL to browser, don't block server startup
    log.info("Tidal: starting OAuth flow …")
    login, future = session.login_oauth()
    with state_lock:
        raw_url = login.verification_uri_complete or ""
        if raw_url and not raw_url.startswith("http"):
            raw_url = "https://" + raw_url
        state["oauth_url"] = raw_url
        state["oauth_done"] = False

    def _wait_for_login():
        global _tidal_session
        try:
            future.result()
            TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKEN_FILE.write_text(
                json.dumps(
                    {
                        "token_type": session.token_type,
                        "access_token": session.access_token,
                        "refresh_token": session.refresh_token,
                        "expiry_time": str(session.expiry_time),
                    }
                )
            )
            _tidal_session = session
            with state_lock:
                state["oauth_done"] = True
                state["oauth_url"] = None
            log.info("Tidal: OAuth complete, token cached.")
        except Exception as e:
            log.error("Tidal OAuth failed: %s", e)

    threading.Thread(target=_wait_for_login, daemon=True).start()
    return None  # not yet authenticated


# ─────────────────────────────────────────────────────────
#  Playlist index (fetched from GitHub at startup)
# ─────────────────────────────────────────────────────────
def fetch_playlists_index() -> pd.DataFrame:
    global _playlists_df
    if _playlists_df is not None:
        return _playlists_df
    log.info("Fetching playlists index from %s …", PLAYLISTS_INDEX_URL)
    r = req.get(PLAYLISTS_INDEX_URL, timeout=10)
    r.raise_for_status()
    # Try tab-separated first, fall back to comma
    try:
        df = pd.read_csv(io.StringIO(r.text), sep="\t", quotechar='"')
        if "File" not in df.columns:
            raise ValueError
    except Exception:
        df = pd.read_csv(io.StringIO(r.text), sep=",", quotechar='"')
    log.info("Loaded %d playlists.", len(df))
    _playlists_df = df
    return df


def fetch_card_csv(filename: str) -> pd.DataFrame:
    url = RAW_BASE_URL + filename
    log.info("Fetching card CSV: %s", url)
    r = req.get(url, timeout=15)
    r.raise_for_status()
    # CSVs in the repo are tab-separated
    try:
        df = pd.read_csv(io.StringIO(r.text), sep="\t", quotechar='"')
        if "Card#" not in df.columns:
            raise ValueError("No Card# column — trying comma separator")
    except Exception:
        df = pd.read_csv(io.StringIO(r.text), sep=",", quotechar='"')
    df["Card#"] = df["Card#"].astype(int)
    df = df.set_index("Card#")
    return df


# ─────────────────────────────────────────────────────────
#  Tidal search + cache
# ─────────────────────────────────────────────────────────
def cache_path_for(filename: str) -> Path:
    stem = Path(filename).stem
    return CACHE_DIR / f"{stem}-tidal-cache.csv"


def load_tidal_cache(filename: str) -> dict:
    cp = cache_path_for(filename)
    if not cp.exists():
        return {}
    try:
        df = pd.read_csv(cp, index_col="Card#")
        result = {}
        for idx, row in df.iterrows():
            tid = row["TidalID"]
            if pd.notna(tid):
                result[str(int(idx))] = int(tid)
        return result
    except Exception as e:
        log.warning("Could not load cache %s: %s", cp, e)
        return {}


def save_tidal_cache(filename: str, rows: list):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cp = cache_path_for(filename)
    pd.DataFrame(rows).set_index("Card#").to_csv(cp)
    log.info("Tidal cache saved to %s.", cp)


def search_tidal(title: str, artist: str):
    session = get_tidal_session()
    if session is None:
        raise RuntimeError("Not authenticated with Tidal yet.")
    query = f"{title} {artist}".strip()
    log.info("Tidal search: %r", query)
    results = session.search(query, models=[tidalapi.Track], limit=5)
    tracks = results.get("tracks", [])
    if not tracks:
        return None
    for t in tracks:
        if title.lower() in t.name.lower():
            return t
    return tracks[0]


def build_cache_async(filename: str, df: pd.DataFrame):
    """Build the Tidal ID cache in a background thread."""
    cp = cache_path_for(filename)
    if cp.exists():
        log.info("Cache already exists for %s.", filename)
        with state_lock:
            state["cache_status"] = "done"
        return

    with state_lock:
        state["cache_status"] = "building"
        state["cache_progress"] = 0
        state["cache_total"] = len(df)

    rows = []
    for i, (card_number, row) in enumerate(df.iterrows()):
        artist = str(row.get("Artist", ""))
        title = str(row.get("Title", ""))
        tidal_id = None
        try:
            track = search_tidal(title, artist)
            tidal_id = track.id if track else None
        except Exception as e:
            log.warning("Card %s (%s – %s): %s", card_number, artist, title, e)
        log.info(
            "  [%d/%d] Card %-5s  %-25s → %s",
            i + 1,
            len(df),
            card_number,
            f"{artist[:12]} – {title[:12]}",
            tidal_id or "NOT FOUND",
        )
        rows.append({"Card#": card_number, "TidalID": tidal_id})
        with state_lock:
            state["cache_progress"] = i + 1
        time.sleep(0.15)

    save_tidal_cache(filename, rows)
    with state_lock:
        state["cache_status"] = "done"


# ─────────────────────────────────────────────────────────
#  Card processing pipeline
# ─────────────────────────────────────────────────────────
def extract_card_number(url: str):
    m = re.search(r"/(\d+)$", url)
    return m.group(1) if m else None


def process_card(card_number: str):
    try:
        with state_lock:
            filename = state.get("active_file")
            df = _active_df

        if df is None or filename is None:
            raise RuntimeError("No game selected. Please choose a game first.")

        # Look up card
        try:
            row = df.loc[int(card_number)]
            artist = str(row.get("Artist", ""))
            title = str(row.get("Title", ""))
        except KeyError:
            raise RuntimeError(f"Card #{card_number} not found in the selected game.")

        # Server-side only — never sent to browser
        log.info("Card %s → %s – %s", card_number, artist, title)

        with state_lock:
            state.update(
                {
                    "status": "searching",
                    "stream_url": None,
                    "tidal_url": None,
                    "error": None,
                }
            )

        # Tidal lookup: prefer cache
        cache = load_tidal_cache(filename)
        cached = cache.get(str(int(card_number)))
        session = get_tidal_session()
        if session is None:
            raise RuntimeError(
                "Not authenticated with Tidal. Please complete OAuth first."
            )

        if cached:
            log.info("Cache hit: card %s → Tidal ID %s", card_number, cached)
            track = session.track(cached)
        else:
            track = search_tidal(title, artist)

        if not track:
            raise RuntimeError(f"Not found on Tidal: {title} – {artist}")

        log.info(
            "Tidal match: %s – %s (id=%s)", track.artist.name, track.name, track.id
        )

        tidal_url = f"https://listen.tidal.com/track/{track.id}"
        stream_url = track.get_url()

        with state_lock:
            state.update({"tidal_url": tidal_url, "stream_url": stream_url})

        # Countdown
        for i in range(COUNTDOWN_SEC, 0, -1):
            with state_lock:
                state.update({"status": "countdown", "countdown": i})
            time.sleep(1)

        with state_lock:
            state.update({"status": "playing", "countdown": 0})

    except Exception as e:
        log.error("process_card: %s", e)
        with state_lock:
            state.update({"status": "error", "error": str(e)})


# ─────────────────────────────────────────────────────────
#  Flask API
# ─────────────────────────────────────────────────────────
@app.route("/api/state")
def api_state():
    with state_lock:
        return jsonify(dict(state))


@app.route("/api/oauth_status")
def api_oauth_status():
    with state_lock:
        return jsonify({"done": state["oauth_done"], "url": state["oauth_url"]})


@app.route("/api/playlists")
def api_playlists():
    try:
        df = fetch_playlists_index()
        games = []
        for _, row in df[["File", "Game"]].dropna().iterrows():
            games.append(
                {
                    "file": row["File"],
                    "game": row["Game"],
                    "cached": cache_path_for(row["File"]).exists(),
                }
            )
        return jsonify({"games": games})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/select_game", methods=["POST"])
def api_select_game():
    global _active_df
    data = request.get_json(force=True)
    filename = data.get("file")
    game = data.get("game")
    if not filename:
        return jsonify({"error": "missing file"}), 400
    try:
        df = fetch_card_csv(filename)
        _active_df = df
        with state_lock:
            state.update(
                {
                    "active_game": game,
                    "active_file": filename,
                    "cache_status": "idle",
                    "cache_progress": 0,
                    "cache_total": len(df),
                    "status": "idle",
                    "error": None,
                }
            )
        # Start cache build in background (no-op if cache already exists)
        threading.Thread(
            target=build_cache_async, args=(filename, df), daemon=True
        ).start()
        return jsonify({"ok": True, "cards": len(df)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scan", methods=["POST"])
def api_scan():
    data = request.get_json(force=True)
    url = data.get("url", "")
    if "hitstergame.com" not in url:
        return jsonify({"error": "not a Hitster QR"}), 400
    card_number = extract_card_number(url)
    if not card_number:
        return jsonify({"error": "could not parse card number"}), 400
    with state_lock:
        if state["status"] in ("searching", "countdown"):
            return jsonify({"busy": True}), 200
        state.update({"status": "searching", "card_number": card_number, "error": None})
    threading.Thread(target=process_card, args=(card_number,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/play", methods=["POST"])
def api_play():
    data = request.get_json(force=True)
    card_number = str(data.get("card_number", "")).strip()
    if not card_number:
        return jsonify({"error": "missing card_number"}), 400
    with state_lock:
        if state["status"] in ("searching", "countdown"):
            return jsonify({"error": "busy"}), 409
        state.update({"status": "searching", "card_number": card_number, "error": None})
    threading.Thread(target=process_card, args=(card_number,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    with state_lock:
        state.update(
            {
                "status": "idle",
                "error": None,
                "card_number": None,
                "stream_url": None,
                "tidal_url": None,
                "countdown": 0,
            }
        )
    return jsonify({"ok": True})


# ─────────────────────────────────────────────────────────
#  HTML / JS frontend
# ─────────────────────────────────────────────────────────
HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Hitster × Tidal</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#08080e;--surface:#101018;--border:#1c1c2c;
  --cyan:#00e5c0;--pink:#ff2d78;--text:#f0f0fa;--muted:#4a4a6a;--radius:18px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{min-height:100%;background:var(--bg);color:var(--text);
  font-family:'DM Sans',sans-serif;overflow-x:hidden}
.page{max-width:440px;margin:0 auto;padding:1.5rem 1rem 4rem;
  display:flex;flex-direction:column;gap:1.25rem}
.hdr{display:flex;align-items:center;justify-content:space-between}
.logo{font-family:'Bebas Neue',sans-serif;font-size:1.9rem;letter-spacing:.06em;
  background:linear-gradient(100deg,var(--cyan),var(--pink));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.reset-btn{background:none;border:1px solid var(--border);color:var(--muted);
  padding:.35rem .75rem;border-radius:8px;font-size:.8rem;cursor:pointer}
.reset-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.card{background:var(--surface);border:1px solid var(--border);
  border-radius:var(--radius);padding:1.5rem;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:-60px;right:-60px;width:180px;height:180px;
  border-radius:50%;background:radial-gradient(circle,rgba(0,229,192,.07) 0%,transparent 70%);
  pointer-events:none}
.section-label{font-size:.72rem;color:var(--muted);text-transform:uppercase;
  letter-spacing:.1em;margin-bottom:.6rem}
/* pill */
.pill{display:inline-flex;align-items:center;gap:.4rem;padding:.2rem .7rem;
  border-radius:99px;font-size:.72rem;letter-spacing:.08em;
  text-transform:uppercase;font-weight:700;margin-bottom:1rem}
.dot{width:6px;height:6px;border-radius:50%}
.s-idle .dot{background:var(--muted)}
.s-idle{background:rgba(74,74,106,.25);color:var(--muted)}
.s-searching{background:rgba(0,229,192,.12);color:var(--cyan)}
.s-searching .dot{background:var(--cyan);animation:blink .7s infinite}
.s-countdown{background:rgba(255,45,120,.14);color:var(--pink)}
.s-countdown .dot{background:var(--pink);animation:blink .4s infinite}
.s-playing{background:rgba(0,229,192,.18);color:var(--cyan)}
.s-playing .dot{background:var(--cyan)}
.s-error{background:rgba(255,45,120,.14);color:var(--pink)}
.s-error .dot{background:var(--pink)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
/* oauth */
.oauth-box{text-align:center;padding:.5rem 0}
.oauth-box p{font-size:.88rem;color:var(--muted);margin-bottom:.9rem;line-height:1.5}
.oauth-link{display:inline-block;padding:.75rem 1.5rem;
  background:linear-gradient(135deg,var(--cyan),#00b89a);
  color:#000;font-weight:700;border-radius:12px;text-decoration:none;font-size:.95rem}
.oauth-wait{font-size:.8rem;color:var(--muted);margin-top:.75rem}
/* game selector */
select{width:100%;padding:.65rem .9rem;background:var(--bg);
  border:1px solid var(--border);border-radius:10px;color:var(--text);
  font-family:'DM Sans',sans-serif;font-size:.9rem;outline:none;
  -webkit-appearance:none;appearance:none;cursor:pointer;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%234a4a6a' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right .9rem center}
select:focus{border-color:var(--cyan)}
.select-btn{width:100%;margin-top:.75rem;padding:.75rem;
  background:linear-gradient(135deg,var(--cyan),#00b89a);
  color:#000;font-weight:700;font-size:.95rem;
  border:none;border-radius:12px;cursor:pointer;transition:opacity .2s}
.select-btn:hover{opacity:.85}
.select-btn:disabled{opacity:.4;cursor:not-allowed}
/* cache progress */
.progress-wrap{margin-top:.9rem}
.progress-bar-bg{background:var(--border);border-radius:99px;height:6px;overflow:hidden}
.progress-bar{background:linear-gradient(90deg,var(--cyan),#00b89a);
  height:100%;border-radius:99px;transition:width .3s}
.progress-label{font-size:.75rem;color:var(--muted);margin-top:.4rem}
/* scanner */
.scanner-wrap{position:relative;width:100%;border-radius:14px;overflow:hidden;
  background:#000;aspect-ratio:1/1;display:flex;align-items:center;justify-content:center}
#qr-video{width:100%;height:100%;object-fit:cover;display:block}
.scan-frame{position:absolute;inset:0;pointer-events:none}
.scan-frame::before,.scan-frame::after,
.scan-frame span::before,.scan-frame span::after{
  content:'';position:absolute;width:28px;height:28px;border-color:var(--cyan);
  border-style:solid;border-width:0}
.scan-frame::before{top:14px;left:14px;border-top-width:3px;border-left-width:3px;border-radius:6px 0 0 0}
.scan-frame::after{top:14px;right:14px;border-top-width:3px;border-right-width:3px;border-radius:0 6px 0 0}
.scan-frame span::before{bottom:14px;left:14px;border-bottom-width:3px;border-left-width:3px;border-radius:0 0 0 6px}
.scan-frame span::after{bottom:14px;right:14px;border-bottom-width:3px;border-right-width:3px;border-radius:0 0 6px 0}
.scan-line{position:absolute;left:20px;right:20px;height:2px;
  background:linear-gradient(90deg,transparent,var(--cyan),transparent);
  animation:scanmove 2s ease-in-out infinite}
@keyframes scanmove{0%{top:15%}50%{top:82%}100%{top:15%}}
.cam-placeholder{color:var(--muted);font-size:.9rem;text-align:center;padding:2rem}
.cam-placeholder .big{font-size:2.5rem;display:block;margin-bottom:.5rem}
.start-btn{width:100%;margin-top:.75rem;padding:.85rem;
  background:linear-gradient(135deg,var(--cyan),#00b89a);
  color:#000;font-family:'DM Sans',sans-serif;font-weight:700;font-size:1rem;
  border:none;border-radius:12px;cursor:pointer;transition:opacity .2s}
.start-btn:disabled{opacity:.4;cursor:not-allowed}
.start-btn:not(:disabled):hover{opacity:.85}
.stop-btn{width:100%;margin-top:.75rem;padding:.85rem;background:transparent;
  border:1px solid var(--border);color:var(--muted);
  font-family:'DM Sans',sans-serif;font-weight:700;font-size:.9rem;
  border-radius:12px;cursor:pointer}
.stop-btn:hover{border-color:var(--pink);color:var(--pink)}
/* countdown */
.cd-wrap{display:flex;flex-direction:column;align-items:center;gap:.75rem;padding:.75rem 0}
.ring{position:relative;width:96px;height:96px}
.ring svg{transform:rotate(-90deg)}
.ring-num{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--pink)}
.cd-lbl{font-size:.78rem;color:var(--muted);letter-spacing:.1em;text-transform:uppercase}
/* audio */
.audio-wrap{margin-top:1rem}
audio{width:100%;border-radius:10px;outline:none}
.play-overlay{display:none;align-items:center;justify-content:center;margin-top:.75rem}
.play-overlay button{padding:.8rem 2rem;
  background:linear-gradient(135deg,var(--cyan),#00b89a);
  color:#000;font-family:'DM Sans',sans-serif;font-weight:700;font-size:1rem;
  border:none;border-radius:12px;cursor:pointer}
.open-btn{display:block;width:100%;margin-top:.75rem;padding:.75rem;
  background:transparent;border:1px solid var(--border);color:var(--muted);
  font-weight:600;font-size:.85rem;border-radius:12px;
  text-decoration:none;text-align:center}
.open-btn:hover{border-color:var(--cyan);color:var(--cyan)}
/* error */
.err{font-size:.83rem;color:var(--pink);background:rgba(255,45,120,.08);
  border-radius:10px;padding:.7rem;word-break:break-word;line-height:1.5}
/* manual */
.row{display:flex;gap:.6rem}
input[type=text]{flex:1;padding:.65rem .9rem;background:var(--bg);
  border:1px solid var(--border);border-radius:10px;
  color:var(--text);font-family:'DM Sans',sans-serif;font-size:.95rem;outline:none}
input[type=text]:focus{border-color:var(--cyan)}
.go-btn{padding:.65rem 1.1rem;background:transparent;border:1px solid var(--cyan);
  color:var(--cyan);border-radius:10px;font-weight:700;font-size:.9rem;cursor:pointer}
.go-btn:hover{background:rgba(0,229,192,.1)}
</style>
</head>
<body>
<div class="page">

  <div class="hdr">
    <div class="logo">Hitster × Tidal</div>
    <button class="reset-btn" onclick="doReset()">↺ Reset</button>
  </div>

  <!-- 1. Tidal OAuth -->
  <div class="card" id="oauth-card" style="display:none">
    <div class="section-label">Tidal Login Required</div>
    <div class="oauth-box">
      <p>Open the link below in any browser and log in to your Tidal account.<br>
         This only needs to be done once — the token is stored on the server.</p>
      <a id="oauth-link" href="#" target="_blank" class="oauth-link">🔐 Log in to Tidal</a>
      <div class="oauth-wait">⏳ Waiting for login confirmation…</div>
    </div>
  </div>

  <!-- 2. Game selector -->
  <div class="card" id="game-card">
    <div class="section-label">Select Game</div>
    <select id="game-select">
      <option value="">— loading games… —</option>
    </select>
    <button class="select-btn" id="select-btn" onclick="selectGame()" disabled>
      Load Game &amp; Start Caching
    </button>
    <div class="progress-wrap" id="progress-wrap" style="display:none">
      <div class="progress-bar-bg"><div class="progress-bar" id="progress-bar" style="width:0%"></div></div>
      <div class="progress-label" id="progress-label">Building Tidal cache…</div>
    </div>
  </div>

  <!-- 3. Scanner -->
  <div class="card" id="scanner-card" style="display:none">
    <div class="pill s-idle" id="scan-pill"><span class="dot"></span><span>Camera</span></div>
    <div class="scanner-wrap">
      <div class="cam-placeholder" id="cam-placeholder">
        <span class="big">📷</span>Press Start to scan a card
      </div>
      <video id="qr-video" playsinline autoplay muted style="display:none"></video>
      <div class="scan-frame" id="scan-frame" style="display:none"><span></span>
        <div class="scan-line"></div>
      </div>
      <canvas id="qr-canvas" style="display:none"></canvas>
    </div>
    <button class="start-btn" id="start-btn" onclick="startScanner()">Start Scanner</button>
    <button class="stop-btn"  id="stop-btn"  style="display:none" onclick="stopScanner()">Stop Camera</button>
  </div>

  <!-- 4. Track / status -->
  <div class="card" id="track-card" style="display:none"></div>

  <!-- 5. Manual entry -->
  <div class="card" id="manual-card" style="display:none">
    <div class="section-label">Manual card number</div>
    <div class="row">
      <input id="manual-input" type="text" maxlength="5" placeholder="e.g. 42"
             oninput="this.value=this.value.replace(/\D/g,'')">
      <button class="go-btn" onclick="submitManual()">▶ Play</button>
    </div>
  </div>

</div>

<script src="https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.min.js"></script>
<script>
const COUNTDOWN_MAX = {{ countdown }};
let audioStarted = false;
let currentAudio = null;  
let gameLoaded   = false;
let gamesData    = [];

// ── OAuth ──────────────────────────────────────────────────
async function checkOAuth() {
  const r = await fetch('/api/oauth_status');
  const d = await r.json();
  const card = document.getElementById('oauth-card');
  if (d.url && !d.done) {
    card.style.display = 'block';
    const link = document.getElementById('oauth-link');
    link.href = d.url;
    link.textContent = '🔐 Log in to Tidal';
  } else {
    card.style.display = 'none';
  }
  return d.done;
}

// ── Game selector ──────────────────────────────────────────
async function loadGames() {
  try {
    const r = await fetch('/api/playlists');
    const d = await r.json();
    gamesData = d.games || [];
    const sel = document.getElementById('game-select');
    sel.innerHTML = '<option value="">— select a game —</option>' +
      gamesData.map(g =>
        `<option value="${g.file}">${g.game}${g.cached ? ' ✓' : ''}</option>`
      ).join('');
    // Pre-select first cached game if any
    const firstCached = gamesData.find(g => g.cached);
    if (firstCached) sel.value = firstCached.file;
    document.getElementById('select-btn').disabled = false;
  } catch(e) {
    document.getElementById('game-select').innerHTML =
      '<option value="">⚠ Could not load games</option>';
  }
}

async function selectGame() {
  const sel  = document.getElementById('game-select');
  const file = sel.value;
  const game = sel.options[sel.selectedIndex]?.text || file;
  if (!file) return;

  const btn = document.getElementById('select-btn');
  btn.disabled = true;
  btn.textContent = 'Loading…';

  try {
    const r = await fetch('/api/select_game', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({file, game})
    });
    const d = await r.json();
    if (d.error) { alert('Error: ' + d.error); btn.disabled=false; btn.textContent='Load Game & Start Caching'; return; }
    gameLoaded = true;
    document.getElementById('scanner-card').style.display = 'block';
    document.getElementById('manual-card').style.display  = 'block';
    btn.textContent = '✓ Game loaded (' + d.cards + ' cards)';
  } catch(e) {
    alert('Failed to load game: ' + e.message);
    btn.disabled = false;
    btn.textContent = 'Load Game & Start Caching';
  }
}

// ── QR scanner ─────────────────────────────────────────────
let videoStream=null, scanning=false, lastScanned=null;
const video   = document.getElementById('qr-video');
const canvas  = document.getElementById('qr-canvas');
const ctx     = canvas.getContext('2d',{willReadFrequently:true});
const frame   = document.getElementById('scan-frame');
const pholder = document.getElementById('cam-placeholder');
const scanPill= document.getElementById('scan-pill');

async function startScanner(){
  try{
    videoStream=await navigator.mediaDevices.getUserMedia(
      {video:{facingMode:'environment',width:{ideal:1280},height:{ideal:1280}}});
    video.srcObject=videoStream; await video.play();
    video.style.display='block'; pholder.style.display='none';
    frame.style.display='block';
    document.getElementById('start-btn').style.display='none';
    document.getElementById('stop-btn').style.display='block';
    scanPill.className='pill s-searching';
    scanPill.querySelector('span:last-child').textContent='Scanning';
    scanning=true; requestAnimationFrame(tickScan);
  }catch(e){alert('Camera error: '+e.message);}
}

function stopScanner(){
  scanning=false;
  if(videoStream){videoStream.getTracks().forEach(t=>t.stop());videoStream=null;}
  video.style.display='none'; frame.style.display='none';
  pholder.style.display='block';
  document.getElementById('start-btn').style.display='block';
  document.getElementById('stop-btn').style.display='none';
  scanPill.className='pill s-idle';
  scanPill.querySelector('span:last-child').textContent='Camera';
}

function tickScan(){
  if(!scanning) return;
  if(video.readyState===video.HAVE_ENOUGH_DATA){
    canvas.width=video.videoWidth; canvas.height=video.videoHeight;
    ctx.drawImage(video,0,0);
    const img=ctx.getImageData(0,0,canvas.width,canvas.height);
    const code=jsQR(img.data,canvas.width,canvas.height,{inversionAttempts:'attemptBoth'});
    if(code && code.data!==lastScanned && code.data.includes('hitstergame.com')){
      lastScanned=code.data;
      setTimeout(()=>{lastScanned=null;},4000);
      audioStarted=false;
      currentAudio=null;
      fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({url:code.data})});
    }
  }
  requestAnimationFrame(tickScan);
}

// ── State polling ──────────────────────────────────────────
async function poll(){
  try{
    const r=await fetch('/api/state');
    const d=await r.json();

    // OAuth check
    if(!d.oauth_done && d.oauth_url){
      document.getElementById('oauth-card').style.display='block';
      document.getElementById('oauth-link').href=d.oauth_url;
    } else {
      document.getElementById('oauth-card').style.display='none';
    }

    // Cache progress
    if(d.cache_status==='building' && d.cache_total>0){
      const pct = Math.round(100*d.cache_progress/d.cache_total);
      document.getElementById('progress-wrap').style.display='block';
      document.getElementById('progress-bar').style.width=pct+'%';
      document.getElementById('progress-label').textContent=
        `Building Tidal cache… ${d.cache_progress} / ${d.cache_total}`;
    } else if(d.cache_status==='done'){
      document.getElementById('progress-wrap').style.display='block';
      document.getElementById('progress-bar').style.width='100%';
      document.getElementById('progress-label').textContent='✓ Cache ready';
    }

    renderTrackCard(d);
  }catch(e){}
}

function renderTrackCard(d){
  const el=document.getElementById('track-card');
  if(d.status==='idle'){el.style.display='none';return;}
  el.style.display='block';

  const pillTxt={searching:'Searching…',countdown:'Get ready!',
                 playing:'Playing',error:'Error'}[d.status]||d.status;
  let body='';

  if(d.status==='searching'){
    if(currentAudio){ currentAudio.pause(); currentAudio.src=''; currentAudio=null; } 
    body=`<div style="text-align:center;padding:1.5rem;color:var(--muted)">
      🔍 Looking up card #${d.card_number||'?'} …</div>`;
  } else if(d.status==='countdown'){
    body=renderCountdown(d.countdown);
  } else if(d.status==='playing'){
    let player='';
    if(d.stream_url){
      player=`<div class="audio-wrap">
        <audio id="tidal-audio" controls>
          <source src="${d.stream_url}" type="audio/mp4">
          <source src="${d.stream_url}">
        </audio>
      </div>
      <div class="play-overlay" id="play-overlay" style="display:flex">
        <button onclick="manualPlay()">▶ Tap to Play</button>
      </div>`;
    }
    const fb=d.tidal_url
      ?`<a class="open-btn" href="${d.tidal_url}" target="_blank">↗ Open in Tidal app</a>`:'';
    body=`${player}${fb}`;
  } else if(d.status==='error'){
    body=`<div class="err">⚠ ${d.error||'Unknown error'}</div>`;
  }

  const key=d.status+'|'+d.countdown+'|'+(d.stream_url||'');
  if(el.dataset.rendered!==key){
    el.dataset.rendered=key;
    // Stop any currently playing audio before replacing the element
    const prev = document.getElementById('tidal-audio');
    if (prev) { prev.pause(); prev.src = ''; }
    el.innerHTML=`<div class="pill s-${d.status}"><span class="dot"></span><span>${pillTxt}</span></div>${body}`;
    if(d.status==='playing'){
      // Slight delay so the <audio> element is in the DOM
      setTimeout(tryAutoplay, 80);
    }
  } else if(d.status==='playing' && !audioStarted){
    // DOM unchanged but playback not confirmed yet — keep retrying
    tryAutoplay();
  }
}

function renderCountdown(n){
  const r=42,circ=(2*Math.PI*r).toFixed(1);
  const offset=(2*Math.PI*r*(1-n/Math.max(COUNTDOWN_MAX,1))).toFixed(1);
  return `<div class="cd-wrap">
    <div class="ring">
      <svg width="96" height="96" viewBox="0 0 96 96">
        <circle fill="none" stroke="var(--border)" stroke-width="5" cx="48" cy="48" r="${r}"/>
        <circle fill="none" stroke="var(--pink)" stroke-width="5" cx="48" cy="48" r="${r}"
          stroke-linecap="round" stroke-dasharray="${circ}" stroke-dashoffset="${offset}"/>
      </svg>
      <div class="ring-num">${n}</div>
    </div>
    <span class="cd-lbl">Starting in…</span>
  </div>`;
}

function tryAutoplay() {
  const audio = document.getElementById('tidal-audio');
  if (!audio || audioStarted) return;
  currentAudio = audio; 
  audio.play().then(() => {
    audioStarted = true;
    document.getElementById('play-overlay').style.display = 'none';
  }).catch(() => { /* tap-to-play button stays visible */ });
}
function manualPlay() {
  const a = document.getElementById('tidal-audio');
  if (!a) return;
  currentAudio = a; 
  a.play().then(() => {
    audioStarted = true;
    document.getElementById('play-overlay').style.display = 'none';
  }).catch(() => {});
}

// Retry play on any user interaction — covers the case where the gesture
// happened before the <audio> element existed
document.addEventListener('click',      () => { if (!audioStarted) tryAutoplay(); });
document.addEventListener('touchstart', () => { if (!audioStarted) tryAutoplay(); }, {passive:true});

function submitManual(){
  const val=document.getElementById('manual-input').value.trim();
  if(!val) return;
  audioStarted=false;
  currentAudio=null;
  fetch('/api/play',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({card_number:val})});
}
function doReset(){audioStarted=false;currentAudio=null;fetch('/api/reset',{method:'POST'});}

// ── Boot ───────────────────────────────────────────────────
(async()=>{
  await checkOAuth();
  await loadGames();
  setInterval(poll,800);
})();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML, countdown=COUNTDOWN_SEC)


# ─────────────────────────────────────────────────────────
#  Self-signed TLS cert
# ─────────────────────────────────────────────────────────
def ensure_self_signed_cert():
    cert_path = _DATA_ROOT / "cert.pem"
    key_path = _DATA_ROOT / "key.pem"
    if cert_path.exists() and key_path.exists():
        log.info("TLS: reusing existing cert.")
        return str(cert_path), str(key_path)
    log.info("TLS: generating self-signed certificate …")
    import datetime
    import ipaddress
    import socket as _socket

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    try:
        local_ip = _socket.gethostbyname(_socket.gethostname())
    except Exception:
        local_ip = "127.0.0.1"

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subj = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, local_ip)])
    san = x509.SubjectAlternativeName(
        [
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
        ]
    )
    try:
        san = x509.SubjectAlternativeName(
            [
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                x509.IPAddress(ipaddress.IPv4Address(local_ip)),
            ]
        )
    except Exception:
        pass

    cert = (
        x509.CertificateBuilder()
        .subject_name(subj)
        .issuer_name(subj)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=3650))
        .add_extension(san, critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    log.info("TLS: cert written to /data/")
    return str(cert_path), str(key_path)


# ─────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Start Tidal auth (non-blocking — URL shown in UI if needed)
    get_tidal_session()

    # Pre-fetch playlist index
    try:
        fetch_playlists_index()
    except Exception as e:
        log.warning("Could not fetch playlists index: %s", e)

    cert_file, key_file = ensure_self_signed_cert()

    import socket

    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "localhost"

    log.info("=" * 55)
    log.info("  Desktop:  https://localhost:%d", FLASK_PORT)
    log.info("  Phone:    https://%s:%d", local_ip, FLASK_PORT)
    log.info("  Accept cert warning once: Advanced → Proceed")
    log.info("=" * 55)

    app.run(
        host="0.0.0.0",
        port=FLASK_PORT,
        debug=False,
        use_reloader=False,
        ssl_context=(cert_file, key_file),
    )
