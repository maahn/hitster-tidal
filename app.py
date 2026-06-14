#!/usr/bin/env python3
"""
Hitster × Tidal Player
-----------------------
Scan a Hitster QR code → look up the track via a community CSV →
search/cache on Tidal → stream in-browser via tidalapi.

Multiple independent game sessions are supported — each browser tab/device
gets its own session cookie and maintains its own state.

See README.md for full setup instructions.
"""

import io
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path

import pandas as pd
import requests as req
import tidalapi
from flask import Flask, jsonify, make_response, render_template_string, request

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

_IN_DOCKER = Path("/.dockerenv").exists()
_DATA_ROOT = Path("/data") if _IN_DOCKER else Path(__file__).parent / "data"

CACHE_DIR = Path(os.environ.get("CACHE_DIR", str(_DATA_ROOT / "cache")))
TOKEN_FILE = Path(os.environ.get("TOKEN_FILE", str(_DATA_ROOT / "tidal_token.json")))
COUNTDOWN_SEC = int(os.environ.get("COUNTDOWN_SEC", "0"))
FLASK_PORT = int(os.environ.get("FLASK_PORT", "6001"))
SESSION_COOKIE = "hitster_session"

# ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
app = Flask(__name__)

import logging as _logging


class _NoStateFilter(_logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "/api/state" not in msg and "/api/oauth_status" not in msg


_logging.getLogger("werkzeug").addFilter(_NoStateFilter())


# ─────────────────────────────────────────────────────────
#  Per-session state
# ─────────────────────────────────────────────────────────
# sessions: { session_id -> { state: dict, lock: Lock, active_df: DataFrame|None } }
sessions: dict = {}
sessions_lock = threading.Lock()


def _blank_state() -> dict:
    return {
        "status": "idle",
        "card_number": None,
        "stream_url": None,
        "tidal_url": None,
        "countdown": 0,
        "error": None,
        "active_game": None,
        "active_file": None,
        "cache_status": "idle",
        "cache_progress": 0,
        "cache_total": 0,
    }


def get_session(session_id: str) -> dict:
    """Return the session dict, creating it if needed."""
    with sessions_lock:
        if session_id not in sessions:
            sessions[session_id] = {
                "state": _blank_state(),
                "lock": threading.Lock(),
                "active_df": None,
            }
        return sessions[session_id]


def get_or_create_session_id() -> tuple[str, bool]:
    """Return (session_id, is_new) from the request cookie."""
    sid = request.cookies.get(SESSION_COOKIE)
    is_new = not sid or sid not in sessions
    if is_new:
        sid = str(uuid.uuid4())
    return sid, is_new


def session_response(resp, session_id: str, is_new: bool):
    """Attach the session cookie if it was just created."""
    if is_new:
        resp.set_cookie(SESSION_COOKIE, session_id, samesite="Lax", httponly=True)
    return resp


# ─────────────────────────────────────────────────────────
#  Global shared resources (Tidal session, playlists index)
# ─────────────────────────────────────────────────────────
_tidal_session = None
_tidal_lock = threading.Lock()
_tidal_oauth = {"url": None, "done": False}  # shown in every browser until login
_playlists_df = None


def get_tidal_session() -> tidalapi.Session | None:
    global _tidal_session
    with _tidal_lock:
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
                with _tidal_lock:
                    _tidal_session = session
                _tidal_oauth["done"] = True
                return session
        except Exception as e:
            log.warning("Tidal restore failed: %s", e)

    log.info("Tidal: starting OAuth flow …")
    login, future = session.login_oauth()
    raw_url = login.verification_uri_complete or ""
    if raw_url and not raw_url.startswith("http"):
        raw_url = "https://" + raw_url
    _tidal_oauth["url"] = raw_url
    _tidal_oauth["done"] = False

    def _wait():
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
            with _tidal_lock:
                _tidal_session = session
            _tidal_oauth["url"] = None
            _tidal_oauth["done"] = True
            log.info("Tidal: OAuth complete, token cached.")
        except Exception as e:
            log.error("Tidal OAuth failed: %s", e)

    threading.Thread(target=_wait, daemon=True).start()
    return None


def fetch_playlists_index() -> pd.DataFrame:
    global _playlists_df
    if _playlists_df is not None:
        return _playlists_df
    log.info("Fetching playlists index …")
    r = req.get(PLAYLISTS_INDEX_URL, timeout=10)
    r.raise_for_status()
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
    try:
        df = pd.read_csv(io.StringIO(r.text), sep="\t", quotechar='"')
        if "Card#" not in df.columns:
            raise ValueError
    except Exception:
        df = pd.read_csv(io.StringIO(r.text), sep=",", quotechar='"')
    df["Card#"] = df["Card#"].astype(int)
    return df.set_index("Card#")


# ─────────────────────────────────────────────────────────
#  Tidal cache (shared across sessions — keyed by filename)
# ─────────────────────────────────────────────────────────
def cache_path_for(filename: str) -> Path:
    return CACHE_DIR / f"{Path(filename).stem}-tidal-cache.csv"


def load_tidal_cache(filename: str) -> dict:
    cp = cache_path_for(filename)
    if not cp.exists():
        return {}
    try:
        df = pd.read_csv(cp, index_col="Card#")
        return {
            str(int(idx)): int(row["TidalID"])
            for idx, row in df.iterrows()
            if pd.notna(row["TidalID"])
        }
    except Exception as e:
        log.warning("Could not load cache %s: %s", cp, e)
        return {}


def save_tidal_cache(filename: str, rows: list):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).set_index("Card#").to_csv(cache_path_for(filename))
    log.info("Tidal cache saved: %s", cache_path_for(filename))


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


# One cache-build lock per filename so two sessions don't double-build
_cache_build_locks: dict[str, threading.Lock] = {}
_cache_build_locks_lock = threading.Lock()


def build_cache_async(filename: str, df: pd.DataFrame, sess: dict):
    with _cache_build_locks_lock:
        if filename not in _cache_build_locks:
            _cache_build_locks[filename] = threading.Lock()
    file_lock = _cache_build_locks[filename]

    def _run():
        if not file_lock.acquire(blocking=False):
            # Another session is already building this cache — just wait and report done
            file_lock.acquire()
            file_lock.release()
            with sess["lock"]:
                sess["state"]["cache_status"] = "done"
            return

        try:
            if cache_path_for(filename).exists():
                with sess["lock"]:
                    sess["state"]["cache_status"] = "done"
                return

            with sess["lock"]:
                sess["state"].update(
                    {
                        "cache_status": "building",
                        "cache_progress": 0,
                        "cache_total": len(df),
                    }
                )
            rows = []
            for i, (card_number, row) in enumerate(df.iterrows()):
                artist = str(row.get("Artist", ""))
                title_ = str(row.get("Title", ""))
                tidal_id = None
                try:
                    track = search_tidal(title_, artist)
                    tidal_id = track.id if track else None
                except Exception as e:
                    log.warning("Card %s: %s", card_number, e)
                log.info(
                    "  [%d/%d] Card %-5s  %-25s → %s",
                    i + 1,
                    len(df),
                    card_number,
                    f"{artist[:12]} – {title_[:12]}",
                    tidal_id or "NOT FOUND",
                )
                rows.append({"Card#": card_number, "TidalID": tidal_id})
                with sess["lock"]:
                    sess["state"]["cache_progress"] = i + 1
                time.sleep(0.15)

            save_tidal_cache(filename, rows)
            with sess["lock"]:
                sess["state"]["cache_status"] = "done"
        finally:
            file_lock.release()

    threading.Thread(target=_run, daemon=True).start()


# ─────────────────────────────────────────────────────────
#  Card processing pipeline (per session)
# ─────────────────────────────────────────────────────────
def extract_card_number(url: str):
    m = re.search(r"/(\d+)$", url)
    return m.group(1) if m else None


def process_card(session_id: str, card_number: str):
    sess = get_session(session_id)
    try:
        with sess["lock"]:
            filename = sess["state"].get("active_file")
            df = sess["active_df"]

        if df is None or filename is None:
            raise RuntimeError("No game selected. Please choose a game first.")

        try:
            row = df.loc[int(card_number)]
            artist = str(row.get("Artist", ""))
            title = str(row.get("Title", ""))
        except KeyError:
            raise RuntimeError(f"Card #{card_number} not found in the selected game.")

        log.info("[%s] Card %s → %s – %s", session_id[:8], card_number, artist, title)

        with sess["lock"]:
            sess["state"].update(
                {
                    "status": "searching",
                    "stream_url": None,
                    "tidal_url": None,
                    "error": None,
                }
            )

        cache = load_tidal_cache(filename)
        cached = cache.get(str(int(card_number)))
        tidal = get_tidal_session()
        if tidal is None:
            raise RuntimeError(
                "Not authenticated with Tidal. Please complete OAuth first."
            )

        if cached:
            log.info(
                "[%s] Cache hit: card %s → Tidal ID %s",
                session_id[:8],
                card_number,
                cached,
            )
            track = tidal.track(cached)
        else:
            track = search_tidal(title, artist)

        if not track:
            raise RuntimeError(f"Not found on Tidal: {title} – {artist}")

        log.info(
            "[%s] Tidal match: %s – %s (id=%s)",
            session_id[:8],
            track.artist.name,
            track.name,
            track.id,
        )

        tidal_url = f"https://listen.tidal.com/track/{track.id}"
        stream_url = track.get_url()

        with sess["lock"]:
            sess["state"].update({"tidal_url": tidal_url, "stream_url": stream_url})

        for i in range(COUNTDOWN_SEC, 0, -1):
            with sess["lock"]:
                sess["state"].update({"status": "countdown", "countdown": i})
            time.sleep(1)

        with sess["lock"]:
            sess["state"].update({"status": "playing", "countdown": 0})

    except Exception as e:
        log.error("[%s] process_card: %s", session_id[:8], e)
        with sess["lock"]:
            sess["state"].update({"status": "error", "error": str(e)})


# ─────────────────────────────────────────────────────────
#  Flask API
# ─────────────────────────────────────────────────────────
@app.route("/api/state")
def api_state():
    sid, is_new = get_or_create_session_id()
    sess = get_session(sid)
    with sess["lock"]:
        data = dict(sess["state"])
    data["oauth_done"] = _tidal_oauth["done"]
    data["oauth_url"] = _tidal_oauth["url"]
    resp = make_response(jsonify(data))
    return session_response(resp, sid, is_new)


@app.route("/api/oauth_status")
def api_oauth_status():
    return jsonify({"done": _tidal_oauth["done"], "url": _tidal_oauth["url"]})


@app.route("/api/playlists")
def api_playlists():
    try:
        df = fetch_playlists_index()
        games = [
            {
                "file": row["File"],
                "game": row["Game"],
                "cached": cache_path_for(row["File"]).exists(),
            }
            for _, row in df[["File", "Game"]].dropna().iterrows()
        ]
        return jsonify({"games": games})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/select_game", methods=["POST"])
def api_select_game():
    sid, is_new = get_or_create_session_id()
    sess = get_session(sid)
    data = request.get_json(force=True)
    filename = data.get("file")
    game = data.get("game")
    if not filename:
        return jsonify({"error": "missing file"}), 400
    try:
        df = fetch_card_csv(filename)
        sess["active_df"] = df
        with sess["lock"]:
            sess["state"].update(
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
        build_cache_async(filename, df, sess)
        resp = make_response(jsonify({"ok": True, "cards": len(df)}))
        return session_response(resp, sid, is_new)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scan", methods=["POST"])
def api_scan():
    sid, is_new = get_or_create_session_id()
    sess = get_session(sid)
    data = request.get_json(force=True)
    url = data.get("url", "")
    if "hitstergame.com" not in url:
        return jsonify({"error": "not a Hitster QR"}), 400
    card_number = extract_card_number(url)
    if not card_number:
        return jsonify({"error": "could not parse card number"}), 400
    with sess["lock"]:
        if sess["state"]["status"] in ("searching", "countdown"):
            return jsonify({"busy": True}), 200
        sess["state"].update(
            {"status": "searching", "card_number": card_number, "error": None}
        )
    threading.Thread(target=process_card, args=(sid, card_number), daemon=True).start()
    resp = make_response(jsonify({"ok": True}))
    return session_response(resp, sid, is_new)


@app.route("/api/play", methods=["POST"])
def api_play():
    sid, is_new = get_or_create_session_id()
    sess = get_session(sid)
    data = request.get_json(force=True)
    card_number = str(data.get("card_number", "")).strip()
    if not card_number:
        return jsonify({"error": "missing card_number"}), 400
    with sess["lock"]:
        if sess["state"]["status"] in ("searching", "countdown"):
            return jsonify({"error": "busy"}), 409
        sess["state"].update(
            {"status": "searching", "card_number": card_number, "error": None}
        )
    threading.Thread(target=process_card, args=(sid, card_number), daemon=True).start()
    resp = make_response(jsonify({"ok": True}))
    return session_response(resp, sid, is_new)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    sid, is_new = get_or_create_session_id()
    sess = get_session(sid)
    with sess["lock"]:
        sess["state"].update(
            {
                "status": "idle",
                "error": None,
                "card_number": None,
                "stream_url": None,
                "tidal_url": None,
                "countdown": 0,
            }
        )
    resp = make_response(jsonify({"ok": True}))
    return session_response(resp, sid, is_new)


# ─────────────────────────────────────────────────────────
#  HTML / JS frontend  (unchanged from single-session version)
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
.hdr{display:flex;align-items:center;justify-content:space-between;gap:.5rem}
.logo{font-family:'Bebas Neue',sans-serif;font-size:1.9rem;letter-spacing:.06em;
  background:linear-gradient(100deg,var(--cyan),var(--pink));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;flex:1}
.reset-btn{background:none;border:1px solid var(--border);color:var(--muted);
  padding:.35rem .75rem;border-radius:8px;font-size:.8rem;cursor:pointer;white-space:nowrap}
.reset-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.card{background:var(--surface);border:1px solid var(--border);
  border-radius:var(--radius);padding:1.5rem;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:-60px;right:-60px;width:180px;height:180px;
  border-radius:50%;background:radial-gradient(circle,rgba(0,229,192,.07) 0%,transparent 70%);
  pointer-events:none}
.section-label{font-size:.72rem;color:var(--muted);text-transform:uppercase;
  letter-spacing:.1em;margin-bottom:.6rem}
.pill{display:inline-flex;align-items:center;gap:.4rem;padding:.2rem .7rem;
  border-radius:99px;font-size:.72rem;letter-spacing:.08em;
  text-transform:uppercase;font-weight:700;margin-bottom:1rem}
.dot{width:6px;height:6px;border-radius:50%}
.s-idle{background:rgba(74,74,106,.25);color:var(--muted)}
.s-idle .dot{background:var(--muted)}
.s-searching{background:rgba(0,229,192,.12);color:var(--cyan)}
.s-searching .dot{background:var(--cyan);animation:blink .7s infinite}
.s-countdown{background:rgba(255,45,120,.14);color:var(--pink)}
.s-countdown .dot{background:var(--pink);animation:blink .4s infinite}
.s-playing{background:rgba(0,229,192,.18);color:var(--cyan)}
.s-playing .dot{background:var(--cyan)}
.s-error{background:rgba(255,45,120,.14);color:var(--pink)}
.s-error .dot{background:var(--pink)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.oauth-box{text-align:center;padding:.5rem 0}
.oauth-box p{font-size:.88rem;color:var(--muted);margin-bottom:.9rem;line-height:1.5}
.oauth-link{display:inline-block;padding:.75rem 1.5rem;
  background:linear-gradient(135deg,var(--cyan),#00b89a);
  color:#000;font-weight:700;border-radius:12px;text-decoration:none;font-size:.95rem}
.oauth-wait{font-size:.8rem;color:var(--muted);margin-top:.75rem}
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
.progress-wrap{margin-top:.9rem}
.progress-bar-bg{background:var(--border);border-radius:99px;height:6px;overflow:hidden}
.progress-bar{background:linear-gradient(90deg,var(--cyan),#00b89a);
  height:100%;border-radius:99px;transition:width .3s}
.progress-label{font-size:.75rem;color:var(--muted);margin-top:.4rem}
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
.cd-wrap{display:flex;flex-direction:column;align-items:center;gap:.75rem;padding:.75rem 0}
.ring{position:relative;width:96px;height:96px}
.ring svg{transform:rotate(-90deg)}
.ring-num{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--pink)}
.cd-lbl{font-size:.78rem;color:var(--muted);letter-spacing:.1em;text-transform:uppercase}
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
.err{font-size:.83rem;color:var(--pink);background:rgba(255,45,120,.08);
  border-radius:10px;padding:.7rem;word-break:break-word;line-height:1.5}
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
    <button class="reset-btn" onclick="changeGame()">⇄ Game</button>
  </div>

  <div class="card" id="oauth-card" style="display:none">
    <div class="section-label">Tidal Login Required</div>
    <div class="oauth-box">
      <p>Open the link below in any browser and log in to your Tidal account.<br>
         This only needs to be done once — the token is stored on the server.</p>
      <a id="oauth-link" href="#" target="_blank" class="oauth-link">🔐 Log in to Tidal</a>
      <div class="oauth-wait">⏳ Waiting for login confirmation…</div>
    </div>
  </div>

  <div class="card" id="game-card">
    <div class="section-label">Select Game</div>
    <select id="game-select"><option value="">— loading games… —</option></select>
    <button class="select-btn" id="select-btn" onclick="selectGame()" disabled>
      Load Game &amp; Start Caching
    </button>
    <div class="progress-wrap" id="progress-wrap" style="display:none">
      <div class="progress-bar-bg"><div class="progress-bar" id="progress-bar" style="width:0%"></div></div>
      <div class="progress-label" id="progress-label">Building Tidal cache…</div>
    </div>
  </div>

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
    <button class="stop-btn" id="stop-btn" style="display:none" onclick="stopScanner()">Stop Camera</button>
  </div>

  <div class="card" id="track-card" style="display:none"></div>

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

async function checkOAuth() {
  const r = await fetch('/api/oauth_status');
  const d = await r.json();
  const card = document.getElementById('oauth-card');
  if (d.url && !d.done) {
    card.style.display = 'block';
    document.getElementById('oauth-link').href = d.url;
  } else {
    card.style.display = 'none';
  }
  return d.done;
}

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
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({file, game})
    });
    const d = await r.json();
    if (d.error) {
      alert('Error: ' + d.error);
      btn.disabled = false;
      btn.textContent = 'Load Game & Start Caching';
      return;
    }
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

function changeGame() {
  audioStarted = false;
  if (currentAudio) { currentAudio.pause(); currentAudio.src = ''; currentAudio = null; }
  fetch('/api/reset', {method:'POST'});
  document.getElementById('scanner-card').style.display  = 'none';
  document.getElementById('manual-card').style.display   = 'none';
  document.getElementById('track-card').style.display    = 'none';
  document.getElementById('select-btn').textContent      = 'Load Game & Start Caching';
  document.getElementById('select-btn').disabled         = false;
  document.getElementById('progress-wrap').style.display = 'none';
  gameLoaded = false;
}

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
      if(currentAudio){currentAudio.pause();currentAudio.src='';currentAudio=null;}
      fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({url:code.data})});
    }
  }
  requestAnimationFrame(tickScan);
}

async function poll(){
  try{
    const r=await fetch('/api/state');
    const d=await r.json();
    if(!d.oauth_done && d.oauth_url){
      document.getElementById('oauth-card').style.display='block';
      document.getElementById('oauth-link').href=d.oauth_url;
    } else {
      document.getElementById('oauth-card').style.display='none';
    }
    if(d.cache_status==='building' && d.cache_total>0){
      const pct=Math.round(100*d.cache_progress/d.cache_total);
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
    if(currentAudio){currentAudio.pause();currentAudio.src='';currentAudio=null;}
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
    el.innerHTML=`<div class="pill s-${d.status}"><span class="dot"></span><span>${pillTxt}</span></div>${body}`;
    if(d.status==='playing') setTimeout(tryAutoplay,80);
  } else if(d.status==='playing' && !audioStarted){
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

function tryAutoplay(){
  const audio=document.getElementById('tidal-audio');
  if(!audio||audioStarted) return;
  currentAudio=audio;
  audio.play().then(()=>{
    audioStarted=true;
    document.getElementById('play-overlay').style.display='none';
  }).catch(()=>{});
}
function manualPlay(){
  const a=document.getElementById('tidal-audio');
  if(!a) return;
  currentAudio=a;
  a.play().then(()=>{
    audioStarted=true;
    document.getElementById('play-overlay').style.display='none';
  }).catch(()=>{});
}
document.addEventListener('click',      ()=>{if(!audioStarted)tryAutoplay();});
document.addEventListener('touchstart', ()=>{if(!audioStarted)tryAutoplay();},{passive:true});

function submitManual(){
  const val=document.getElementById('manual-input').value.trim();
  if(!val) return;
  audioStarted=false;
  if(currentAudio){currentAudio.pause();currentAudio.src='';currentAudio=null;}
  fetch('/api/play',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({card_number:val})});
}
function doReset(){
  audioStarted=false;
  if(currentAudio){currentAudio.pause();currentAudio.src='';currentAudio=null;}
  fetch('/api/reset',{method:'POST'});
}

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
    sid, is_new = get_or_create_session_id()
    get_session(sid)  # ensure session exists
    resp = make_response(render_template_string(HTML, countdown=COUNTDOWN_SEC))
    return session_response(resp, sid, is_new)


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
    sans = [
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
    ]
    try:
        sans.append(x509.IPAddress(ipaddress.IPv4Address(local_ip)))
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
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
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
    log.info("TLS: cert written.")
    return str(cert_path), str(key_path)


# ─────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    get_tidal_session()
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
