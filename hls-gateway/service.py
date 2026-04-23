#!/usr/bin/env python3
"""HLS Gateway on-demand for tvheadend. Spawns ffmpeg per channel on request,
stops after idle timeout. Serves an HLS playlist with 2h DVR window."""
import os, re, sys, signal, json, time, shutil, subprocess, threading, urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from flask import Flask, send_from_directory, abort, Response, request

HLS_DIR        = Path("/data/hls")
COMSKIP_INI    = HLS_DIR / ".comskip.ini"
COMSKIP_INI_TEXT = """\
; Overrides for comskip defaults, written at service startup.
; Tuned for German private TV (RTL/VOX/Sat.1/ProSieben/Kabel Eins/etc.).

; --- Block-length ceilings ---
; DE-private Werbepausen are often 10-15 min with station promos
; inside; default 10-min ceiling splits one break into two entries.
max_commercialbreak=900
max_commercial_size=150

; --- Minimum thresholds, prevent sponsor cards / promos / single
;     teaser interruptions being mis-detected as ad breaks ---
min_commercialbreak=60       ; default 20 — a real break is >= 60 s total
min_commercial_size=20       ; default 4  — a real spot is >= 20 s
min_show_segment_length=120  ; merge breaks separated by < 2 min of show

; --- Detection methods ---
; default detect_method=47 (Black + Logo + Scene + Resolution + AR).
; Drop AR (32) — every DE channel runs 16:9 since ~2010, AR detection
; produces false positives on dark Letterbox film scenes.
detect_method=15

; Confirm block boundaries with audio silence — DE-private ads have
; a reliable audio dip at start/end.
validate_silence=1

; --- Intro/outro grace ---
; First/last 60 s of each show are excluded from detection — show
; intros (Vorspann) often contain blackframes that look ad-like.
intro_max_seconds=60
outro_max_seconds=60

; --- Logo sensitivity ---
; Default 0.80 = block boundary needs 80 % logo-absence confidence.
; That confidence builds slowly when the logo fades out gradually
; (rtlzwei does this), pushing detected ad-start 15-20 s past the
; visual logo-disappear. 0.60 is more responsive at slight cost in
; false-positives; the .scanning + min_commercialbreak=60 pipelines
; on top filter those out anyway.
logo_threshold=0.60

; --- Logo search area ---
; Constrain logo training to the TOP HALF of the frame. Without this,
; comskip's stable-edge-detection can latch onto a news ticker /
; sponsor strip at the bottom (witnessed on rtlzwei: cached template
; was y=475-553 on a 576-tall frame = bottom-of-screen banner, not
; the actual channel logo). Every DE private/public TV logo is in a
; top corner — the bottom-half scan is pure false-positive surface.
subtitles=1
"""


# Per-channel ini overrides, merged on top of COMSKIP_INI_TEXT at
# comskip invocation time. Comskip's ini parser is first-match-wins,
# so per-channel values are PREPENDED to the base ini (not appended).
# Add entries here when a channel needs a tighter threshold, a
# different logo position, etc.
# Per-channel logo position constraints. Comskip only knows three
# logo-position flags (comskip.c:798-800):
#   subtitles=1       → search top half only
#   logo_at_bottom=1  → search bottom half only
#   logo_at_side=1    → search right half only
# There's no _at_top / _at_left / _at_right, so:
#   top-left   logos → subtitles=1 only (global default; no x flag possible)
#   top-right  logos → subtitles=1 + logo_at_side=1   = TR quadrant
#   bottom-... logos → subtitles=0 + logo_at_bottom=1 (+ logo_at_side
#                      for BR) — must override the global subtitles=1
COMSKIP_INI_PER_CHANNEL = {
    # rtlzwei: bottom-right logo. Override global top-half restriction.
    # Soft logo fade also makes even 0.60 occasionally late — tighter
    # threshold + aggressive_logo_rejection to catch logo-loss faster.
    "rtlzwei": {
        "subtitles": "0",
        "logo_at_bottom": "1",
        "logo_at_side": "1",
        "logo_threshold": "0.50",
        "aggressive_logo_rejection": "1",
    },
    # Top-right corner — combined with global subtitles=1 → top-right
    # quadrant only. ~2× faster logo learning, fewer false positives
    # from left-side promo bugs / DOG watermarks.
    "prosieben":  {"logo_at_side": "1"},
    "kabel-eins": {"logo_at_side": "1"},
    "sixx":       {"logo_at_side": "1"},
    # Top-left corner (vox, rtl, kika-hd, toggo-plus, nitro) gets the
    # global subtitles=1 only — comskip has no logo_at_left flag, so
    # no per-channel entry needed.
}


def comskip_ini_for(slug):
    """Return path to a comskip ini for this channel. Writes a
    per-channel temp ini if there are overrides, otherwise returns
    the base. Base is written at service startup by main()."""
    overrides = COMSKIP_INI_PER_CHANNEL.get(slug or "", {})
    base_path = HLS_DIR / ".comskip.ini"
    if not overrides:
        return base_path
    out_path = HLS_DIR / f".comskip-{slug}.ini"
    if (not out_path.exists()
            or out_path.stat().st_mtime < base_path.stat().st_mtime):
        try:
            header = f"; --- per-channel overrides: {slug} ---\n"
            for k, v in overrides.items():
                header += f"{k}={v}\n"
            out_path.write_text(header + "\n" + base_path.read_text())
        except Exception as e:
            print(f"comskip-{slug}.ini write: {e}", flush=True)
            return base_path
    return out_path
TVH_BASE       = os.environ.get("TVH_BASE", "http://localhost:9981")
HOST_URL       = os.environ.get("HOST_URL", "http://raspberrypi5lan:8080")
IDLE_TIMEOUT   = int(os.environ.get("IDLE_TIMEOUT", "120"))
MAX_WARM_STREAMS = int(os.environ.get("MAX_WARM_STREAMS", "3"))
WARM_TTL_SECONDS = int(os.environ.get("WARM_TTL_SECONDS", "180"))
# Watched recordings older than this get auto-deleted by the cleanup
# loop. tvheadend's `watched` field is read-only/computed; the user-
# settable proxy is `playcount` (>0 means watched). Player auto-marks
# at AUTO_WATCHED_THRESHOLD playback fraction.
WATCHED_AUTO_DELETE_DAYS = int(os.environ.get("WATCHED_AUTO_DELETE_DAYS", "7"))
AUTO_WATCHED_THRESHOLD   = 0.90
# Channels to keep warm permanently. Initial seed from env var, then
# persisted to disk so UI toggles survive restarts. LRU eviction never
# touches these; a background loop re-spawns ffmpeg if they die. Cap
# is auto-raised so viewing still has a free slot.
ALWAYS_WARM = {s.strip() for s in
               os.environ.get("ALWAYS_WARM", "").split(",") if s.strip()}
always_warm_lock = threading.Lock()
FAV_TAG_UUID   = os.environ.get("FAV_TAG_UUID", "ed43d130b6d7f8e56b063db6de8d2b06")
SEGMENT_TIME   = 1                                    # shorter = faster first-load
WINDOW_SECONDS = 2 * 3600
LIST_SIZE      = WINDOW_SECONDS // SEGMENT_TIME       # 7200 segments (2h)
CODEC_CACHE_FILE = HLS_DIR / ".codec_cache.json"
STATS_FILE       = HLS_DIR / ".usage_stats.json"
EPG_ARCHIVE_FILE = HLS_DIR / ".epg_archive.jsonl"
ALWAYS_WARM_FILE = HLS_DIR / ".always_warm.json"
EPG_META_FILE    = HLS_DIR / ".epg_meta.json"  # tvmaze poster + rating cache
EPG_META_TTL_S   = 30 * 86400                  # re-fetch each title monthly
CH_LOGO_DIR      = Path(__file__).parent / "static" / "ch-logos"
TMDB_API_KEY     = os.environ.get("TMDB_API_KEY", "").strip()  # optional: better DE show coverage
PIN_HARD_MAX = 3   # tuner-driven upper bound; minus active/scheduled DVR jobs
TUNER_TOTAL = int(os.environ.get("TUNER_TOTAL", "4"))   # FRITZ!Box DVB-C tuners
EPG_SNAPSHOT_INTERVAL = 600   # 10 min
EPG_ARCHIVE_KEEP_DAYS = 14

# How long the sponsor/"Präsentiert von ..."-Einblendung typically is
# per channel. The blackframe-extender adds this to the detected
# transition blackframe after each comskip ad block. Channels not
# listed here use SPONSOR_DURATION_DEFAULT; set to 0 to disable the
# extension entirely for a channel.
SPONSOR_DURATION_DEFAULT = 20.0
SPONSOR_DURATION_BY_CHANNEL = {
    "vox":        25.0,
    "rtl":        20.0,
    "rtlzwei":    20.0,
    "prosieben":  20.0,
    "sat-1":      20.0,
    "kabel-eins": 20.0,
    "sixx":       20.0,
    "super-rtl":  20.0,
    "nitro":      20.0,
    "ntv":        20.0,
    "sport1":     20.0,
}

# Per-channel minimum backward extension when blackframe-extend
# finds nothing usable. Channels with soft logo fades (logo dims
# rather than hard-cuts) leave comskip 15-20 s behind the visual
# ad-start; without a blackframe to snap to we'd otherwise show
# the skip button that late.
START_LAG_FALLBACK = {
    "rtlzwei": 20.0,
}

app = Flask(__name__)


@app.after_request
def _no_cache_dynamic(resp):
    """All dynamically-rendered responses (HTML pages + JSON API
    endpoints) should bypass browser cache. iOS Safari is
    particularly aggressive — without explicit headers it caches:
      - HTML pages → users keep running stale JS for days
      - JSON API responses → polling endpoints like
        /api/warm-status return stale state right after a POST,
        causing the just-toggled pin button to flicker back to its
        old class because refreshWarm sees the cached "still
        pinned" answer.
    Static segments etc. are served by Caddy directly and aren't
    routed through Flask, so they're unaffected."""
    ct = resp.headers.get("Content-Type", "")
    if ct.startswith("text/html") or ct.startswith("application/json"):
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
    return resp


channels     = {}    # slug -> {"process","last_seen","started_at"}
channel_map  = {}    # slug -> {"name","uuid"}
codec_cache  = {}    # slug -> {"video": "h264"|..., "audio": "aac"|...}
stats        = {}    # slug -> {"starts", "watch_seconds", "last_watched"}
cmap_lock    = threading.RLock()
active_lock  = threading.RLock()
codec_lock   = threading.RLock()
stats_lock   = threading.RLock()

# codecs iOS/Safari supports natively (pass-through without transcoding)
SAFE_VIDEO = {"h264", "hevc"}
SAFE_AUDIO = {"aac"}

BASE_CSS = """
:root {
    --bg: #fafafa; --fg: #222; --muted: #777;
    --border: #ddd; --stripe: #f0f0f0;
    --link: #0366d6; --code-bg: #f3f3f3;
    /* Dark plaque works for both bright-coloured and white-on-transparent picons */
    --logo-bg: #1f1f1f;
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg: #1a1a1a; --fg: #e4e4e4; --muted: #999;
        --border: #333; --stripe: #242424;
        --link: #79b8ff; --code-bg: #2a2a2a;
        --logo-bg: #1f1f1f;
    }
}
* { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg); color: var(--fg);
    max-width: 720px; margin: 0 auto; padding: 1.2em;
    line-height: 1.5;
}
h1, h2 { color: var(--fg); }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
code {
    font-size: 0.8em; color: var(--muted);
    background: var(--code-bg);
    padding: 1px 6px; border-radius: 3px;
    word-break: break-all;
}
ul.tools { line-height: 1.8; padding-left: 1.2em; }
ul.channels {
    list-style: none; padding: 0; margin: 0;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(var(--tile-min, 110px), 1fr));
    gap: 10px;
}
ul.channels .logo {
    width: var(--logo-w, 80px); height: var(--logo-h, 60px);
}
body.tile-sm { --tile-min: 80px; --logo-w: 60px; --logo-h: 45px; }
body.tile-lg { --tile-min: 150px; --logo-w: 110px; --logo-h: 82px; }
ul.channels li {
    position: relative;
    display: flex; flex-direction: column; align-items: center;
    justify-content: flex-start; gap: 6px;
    padding: 12px 6px 8px;
    border: 2px solid var(--mux-color, var(--border));
    border-radius: 8px;
    background: var(--stripe);
    transition: border-left-width .12s, padding-left .12s;
}
ul.channels li:has(.buffer-bar.running) {
    border-left-width: 5px;
    padding-left: 3px;   /* compensate so inner content doesn't shift */
}
.now-title {
    font-size: .72em; line-height: 1.2; color: var(--muted);
    text-align: center;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    overflow: hidden;
    word-break: break-word;
    max-width: 100%;
    min-height: 0;
}
.now-title:empty { display: none; }
ul.channels .logo {
    display: flex; align-items: center; justify-content: center;
    overflow: hidden;
}
ul.channels .logo img { max-width: 100%; max-height: 100%; }

/* ===== EPG layout: wrap scrolls h+v, channels column sticky ===== */
.epg-wrap {
    display: flex;
    border: 1px solid var(--border); border-radius: 6px;
    margin: 1em 0;
    max-height: calc(100vh - 170px);
    overflow: auto;
    overscroll-behavior: contain;
}
.epg-channels {
    flex: 0 0 64px;
    background: var(--bg);
    border-right: 1px solid var(--border);
    position: sticky; left: 0; z-index: 2;
}
.epg-tl-scroll {
    flex: 1 1 auto;
    overflow: visible;
}
.epg-tl-inner {
    position: relative;
    /* width is set inline per request */
}
.ch-cell {
    height: 52px;
    display: flex; align-items: center; justify-content: center;
    padding: 4px 6px;
    border-bottom: 1px solid var(--border);
    text-decoration: none; color: var(--fg);
    box-sizing: border-box;
}
.ch-cell:last-child { border-bottom: none; }
.ch-cell img { width: 48px; height: 36px; object-fit: contain;
               flex: 0 0 48px; }
.ch-cell .name { font-weight: 600; font-size: .75em;
                 white-space: nowrap; overflow: hidden;
                 text-overflow: ellipsis; text-align: center; }
.ch-cell .name.fallback { max-width: 52px; }
.ch-cell.header {
    height: 32px;
    background: var(--stripe);
    color: var(--muted);
    font-size: .8em;
}
.tl-row {
    height: 52px;
    position: relative;
    border-bottom: 1px solid var(--border);
    box-sizing: border-box;
}
.tl-row:last-child { border-bottom: none; }
.tl-row.header {
    height: 32px;
    background: var(--stripe);
}
.epg-event {
    position: absolute; top: 4px; bottom: 4px;
    background: var(--code-bg);
    padding: 3px 6px; border-radius: 4px;
    overflow: hidden;
    border: 1px solid var(--border);
    font-size: .82em; line-height: 1.2;
    box-sizing: border-box;
    text-decoration: none; color: inherit;
    -webkit-touch-callout: none;
    -webkit-user-select: none; user-select: none;
}
.epg-event.scheduled::after {
    content: ""; position: absolute;
    top: 4px; right: 4px; width: 8px; height: 8px;
    background: #27ae60; border-radius: 50%;
    box-shadow: 0 0 0 2px var(--code-bg);
}
.epg-event.now.scheduled::after { box-shadow: 0 0 0 2px #1565c0; }
.epg-event.lp-active {
    box-shadow: 0 0 0 2px #e74c3c inset;
    transition: box-shadow .1s;
}
.epg-event .t { font-weight: 600; display: block;
                white-space: nowrap; overflow: hidden;
                text-overflow: ellipsis; }
.epg-event .ts { color: var(--muted); font-size: .75em; }
/* Short events (<10 min) — strip padding, shrink font, hide the
   timestamp line so the title gets every pixel it can. */
.epg-event.tight { padding: 2px 3px; font-size: .66em;
                    font-stretch: condensed; }
.epg-event.tight .ts { display: none; }
.epg-event.tight .t { letter-spacing: -.03em; line-height: 1.1;
                       white-space: normal;
                       display: -webkit-box;
                       -webkit-line-clamp: 3;
                       -webkit-box-orient: vertical;
                       text-overflow: clip;
                       word-break: break-word; }
.epg-event.now {
    background: #1565c0; color: #fff; border-color: #0d47a1;
}
.epg-event.now .ts { color: rgba(255,255,255,.85); }
.epg-event.past { opacity: 0.55; }
.epg-time-marker {
    position: absolute; top: 8px;
    font-size: .75em; color: var(--muted);
    border-left: 1px solid var(--border);
    padding-left: 4px; padding-top: 2px;
}

.epg-now-line {
    position: absolute; top: 0; bottom: 0;
    width: 2px; background: #e74c3c;
    z-index: 4; pointer-events: none;
}
.epg-now-line::before {
    content: ""; position: absolute;
    top: -4px; left: -4px;
    width: 10px; height: 10px;
    background: #e74c3c; border-radius: 50%;
}
.epg-controls {
    display: flex; align-items: center; gap: 1em;
    margin: 1em 0 .4em;
}
.btn-now {
    display: inline-block;
    background: var(--code-bg); color: var(--fg);
    padding: .4em .9em; border-radius: 4px;
    font-size: .9em; font-weight: 600;
    text-decoration: none;
    border: 1px solid var(--border);
}
.btn-now:hover { background: var(--stripe); text-decoration: none; }

table { border-collapse: collapse; width: 100%; }
th, td {
    padding: .5em .7em; text-align: left;
    border-bottom: 1px solid var(--border);
}
tr:nth-child(even) td { background: var(--stripe); }
.rank { color: var(--muted); }
"""


def slugify(name):
    s = name.lower()
    s = re.sub(r'[äöüß]', lambda m: {'ä':'ae','ö':'oe','ü':'ue','ß':'ss'}[m.group()], s)
    s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
    return s


def load_favorites():
    data = json.loads(urllib.request.urlopen(
        f"{TVH_BASE}/api/channel/grid?limit=500", timeout=10).read())
    # Map service UUID → transponder mux so we can tell the user which
    # channels share a physical tuner. Channels on the same mux stream
    # for free (tvheadend shares the transponder).
    svc_to_mux = {}
    try:
        sdata = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/mpegts/service/grid?limit=2000",
            timeout=10).read())
        for s in sdata.get("entries", []):
            svc_to_mux[s["uuid"]] = (
                s.get("multiplex_uuid", ""),
                s.get("multiplex", ""),
            )
    except Exception as e:
        print(f"service grid fetch: {e}", flush=True)
    new_map = {}
    for e in data.get("entries", []):
        if FAV_TAG_UUID not in e.get("tags", []):
            continue
        icon = e.get("icon_public_url") or ""
        # tvheadend returns icon URLs as relative like "imagecache/123".
        # We proxy /imagecache/* via the same HOST_URL to avoid mixed-content
        # (if HOST_URL is https) — Caddy reverse-proxies imagecache to tvheadend.
        if icon and not icon.startswith("http"):
            icon = f"{HOST_URL}/{icon.lstrip('/')}"
        svcs = e.get("services", [])
        mux_uuid, mux_name = svc_to_mux.get(svcs[0], ("", "")) if svcs else ("", "")
        new_map[slugify(e["name"])] = {
            "name": e["name"], "uuid": e["uuid"], "icon": icon,
            "mux_uuid": mux_uuid, "mux_name": mux_name,
        }
    with cmap_lock:
        channel_map.clear()
        channel_map.update(new_map)
    print(f"Loaded {len(new_map)} favorite channels", flush=True)


def probe_codecs(slug):
    """ffprobe the channel briefly, return {'video': ..., 'audio': ...} or None."""
    info = channel_map.get(slug)
    if not info:
        return None
    tvh_url = f"{TVH_BASE}/stream/channel/{info['uuid']}?profile=pass"
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-analyzeduration", "800000", "-probesize", "500000",
             "-show_entries", "stream=codec_type,codec_name",
             "-of", "json", tvh_url],
            capture_output=True, timeout=15)
        if r.returncode != 0:
            print(f"[{slug}] probe returncode={r.returncode}", flush=True)
            return None
        data = json.loads(r.stdout.decode() or "{}")
        codecs = {"video": None, "audio": None}
        for s in data.get("streams", []):
            t = s.get("codec_type"); n = s.get("codec_name")
            if t == "video" and not codecs["video"]: codecs["video"] = n
            elif t == "audio" and not codecs["audio"]: codecs["audio"] = n
        return codecs
    except Exception as e:
        print(f"[{slug}] probe exception: {e}", flush=True)
        return None


def load_codec_cache():
    if not CODEC_CACHE_FILE.exists():
        return
    try:
        data = json.loads(CODEC_CACHE_FILE.read_text())
        with codec_lock:
            codec_cache.update(data)
        print(f"Loaded {len(data)} cached codecs from disk", flush=True)
    except Exception as e:
        print(f"load codec cache: {e}", flush=True)


def save_codec_cache():
    try:
        with codec_lock:
            data = dict(codec_cache)
        CODEC_CACHE_FILE.write_text(json.dumps(data, indent=1))
    except Exception as e:
        print(f"save codec cache: {e}", flush=True)


def get_codecs(slug):
    with codec_lock:
        if slug in codec_cache:
            return codec_cache[slug]
    c = probe_codecs(slug)
    if not c:
        c = {"video": "unknown", "audio": "unknown"}
    with codec_lock:
        codec_cache[slug] = c
    save_codec_cache()
    print(f"[{slug}] codecs: v={c['video']} a={c['audio']}", flush=True)
    return c


def load_stats():
    if not STATS_FILE.exists():
        return
    try:
        data = json.loads(STATS_FILE.read_text())
        with stats_lock:
            stats.update(data)
        print(f"Loaded usage stats for {len(data)} channels", flush=True)
    except Exception as e:
        print(f"load stats: {e}", flush=True)


def save_stats():
    try:
        with stats_lock:
            data = dict(stats)
        STATS_FILE.write_text(json.dumps(data, indent=1))
    except Exception as e:
        print(f"save stats: {e}", flush=True)


def record_start(slug):
    with stats_lock:
        s = stats.setdefault(slug, {"starts": 0, "watch_seconds": 0,
                                     "last_watched": None})
        s["starts"] += 1
        s["last_watched"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_stats()


def record_stop(slug, started_at):
    duration = max(0, time.time() - started_at)
    with stats_lock:
        s = stats.setdefault(slug, {"starts": 0, "watch_seconds": 0,
                                     "last_watched": None})
        s["watch_seconds"] += duration
    save_stats()


def prewarm_codecs():
    """Probe all channels that aren't in cache, sequentially, in background."""
    time.sleep(3)   # let startup settle
    with cmap_lock:
        slugs = sorted(channel_map.keys())
    to_probe = [s for s in slugs if s not in codec_cache]
    if not to_probe:
        print("[prewarm] all channels already cached", flush=True)
        return
    print(f"[prewarm] probing {len(to_probe)} channels in background...", flush=True)
    for slug in to_probe:
        get_codecs(slug)
        time.sleep(0.5)   # be gentle on tuners
    print(f"[prewarm] done", flush=True)


class AdoptedProcess:
    """Popen-compatible stand-in for an ffmpeg process that survived
    a container restart (we spawn with start_new_session=True so the
    children get orphaned from PID 1 instead of killed). We re-attach
    by PID on the next hls-gateway startup."""
    def __init__(self, pid):
        self.pid = pid
    def poll(self):
        # os.kill(pid, 0) treats zombies as alive — they're still in
        # the process table. Read /proc/<pid>/status and treat state
        # 'Z' as dead, and try to reap the zombie so the always-warm
        # loop can respawn cleanly.
        try:
            os.kill(self.pid, 0)
        except OSError:
            return 0
        try:
            with open(f"/proc/{self.pid}/status") as f:
                for line in f:
                    if line.startswith("State:"):
                        if line.split()[1] == "Z":
                            try: os.waitpid(self.pid, os.WNOHANG)
                            except (ChildProcessError, OSError): pass
                            return 0
                        break
        except FileNotFoundError:
            return 0
        except Exception:
            pass
        return None
    def terminate(self):
        try: os.kill(self.pid, 15)
        except OSError: pass
    def kill(self):
        try: os.kill(self.pid, 9)
        except OSError: pass
    def wait(self, timeout=None):
        deadline = time.time() + (timeout or 1e9)
        while time.time() < deadline:
            if self.poll() is not None:
                return 0
            time.sleep(0.1)
        raise subprocess.TimeoutExpired(cmd="", timeout=timeout)


def adopt_surviving_ffmpegs():
    """Look for PID files left behind by ffmpegs from a prior container
    run. For each still-alive ffmpeg, register it in `channels` so the
    HLS buffer continues uninterrupted."""
    if not HLS_DIR.exists():
        return
    adopted = 0
    for ch_dir in HLS_DIR.iterdir():
        if not ch_dir.is_dir() or ch_dir.name.startswith((".", "_")):
            continue
        pid_file = ch_dir / ".ffmpeg.pid"
        if not pid_file.exists():
            continue
        try:
            data = json.loads(pid_file.read_text())
            pid = int(data["pid"])
            started_at = float(data.get("started_at", time.time()))
        except Exception:
            try: pid_file.unlink()
            except Exception: pass
            continue
        stub = AdoptedProcess(pid)
        if stub.poll() is not None:
            try: pid_file.unlink()
            except Exception: pass
            continue
        slug = ch_dir.name
        with cmap_lock:
            if slug not in channel_map:
                continue
        with active_lock:
            channels[slug] = {"process": stub,
                              "last_seen": time.time(),
                              "started_at": started_at}
        adopted += 1
        age = int(time.time() - started_at)
        print(f"[{slug}] adopted ffmpeg pid={pid} "
              f"(buffer age {age}s)", flush=True)
    if adopted:
        print(f"adopted {adopted} surviving ffmpeg processes", flush=True)


def start_ffmpeg(slug):
    info = channel_map.get(slug)
    if not info:
        return None
    ch_dir = HLS_DIR / slug
    if ch_dir.exists():
        shutil.rmtree(ch_dir)
    ch_dir.mkdir(parents=True, exist_ok=True)

    codecs = get_codecs(slug)
    if codecs["video"] in SAFE_VIDEO:
        video_opts = ["-c:v", "copy"]
    else:
        # MPEG-2 or unknown → transcode to H.264 for iOS compatibility.
        # Scale anamorphic SD (720x576 SAR 64:45) to square pixels so
        # iOS/Safari renders the correct 16:9 aspect instead of 4:3-ish.
        video_opts = [
            "-vf", "scale=trunc(iw*sar/2)*2:trunc(ih/2)*2,setsar=1",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-profile:v", "main",
            "-pix_fmt", "yuv420p",
            "-g", "50",                  # keyframe every ~2s
            "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
        ]

    if codecs["audio"] in SAFE_AUDIO:
        audio_opts = ["-c:a", "copy"]
    else:
        audio_opts = ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]

    tvh_url = f"{TVH_BASE}/stream/channel/{info['uuid']}?profile=pass"
    cmd = [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "warning",
        "-fflags", "+genpts+discardcorrupt",
        "-i", tvh_url,
        "-map", "0:v:0", "-map", "0:a:0",
        *video_opts, *audio_opts,
        "-start_at_zero",
        "-avoid_negative_ts", "make_zero",
        "-f", "hls",
        "-hls_time", str(SEGMENT_TIME),
        "-hls_list_size", str(LIST_SIZE),
        "-hls_flags",
        "delete_segments+append_list+independent_segments+program_date_time",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", str(ch_dir / "seg_%06d.ts"),
        str(ch_dir / "index.m3u8"),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL,
                             start_new_session=True)
    mode = "passthrough" if codecs["video"] in SAFE_VIDEO and codecs["audio"] in SAFE_AUDIO \
           else f"transcode v={codecs['video']} a={codecs['audio']}"
    # Record pid + start time so we can re-adopt the ffmpeg if the
    # hls-gateway container restarts (segments keep rolling meanwhile).
    try:
        (ch_dir / ".ffmpeg.pid").write_text(json.dumps({
            "pid": proc.pid, "started_at": time.time(),
        }))
    except Exception as e:
        print(f"[{slug}] pid-file write: {e}", flush=True)
    print(f"[{slug}] ffmpeg pid={proc.pid} ({mode})", flush=True)
    return proc


def stop_channel(slug):
    with active_lock:
        info = channels.pop(slug, None)
    if info:
        if info["process"].poll() is None:
            try:
                info["process"].terminate()
                try: info["process"].wait(timeout=5)
                except Exception: info["process"].kill()
            except Exception: pass
        record_stop(slug, info.get("started_at", time.time()))
        print(f"[{slug}] stopped", flush=True)
    # Best-effort cleanup of the pid file so an orphaned ffmpeg from a
    # prior container run isn't re-adopted after we intentionally stopped
    try: (HLS_DIR / slug / ".ffmpeg.pid").unlink()
    except Exception: pass


def ensure_running(slug):
    """Start ffmpeg for this channel if not already running. Keeps
    recently-watched channels warm (idle ffmpeg still filling DVR
    buffer) so switching back gives instant timeshift. If we hit
    MAX_WARM_STREAMS, evict LRU warm channels first to free tuners."""
    # Any viewer request wakes a dormant pin back up.
    with _dormant_pins_lock:
        _dormant_pins.discard(slug)
    victims = []
    with active_lock:
        info = channels.get(slug)
        if info and info["process"].poll() is None:
            info["last_seen"] = time.time()
            return
        if info:
            record_stop(slug, info.get("started_at", time.time()))
            channels.pop(slug, None)
        others = [(s, i["last_seen"]) for s, i in channels.items()
                  if i["process"].poll() is None and s != slug
                  and s not in ALWAYS_WARM]   # never evict permanent
        others.sort(key=lambda x: x[1])
        while len(others) >= MAX_WARM_STREAMS:
            victims.append(others.pop(0)[0])
        proc = start_ffmpeg(slug)
        if proc:
            now = time.time()
            channels[slug] = {"process": proc, "last_seen": now,
                              "started_at": now}
            record_start(slug)
    for v in victims:
        print(f"[{v}] LRU-evicted to free tuner for {slug}", flush=True)
        stop_channel(v)


def idle_killer_loop():
    """Stop non-pinned warm streams after WARM_TTL_SECONDS of idleness.
    Short TTL (3 min default) frees the DVB tuner + CPU + SSD writes
    quickly when the user genuinely walks away from a channel, while
    still riding through brief cross-channel comparisons or quick
    info-screens that bring the user back within the grace period.
    Pinned (ALWAYS_WARM) channels are exempt and run forever."""
    while True:
        time.sleep(30)
        try:
            now = time.time()
            with active_lock:
                stale = [s for s, i in channels.items()
                         if now - i["last_seen"] > WARM_TTL_SECONDS
                         and s not in ALWAYS_WARM]
            for s in stale:
                print(f"[{s}] warm TTL expired ({WARM_TTL_SECONDS}s)",
                      flush=True)
                stop_channel(s)
        except Exception as e:
            print(f"idle loop: {e}", flush=True)


def load_always_warm():
    global MAX_WARM_STREAMS
    if ALWAYS_WARM_FILE.exists():
        try:
            data = json.loads(ALWAYS_WARM_FILE.read_text())
            with always_warm_lock:
                ALWAYS_WARM.clear()
                ALWAYS_WARM.update(data.get("slugs", []))
        except Exception as e:
            print(f"load always-warm: {e}", flush=True)
    if ALWAYS_WARM:
        MAX_WARM_STREAMS = max(MAX_WARM_STREAMS, len(ALWAYS_WARM) + 1)
    print(f"always-warm: {sorted(ALWAYS_WARM) or '(none)'}  "
          f"max_warm={MAX_WARM_STREAMS}", flush=True)


def save_always_warm():
    try:
        with always_warm_lock:
            payload = {"slugs": sorted(ALWAYS_WARM)}
        ALWAYS_WARM_FILE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        print(f"save always-warm: {e}", flush=True)


def set_always_warm(slug, on):
    """Toggle a channel's pinned-warm state. Returns True if state
    was changed. Pinning spawns ffmpeg; unpinning keeps the stream
    running but re-subjects it to normal LRU eviction (TTL or
    replaced-by-next-viewed-channel)."""
    global MAX_WARM_STREAMS
    changed = False
    with always_warm_lock:
        if on and slug not in ALWAYS_WARM:
            ALWAYS_WARM.add(slug); changed = True
        elif not on and slug in ALWAYS_WARM:
            ALWAYS_WARM.discard(slug); changed = True
    if changed:
        MAX_WARM_STREAMS = max(
            int(os.environ.get("MAX_WARM_STREAMS", "3")),
            len(ALWAYS_WARM) + 1)
        save_always_warm()
        if on:
            ensure_running(slug)
    return changed


PIN_IDLE_HOURS = 6
_dormant_pins = set()
_dormant_pins_lock = threading.Lock()


def always_warm_loop():
    """Background watchdog for pinned channels. Respawns a dead
    ffmpeg for anything in ALWAYS_WARM unless the channel is dormant
    (hit the idle timeout). Kills a warm ffmpeg after PIN_IDLE_HOURS
    without a viewer — saves a tuner + CPU while still auto-restarting
    on the next tap (ensure_running clears the dormant flag)."""
    time.sleep(15)
    idle_cutoff = PIN_IDLE_HOURS * 3600
    while True:
        try:
            now = time.time()
            for slug in list(ALWAYS_WARM):
                with _dormant_pins_lock:
                    dormant = slug in _dormant_pins
                with active_lock:
                    info = channels.get(slug)
                    alive = info and info["process"].poll() is None
                if alive:
                    idle = now - (info.get("last_seen") or now)
                    if idle > idle_cutoff:
                        print(f"[{slug}] pin idle {idle/3600:.1f}h — "
                              f"stopping, will re-arm on next tap",
                              flush=True)
                        stop_channel(slug)
                        with _dormant_pins_lock:
                            _dormant_pins.add(slug)
                elif not dormant:
                    print(f"[{slug}] always-warm respawn", flush=True)
                    ensure_running(slug)
        except Exception as e:
            print(f"always-warm loop: {e}", flush=True)
        time.sleep(30)


def ffmpeg_watchdog_loop():
    """Check every 30 s that each running ffmpeg is still writing
    segments. If the latest seg file hasn't changed in 90 s, the
    ffmpeg is stuck (tuner lost / SAT>IP timeout / decode freeze):
    kill it and let ensure_running respawn it. Preserves the 2 h
    DVR buffer because old segments stay on disk."""
    time.sleep(45)
    STUCK_TIMEOUT = 90.0
    last_seen = {}   # slug -> (latest_seg_mtime, observed_at)
    while True:
        try:
            now = time.time()
            with active_lock:
                running = [(s, i) for s, i in channels.items()
                           if i["process"].poll() is None]
            for slug, info in running:
                ch_dir = HLS_DIR / slug
                try:
                    segs = sorted(ch_dir.glob("seg_*.ts"))
                    mtime = segs[-1].stat().st_mtime if segs else 0
                except Exception:
                    continue
                prev = last_seen.get(slug)
                if prev is None or prev[0] != mtime:
                    last_seen[slug] = (mtime, now)
                    continue
                if now - prev[1] > STUCK_TIMEOUT:
                    # Only kill if ffmpeg has actually had time to settle.
                    if (now - info.get("started_at", now)) < 60:
                        continue
                    print(f"[{slug}] watchdog: no new segment in "
                          f"{int(now-prev[1])}s — killing + respawning",
                          flush=True)
                    stop_channel(slug)
                    last_seen.pop(slug, None)
                    ensure_running(slug)
        except Exception as e:
            print(f"watchdog loop: {e}", flush=True)
        time.sleep(30)


def state_backup_loop():
    """Copy the important JSON state files from /mnt/tv (SSD) to a
    tiny backup dir inside the container image's writeable mount
    every 10 min. Protects against SSD failure: if the disk drops
    out, .always_warm.json, .mediathek_recordings.json etc. still
    survive and can be restored manually."""
    import shutil as _sh
    targets = [
        ".always_warm.json",
        ".mediathek_recordings.json",
        ".usage_stats.json",
        ".live_ads.json",
        ".codec_cache.json",
    ]
    backup_dir = Path("/state-backup")
    if not backup_dir.exists():
        # Mount not configured — silently no-op instead of crashing.
        return
    time.sleep(60)
    while True:
        try:
            for name in targets:
                src = HLS_DIR / name
                if not src.exists():
                    continue
                try:
                    _sh.copy2(src, backup_dir / name)
                except Exception as e:
                    print(f"[state-backup] {name}: {e}", flush=True)
        except Exception as e:
            print(f"state-backup loop: {e}", flush=True)
        time.sleep(600)


def disk_cleanup_loop():
    """When /mnt/tv falls below DISK_MIN_FREE_GB, delete the oldest
    `_rec_<uuid>/` HLS-VOD remux directories (LRU by mtime) until
    there's again at least DISK_TARGET_FREE_GB free. The original .ts
    recordings in /recordings stay — only the remuxed copies go, and
    they're lazy-rebuilt on next playback."""
    import shutil as _sh
    DISK_MIN_FREE_GB = 8.0
    DISK_TARGET_FREE_GB = 15.0
    time.sleep(90)
    while True:
        try:
            usage = _sh.disk_usage(HLS_DIR)
            free_gb = usage.free / (1024 ** 3)
            if free_gb < DISK_MIN_FREE_GB:
                rec_dirs = []
                for p in HLS_DIR.glob("_rec_*"):
                    if p.is_dir():
                        try: rec_dirs.append((p.stat().st_mtime, p))
                        except Exception: pass
                rec_dirs.sort()   # oldest first
                for mtime, p in rec_dirs:
                    usage = _sh.disk_usage(HLS_DIR)
                    if usage.free / (1024 ** 3) >= DISK_TARGET_FREE_GB:
                        break
                    try:
                        size_mb = sum(f.stat().st_size
                                      for f in p.rglob("*")
                                      if f.is_file()) / (1024 ** 2)
                        _sh.rmtree(p, ignore_errors=True)
                        print(f"[disk-cleanup] removed {p.name} "
                              f"({size_mb:.0f} MB)", flush=True)
                    except Exception as e:
                        print(f"[disk-cleanup] rm {p}: {e}", flush=True)
        except Exception as e:
            print(f"disk-cleanup loop: {e}", flush=True)
        time.sleep(300)


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/")
def index():
    now = time.time()
    # Snapshot stats + live sessions (contribute to sort)
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    with active_lock:
        for s, i in channels.items():
            st_snap.setdefault(s, {"starts": 0, "watch_seconds": 0})
            st_snap[s]["watch_seconds"] = st_snap[s].get("watch_seconds", 0) \
                                           + (now - i.get("started_at", now))

    with cmap_lock:
        items = list(channel_map.items())
    # Sort: pinned first, then watch_seconds DESC, starts DESC, name ASC
    items.sort(key=lambda kv: (
        0 if kv[0] in ALWAYS_WARM else 1,
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower(),
    ))

    # Assign a stable colour per mux transponder so viewers can tell
    # at a glance which channels share a tuner. Only show the dot if
    # the mux has 2+ favourites on it — a solo channel's mux info is
    # noise.
    mux_members = {}
    for s, info in items:
        mu = info.get("mux_uuid") or ""
        if mu:
            mux_members.setdefault(mu, []).append(s)
    # Six hues chosen so every pair stays distinguishable at an 8 px
    # dot: black, green, blue, pink, yellow, brown. No orange (reads
    # as red), no cyan/purple (collapse into green/blue at that size).
    MUX_PALETTE = [
        "#1976d2",   # blue
        "#27ae60",   # green
        "#ff9800",   # orange
        "#8e44ad",   # purple
        "#f1c40f",   # yellow
        "#00bcd4",   # cyan
        "#c0392b",   # red
        "#8bc34a",   # lime
        "#2c3e50",   # slate
        "#795548",   # brown
        "#009688",   # teal
        "#673ab7",   # deep-purple
    ]
    mux_colour = {}
    for i, mu in enumerate(sorted(mux_members.keys())):
        mux_colour[mu] = MUX_PALETTE[i % len(MUX_PALETTE)]

    rows = []
    for s, info in items:
        watch_url = f"{HOST_URL}/watch/{s}"
        dvr_url   = f"{HOST_URL}/hls/{s}/dvr.m3u8"
        icon = _channel_logo_url(s, info.get("icon", ""))
        logo = (f'<img src="{icon}" alt="" loading="lazy">'
                if icon else "")
        st = st_snap.get(s, {})
        starts = st.get("starts", 0)
        hours = st.get("watch_seconds", 0) / 3600
        usage = (f'<span class="usage">{starts}× · {hours:.1f} h</span>'
                 if starts > 0 else
                 '<span class="usage muted">neu</span>')
        mu = info.get("mux_uuid", "")
        mux_dot = ""
        if mu in mux_colour:
            mates = [channel_map[m]["name"] for m in mux_members[mu]
                     if m != s]
            title = (f'{info.get("mux_name","Mux")} — teilt Tuner mit '
                     f'{", ".join(mates)}')
            mux_dot = (f'<span class="mux-dot" '
                       f'style="background:{mux_colour[mu]}" '
                       f'title="{title}"></span>')
        mux_style = (f' style="--mux-color:{mux_colour[mu]}"'
                     if mu in mux_colour else "")
        mux_attr = f' data-mux="{mu}"' if mu else ""
        mt_attr  = ' data-mediathek="1"' if s in MEDIATHEK_LIVE else ""
        rows.append(
            f'<li data-slug="{s}"{mux_attr}{mt_attr} title="{info["name"]}"{mux_style}>'
            f'<button class="pin-btn" data-slug="{s}" title="Dauer-warm">📌</button>'
            f'<a class="logo" href="{watch_url}">{logo}</a>'
            f'<span class="buffer-bar" data-slug="{s}"></span>'
            f'<span class="now-title" data-slug="{s}"></span>'
            f'</li>')
    tools = [
        ("Alle Kanäle als M3U (für IPTV-Apps)", f"{HOST_URL}/playlist.m3u"),
        ("Nutzungsstatistik / Top-Sender",      f"{HOST_URL}/stats"),
        ("Status (gerade aktive Streams)",      f"{HOST_URL}/status"),
        ("Kanal-Liste neu laden",              f"{HOST_URL}/reload"),
    ]
    tool_rows = "".join(
        f'<li><a href="{url}">{label}</a><br>'
        f'<code style="font-size:0.8em;color:#888">{url}</code></li>'
        for label, url in tools)
    extra_css = (
        ".buffer-bar{position:absolute;left:4px;right:4px;bottom:3px;"
        "height:4px;border-radius:3px;background:transparent;z-index:3}"
        "ul.channels li.scanning::before{content:'🔍';position:absolute;"
        "top:4px;left:6px;font-size:.8em;z-index:2;"
        "animation:scanpulse 1.2s ease-in-out infinite}"
        "@keyframes scanpulse{0%,100%{opacity:.4}50%{opacity:1}}"
        ".buffer-bar.running{background:#00000022;cursor:pointer;"
        "box-shadow:inset 0 0 0 1px #0001}"
        "@media (prefers-color-scheme:dark){"
        ".buffer-bar.running{background:#ffffff33;box-shadow:none}"
        "}"
        ".buffer-bar::before{content:'';display:block;height:100%;"
        "width:var(--pct,0);background:#27ae60;border-radius:3px;"
        "transition:width .4s}"
        # Invisible tap-extender: bottom band of the tile is clickable.
        ".buffer-bar.running::after{content:'';position:absolute;"
        "left:-4px;right:-4px;top:-14px;bottom:-6px}"
        ".pin-btn{position:absolute;top:-9px;right:-8px;"
        "background:none;border:0;cursor:pointer;font-size:1.1em;"
        "opacity:.35;padding:3px;line-height:1;transform:rotate(35deg);"
        "transform-origin:center;"
        "transition:opacity .15s,transform .15s,filter .15s;z-index:2;"
        # Unpinned: grayscale + drop-shadow desaturates the emoji so it
        # reads as "off". opacity alone wasn't enough on iOS — the
        # native red push-pin emoji rendered as full colour even at
        # opacity .2, indistinguishable from the active state.
        "filter:grayscale(1) drop-shadow(0 1px 2px #0008)}"
        # Hover gated on a device with real hover capability —
        # iOS Safari latches :hover after a tap and never clears it
        # without a navigation, leaving the just-tapped pin stuck in
        # the half-grayscale "kind of pinned" middle state until a
        # page reload.
        "@media (hover:hover){"
        ".pin-btn:hover{opacity:.75;transform:rotate(45deg) scale(1.1);"
        "filter:grayscale(.5) drop-shadow(0 1px 2px #0008)}"
        "}"
        # Note: 'filter: none drop-shadow(...)' is invalid CSS — 'none'
        # cancels all filters, can't compose with other functions.
        # Just drop-shadow alone: no grayscale → emoji renders in full
        # red; the implicit reset of grayscale relative to the parent
        # .pin-btn rule is what we want.
        ".pin-btn.active{opacity:1;transform:rotate(45deg);"
        "filter:drop-shadow(0 2px 3px #0009)}"
        "ul.channels li:has(.pin-btn.active){"
        "box-shadow:0 3px 10px #0003;transform:translateY(-1px)}"
        ".pin-btn.dormant{opacity:.6}"
        ".tuner-badge{display:inline-block;font-size:.72em;font-weight:600;"
        "padding:3px 10px;border-radius:12px;"
        "background:#34495e;color:#fff;letter-spacing:.03em}"
        ".tuner-badge:empty{display:none}"
        ".tuner-badge.tight{background:#e67e22}"
        ".tuner-badge.full{background:#c0392b}"
        ".host-badges{display:flex;flex-wrap:wrap;gap:6px;margin:-4px 0 12px}"
        ".channels-head{display:flex;align-items:center;gap:10px}"
        ".tile-size-btn{background:transparent;border:1px solid var(--border);"
        "border-radius:6px;padding:2px 9px;cursor:pointer;font-size:.9em;"
        "color:var(--muted);line-height:1}"
        ".tile-size-btn:hover{background:var(--stripe)}"
        ".quick-links{display:flex;gap:10px;margin:16px 0 24px}"
        ".quick-links a{flex:1;background:var(--stripe);border:1px solid var(--border);"
        "border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;"
        "align-items:flex-start;gap:2px;text-decoration:none;color:var(--fg);"
        "font-weight:600;font-size:1em;transition:background .15s}"
        ".quick-links a:hover{background:var(--code-bg);text-decoration:none}"
        ".quick-links span{font-size:1.6em;line-height:1}"
        ".quick-links small{font-weight:400;color:var(--muted);font-size:.82em}"
        "h2.tools-head{margin-top:2em;font-size:1em;color:var(--muted);"
        "font-weight:500;border-top:1px solid var(--border);padding-top:1em}"
        "ul.tools li{font-size:.9em;opacity:.75}"
        ".mux-dot{display:inline-block;width:8px;height:8px;"
        "border-radius:50%;margin-right:6px;vertical-align:1px;"
        "cursor:help}"
        # Subtle blue accent line at the bottom-inside edge of the tile,
        # marks channels that have a public-broadcaster Mediathek-Live
        # fallback stream (MEDIATHEK_LIVE). Sits below the buffer-bar
        # (which is bottom:3px height:4px) without overlap.
        "ul.channels li[data-mediathek]{"
        "box-shadow:inset 0 -3px 0 #2980b9}"
        ".rec-modal{position:fixed;inset:0;background:#000a;"
        "display:flex;align-items:center;justify-content:center;"
        "z-index:100}"
        ".rec-dialog{background:var(--bg);color:var(--fg);"
        "padding:18px;border-radius:10px;max-width:320px;width:100%;"
        "border:1px solid var(--border)}"
        ".rec-dialog button{padding:8px 14px;border-radius:6px;"
        "border:1px solid var(--border);background:var(--stripe);"
        "color:var(--fg);font-weight:600;cursor:pointer;font-size:1em}"
        ".rec-dialog button.cancel{background:transparent;font-weight:400}"
    )
    js = (
        "let _warmFailures=0;"
        "function setRecoveryBanner(on){"
        "  const b=document.getElementById('recovery-banner');"
        "  if(b)b.style.display=on?'block':'none';"
        "}"
        "async function refreshWarm(){"
        "  try{"
        "    const r=await fetch('/api/warm-status');"
        "    if(!r.ok)throw new Error('http '+r.status);"
        "    const d=await r.json();"
        "    _warmFailures=0;"
        "    setRecoveryBanner(false);"
        "    const W=d.window_seconds||7200;"
        "    const scanSlug=d.adskip_slug||null;"
        "    for(const li of document.querySelectorAll('ul.channels li')){"
        "      li.classList.toggle('scanning',li.dataset.slug===scanSlug);"
        "    }"
        "    for(const nt of document.querySelectorAll('.now-title')){"
        "      const s=nt.dataset.slug;"
        "      const e=d.channels[s];"
        "      nt.textContent=e&&e.now?e.now:'';"
        "    }"
        "    for(const bar of document.querySelectorAll('.buffer-bar')){"
        "      const s=bar.dataset.slug;"
        "      const e=d.channels[s];"
        "      if(!e||!e.running){"
        "        bar.style.setProperty('--pct','0');bar.title='';"
        "        bar.classList.remove('running','pinned');"
        "      } else {"
        "        const bs=e.buffer_seconds;"
        "        const full=bs>=W-30;"
        "        let time;"
        "        if(full)time='2h';"
        "        else if(bs>=3600)time=Math.floor(bs/3600)+'h'+Math.floor((bs%3600)/60)+'m';"
        "        else if(bs>=60)time=Math.floor(bs/60)+'m';"
        "        else time=bs+'s';"
        "        bar.classList.add('running');"
        "        bar.classList.toggle('pinned',!!e.always_warm);"
        "        bar.title=e.always_warm?'Dauer-warm · Puffer '+time+(full?' (voll)':''):"
        "          'Warm-Tuner · Puffer '+time+(full?' (voll)':'')+' · Klick: Tuner freigeben';"
        "        bar.style.setProperty('--pct',Math.min(100,bs*100/W)+'%');"
        "      }"
        "    }"
        "    const budget=(d.pin_budget||0);"
        "    for(const b of document.querySelectorAll('.pin-btn')){"
        "      const s=b.dataset.slug;"
        "      const e=d.channels[s];"
        "      const pinned=e&&e.always_warm;"
        "      const dormant=pinned&&e&&e.dormant;"
        "      b.classList.toggle('active',!!pinned);"
        "      b.classList.toggle('dormant',!!dormant);"
        "      b.textContent=dormant?'💤':'📌';"
        "      if(!pinned&&budget<=0){"
        "        b.style.display='none';"
        "      } else {"
        "        b.style.display='';"
        "      }"
        "      b.title=dormant?'Pin ruht (6h ohne Zugriff) — Tap weckt':"
        "              pinned?'Dauer-warm aktiv — klick zum Deaktivieren':"
        "                     'Kanal dauer-warm halten (max '+(d.pin_limit||0)+' bei '+(d.pin_dvr_reserve||0)+' DVR-Jobs)';"
        "    }"
        "    const tb=document.getElementById('tuner-badge');"
        "    if(tb){"
        "      const u=d.tuners_used,t=d.tuners_total||4,epg=d.tuners_epggrab||0;"
        "      if(u===null||u===undefined){tb.textContent='';tb.className='tuner-badge';}"
        "      else{"
        "        const epgSuffix=epg>0?' ('+epg+'× EPG)':'';"
        "        tb.textContent='📡 '+u+'/'+t+' Tuner'+epgSuffix;"
        "        tb.className='tuner-badge'+(u>=t?' full':u>=t-1?' tight':'');"
        "        tb.title='DVB-C Tuner in Nutzung (FRITZ!Box SAT>IP). '+"
        "          (epg>0?epg+' davon für EPG-OTA-Grab (läuft nach tvheadend-Restart auto durch). ':'')+"
        "          'Kanäle auf demselben Mux teilen sich einen Tuner.';"
        "      }"
        "    }"
        "    const fb=document.getElementById('ffmpeg-badge');"
        "    if(fb){"
        "      const n=d.ffmpeg_count;"
        "      if(n===null||n===undefined){fb.textContent='';fb.className='tuner-badge';}"
        "      else{"
        "        fb.textContent='⚙ '+n+' ffmpeg';"
        "        fb.className='tuner-badge'+(n>=8?' full':n>=5?' tight':'');"
        "        fb.title='Laufende ffmpeg-Prozesse: Live-Streams, Recording-Remuxe, Thumbnails, Ad-Detection.';"
        "      }"
        "    }"
        "    const lb=document.getElementById('load-badge');"
        "    if(lb){"
        "      if(d.load1===null||d.load1===undefined){lb.textContent='';lb.className='tuner-badge';}"
        "      else{"
        "        const c=d.cpu_count||4;"
        "        const ratio=d.load1/c;"
        "        lb.textContent='💻 '+d.load1.toFixed(2)+'/'+c;"
        "        lb.className='tuner-badge'+(ratio>=1.5?' full':ratio>=1.0?' tight':'');"
        "        lb.title='Load-Average über 1 min geteilt durch Kerne.';"
        "      }"
        "    }"
        "    const mb=document.getElementById('mem-badge');"
        "    if(mb&&d.mem_total_mb){"
        "      const used=d.mem_total_mb-d.mem_avail_mb;"
        "      const pct=used/d.mem_total_mb;"
        "      mb.textContent='🧠 '+(used/1024).toFixed(1)+'/'+(d.mem_total_mb/1024).toFixed(1)+'G';"
        "      mb.className='tuner-badge'+(pct>=0.9?' full':pct>=0.75?' tight':'');"
        "      mb.title='RAM genutzt / gesamt (Pi 5 mit '+(d.mem_total_mb/1024).toFixed(0)+' GB).';"
        "    }"
        "    const tb2=document.getElementById('temp-badge');"
        "    if(tb2&&d.cpu_temp_c){"
        "      tb2.textContent='🌡 '+d.cpu_temp_c+'°';"
        "      tb2.className='tuner-badge'+(d.cpu_temp_c>=75?' full':d.cpu_temp_c>=65?' tight':'');"
        "      tb2.title='CPU-Temperatur. Throttling ab ~80 °C.';"
        "    }"
        "    const db=document.getElementById('disk-badge');"
        "    if(db&&d.disk_free_gb!=null){"
        "      const pct=1-d.disk_free_gb/d.disk_total_gb;"
        "      db.textContent='💾 '+d.disk_free_gb+'G frei';"
        "      db.className='tuner-badge'+(d.disk_free_gb<10?' full':d.disk_free_gb<25?' tight':'');"
        "      db.title='/mnt/tv freier Platz auf der SSD ('+d.disk_total_gb+' GB gesamt).';"
        "    }"
        "  }catch(e){"
        "    _warmFailures++;"
        "    if(_warmFailures>=1)setRecoveryBanner(true);"
        "  }"
        "}"
        "function reorderPinned(){"
        "  const ul=document.querySelector('ul.channels');if(!ul)return;"
        "  const items=Array.from(ul.children);"
        "  const pinned=items.filter(li=>li.querySelector('.pin-btn.active'));"
        "  const rest=items.filter(li=>!li.querySelector('.pin-btn.active'));"
        "  for(const li of pinned)ul.appendChild(li);"
        "  for(const li of rest)ul.appendChild(li);"
        "}"
        "function showPinToast(slug, on, name){"
        "  let t=document.getElementById('pin-toast');"
        "  if(!t){t=document.createElement('div');t.id='pin-toast';"
        "    t.style.cssText='position:fixed;top:14px;left:50%;"
        "transform:translateX(-50%);background:#222;color:#fff;"
        "padding:10px 18px;border-radius:24px;font-weight:600;"
        "box-shadow:0 4px 14px #0006;z-index:1000;font-size:.95em;"
        "transition:opacity .3s';document.body.appendChild(t);}"
        "  t.textContent=(on?'📌 ':'📍 ')+name+(on?' angepinnt':' losgelöst');"
        "  t.style.opacity='1';"
        "  clearTimeout(window._pinToastT);"
        "  window._pinToastT=setTimeout(()=>{t.style.opacity='0';},2200);"
        "}"
        "async function togglePin(btn){"
        "  const slug=btn.dataset.slug;"
        "  const on=!btn.classList.contains('active');"
        "  /* Toast immediately — disambiguates which channel the click"
        "     actually hit (the visual list is JS-reordered, easy to"
        "     mis-tap when tiles look similar). */"
        "  const li=btn.closest('li');"
        "  const name=li?(li.title||slug):slug;"
        "  showPinToast(slug, on, name);"
        "  btn.classList.toggle('active',on);"
        "  try{"
        "    const r=await fetch('/api/always-warm/'+slug,{method:'POST',"
        "      headers:{'content-type':'application/json'},"
        "      body:JSON.stringify({on:on})});"
        "    /* Reload to get a fresh server-side sort. JS-side sort"
        "       can't get the unpinned channel back to its natural"
        "       usage position on its own — the captured origOrder"
        "       still has it near the front from when it was pinned"
        "       at page-render time. Reload is ~200-500 ms and"
        "       guarantees the visual matches the server.  Gated on"
        "       r.ok so a 502 during ssd-recovery doesn't put us in"
        "       a reload loop — fetch only throws on network errors,"
        "       not on HTTP 5xx, so we'd otherwise reload forever. */"
        "    if(r.ok)setTimeout(()=>location.reload(),350);"
        "  }catch(e){}"
        "}"
        "document.addEventListener('click',e=>{"
        "  const b=e.target.closest('.pin-btn');"
        "  if(b){e.preventDefault();togglePin(b);return;}"
        "  const w=e.target.closest('.buffer-bar.running');"
        "  if(w&&!w.classList.contains('pinned')){"
        "    e.preventDefault();e.stopPropagation();"
        "    const slug=w.dataset.slug;"
        "    if(!slug)return;"
        "    fetch('/stop/'+slug).then(()=>refreshWarm()).catch(()=>{});"
        "  }"
        "});"
        # Long-press on a channel tile opens the quick-record modal.
        "let lpTimer=null,lpSlug=null,lpName=null,lpFired=false;"
        "function lpCancel(){if(lpTimer){clearTimeout(lpTimer);lpTimer=null;}}"
        "function lpStart(ev){"
        "  const a=ev.target.closest('li .logo,li .meta a');if(!a)return;"
        "  const li=a.closest('li');if(!li)return;"
        "  const pinBtn=li.querySelector('.pin-btn');"
        "  lpSlug=pinBtn?pinBtn.dataset.slug:null;"
        "  lpName=li.querySelector('.meta a')?"
        "         li.querySelector('.meta a').textContent.trim():lpSlug;"
        "  if(!lpSlug)return;"
        "  lpFired=false;"
        "  lpTimer=setTimeout(()=>{lpFired=true;lpTimer=null;showRecModal();},550);"
        "}"
        "function showRecModal(){"
        "  const m=document.createElement('div');"
        "  m.className='rec-modal';"
        "  m.innerHTML="
        "    '<div class=\"rec-dialog\">'+"
        "    '<b>Aufnahme starten:</b><br>'+lpName+"
        "    '<div style=\"margin-top:12px;display:flex;gap:8px;flex-wrap:wrap\">'+"
        "    '<button data-d=\"3600\">1 h</button>'+"
        "    '<button data-d=\"7200\">2 h</button>'+"
        "    '<button data-d=\"10800\">3 h</button>'+"
        "    '<button data-d=\"0\" class=\"cancel\">Abbrechen</button>'+"
        "    '</div></div>';"
        "  m.addEventListener('click',async ev=>{"
        "    if(ev.target===m){m.remove();return;}"
        "    const btn=ev.target.closest('button');if(!btn)return;"
        "    const d=parseInt(btn.dataset.d||'0',10);"
        "    m.remove();"
        "    if(d>0){"
        "      try{await fetch('/record/'+lpSlug+'?duration='+d);"
        "          alert('Aufnahme gestartet ('+Math.round(d/3600)+' h)');"
        "      }catch(e){alert('Fehler: '+e);}"
        "    }"
        "  });"
        "  document.body.appendChild(m);"
        "}"
        "document.addEventListener('pointerdown',lpStart);"
        "document.addEventListener('pointerup',()=>{"
        "  lpCancel();"
        "},true);"
        "document.addEventListener('pointermove',()=>lpCancel(),true);"
        "document.addEventListener('click',ev=>{"
        "  if(lpFired){ev.preventDefault();ev.stopPropagation();lpFired=false;}"
        "},true);"
        # Tile-size toggle: cycles through sm/md/lg, persisted.
        "(function(){"
        "  const sizes=['md','sm','lg'];"
        "  const icons={sm:'⊡',md:'⊟',lg:'⊞'};"
        "  let cur=localStorage.getItem('tile-size')||'md';"
        "  const apply=()=>{"
        "    document.body.classList.remove('tile-sm','tile-lg');"
        "    if(cur!=='md')document.body.classList.add('tile-'+cur);"
        "    const btn=document.getElementById('tile-size');"
        "    if(btn)btn.textContent=icons[cur]||'⊟';"
        "  };"
        "  apply();"
        "  document.getElementById('tile-size').addEventListener('click',()=>{"
        "    cur=sizes[(sizes.indexOf(cur)+1)%sizes.length];"
        "    localStorage.setItem('tile-size',cur);apply();"
        "  });"
        "})();"
        # Sort toggle: 'usage' (server default) or 'mux' (group by mux).
        # Exposes window.applyChannelSort so togglePin can re-sort after
        # an unpin (otherwise the un-pinned channel just slides one
        # position down rather than returning to its usage-sorted spot).
        "(function(){"
        "  const ul=document.querySelector('ul.channels');"
        "  const btn=document.getElementById('sort-toggle');"
        "  if(!ul||!btn)return;"
        "  const origOrder=Array.from(ul.children).map(li=>li.dataset.slug);"
        "  let mode=localStorage.getItem('sort-mode')||'usage';"
        "  const icons={usage:'📊',mux:'📡'};"
        "  const labels={usage:'Nutzung',mux:'Nach Mux'};"
        "  const apply=()=>{"
        "    btn.textContent=icons[mode];"
        "    btn.title='Sortierung: '+labels[mode]+' (Klick zum Umschalten)';"
        "    const items=Array.from(ul.children);"
        "    if(mode==='mux'){"
        "      items.sort((a,b)=>{"
        "        const ma=a.dataset.mux||'zzz';const mb=b.dataset.mux||'zzz';"
        "        if(ma!==mb)return ma<mb?-1:1;"
        "        return origOrder.indexOf(a.dataset.slug)-origOrder.indexOf(b.dataset.slug);"
        "      });"
        "    } else {"
        "      items.sort((a,b)=>"
        "        origOrder.indexOf(a.dataset.slug)-origOrder.indexOf(b.dataset.slug));"
        "    }"
        "    for(const li of items)ul.appendChild(li);"
        "    reorderPinned();"
        "  };"
        "  window.applyChannelSort=apply;"
        "  apply();"
        "  btn.addEventListener('click',()=>{"
        "    mode=mode==='usage'?'mux':'usage';"
        "    localStorage.setItem('sort-mode',mode);apply();"
        "  });"
        "})();"
        "refreshWarm();setInterval(refreshWarm,5000);"
    )
    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}{extra_css}</style></head>"
            f"<body>"
            f"<div id='recovery-banner' style='display:none;"
            f"background:#f39c12;color:#000;padding:10px 14px;border-radius:8px;"
            f"margin:0 0 12px;font-weight:600;text-align:center'>"
            f"⚠ Speicher-Recovery läuft — Live-TV in ~15 s wieder verfügbar"
            f"</div>"
            f"<h1>HLS Gateway</h1>"
            f"<div class='host-badges'>"
            f"<span id='tuner-badge' class='tuner-badge'></span>"
            f"<span id='ffmpeg-badge' class='tuner-badge'></span>"
            f"<span id='load-badge' class='tuner-badge'></span>"
            f"<span id='mem-badge' class='tuner-badge'></span>"
            f"<span id='temp-badge' class='tuner-badge'></span>"
            f"<span id='disk-badge' class='tuner-badge'></span>"
            f"</div>"
            f"<div class='quick-links'>"
            f"<a href='{HOST_URL}/epg'><span>📅</span>Programm<small>EPG-Guide + Chapter-Ticks</small></a>"
            f"<a href='{HOST_URL}/recordings'><span>📼</span>Aufnahmen<small>DVR + Mediathek-Recordings</small></a>"
            f"</div>"
            f"<h2 class='channels-head'>Kanäle"
            f" <button id='tile-size' class='tile-size-btn' "
            f"title='Kachelgröße umschalten'>⊟</button>"
            f" <button id='sort-toggle' class='tile-size-btn' "
            f"title='Sortierung umschalten'>📊</button></h2>"
            f"<ul class='channels'>{''.join(rows)}</ul>"
            f"<h2 class='tools-head'>Tools</h2>"
            f"<ul class='tools'>{tool_rows}</ul>"
            f"<script>{js}</script>"
            f"</body></html>")
    return body


@app.route("/playlist.m3u")
def playlist_m3u():
    lines = ["#EXTM3U"]
    with cmap_lock:
        for slug, info in sorted(channel_map.items(),
                                  key=lambda kv: kv[1]["name"].lower()):
            attrs = [f'tvg-id="{slug}"', f'tvg-name="{info["name"]}"']
            logo_url = _channel_logo_url(slug, info.get("icon", ""))
            if logo_url:
                attrs.append(f'tvg-logo="{logo_url}"')
            lines.append(f'#EXTINF:-1 {" ".join(attrs)},{info["name"]}')
            lines.append(f"{HOST_URL}/hls/{slug}/index.m3u8")
    return _cors(Response("\n".join(lines), mimetype="audio/x-mpegurl"))


@app.route("/hls/<slug>/index.m3u8")
def hls_playlist(slug):
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    ch_dir = HLS_DIR / slug
    playlist_path = ch_dir / "index.m3u8"
    # On cold-start, wait until enough segments exist for iOS to start
    # rendering video (not just showing a still image). iOS' native
    # HLS buffers aggressively — we need ~10s of content ready.
    MIN_SEGMENTS = 10
    deadline = time.time() + 30
    while time.time() < deadline:
        if playlist_path.exists() and playlist_path.stat().st_size > 100:
            segs = sorted(ch_dir.glob("seg_*.ts"))
            if len(segs) >= MIN_SEGMENTS:
                break
        time.sleep(0.15)
    if not playlist_path.exists():
        abort(503, "stream not ready yet")
    # Send the full playlist with an EXT-X-START hint so iOS jumps close
    # to live. DVR range intact for user seek-back.
    content = playlist_path.read_text()
    if "#EXT-X-START" not in content:
        content = content.replace(
            "#EXTM3U",
            "#EXTM3U\n#EXT-X-START:TIME-OFFSET=-6",
            1)
    resp = Response(content, mimetype="application/vnd.apple.mpegurl")
    resp.headers["Cache-Control"] = "no-cache"
    return _cors(resp)


@app.route("/hls/<slug>/dvr.m3u8")
def hls_playlist_dvr(slug):
    """Full 2h DVR playlist (untrimmed), for timeshift playback."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    ch_dir = HLS_DIR / slug
    playlist_path = ch_dir / "index.m3u8"
    deadline = time.time() + 25
    while time.time() < deadline:
        if playlist_path.exists() and playlist_path.stat().st_size > 100:
            segs = sorted(ch_dir.glob("seg_*.ts"))
            if len(segs) >= 2:
                break
        time.sleep(0.15)
    if not playlist_path.exists():
        abort(503, "stream not ready yet")
    resp = send_from_directory(ch_dir, "index.m3u8",
                                mimetype="application/vnd.apple.mpegurl")
    resp.headers["Cache-Control"] = "no-cache"
    return _cors(resp)


@app.route("/hls/<slug>/<filename>")
def hls_segment(slug, filename):
    with cmap_lock:
        if slug not in channel_map:
            abort(404)
    with active_lock:
        if slug in channels:
            channels[slug]["last_seen"] = time.time()
    mime = "video/mp2t" if filename.endswith(".ts") \
           else "application/vnd.apple.mpegurl" if filename.endswith(".m3u8") \
           else "application/octet-stream"
    return _cors(send_from_directory(HLS_DIR / slug, filename, mimetype=mime))


_epg_cache = {"data": None, "expires": 0}
_epg_lock = threading.Lock()

# EPG archive: {(channel_slug, start): {...event...}}
_epg_archive = {}
_epg_archive_lock = threading.Lock()


def load_epg_archive():
    """Load archive, drop expired entries, and compact the file on disk."""
    if not EPG_ARCHIVE_FILE.exists():
        return
    cutoff = time.time() - EPG_ARCHIVE_KEEP_DAYS * 86400
    raw_count = 0
    try:
        with open(EPG_ARCHIVE_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                raw_count += 1
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("stop", 0) < cutoff:
                    continue
                key = (e["slug"], e["start"])
                with _epg_archive_lock:
                    # keep newer version on dup
                    _epg_archive[key] = e
        kept = len(_epg_archive)
        print(f"EPG archive: loaded {kept} (dropped {raw_count - kept} "
              f"expired/duplicate)", flush=True)
        # compact file if we dropped a lot
        if raw_count > kept * 1.3 or raw_count - kept > 500:
            tmp = EPG_ARCHIVE_FILE.with_suffix(".tmp")
            with open(tmp, "w") as f:
                with _epg_archive_lock:
                    for rec in _epg_archive.values():
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            tmp.replace(EPG_ARCHIVE_FILE)
            print(f"EPG archive: compacted to {kept} entries", flush=True)
    except Exception as e:
        print(f"load EPG archive: {e}", flush=True)


def append_epg_archive(events_by_slug):
    """Write newly-seen events to archive file. Skips duplicates."""
    new_lines = []
    with _epg_archive_lock:
        for slug, events in events_by_slug.items():
            for e in events:
                key = (slug, e["start"])
                if key in _epg_archive:
                    continue
                rec = {"slug": slug, "start": e["start"], "stop": e["stop"],
                       "title": e.get("title", ""),
                       "subtitle": e.get("subtitle", ""),
                       "event_id": e.get("event_id")}
                _epg_archive[key] = rec
                new_lines.append(json.dumps(rec, ensure_ascii=False))
    if new_lines:
        try:
            with open(EPG_ARCHIVE_FILE, "a") as f:
                f.write("\n".join(new_lines) + "\n")
        except Exception as e:
            print(f"append archive: {e}", flush=True)


def epg_snapshot_loop():
    """Every N seconds, fetch live EPG and append new events to archive."""
    time.sleep(15)  # let startup settle
    while True:
        try:
            data = fetch_epg(window_before=60, window_after=12 * 3600,
                              force=True)
            append_epg_archive(data["events"])
        except Exception as e:
            print(f"snapshot loop: {e}", flush=True)
        time.sleep(EPG_SNAPSHOT_INTERVAL)

def _fetch_channel_events(ch_uuid, now_ts, horizon_ts):
    try:
        params = urllib.parse.urlencode({
            "limit": 60, "channel": ch_uuid, "sort": "start",
        })
        url = f"{TVH_BASE}/api/epg/events/grid?{params}"
        data = json.loads(urllib.request.urlopen(url, timeout=6).read())
        out = []
        for e in data.get("entries", []):
            s = e.get("start", 0); stop = e.get("stop", 0)
            # `now_ts` here is actually the window_start we were called
            # with; filter events that ended before our window opens.
            if stop <= now_ts or s >= horizon_ts:
                continue
            out.append({"start": s, "stop": stop,
                        "title": e.get("title", "?"),
                        "subtitle": e.get("subtitle", ""),
                        "event_id": e.get("eventId") or e.get("id")})
        return out
    except Exception as e:
        print(f"epg fetch {ch_uuid[:8]}: {e}", flush=True)
        return []


def fetch_epg(window_before=900, window_after=6 * 3600, force=False):
    """Fetch EPG for all favorite channels within given window.

    Cached ~3 min unless force=True. The returned events list per channel
    is merged with archived events for the past portion of the window.
    """
    now_ts = int(time.time())
    if not force:
        with _epg_lock:
            cached = _epg_cache["data"]
            # Only reuse the cache when its window fully covers what we
            # were asked for — otherwise older/past or farther-future
            # events would be missing (even though the archive has them).
            if (cached and now_ts < _epg_cache["expires"]
                and cached["window_start"] <= now_ts - window_before
                and cached["window_end"]   >= now_ts + window_after):
                return cached
    win_start = now_ts - window_before
    win_end   = now_ts + window_after
    with cmap_lock:
        items = [(s, info) for s, info in channel_map.items()]
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(
            lambda x: _fetch_channel_events(x[1]["uuid"], win_start, win_end),
            items))
    events_by_slug = dict(zip([s for s, _ in items], results))

    # Merge archived events (covers past — tvheadend deletes those)
    with _epg_archive_lock:
        for (slug, start), rec in _epg_archive.items():
            if slug not in events_by_slug:
                continue
            if rec["stop"] < win_start or start > win_end:
                continue
            # skip if we already have a live event at this start
            if any(e["start"] == start for e in events_by_slug[slug]):
                continue
            events_by_slug[slug].append({
                "start": rec["start"], "stop": rec["stop"],
                "title": rec.get("title", ""),
                "subtitle": rec.get("subtitle", ""),
                "event_id": rec.get("event_id"),
            })
    # sort each channel's events
    for slug in events_by_slug:
        events_by_slug[slug].sort(key=lambda e: e["start"])

    out = {"window_start": win_start, "window_end": win_end,
           "now": now_ts, "events": events_by_slug}
    with _epg_lock:
        _epg_cache["data"] = out
        _epg_cache["expires"] = now_ts + 180
    return out


@app.route("/epg")
def epg_grid():
    # Widen window: from 12h ago to 18h ahead — covers "today + tomorrow"
    try:
        hours_back = max(0, min(48, int(request.args.get("back", "12"))))
        hours_fwd  = max(1, min(72, int(request.args.get("fwd",  "18"))))
    except ValueError:
        hours_back, hours_fwd = 12, 18
    data = fetch_epg(window_before=hours_back * 3600,
                     window_after=hours_fwd * 3600)
    now_ts = data["now"]
    win_start = data["window_start"]
    win_end   = data["window_end"]
    px_per_min = 5
    total_min = (win_end - win_start) // 60
    total_px  = total_min * px_per_min

    # time markers every 30 min
    time_markers = []
    # align to next half-hour inside window
    ts = win_start - (win_start % 1800) + 1800
    while ts < win_end:
        offset_px = ((ts - win_start) // 60) * px_per_min
        label = time.strftime("%H:%M", time.localtime(ts))
        time_markers.append((offset_px, label))
        ts += 1800

    with cmap_lock:
        # sort same as main page: pinned first, then usage-based
        items = list(channel_map.items())
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    items.sort(key=lambda kv: (
        0 if kv[0] in ALWAYS_WARM else 1,
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower()))

    # Map (channel_uuid, start_ts) -> dvr_uuid for already-scheduled entries
    scheduled = {}
    try:
        dvr_data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid_upcoming?limit=500",
            timeout=5).read())
        for d in dvr_data.get("entries", []):
            scheduled[(d.get("channel"), d.get("start"))] = d.get("uuid")
    except Exception:
        pass

    # Build channel column (left, non-scrolling)
    channel_col = ['<div class="ch-cell header"><span class="name">'
                   'Uhrzeit →</span></div>']
    # Build timeline column (right, horizontally scrollable)
    marker_html = "".join(
        f'<div class="epg-time-marker" style="left:{px}px">{lbl}</div>'
        for px, lbl in time_markers)
    tl_rows = [f'<div class="tl-row header">{marker_html}</div>']

    for slug, info in items:
        icon = _channel_logo_url(slug, info.get("icon", ""))
        name_escaped = info["name"].replace('"', "&quot;")
        stream_url = f"{HOST_URL}/watch/{slug}"
        if icon:
            inner = f'<img src="{icon}" alt="{name_escaped}" loading="lazy">'
        else:
            inner = f'<span class="name fallback">{info["name"]}</span>'
        channel_col.append(
            f'<a class="ch-cell" href="{stream_url}" title="{name_escaped}">'
            f'{inner}</a>')

        evts = data["events"].get(slug, [])
        event_html = []
        for e in evts:
            start = max(e["start"], win_start)
            stop  = min(e["stop"],  win_end)
            left_px  = ((start - win_start) // 60) * px_per_min
            width_px = max(int(((stop - start) / 60) * px_per_min) - 2, 40)
            is_now  = e["start"] <= now_ts < e["stop"]
            is_past = e["stop"] <= now_ts
            is_tight = (e["stop"] - e["start"]) < 10 * 60
            cls = "epg-event"
            if is_now: cls += " now"
            elif is_past: cls += " past"
            if is_tight: cls += " tight"
            ts_label = time.strftime("%H:%M", time.localtime(e["start"]))
            title = (e["title"] or "—").replace("<", "&lt;")
            eid = e.get("event_id") or ""
            dvr_uuid = scheduled.get((info["uuid"], e["start"]))
            data_attrs = ""
            if dvr_uuid:
                cls += " scheduled"
                data_attrs += f' data-uuid="{dvr_uuid}"'
            # Use the tvheadend event_id when we have it. Past events
            # that are archive-only (old entries without event_id) get
            # a synthetic "arc_<slug>_<start>" key the lookup endpoint
            # understands — so yesterday's Tatort still gets Mediathek
            # even though tvheadend dropped it from its live EPG.
            if eid:
                data_attrs += f' data-eid="{eid}"'
            elif is_past:
                data_attrs += f' data-eid="arc_{slug}_{e["start"]}"'
            event_html.append(
                f'<a class="{cls}" '
                f'style="left:{left_px}px;width:{width_px}px" '
                f'href="{stream_url}" aria-label="{title} — {ts_label}"'
                f'{data_attrs}>'
                f'<span class="t">{title}</span>'
                f'<span class="ts">{ts_label}</span></a>')
        tl_rows.append(f'<div class="tl-row">{"".join(event_html)}</div>')

    # Current time marker — position inside timeline only (no offset for channel col)
    now_offset_px = int((now_ts - win_start) / 60 * px_per_min)

    # Next 20:15 (Primetime) within the window
    now_lt = time.localtime(now_ts)
    today_2015 = time.mktime((now_lt.tm_year, now_lt.tm_mon, now_lt.tm_mday,
                              20, 15, 0, 0, 0, -1))
    prime_ts = today_2015 if today_2015 >= now_ts else today_2015 + 86400
    if prime_ts > win_end:
        prime_ts = today_2015
    prime_px = int((prime_ts - win_start) / 60 * px_per_min)
    now_line = (f'<div class="epg-now-line" '
                f'style="left:{now_offset_px}px"></div>')

    grid_html = (f'<div class="epg-wrap" id="tlscroll">'
                 f'<div class="epg-channels">{"".join(channel_col)}</div>'
                 f'<div class="epg-tl-scroll">'
                 f'<div class="epg-tl-inner" style="width:{total_px}px">'
                 f'{"".join(tl_rows)}{now_line}</div>'
                 f'</div></div>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f"html,body{{height:100%;overflow:hidden}}"
            f"body{{max-width:none;margin:0;padding:0;display:flex;flex-direction:column}}"
            f".epg-wrap{{flex:1 1 auto;min-height:0;max-height:none;margin-bottom:0}}"
            f"@media (pointer:coarse){{"
            f"html,body{{height:auto;overflow:visible}}"
            f"body{{display:block}}"
            f".epg-wrap{{flex:none;max-height:none}}"
            f"}}"
            f".epg-header{{display:flex;align-items:center;flex-wrap:wrap;"
            f"gap:8px 14px;padding:6px 10px;border-bottom:1px solid var(--border);"
            f"background:var(--bg);font-size:.9em}}"
            f".epg-header h1{{margin:0;font-size:1.1em;font-weight:600}}"
            # Bigger tap target on touch — wrap arrow + title in one
            # <a class=home-link>, give it padding + hover-tint.
            f".home-link{{display:inline-flex;align-items:center;gap:8px;"
            f"padding:8px 12px;margin:-6px -8px;border-radius:6px;"
            f"text-decoration:none;color:inherit;-webkit-tap-highlight-color:transparent}}"
            f".home-link .arrow{{font-size:1.4em;line-height:1}}"
            f"@media (hover:hover){{.home-link:hover{{background:var(--stripe)}}}}"
            f".epg-header .meta{{color:var(--muted);font-size:.85em}}"
            f".epg-header .spacer{{flex:1 1 auto}}"
            f".auto-refresh{{display:inline-flex;align-items:center;gap:.35em;"
            f"font-size:.85em;color:var(--muted);cursor:pointer;user-select:none}}"
            f".auto-refresh input{{accent-color:#1565c0;cursor:pointer}}"
            f".epg-search{{padding:4px 10px;border-radius:6px;"
            f"border:1px solid var(--border);background:var(--stripe);"
            f"color:var(--fg);font-size:14px;min-width:160px}}"
            f".epg-event.filter-hidden{{opacity:.15;pointer-events:none}}"
            f".epg-event.filter-match{{outline:2px solid #f1c40f;"
            f"outline-offset:-2px;z-index:5}}"
            f"</style></head><body>"
            f"<div class='epg-header'>"
            f"<a class='home-link' href='{HOST_URL}/'>"
            f"<span class='arrow'>←</span><h1>Programm</h1></a>"
            f"<span class='meta'>Stand {time.strftime('%H:%M', time.localtime(now_ts))}</span>"
            f"<a class='btn-now' href='#' onclick='jumpNow();return false'>"
            f"▶︎ Jetzt</a>"
            f"<a class='btn-now' href='#' onclick='jumpPrime();return false'>"
            f"🕗 20:15</a>"
            f"<input type='search' id='epg-search' placeholder='🔍 Titel filtern…' "
            f"class='epg-search'>"
            f"<label class='auto-refresh' title='Seite alle 60 s neu laden'>"
            f"<input type='checkbox' id='auto-refresh'>"
            f"<span>🔄 60s</span></label>"
            f"<span class='spacer'></span>"
            f"<span class='meta'>"
            f"<a href='?back=6&fwd=12'>6h</a> · "
            f"<a href='?back=12&fwd=18'>12h</a> · "
            f"<a href='?back=24&fwd=24'>24h</a></span>"
            f"</div>"
            f"{grid_html}"
            f"<script>"
            f"const NOW_PX={now_offset_px};"
            f"const PRIME_PX={prime_px};"
            f"function jumpNow(){{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollTo({{left:Math.max(0,NOW_PX-100),behavior:'smooth'}});"
            f"}}"
            f"function jumpPrime(){{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollTo({{left:Math.max(0,PRIME_PX-100),behavior:'smooth'}});"
            f"}}"
            # Title filter: dim non-matching events, highlight matches.
            f"function filterEpg(q){{"
            f"  q=(q||'').trim().toLowerCase();"
            f"  const evs=document.querySelectorAll('.epg-event');"
            f"  for(const el of evs){{"
            f"    el.classList.remove('filter-hidden','filter-match');"
            f"    if(!q)continue;"
            f"    const t=(el.textContent||'').toLowerCase();"
            f"    if(t.includes(q)){{el.classList.add('filter-match');}}"
            f"    else{{el.classList.add('filter-hidden');}}"
            f"  }}"
            f"}}"
            f"const searchBox=document.getElementById('epg-search');"
            f"if(searchBox){{"
            f"  searchBox.addEventListener('input',ev=>filterEpg(ev.target.value));"
            f"  const url=new URL(location.href);"
            f"  const initial=url.searchParams.get('q');"
            f"  if(initial){{searchBox.value=initial;filterEpg(initial);}}"
            f"}}"
            f"const LP_MS=500, LP_MOVE=10;"
            f"let lpTimer=null,lpX=0,lpY=0,lpEl=null,lpFired=false;"
            f"function lpCancel(){{"
            f"  if(lpTimer){{clearTimeout(lpTimer);lpTimer=null;}}"
            f"  if(lpEl){{lpEl.classList.remove('lp-active');lpEl=null;}}"
            f"}}"
            f"function lpStart(ev){{"
            f"  const el=ev.target.closest('.epg-event');"
            f"  if(!el)return;"
            f"  const hasEid=el.dataset.eid||el.dataset.uuid;"
            f"  if(!hasEid)return;"
            f"  const p=ev.touches?ev.touches[0]:ev;"
            f"  lpX=p.clientX;lpY=p.clientY;lpEl=el;lpFired=false;"
            f"  el.classList.add('lp-active');"
            f"  clearTimeout(lpTimer);"
            f"  lpTimer=setTimeout(()=>{{"
            f"    lpFired=true;"
            f"    try{{navigator.vibrate&&navigator.vibrate(25);}}catch(e){{}}"
            f"    const tgt=lpEl;lpCancel();handleLP(tgt);"
            f"  }},LP_MS);"
            f"}}"
            f"function lpMove(ev){{"
            f"  if(!lpTimer)return;"
            f"  const p=ev.touches?ev.touches[0]:ev;"
            f"  if(Math.abs(p.clientX-lpX)>LP_MOVE||Math.abs(p.clientY-lpY)>LP_MOVE)"
            f"    lpCancel();"
            f"}}"
            # Minimal 3-button modal for long-press. confirm() is binary
            # and can't offer "episode vs series", so we roll our own.
            # pointer-events:none on the host anchor during the dialog
            # absorbs iOS' queued synthesized click, which otherwise
            # navigates the page behind the modal.
            f"function lpDialog(opts){{"
            f"  return new Promise(resolve=>{{"
            f"    const bg=document.createElement('div');"
            f"    bg.style.cssText='position:fixed;inset:0;background:#000a;"
            f"z-index:1000;display:flex;align-items:center;justify-content:center;"
            f"padding:20px';"
            f"    const box=document.createElement('div');"
            f"    box.style.cssText='background:var(--bg,#fff);color:var(--fg,#000);"
            f"padding:18px;border-radius:10px;max-width:320px;width:100%;"
            f"font-size:.95em;box-shadow:0 8px 32px #0008';"
            f"    box.innerHTML='<div style=\"font-weight:600;margin-bottom:14px;"
            f"line-height:1.35\">'+opts.msg+'</div>';"
            f"    const row=document.createElement('div');"
            f"    row.style.cssText='display:flex;flex-direction:column;gap:8px';"
            f"    for(const b of opts.buttons){{"
            f"      const btn=document.createElement('button');"
            f"      btn.textContent=b.label;"
            f"      btn.style.cssText='padding:10px;border-radius:8px;border:0;"
            f"font-size:1em;cursor:pointer;'+(b.primary?'background:#e74c3c;color:#fff;"
            f"font-weight:600':'background:#ccc;color:#000');"
            f"      btn.onclick=()=>{{bg.remove();resolve(b.value);}};"
            f"      row.appendChild(btn);"
            f"    }}"
            f"    box.appendChild(row);bg.appendChild(box);"
            f"    document.body.appendChild(bg);"
            f"  }});"
            f"}}"
            f"function handleLP(el){{"
            f"  const ttl=(el.querySelector('.t')||{{}}).textContent||'diese Sendung';"
            f"  el.style.pointerEvents='none';"
            f"  const release=()=>setTimeout(()=>{{"
            f"    el.style.pointerEvents='';"
            f"  }},400);"
            f"  if(el.dataset.uuid){{"
            f"    lpDialog({{msg:'Geplante Aufnahme entfernen?<br><br>'+ttl,"
            f"      buttons:[{{label:'Entfernen',value:'yes',primary:true}},"
            f"               {{label:'Abbrechen',value:''}}]}}).then(v=>{{"
            f"      release();"
            f"      if(v==='yes')fetch('{HOST_URL}/cancel-recording/'+el.dataset.uuid)"
            f"        .then(r=>r.json()).then(d=>{{"
            f"          if(d.ok){{el.classList.remove('scheduled');"
            f"            delete el.dataset.uuid;}}"
            f"        }}).catch(()=>{{}});"
            f"    }});"
            f"  }} else if(el.dataset.eid){{"
            f"    const isPast=el.classList.contains('past');"
            # Query the Mediathek lookup in parallel. Past events can
            # only be retrieved via Mediathek, so we build a different
            # button list for them (no DVR options).
            f"    const mtFetch=fetch('{HOST_URL}/api/mediathek-lookup/'"
            f"+el.dataset.eid).then(r=>r.json()).catch(()=>({{match:null}}));"
            f"    const dialogBtns=isPast?[]:[{{label:'Einzelne Episode',"
            f"value:'ep',primary:true}},{{label:'Ganze Serie',value:'series'}}];"
            f"    dialogBtns.push({{label:'Abbrechen',value:''}});"
            f"    mtFetch.then(m=>{{"
            f"      if(m&&m.match){{"
            f"        const avail=new Date(m.match.available_to*1000);"
            f"        const dStr=String(avail.getDate()).padStart(2,'0')+'.'"
            f"+String(avail.getMonth()+1).padStart(2,'0');"
            f"        const srcLabel=m.match.source==='zdf'?'ZDF':'ARD';"
            f"        const mtBtn={{label:'Aus '+srcLabel+' Mediathek (bis '+dStr+')',"
            f"value:'mediathek'}};"
            # Past events: "Jetzt abspielen" (primary, direct playback,
            # nothing persisted) plus "Aus Mediathek speichern" (save
            # a virtual recording for later). Future events: Mediathek
            # option slots between Episode and Serie as before.
            f"        if(isPast){{"
            f"          dialogBtns.splice(dialogBtns.length-1,0,"
            f"            {{label:'Jetzt abspielen',value:'play',primary:true}},"
            f"            mtBtn);"
            f"        }} else {{dialogBtns.splice(1,0,mtBtn);}}"
            f"      }} else if(isPast){{"
            # Past event with no Mediathek match: there's nothing we
            # can do, show a brief note and bail.
            f"        release();"
            f"        lpDialog({{msg:'Sendung ist bereits ausgestrahlt "
            f"und in der Mediathek nicht (mehr) verfügbar.',"
            f"          buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"        return;"
            f"      }}"
            f"      lpDialog({{msg:'Aufnahme planen?<br><br>'+ttl,"
            f"        buttons:dialogBtns}}).then(v=>{{"
            f"        release();"
            f"        if(v==='ep')fetch('{HOST_URL}/record-event/'+el.dataset.eid)"
            f"          .then(r=>r.json()).then(d=>{{"
            f"            if(d.ok&&d.uuid){{el.classList.add('scheduled');"
            f"              el.dataset.uuid=d.uuid;}}"
            f"          }}).catch(()=>{{}});"
            f"        else if(v==='series')fetch('{HOST_URL}/record-series/'+el.dataset.eid)"
            f"          .then(r=>r.json()).then(d=>{{"
            f"            if(d.ok){{"
            f"              const n=d.scheduled||0;"
            f"              const ep=n===1?'Folge':'Folgen';"
            f"              lpDialog({{msg:'<b>'+d.title+'</b> auf '+d.channel+"
            f"'<br><br>'+n+' '+ep+' aktuell geplant',"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"            }}"
            f"          }}).catch(()=>{{}});"
            f"        else if(v==='play')"
            f"          location.href='{HOST_URL}/mediathek-play/'+el.dataset.eid;"
            f"        else if(v==='mediathek')"
            f"          fetch('{HOST_URL}/api/mediathek-schedule/'+el.dataset.eid,"
            f"            {{method:'POST'}})"
            f"            .then(r=>r.json()).then(d=>{{"
            f"              if(d.ok)lpDialog({{msg:'<b>'+d.title+"
            f"'</b><br><br>Aus Mediathek gespeichert. Unter Aufnahmen "
            f"abrufbar.',"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"              else lpDialog({{msg:'Fehler: '+(d.error||'?'),"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"            }}).catch(()=>{{}});"
            f"      }});"
            f"    }});"
            f"  }} else {{"
            f"    release();"
            f"  }}"
            f"}}"
            f"document.addEventListener('touchstart',lpStart,{{passive:true}});"
            f"document.addEventListener('touchmove',lpMove,{{passive:true}});"
            f"document.addEventListener('touchend',lpCancel);"
            f"document.addEventListener('touchcancel',lpCancel);"
            f"document.addEventListener('mousedown',ev=>{{"
            f"  if(ev.button===0)lpStart(ev);"
            f"}});"
            f"document.addEventListener('mousemove',lpMove);"
            f"document.addEventListener('mouseup',lpCancel);"
            f"document.addEventListener('mouseleave',lpCancel);"
            # Suppress the anchor click that follows a long-press.
            # Also: for past events a plain tap means "I want to watch
            # this show" — which on DVB means "nope, it's over". Route
            # those taps through the long-press handler so the Mediathek
            # option shows up instead of navigating to the live stream
            # of a now-unrelated current programme.
            f"document.addEventListener('click',ev=>{{"
            f"  const evEl=ev.target.closest('.epg-event');"
            f"  if(!evEl)return;"
            f"  if(lpFired){{"
            f"    ev.preventDefault();ev.stopPropagation();lpFired=false;"
            f"    return;"
            f"  }}"
            f"  if(evEl.classList.contains('past')&&evEl.dataset.eid){{"
            f"    ev.preventDefault();ev.stopPropagation();"
            f"    handleLP(evEl);"
            f"  }}"
            f"}},true);"
            f"document.addEventListener('contextmenu',ev=>{{"
            f"  if(ev.target.closest('.epg-event'))ev.preventDefault();"
            f"}});"
            f"window.addEventListener('load',()=>{{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollLeft=Math.max(0,NOW_PX-100);"
            f"  const cb=document.getElementById('auto-refresh');"
            f"  if(cb){{"
            f"    cb.checked=localStorage.getItem('epgAutoRefresh')==='1';"
            f"    let timer=null;"
            f"    function arm(){{"
            f"      if(timer){{clearTimeout(timer);timer=null;}}"
            f"      if(cb.checked)timer=setTimeout(()=>location.reload(),60000);"
            f"    }}"
            f"    cb.addEventListener('change',()=>{{"
            f"      localStorage.setItem('epgAutoRefresh',cb.checked?'1':'0');"
            f"      arm();"
            f"    }});"
            f"    arm();"
            f"  }}"
            f"}});"
            f"</script>"
            f"</body></html>")
    return body


@app.route("/stats")
def usage_stats():
    """Ranking of channels by usage. Also includes ongoing sessions."""
    now = time.time()
    with active_lock:
        live = {s: now - i["started_at"] for s, i in channels.items()}
    with stats_lock:
        snap = {s: dict(v) for s, v in stats.items()}
    for s, secs in live.items():
        snap.setdefault(s, {"starts": 0, "watch_seconds": 0,
                             "last_watched": None})
        snap[s]["watch_seconds"] += secs  # include current live session
    rows = []
    for slug, v in snap.items():
        with cmap_lock:
            name = channel_map.get(slug, {}).get("name", slug)
        rows.append({"slug": slug, "name": name,
                     "starts": v.get("starts", 0),
                     "watch_hours": round(v.get("watch_seconds", 0)/3600, 2),
                     "last_watched": v.get("last_watched")})
    rows.sort(key=lambda r: (r["watch_hours"], r["starts"]), reverse=True)

    if request.args.get("format") != "json":
        html = [f"<html><head><meta name='viewport' "
                f"content='width=device-width,initial-scale=1'>"
                f"<meta name='color-scheme' content='light dark'>"
                f"<style>{BASE_CSS}</style></head><body>"
                f"<h1>Nutzungsstatistik</h1>"
                f"<p><a href='{HOST_URL}/'>← Zurück</a> · "
                f"<a href='{HOST_URL}/stats?format=json'>JSON</a></p>"
                f"<table><tr><th>#</th><th>Sender</th>"
                f"<th>Aufrufe</th><th>Std gesehen</th>"
                f"<th>Zuletzt</th></tr>"]
        for i, r in enumerate(rows, 1):
            html.append(f"<tr><td class='rank'>{i}</td>"
                        f"<td>{r['name']}</td>"
                        f"<td>{r['starts']}</td>"
                        f"<td>{r['watch_hours']}</td>"
                        f"<td>{r['last_watched'] or '—'}</td></tr>")
        html.append("</table></body></html>")
        return "\n".join(html)
    return {"ranking": rows}


@app.route("/root.crt")
def root_cert():
    """Serve Caddy's local CA root for device trust install."""
    path = HLS_DIR / "caddy-root.crt"
    if not path.exists():
        abort(404, "CA cert not found — copy it to /mnt/tv/caddy-root.crt first")
    resp = send_from_directory(HLS_DIR, "caddy-root.crt",
                                mimetype="application/x-x509-ca-cert",
                                as_attachment=False)
    resp.headers["Content-Disposition"] = 'inline; filename="caddy-root.crt"'
    return resp


# ---------------------------------------------------------------------
# Shared player scaffold. Both the live `/watch/<slug>` player and the
# recording `/recording/<uuid>` player inject these. Each mode still
# defines its own seek(d), refresh(), scrub-bar HTML, plus mode-only
# features (chapters, Mediathek fallback, progress loader, etc.).
#
# Requirements the caller must satisfy BEFORE the base JS runs:
#   - An HTML #v, #chrome, #topbar, #hint in the DOM
#   - const PLAYER_HOME = '...';        // fallback URL on close
#   - function seek(d) {...}            // consumed by double-tap
# ---------------------------------------------------------------------
PLAYER_BASE_CSS = """\
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:#000;color:#eee;
 font-family:-apple-system,BlinkMacSystemFont,sans-serif;
 overflow:hidden;-webkit-user-select:none;user-select:none}
#v{width:100%;height:100%;background:#000;object-fit:contain;display:block}
#chrome{position:fixed;bottom:0;left:0;right:0;
 padding:14px max(16px,env(safe-area-inset-right))
         max(20px,env(safe-area-inset-bottom))
         max(16px,env(safe-area-inset-left));
 background:linear-gradient(transparent,#000d);z-index:10;
 transition:opacity .3s}
#topbar{position:fixed;top:0;left:0;right:0;z-index:10;
 padding:max(12px,env(safe-area-inset-top)) max(16px,env(safe-area-inset-right))
         14px max(16px,env(safe-area-inset-left));
 display:flex;justify-content:space-between;align-items:center;
 background:linear-gradient(#000d,transparent);transition:opacity .3s}
.row{display:flex;align-items:center;gap:6px;margin-top:8px;
 flex-wrap:wrap;row-gap:8px}
/* Minimal-controls mode: hide every button in the row except
   #skipad, keep the #cur timer + scrubbar + title visible. Toggle
   via #ctrlMin in the topbar; state persisted in
   localStorage('player-ctrl-min'). Live-edge is reachable by
   dragging the scrub thumb to the rightmost 2 % (seekTo snaps to
   goLive() in that range). #skipad stays because skipping a 4-min
   ad block by hand on the scrubbar is annoying — but in min mode
   we tone it down so it's not a screaming red CTA. */
body.ctrl-min .row > button,
body.ctrl-min .row > #volume-wrap{display:none}
body.ctrl-min #skipad{background:#0006;color:#ddd;padding:4px 9px;
 font-size:.72em;font-weight:500;border:1px solid #fff3}
.spacer{flex:1 1 auto}
.iconbtn{background:#fff2;color:#fff;border:0;width:34px;height:34px;
 border-radius:17px;font-size:1em;cursor:pointer;display:flex;
 align-items:center;justify-content:center;text-decoration:none;
 flex:0 0 auto;line-height:1;font-variant-emoji:text;
 transition:box-shadow .2s,background .2s}
@media (hover:hover){
 .iconbtn:hover{background:#fff4;
  box-shadow:0 0 10px #7bdcff99,0 0 18px #7bdcff55}
}
.iconbtn:active{opacity:.6}
.iconbtn:disabled{background:#555;color:#bbb;cursor:default}
#volume-wrap{display:inline-flex;align-items:center;gap:0;
 flex:0 0 auto;position:relative}
#volume-wrap #vol-slider{width:0;height:4px;cursor:pointer;
 background:#fff4;border-radius:2px;appearance:none;
 -webkit-appearance:none;outline:none;transition:width .2s,
 margin-left .2s,opacity .2s;opacity:0;margin-left:0;
 accent-color:#fff}
@media (hover:hover){
 #volume-wrap:hover #vol-slider{width:80px;margin-left:6px;opacity:1}
}
#volume-wrap.open #vol-slider{width:80px;margin-left:6px;opacity:1}
#vol-slider::-webkit-slider-thumb{appearance:none;-webkit-appearance:none;
 width:12px;height:12px;border-radius:50%;background:#fff;cursor:pointer}
#vol-slider::-moz-range-thumb{width:12px;height:12px;border-radius:50%;
 background:#fff;border:0;cursor:pointer}
.pill{background:#fff2;color:#fff;border:0;padding:7px 12px;
 border-radius:16px;font-weight:600;font-size:.85em;cursor:pointer;
 display:inline-flex;align-items:center;gap:5px;flex:0 0 auto;line-height:1}
.pill:active{opacity:.7}
.pill:disabled{background:#555;color:#bbb;cursor:default}
#skipad{display:none}
#skipad.on{display:inline-flex}
.time{font-variant-numeric:tabular-nums;font-size:.85em;color:#ddd;
 min-width:44px;text-align:center;flex:0 0 auto}
#scrub{position:relative;width:100%;height:22px;
 cursor:pointer;display:flex;align-items:center}
#track{position:absolute;left:0;right:0;top:50%;transform:translateY(-50%);
 height:3px;background:#fff1;border-radius:2px}
#played{position:absolute;top:0;bottom:0;background:#f44;
 border-radius:2px;width:0;left:0}
#thumb{position:absolute;top:50%;transform:translate(-50%,-50%);
 width:13px;height:13px;background:#fff;border-radius:50%;
 pointer-events:none;box-shadow:0 0 2px #0008;left:0;z-index:2}
.ad-block{position:absolute;top:50%;transform:translateY(-50%);
 height:7px;border-radius:2px;pointer-events:none;
 background:repeating-linear-gradient(45deg,
 #ff8a65,#ff8a65 3px,#4d1c0f 3px,#4d1c0f 6px);
 box-shadow:0 0 0 1px #000a;z-index:1}
.hidden{opacity:0;pointer-events:none}
#ttlrow{margin-top:8px;font-size:.85em;padding-left:4px;
 white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#hint{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);
 font-size:1.6em;background:#000c;padding:14px 22px;border-radius:12px;
 opacity:0;transition:opacity .2s;pointer-events:none;z-index:20}
#hint.show{opacity:1}
"""

PLAYER_BASE_JS = """\
const v=document.getElementById('v');
const chromeBar=document.getElementById('chrome');
const topbar=document.getElementById('topbar');
const hint=document.getElementById('hint');
const pp=document.getElementById('pp');
const scrub=document.getElementById('scrub');
const played=document.getElementById('played');
const thumb=document.getElementById('thumb');
v.addEventListener('play',()=>pp.textContent='\u23F8');
v.addEventListener('pause',()=>pp.textContent='\u25B6');
let _chromeT=null;
function show(){
  chromeBar.classList.remove('hidden');
  topbar.classList.remove('hidden');
  clearTimeout(_chromeT);
  _chromeT=setTimeout(()=>{
    if(!v.paused){chromeBar.classList.add('hidden');topbar.classList.add('hidden');}
  },3500);
}
show();
function toggleFs(){
  const fsEl=document.fullscreenElement||document.webkitFullscreenElement;
  if(fsEl){
    (document.exitFullscreen||document.webkitExitFullscreen).call(document);
    return;
  }
  const root=document.documentElement;
  if(root.requestFullscreen){root.requestFullscreen();return;}
  if(root.webkitRequestFullscreen){root.webkitRequestFullscreen();return;}
  if(v.webkitEnterFullscreen){v.webkitEnterFullscreen();}
}
const CTRL_MIN_KEY='player-ctrl-min';
function applyCtrlMin(min){
  document.body.classList.toggle('ctrl-min',!!min);
  const btn=document.getElementById('ctrlMin');
  if(btn){
    btn.textContent=min?'⊞':'⊟';
    btn.setAttribute('aria-label',
      min?'Steuerleiste einblenden':'Steuerleiste verbergen');
  }
}
function toggleCtrlMin(){
  const min=!document.body.classList.contains('ctrl-min');
  try{localStorage.setItem(CTRL_MIN_KEY,min?'1':'0');}catch(e){}
  applyCtrlMin(min);
}
try{applyCtrlMin(localStorage.getItem(CTRL_MIN_KEY)==='1');}catch(e){}
function closePlayer(ev){
  if(ev)ev.preventDefault();
  try{
    const ref=document.referrer?new URL(document.referrer):null;
    if(ref&&ref.origin===location.origin&&history.length>1){
      history.back();return false;
    }
  }catch(e){}
  location.href=PLAYER_HOME;
  return false;
}
// Scrub-bar drag (mouse + touch). Visual-only during drag — actual
// seek happens once on release. Setting v.currentTime per mousemove
// (~30+/s) drowns hls.js in segment-fetch requests and makes drag
// feel laggy/jumpy. With visual-only drag the thumb tracks the
// pointer at full DOM speed and a single seekTo() commits at the end.
// Each per-mode refresh() guards thumb.style.left + played.style.*
// behind `if(!_dragging)` so the playback ticker doesn't fight the
// drag position mid-gesture.
let _dragging=false;
function _dragVisual(ev){
  const r=scrub.getBoundingClientRect();
  const cx=(ev.touches?ev.touches[0]:ev).clientX;
  const x=Math.max(0,Math.min(r.width,cx-r.left));
  const pct=(x/r.width)*100;
  thumb.style.left=pct+'%';
  const playL=parseFloat(played.style.left)||0;
  played.style.width=Math.max(0,pct-playL)+'%';
  const[ws,we]=scrubWindow();
  const w=ws+(pct/100)*(we-ws);
  if(isFinite(w)&&w>0)cur.textContent=new Date(w*1000).toLocaleTimeString(
    'de-DE',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
scrub.addEventListener('mousedown',e=>{_dragging=true;_dragVisual(e);show();});
window.addEventListener('mousemove',e=>{if(_dragging){_dragVisual(e);show();}});
window.addEventListener('mouseup',e=>{if(_dragging){_dragging=false;seekTo(e);}});
scrub.addEventListener('touchstart',e=>{_dragging=true;_dragVisual(e);show();},{passive:true});
scrub.addEventListener('touchmove',e=>{if(_dragging){_dragVisual(e);show();}},{passive:true});
scrub.addEventListener('touchend',e=>{
  if(!_dragging)return;
  _dragging=false;
  /* touchend's .touches is empty — synthesize from changedTouches so
     seekTo() can pull a clientX out of it. */
  const t=e.changedTouches&&e.changedTouches[0];
  if(t)seekTo({touches:[t],clientX:t.clientX});
},{passive:true});
let _lastTapT=0,_lastTapX=0;
v.addEventListener('touchend',e=>{
  if(!e.changedTouches[0])return;
  const now=Date.now();
  const x=e.changedTouches[0].clientX;
  if(now-_lastTapT<300&&Math.abs(x-_lastTapX)<60){
    const delta=x<window.innerWidth/2?-10:10;
    seek(delta);
    hint.textContent=(delta<0?'\u23EA ':'\u23E9 ')+Math.abs(delta)+' s';
    hint.classList.add('show');
    setTimeout(()=>hint.classList.remove('show'),600);
    _lastTapT=0;
  }else{_lastTapT=now;_lastTapX=x;}
},{passive:true});
/* --- Volume + mute control (persisted to localStorage) --------- */
const volWrap=document.getElementById('volume-wrap');
if(volWrap){
  const volIcon=document.getElementById('vol-icon');
  const volSlider=document.getElementById('vol-slider');
  const VOL_KEY='playerVolume';
  let storedVol=1,storedMuted=false;
  try{
    const raw=JSON.parse(localStorage.getItem(VOL_KEY)||'null');
    if(raw){
      if(typeof raw.volume==='number')storedVol=Math.max(0,Math.min(1,raw.volume));
      storedMuted=!!raw.muted;
    }
  }catch(e){}
  function updateVolIcon(){
    if(v.muted||v.volume===0)volIcon.textContent='\U0001F507';
    else if(v.volume<0.5)volIcon.textContent='\U0001F509';
    else volIcon.textContent='\U0001F50A';
    volSlider.value=Math.round((v.muted?0:v.volume)*100);
  }
  function saveVol(){
    try{localStorage.setItem(VOL_KEY,
      JSON.stringify({volume:v.volume,muted:v.muted}));}catch(e){}
  }
  v.volume=storedVol;
  v.muted=storedMuted;
  volIcon.addEventListener('click',e=>{
    e.stopPropagation();
    v.muted=!v.muted;
    if(!v.muted&&v.volume===0)v.volume=0.5;
    updateVolIcon();saveVol();show();
  });
  volSlider.addEventListener('input',e=>{
    const pct=parseInt(volSlider.value,10)/100;
    v.volume=pct;
    v.muted=(pct===0);
    updateVolIcon();saveVol();show();
  });
  v.addEventListener('volumechange',updateVolIcon);
  updateVolIcon();
}
"""


PLAYER_HEAD_META = (
    "<meta name='viewport' content='width=device-width,"
    "initial-scale=1,user-scalable=no,viewport-fit=cover'>"
    "<meta name='color-scheme' content='dark'>"
)


@app.route("/watch/<slug>")
def watch_player(slug):
    """Fullscreen HTML player with swipe-to-switch-channel gestures."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        abort(404, "unknown channel")
    return f"""<!doctype html>
<html><head>{PLAYER_HEAD_META}<title>{info['name']}</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1/dist/hls.min.js"></script>
<style>
{PLAYER_BASE_CSS}
body{{touch-action:pan-y}}
#avail{{position:absolute;top:0;bottom:0;background:#fff4;
  border-radius:2px;width:0;left:0}}
.chapter{{position:absolute;top:50%;transform:translate(-50%,-50%);
  width:32px;height:40px;background:transparent;border:0;padding:0;
  z-index:1;cursor:pointer}}
.chapter::after{{content:'';position:absolute;left:50%;top:50%;
  transform:translate(-50%,-50%);width:6px;height:16px;background:#fff;
  border-radius:2px;opacity:.85;box-shadow:0 0 2px #000a;
  pointer-events:none}}
.chapter.current::after{{background:#ffd84d;height:18px}}
.chapter:active::after{{opacity:.5}}
#skipad{{background:#e74c3c;color:#fff;padding:7px 12px;border:0;
  border-radius:16px;font-weight:600;font-size:.85em;cursor:pointer;
  display:none;flex:0 0 auto}}
#skipad.on{{display:inline-flex}}
.chname{{font-weight:600;color:#fff;margin-right:8px}}
.epg{{color:#bbb;margin-left:8px}}
#srcbadge{{display:inline-block;font-size:.75em;font-weight:600;
  padding:2px 8px;border-radius:10px;background:#7a7a7a;color:#fff;
  vertical-align:1px}}
#srcbadge.mediathek{{background:#2980b9}}
#srcbadge.rec{{background:#c0392b}}
#srcbadge.at-live{{background:#27ae60}}
#unmute{{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);
  z-index:30;background:#fff;color:#000;padding:14px 22px;
  border-radius:30px;font-weight:600;font-size:1em;border:0;cursor:pointer;
  display:none}}
</style></head><body>
<video id='v' autoplay muted playsinline webkit-playsinline
 disablepictureinpicture></video>
<button id='unmute' onclick='doUnmute()'>🔊 Ton an</button>
<div id='topbar'>
 <button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>
 <button id='ctrlMin' class='iconbtn' onclick='toggleCtrlMin()'
   aria-label='Steuerleiste verbergen'>⊟</button>
 <a class='iconbtn' href='{HOST_URL}/' aria-label='Schließen' onclick='return closePlayer(event)'>✕</a>
</div>
<div id='chrome'>
 <div id='scrub'>
  <div id='track'>
   <div id='avail'></div>
   <div id='played'></div>
  </div>
  <div id='thumb'></div>
 </div>
 <div class='row'>
  <button id='restartBtn' class='iconbtn' onclick='goShowStart()'
   aria-label='Sendung von Anfang'>⏮</button>
  <button class='iconbtn' onclick='seek(-10)' aria-label='-10 s'>⏪</button>
  <button id='pp' class='iconbtn' onclick='togglePlay()'
   aria-label='Play/Pause'>▶</button>
  <button class='iconbtn' onclick='seek(10)' aria-label='+10 s'>⏩</button>
  <button id='liveBtn' class='iconbtn' onclick='goShowNext()'
   aria-label='Zur nächsten Sendung'>⏭</button>
  <button id='skipad' onclick='skipCurrentAd()'>Werbung ⏭</button>
  <button class='iconbtn' onclick='prevCh()' aria-label='voriger Kanal'>◀︎</button>
  <button class='iconbtn' onclick='nextCh()' aria-label='nächster Kanal'>▶︎</button>
  <span id='volume-wrap'>
   <button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>
   <input type='range' id='vol-slider' min='0' max='100' value='100'>
  </span>
  <span id='cur' class='time'>0:00</span>
 </div>
 <div id='ttlrow'>
  <span class='chname' id='chname'>{info['name']}</span>
  <span id='srcbadge'>Live</span>
  <span class='epg' id='epg'>…</span>
 </div>
</div>
<div id='hint'></div>

<script>
const HOST='{HOST_URL}';
const PLAYER_HOME=HOST+'/';
let channels=[];let idx=0;let current='{slug}';

// Shared scaffold: element refs, show(), toggleFs, closePlayer,
// double-tap seek handler. Mode-specific `function seek(d)` is defined
// further down and reached at call-time via function hoisting.
{PLAYER_BASE_JS}
const chname=document.getElementById('chname');
const epgEl=document.getElementById('epg');
const unmuteBtn=document.getElementById('unmute');
const liveBtn=document.getElementById('liveBtn');
const srcBadge=document.getElementById('srcbadge');
function setSource(which){{
  srcBadge.classList.remove('mediathek','at-live','rec');
  if(which==='mediathek'){{
    const ch=channels[idx];
    const slug=(ch&&ch.slug)||current||'';
    let label='ARD Mediathek';
    if(slug.startsWith('zdf')||slug==='3sat-hd')label='ZDF Mediathek';
    else if(slug==='arte-hd')label='arte.tv';
    else if(slug==='kika-hd')label='KiKA';
    srcBadge.textContent=label;
    srcBadge.classList.add('mediathek');
  }} else if(which==='rec'){{
    srcBadge.textContent='● Aufnahme';
    srcBadge.classList.add('rec');
  }} else {{
    srcBadge.textContent='Live';
  }}
}}
function updateLiveBadge(){{
  if(srcBadge.classList.contains('mediathek'))return;
  if(isAtLive())srcBadge.classList.add('at-live');
  else srcBadge.classList.remove('at-live');
}}
const cur=document.getElementById('cur');
const avail=document.getElementById('avail');
const WINDOW=7200;  /* fixed 2h scrub coordinate system */

function fmt(s){{
  if(!isFinite(s)||s<0)s=0;
  const m=Math.floor(s/60),ss=Math.floor(s%60);
  return m+':'+String(ss).padStart(2,'0');
}}
function seekableRange(){{
  if(v.seekable&&v.seekable.length){{
    return [v.seekable.start(0),v.seekable.end(0)];
  }}
  return [0,0];
}}
function isAtLive(){{
  const[s,e]=seekableRange();
  return (e-s)<2||(e-(v.currentTime||0))<8;
}}
/* Scrub bar coordinate system: fixed 2h window ending at "now".
   Both seekable content and chapter markers are mapped into it. */
let recChain=null;       /* array of chain entries (uuid/start/stop/hls_url) */
let recCurrentIdx=-1;    /* index into recChain of the currently loaded VOD */
let recWindow=null;      /* [chain_start_ts, chain_stop_or_now_ts] */
function scrubWindow(){{
  if(onRecording&&recWindow){{
    return [recWindow[0],recWindow[1]];
  }}
  const now=Date.now()/1000;
  /* Live view but we know there's a recording chain: extend scrub
     window leftward to the chain's earliest recording so the user
     sees every chain boundary without having to jump in first. */
  if(recChain&&recChain.length){{
    const earliest=recChain[0].start;
    return [Math.min(now-WINDOW,earliest),now];
  }}
  return [now-WINDOW,now];
}}
let recChainAds=[];   /* array of [wallStart,wallStop] from chain's comskip */
function loadRecordingChain(slug){{
  return fetch(HOST+'/api/recording-window/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    const ch=(d&&d.chain)||[];
    recChain=ch.length?ch:null;
    recChainAds=[];
    if(recChain){{
      for(const rec of recChain){{
        fetch(HOST+'/recording/'+rec.uuid+'/ads').then(r=>r.json()).then(a=>{{
          if(slug!==current)return;
          for(const[s,e]of(a.ads||[])){{
            recChainAds.push([rec.start+s,rec.start+e]);
          }}
          renderLiveAds();
        }}).catch(()=>{{}});
      }}
    }}
    renderChapters();renderLiveAds();
  }}).catch(()=>{{}});
}}
function scrubPct(wallTs){{
  const[ws,we]=scrubWindow();
  return Math.max(0,Math.min(100,((wallTs-ws)/(we-ws))*100));
}}
function refresh(){{
  const[s,e]=seekableRange();
  const curWall=wallAt(v.currentTime||0);
  let seekStartW,seekEndW;
  if(onRecording&&recChain&&recChain.length){{
    seekStartW=recChain[0].start;
    const last=recChain[recChain.length-1];
    seekEndW=last.running?Date.now()/1000:last.stop;
  }} else {{
    seekStartW=e>s?wallAt(s):curWall;
    seekEndW  =e>s?wallAt(e):curWall;
  }}
  const availL=scrubPct(seekStartW);
  const availR=scrubPct(seekEndW);
  avail.style.left=availL+'%';
  avail.style.width=(availR-availL)+'%';
  const playL=scrubPct(seekStartW);
  const playR=scrubPct(curWall);
  played.style.left=playL+'%';
  if(!_dragging){{
    played.style.width=Math.max(0,playR-playL)+'%';
    thumb.style.left=scrubPct(curWall)+'%';
  }}
  /* Show wall-clock time of current playback position, not offset to
     live edge — more intuitive and doesn't suffer from segment-jitter. */
  if(!pauseState){{
    if(isAtLive()){{
      cur.textContent='live';
    }} else {{
      const w=curWall;
      if(isFinite(w)&&w>0){{
        cur.textContent=new Date(w*1000).toLocaleTimeString('de-DE',
          {{hour:'2-digit',minute:'2-digit',second:'2-digit'}});
      }}
    }}
  }}
}}
v.addEventListener('timeupdate',()=>{{refresh();updateLiveBadge();refreshSkipAdBtn();}});
v.addEventListener('progress',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('loadeddata',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('canplay',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('loadedmetadata',()=>{{
  if(!onMediathek&&!onRecording)goLive();
  refresh();renderChapters();renderLiveAds();
}});
setInterval(()=>{{if(!v.paused){{renderChapters();renderLiveAds();}}}},5000);
/* Live-ads delivery: SSE stream that pushes a new payload whenever
   the Mac scanner saves a fresh .live_ads.json. Replaces the prior
   30 s polling — skip button now appears with the detection latency
   instead of +0-30 s extra. EventSource auto-reconnects on transient
   network errors, so no manual fallback timer is needed. */
let _liveAdsES=null;
let _liveAdsESSlug=null;
function subscribeLiveAds(slug){{
  if(_liveAdsESSlug===slug&&_liveAdsES)return;
  unsubscribeLiveAds();
  _liveAdsESSlug=slug;
  loadLiveAds(slug);  /* paint immediately, don't wait for first push */
  try{{
    const es=new EventSource(HOST+'/api/live-ads-stream/'+slug);
    es.onmessage=(e)=>{{
      if(slug!==current)return;
      try{{
        const d=JSON.parse(e.data);
        liveAds=d.ads||[];
        renderLiveAds();refreshSkipAdBtn();
      }}catch(err){{}}
    }};
    _liveAdsES=es;
  }}catch(err){{}}
}}
function unsubscribeLiveAds(){{
  if(_liveAdsES){{try{{_liveAdsES.close();}}catch(e){{}}_liveAdsES=null;}}
  _liveAdsESSlug=null;
}}
function goLive(){{
  /* Mediathek-live channel: stay inside the Mediathek HLS and just
     seek to its live edge — avoids spawning a DVB-C tuner. hls.js
     needs stopLoad + startLoad to realign with fresh segments,
     otherwise quick consecutive seeks can freeze on a stale frame. */
  if(onMediathek){{
    const[s,e]=seekableRange();
    if(e>s){{
      const target=Math.max(s+0.5,e-4);
      if(hlsInst){{
        try{{hlsInst.stopLoad();hlsInst.startLoad(target);}}catch(err){{}}
      }}
      v.currentTime=target;
      v.play().catch(()=>{{}});
      /* Safety retry: if playback isn't progressing after 800 ms,
         nudge currentTime to the latest edge and replay. */
      setTimeout(()=>{{
        if(!onMediathek||v.paused)return;
        const[,ee]=seekableRange();
        if(ee>target+3&&Math.abs(v.currentTime-target)<0.3){{
          v.currentTime=Math.max(s+0.5,ee-3);
          v.play().catch(()=>{{}});
        }}
      }},800);
      show();return;
    }}
  }}
  if(onMediathek||onRecording){{
    onMediathek=false;onRecording=false;
    recWindow=null;recChain=null;recCurrentIdx=-1;
    loadDvbcSrc(current);
    v.addEventListener('loadedmetadata',()=>{{
      const[,e]=seekableRange();
      if(e>0)v.currentTime=e-2;
      v.play().catch(()=>{{}});
    }},{{once:true}});
    return;
  }}
  const[,e]=seekableRange();
  if(e>0)v.currentTime=e-2;
}}
/* ⏭ acts as "next show": advance to the start of the next show
   (chain / Mediathek / live currently-airing). At the end of the
   chain or when caught up, falls back to live edge. */
function goShowNext(){{
  /* Recording-chain merged-targets walk forward — symmetric. */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    const cutoffWall=recentJump?_lastJumpedWall:curWall;
    const lastEntry=recChain[recChain.length-1];
    const chainEnd=lastEntry.running?Date.now()/1000:lastEntry.stop;
    const targets=[];
    for(const a of recChainAds){{
      if(a[1]>cutoffWall+0.5&&a[1]<=chainEnd)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>cutoffWall+0.5&&epg.start<=chainEnd)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>x.wall-y.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;_lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      const idx=recChain.findIndex(c=>c.start<=pick.wall&&pick.wall<c.stop);
      if(idx===recCurrentIdx){{
        v.currentTime=pick.wall-recChain[recCurrentIdx].start;show();
      }} else if(idx>=0){{
        switchToRecording(recChain[idx],pick.wall,idx);
      }} else {{
        goLive();return;
      }}
      const hints={{'epg-start':'Sprung an nächsten Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung vorwärts',2500);
      return;
    }}
    /* No more targets in chain — go live if last entry is still running */
    if(lastEntry.running)goLive();
    else showHintMsg('Ende der Aufnahme');
    return;
  }}
  /* Mediathek: jump forward to the next reachable EPG event. */
  if(onMediathek){{
    let cur=currentEvent();
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    if(recentJump)cur=_lastJumpedEv;
    if(cur){{
      const next=epgEvents.find(e=>e.start>=cur.stop);
      if(next&&isReachable(next)){{jumpToEvent(next);return;}}
    }}
    goLive();return;
  }}
  /* Live-buffer step-forward, unified merged-targets walk —
     symmetric to goShowStart. Same target set, same _lastJumpedWall
     cutoff. Picks earliest target after current. Falls through to
     goLive() when nothing's left forward. */
  const recentJumpFwd=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
  fetch(HOST+'/api/live-ads/'+current).then(r=>r.json()).then(adResp=>{{
    const ads=adResp.ads||[];
    const[s,e]=seekableRange();
    if(e<=s+1){{goLive();return;}}
    const nowS=Date.now()/1000;
    const wallS=nowS-(e-s),wallE=nowS;
    const cutoffWall=recentJumpFwd?_lastJumpedWall:wallForCurrent();
    const targets=[];
    for(const a of ads){{
      if(a[1]>cutoffWall+0.5&&a[1]<=wallE)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>cutoffWall+0.5&&epg.start<=wallE)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>x.wall-y.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;
      _lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      v.currentTime=Math.max(s+0.5,Math.min(e-1,pick.wall-wallS));
      show();
      const hints={{'epg-start':'Sprung an nächsten Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung vorwärts',2500);
      return;
    }}
    goLive();
  }}).catch(()=>goLive());
}}
let onRecording=false;
/* When the recording-VOD ends (player reached its final segment),
   switch back to the live stream. Guard against spurious events
   during source swaps by requiring currentTime near duration. */
v.addEventListener('ended',()=>{{
  if(!onRecording)return;
  const dur=v.duration;
  if(!isFinite(dur)||dur<=0)return;
  if(Math.abs(v.currentTime-dur)>2)return;
  /* Advance to the next recording in the chain, if any; else go live. */
  if(recChain&&recCurrentIdx>=0&&recCurrentIdx<recChain.length-1){{
    const nextIdx=recCurrentIdx+1;
    switchToRecording(recChain[nextIdx],recChain[nextIdx].start,nextIdx);
    return;
  }}
  goLive();
}});
/* Safari HLS exposes getStartDate() = wall-clock time of currentTime=0
   (from #EXT-X-PROGRAM-DATE-TIME). That gives reliable mapping between
   video time and real-world time. Fallback: Date.now() - (end-ct). */
function wallAt(t){{
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    /* Active VOD is recChain[recCurrentIdx]; its t=0 maps to that
       entry's start wall-clock time. */
    return recChain[recCurrentIdx].start+(t||0);
  }}
  if(typeof v.getStartDate==='function'){{
    const d=v.getStartDate();
    /* Safari returns epoch-0 before the playlist is loaded — filter
       out anything before 2020 as "not yet available". */
    if(d&&d.getTime()>1577836800000)return d.getTime()/1000+t;
  }}
  const[,e]=seekableRange();
  return Date.now()/1000-(e-t);
}}
function posForWall(ts){{
  const[ws,we]=scrubWindow();
  if(ts<ws||ts>we)return null;
  return ((ts-ws)/(we-ws))*100;
}}
function wallForCurrent(){{
  /* iOS Safari on a live HLS stream initially reports currentTime as
     Number.MAX_VALUE until the player actually settles to a position
     — clamp to the seekable live edge so wallAt doesn't overflow. */
  let t=v.currentTime;
  if(!isFinite(t)||t>1e10){{
    const[,e]=seekableRange();
    t=e>0?e-1:0;
  }}
  return wallAt(t||0);
}}
let epgEvents=[];
let mediathekAvailable=false;
let mediathekWindow=0;
function isReachable(ev){{
  /* Recording-chain: any event overlapping the full chain window. */
  if(onRecording&&recChain&&recChain.length){{
    const first=recChain[0];
    const last=recChain[recChain.length-1];
    const chainEnd=last.running?Date.now()/1000:last.stop;
    if(ev.start>=first.start-2&&ev.start<chainEnd)return true;
  }}
  /* Local: event start must be inside currently-seekable range.  */
  const[ss,ee]=seekableRange();
  if(ee>ss){{
    const wallStart=wallAt(ss);
    if(ev.start>=wallStart-2)return true;
  }}
  /* Mediathek: channel supported + event within the CDN DVR window
     (arte 30 min, ARD 2 h, ZDF 3 h — value per channel).  */
  if(mediathekAvailable&&ev.start>=Date.now()/1000-mediathekWindow)return true;
  return false;
}}
/* Gate chapter rendering until both Mediathek-availability and
   the recording-chain fetches have resolved — otherwise the ticks
   appear but tapping them would race the unfinished prerequisites. */
let _chaptersReady=false;
function renderChapters(){{
  document.querySelectorAll('.chapter').forEach(el=>el.remove());
  if(!_chaptersReady)return;
  const nowWall=Date.now()/1000;
  for(const ev of epgEvents){{
    if(!isReachable(ev))continue;
    const pct=posForWall(ev.start);
    if(pct===null)continue;
    const el=document.createElement('button');
    el.className='chapter';
    el.type='button';
    if(ev.start<=nowWall&&nowWall<ev.stop)el.classList.add('current');
    el.style.left=pct+'%';
    const ts=new Date(ev.start*1000).toLocaleTimeString('de-DE',{{hour:'2-digit',minute:'2-digit'}});
    el.title=ev.title+' ('+ts+')';
    el.addEventListener('touchstart',e=>e.stopPropagation(),{{passive:true}});
    el.addEventListener('mousedown',e=>e.stopPropagation());
    el.addEventListener('click',e=>{{
      e.stopPropagation();e.preventDefault();
      hint.textContent=ts+' · '+ev.title;
      hint.classList.add('show');
      setTimeout(()=>hint.classList.remove('show'),1200);
      jumpToEvent(ev);
    }});
    scrub.appendChild(el);
  }}
}}
function loadMediathekAvail(slug){{
  return fetch(HOST+'/api/mediathek-live/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    mediathekAvailable=!!d.url;
    mediathekWindow=d.window||0;
    renderChapters();
  }}).catch(()=>{{mediathekAvailable=false;mediathekWindow=0;}});
}}
/* EPG start times are when the broadcast SLOT begins, but actual
   content (after station ID / trailers) often starts 10-30 s later.
   Seek a bit before the EPG start so we never miss the opener. ZDF
   tends to have longer slot-padding than ARD, so give it more room.
   KiKa has unusually long branding/trailer pre-rolls (~1:50) before
   each show — use a negative lead-in to skip past them. */
function eventLeadIn(slug){{
  if(slug==='kika-hd')return -100;
  return slug.startsWith('zdf')||slug==='3sat-hd'?5:15;
}}
/* Per-channel offset added to the authoritative broadcast start. ZDFs
   API reports the EPG slot as "effective start" even when the show
   itself begins ~10 s later after a station ID — so we nudge forward. */
function postRollAdjust(slug){{
  if(slug==='zdf-hd'||slug==='zdfinfo-hd'||slug==='zdfneo-hd'
     ||slug==='3sat-hd'||slug==='arte-hd'||slug==='kika-hd'
     ||slug==='phoenix-hd')return 10;
  return 0;
}}
function doJump(actualStart,fromAuthoritative){{
  const leadIn=fromAuthoritative?0:eventLeadIn(current);
  const postRoll=fromAuthoritative?postRollAdjust(current):0;
  const targetWall=actualStart-leadIn+postRoll;
  const[s,e]=seekableRange();
  const wallS=e>s?wallAt(s):null;
  if(!onRecording&&wallS!==null&&targetWall>=wallS){{
    v.currentTime=Math.max(s+0.5,Math.min(e-1,targetWall-wallS));
    show();return;
  }}
  /* Not in live buffer — if the target falls inside a known chain
     recording, load that VOD chain. */
  if(recChain&&recChain.length){{
    const idx=recChain.findIndex(c=>c.start<=targetWall&&targetWall<c.stop);
    if(idx>=0){{
      activateRecordingChain(recChain,idx,targetWall);
      return;
    }}
  }}
  fetch(HOST+'/api/mediathek-live/'+current)
   .then(r=>r.json()).then(d=>{{
     if(!d.url){{
       hint.textContent='Sendung nicht im Puffer';
       hint.classList.add('show');
       setTimeout(()=>hint.classList.remove('show'),2000);
       return;
     }}
     /* Chapter-tap: no safety offset — the Now-Next / EPG time is
        already the intended start point. */
     switchToMediathek(d.url,targetWall,0);
   }}).catch(()=>{{}});
}}
let _lastJumpedEv=null,_lastJumpedTs=0,_lastJumpedKind=null,_lastJumpedWall=0;
function jumpToEvent(ev){{
  _lastJumpedEv=ev;_lastJumpedTs=Date.now();
  fetch(HOST+'/api/show-actual-start/'+current+'?ts='+ev.start)
   .then(r=>r.json()).then(d=>{{
     const auth=(d.source==='ard-nownext'||d.source==='zdf-getepg');
     doJump(d.actual||ev.start,auth);
   }})
   .catch(()=>doJump(ev.start,false));
}}
function currentEvent(){{
  const w=wallForCurrent();
  for(const ev of epgEvents){{
    if(ev.start<=w&&w<ev.stop)return ev;
  }}
  return null;
}}
let onMediathek=false;
function localSrc(){{return HOST+'/hls/'+current+'/dvr.m3u8';}}
function showHintMsg(msg,ms,kind){{
  /* Default kind='info' → no-op. Only kind='error' actually shows the
     toast. The "Sprung an Werbeblock-Ende" / "Sendungsanfang" /
     "Anfang der Aufnahme" etc. cues felt noisy in the player; the
     visual scrub jump itself is enough confirmation that the action
     worked. Errors stay visible because they're actionable. */
  if(kind!=='error')return;
  hint.textContent=msg;hint.classList.add('show');
  setTimeout(()=>hint.classList.remove('show'),ms||2500);
}}
async function goShowStart(){{
  /* If we're already in recording-chain mode and near the start of
     the current chain member, step back to the previous one. */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    /* Recording-chain merged-targets walk — same logic as live-buffer,
       but using recChainAds (already in wall-time) and switching
       between chain entries via switchToRecording when a target lies
       in a different recording. */
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    const cutoffWall=recentJump?_lastJumpedWall:curWall;
    const chainStart=recChain[0].start;
    const targets=[];
    for(const a of recChainAds){{
      if(a[1]>=chainStart&&a[1]<=cutoffWall-0.5)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>=chainStart&&epg.start<=cutoffWall-0.5)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>y.wall-x.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;_lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      const idx=recChain.findIndex(c=>c.start<=pick.wall&&pick.wall<c.stop);
      if(idx===recCurrentIdx){{
        v.currentTime=pick.wall-recChain[recCurrentIdx].start;show();
      }} else if(idx>=0){{
        switchToRecording(recChain[idx],pick.wall,idx);
      }} else {{
        showHintMsg('Sprungziel nicht in Aufnahme',null,'error');return;
      }}
      const hints={{'epg-start':'Sprung an vorherigen Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung zurück',2500);
      return;
    }}
    showHintMsg('Anfang der Aufnahme');
    return;
  }}
  /* Mediathek mode: same step-back idea. If we're near the current
     event's start, jump to the previous (reachable) EPG event. Uses
     _lastJumpedEv as the "current" anchor during the ~2 s window
     right after a jump (before Mediathek playback has settled),
     otherwise a quick double-tap would re-jump to the same event. */
  if(onMediathek){{
    let cur=currentEvent();
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    if(recentJump)cur=_lastJumpedEv;
    const curWall=wallForCurrent();
    const nearStart=cur&&(recentJump||Math.abs(curWall-cur.start)<=10);
    if(cur&&nearStart){{
      const prev=[...epgEvents].reverse().find(e=>e.stop<=cur.start);
      if(prev&&isReachable(prev)){{jumpToEvent(prev);return;}}
    }}
    if(cur){{jumpToEvent(cur);return;}}
  }}
  let ev=currentEvent();
  if(!ev){{
    /* EPG not loaded yet — fetch synchronously and retry. */
    showHintMsg('EPG wird geladen…',4000);
    try{{
      const r=await fetch(HOST+'/api/events/'+current+'?back=7200&fwd=1800');
      const d=await r.json();
      epgEvents=d.events||[];renderChapters();
    }}catch(e){{}}
    ev=currentEvent();
    if(!ev){{showHintMsg('Kein EPG-Event gefunden',null,'error');return;}}
  }}
  /* Live-buffer step-back, unified merged-targets walk.

     Every tap evaluates the same target set:
       - end of every detected ad block in the buffer (= "show
         resumed after this break")
       - start of every EPG event in the buffer (= "this show began")
     Cutoff for "before current position":
       - first tap of a sequence: live edge — picks the latest
         target overall (often the most recent ad-end, sometimes
         the current show start if no ad has aired since)
       - subsequent taps within the 10 s anchor window: the wall-time
         we last jumped to (NOT v.currentTime, which can lag a
         second or two on iOS Safari right after a seek and cause
         "every other tap re-picks the same target")

     If no target lies earlier in the buffer, fall through to the
     classical extend-backward cascade (recording-window → mediathek
     → buffer-start). */
  const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
  fetch(HOST+'/api/live-ads/'+current)
   .then(r=>r.json()).then(adResp=>{{
     const ads=adResp.ads||[];
     const[s,e]=seekableRange();
     if(e<=s+1){{showHintMsg('Buffer leer',null,'error');return;}}
     const nowS=Date.now()/1000;
     const wallS=nowS-(e-s),wallE=nowS;
     const cutoffWall=recentJump?_lastJumpedWall:wallE;
     const targets=[];
     for(const a of ads){{
       if(a[1]>=wallS&&a[1]<=cutoffWall-0.5)targets.push({{wall:a[1],label:'ad-end'}});
     }}
     for(const epg of epgEvents){{
       if(epg.start>=wallS&&epg.start<=cutoffWall-0.5)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
     }}
     targets.sort((x,y)=>y.wall-x.wall);
     if(targets.length){{
       const pick=targets[0];
       _lastJumpedEv=pick.ev||_lastJumpedEv||ev;
       _lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
       v.currentTime=Math.max(s+0.5,Math.min(e-1,pick.wall-wallS));
       show();
       const hints={{'epg-start':'Sprung an vorherigen Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
       showHintMsg(hints[pick.label]||'Sprung zurück',2500);
       return;
     }}
     /* No targets earlier in buffer — try chain → mediathek → buffer-start */
     showHintMsg('Suche Aufnahme…',4000);
     fetch(HOST+'/api/recording-window/'+current)
      .then(r=>r.json()).then(resp=>{{
        const chain=(resp&&resp.chain)||[];
        const targetIdx=chain.findIndex(c=>c.start<=ev.start&&ev.start<c.stop);
        if(targetIdx<0||chain.length===0){{
          fetch(HOST+'/api/mediathek-live/'+current)
           .then(r=>r.json()).then(d=>{{
             if(!d.url){{
               _lastJumpedEv=ev;_lastJumpedTs=Date.now();_lastJumpedKind='buffer';_lastJumpedWall=wallS+0.5;
               v.currentTime=s+0.5;show();
               showHintMsg('Sendungsanfang außerhalb Puffer — Sprung an Buffer-Anfang',3500);
               return;
             }}
             switchToMediathek(d.url,ev.start);
           }}).catch(()=>showHintMsg('Mediathek-Fehler',null,'error'));
          return;
        }}
        activateRecordingChain(chain,targetIdx,ev.start);
      }}).catch(err=>showHintMsg('Recording-Fehler: '+err,null,'error'));
   }}).catch(()=>showHintMsg('Live-Ads-Fehler',null,'error'));
}}
function activateRecordingChain(chain,targetIdx,wallStartTs){{
  recChain=chain;
  const first=chain[0], last=chain[chain.length-1];
  /* The chain window covers from the earliest chain member's start
     to either the last member's stop OR now (if last is running). */
  const nowS=Date.now()/1000;
  const windowEnd=last.running?nowS:last.stop;
  recWindow=[first.start,windowEnd];
  switchToRecording(chain[targetIdx],wallStartTs,targetIdx);
  /* Re-render chapter ticks with the new (full-chain) reachable
     window so all event boundaries are visible at once. */
  setTimeout(renderChapters,100);
}}
function switchToRecording(rec,wallStartTs,idx){{
  /* Load the tvheadend recording as the active HLS source.
     Poll /progress first — the playlist is 202-pending until
     ffmpeg has remuxed ~10 segments. Seek offset = wall-time of
     target minus recording-start wall time. */
  destroyHls();
  onMediathek=false;
  onRecording=true;
  recCurrentIdx=(typeof idx==='number')?idx:0;
  setSource('rec');
  const offset=Math.max(0,wallStartTs-rec.start);
  hint.textContent='Aufnahme wird vorbereitet…';
  hint.classList.add('show');
  const progUrl=HOST+'/recording/'+rec.uuid+'/progress';
  let tries=0;
  const waitReady=()=>{{
    if(current!==rec.slug_at_switch&&tries>0){{
      /* user swiped away, abort */
      hint.classList.remove('show');return;
    }}
    fetch(progUrl).then(r=>r.json()).then(d=>{{
      if(d.done||d.segments>=10){{
        hint.classList.remove('show');
        doRecordingLoad(rec.hls_url,offset);
        return;
      }}
      if(tries++>120){{
        hint.textContent='Aufnahme-Remux dauert zu lange';
        setTimeout(()=>hint.classList.remove('show'),2500);
        return;
      }}
      setTimeout(waitReady,1500);
    }}).catch(()=>setTimeout(waitReady,2500));
  }};
  rec.slug_at_switch=current;
  waitReady();
  show();
}}
function doRecordingLoad(url,offset){{
  const isIos=/iPad|iPhone|iPod/.test(navigator.userAgent);
  if(!isIos&&typeof Hls!=='undefined'&&Hls.isSupported()){{
    const hls=new Hls({{maxBufferLength:30,backBufferLength:7200,
      renderTextTracksNatively:false,subtitleDisplay:false}});
    hlsInst=hls;hlsCurrentUrl=url;
    hls.loadSource(url);
    hls.attachMedia(v);
    hls.once(Hls.Events.LEVEL_LOADED,()=>{{
      v.currentTime=offset;v.play().catch(()=>{{}});
    }});
  }} else {{
    v.src=url;v.load();
    const start=()=>{{
      v.currentTime=offset;v.play().catch(()=>{{}});
    }};
    v.addEventListener('loadedmetadata',start,{{once:true}});
    v.addEventListener('canplay',start,{{once:true}});
  }}
}}
let hlsInst=null;
let hlsCurrentUrl=null;
function destroyHls(){{
  if(hlsInst){{try{{hlsInst.destroy();}}catch(e){{}}hlsInst=null;}}
  hlsCurrentUrl=null;
}}
function switchToMediathek(rawUrl,showStartTs,safetyOffset){{
  onMediathek=true;
  setSource('mediathek');
  destroyHls();
  hlsCurrentUrl=rawUrl;
  if(typeof safetyOffset!=='number')safetyOffset=5;
  if(typeof Hls==='undefined'||!Hls.isSupported()){{
    hint.textContent='Mediathek nicht verfügbar';
    hint.classList.add('show');
    setTimeout(()=>hint.classList.remove('show'),2000);
    return;
  }}
  const hls=new Hls({{
    forceMseHlsOnAppleDevices:true,
    maxBufferLength:30,
    backBufferLength:7200,
    renderTextTracksNatively:false,
    subtitleDisplay:false,
  }});
  hlsInst=hls;
  /* Also force-disable any text tracks the browser added itself. */
  const disableCaptions=()=>{{
    for(const t of v.textTracks)t.mode='disabled';
  }};
  v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);
  setTimeout(disableCaptions,200);
  setTimeout(disableCaptions,1500);
  let seeked=false;
  hls.on(Hls.Events.LEVEL_LOADED,(_,data)=>{{
    if(seeked)return;
    const frags=(data.details&&data.details.fragments)||[];
    /* First try: fragment that contains showStartTs. If none (target
       is older than earliest fragment), fall back to the earliest.  */
    let matchedFrag=null,matchedPdt=0;
    for(const f of frags){{
      if(!f.programDateTime)continue;
      const pdt=f.programDateTime/1000;
      if(pdt<=showStartTs&&showStartTs<pdt+f.duration){{
        matchedFrag=f;matchedPdt=pdt;break;
      }}
    }}
    if(!matchedFrag){{
      for(const f of frags){{
        if(!f.programDateTime)continue;
        const pdt=f.programDateTime/1000;
        if(pdt>=showStartTs){{
          matchedFrag=f;matchedPdt=pdt;break;
        }}
      }}
    }}
    if(matchedFrag){{
      const f=matchedFrag,pdt=matchedPdt;
      const off=Math.max(0,showStartTs-pdt);
      const targetT=f.start+off;
      try{{hls.stopLoad();}}catch(e){{}}
      hls.startLoad(targetT);
      let tries=0;
      const safeTarget=Math.max(0,targetT-safetyOffset);
      const seekWhenReady=()=>{{
        const[ss,ee]=seekableRange();
        if(ss<=safeTarget&&ee>=safeTarget+2){{
          v.currentTime=safeTarget;
          v.play().catch(()=>{{}});
          return;
        }}
        if(tries++>60)return;
        setTimeout(seekWhenReady,250);
      }};
      seekWhenReady();
      seeked=true;
      show();
    }}
  }});
  hls.on(Hls.Events.ERROR,(_,data)=>{{
    if(data.fatal){{
      hint.textContent='Mediathek-Fehler';
      hint.classList.add('show');
      setTimeout(()=>hint.classList.remove('show'),2500);
    }}
  }});
  hls.attachMedia(v);
  hls.loadSource(rawUrl);
  show();
}}
function loadEvents(slug){{
  fetch(HOST+'/api/events/'+slug+'?back=7200&fwd=1800')
   .then(r=>r.json()).then(d=>{{
     if(slug!==current)return;
     epgEvents=d.events||[];renderChapters();
   }}).catch(()=>{{}});
}}
let liveAds=[];
const skipBtn=document.getElementById('skipad');
function allAdsWall(){{
  /* Union of live-buffer ads + chain recording ads (both already
     wall-clock), de-duped/sorted; chain ads survive even when the
     live buffer rolled past their segments. */
  const all=[...liveAds,...recChainAds];
  all.sort((a,b)=>a[0]-b[0]);
  return all;
}}
function renderLiveAds(){{
  document.querySelectorAll('.ad-block').forEach(el=>el.remove());
  const[ws,we]=scrubWindow();
  if(we<=ws)return;
  /* In live-buffer playback, hide markers for ad blocks outside the
     seekable range — those are orphan entries from a previous buffer
     state (e.g. before an SSD remount truncated the buffer) and you
     can't jump to them anyway. In recording-chain mode the markers
     for other chain members ARE reachable via switchToRecording, so
     keep them visible there. */
  let seekS=null,seekE=null;
  const inChain=onRecording&&recChain;
  if(!inChain){{
    /* HAVE_CURRENT_DATA (readyState>=2) — anything below that and
       v.seekable can still report the previous channel's range or
       NaN bounds during the ~1 s between v.src= and the new
       playlist parsing. Render NO markers in that window rather
       than flash stale orphan markers. The video event listeners
       (loadeddata/canplay/loadedmetadata) re-render once it's ready. */
    if(v.readyState<2)return;
    const[s,e]=seekableRange();
    if(!isFinite(s)||!isFinite(e)||e<=s+1)return;
    /* Derive wall-time of the seekable bounds from "now - duration"
       instead of wallAt(s)/wallAt(e). Safari's getStartDate() can
       return the original playlist start (drifting hours-old) for
       sliding HLS windows, making wallAt(s) too early. The live
       edge is by definition ~now, so live edge minus buffer duration
       gives the actual seekable start. */
    const nowS=Date.now()/1000;
    seekE=nowS;
    seekS=nowS-(e-s);
  }}
  for(const[wStart,wStop]of allAdsWall()){{
    if(wStop<=ws||wStart>=we)continue;
    if(seekS!==null&&(wStop<=seekS||wStart>=seekE))continue;
    const left=posForWall(Math.max(wStart,ws));
    const right=posForWall(Math.min(wStop,we));
    if(left===null||right===null||right<=left)continue;
    const el=document.createElement('div');
    el.className='ad-block';
    el.style.left=left+'%';el.style.width=(right-left)+'%';
    scrub.appendChild(el);
  }}
}}
function currentLiveAd(){{
  const w=wallForCurrent();
  for(const[a,b]of allAdsWall()){{if(w>=a&&w<b)return[a,b];}}
  return null;
}}
function skipCurrentAd(){{
  const a=currentLiveAd();if(!a)return;
  const[s,e]=seekableRange();
  const wallS=e>s?wallAt(s):null;
  if(wallS===null)return;
  v.currentTime=Math.max(s+0.5,Math.min(e-1,a[1]+0.5-wallS));
  show();
}}
function refreshSkipAdBtn(){{
  skipBtn.classList.toggle('on',currentLiveAd()!==null);
}}
function loadLiveAds(slug){{
  fetch(HOST+'/api/live-ads/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    liveAds=d.ads||[];renderLiveAds();refreshSkipAdBtn();
  }}).catch(()=>{{}});
}}
function seek(d){{
  /* Recording-chain: allow ±N-second seeks to cross chain borders
     and advance to previous/next VOD (or to live at the end). */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const target=curWall+d;
    const last=recChain[recChain.length-1];
    const nowS=Date.now()/1000;
    if(d>0&&last.running&&target>=last.stop&&target>=nowS-3){{
      goLive();return;
    }}
    const idx=recChain.findIndex(c=>c.start<=target&&target<c.stop);
    if(idx>=0&&idx!==recCurrentIdx){{
      switchToRecording(recChain[idx],target,idx);
      return;
    }}
    /* Same VOD: clamp to 0..duration. */
    const dur=v.duration||0;
    const newT=Math.max(0,Math.min(dur-0.5,(v.currentTime||0)+d));
    v.currentTime=newT;show();return;
  }}
  const[s,e]=seekableRange();
  v.currentTime=Math.max(s,Math.min(e-1,(v.currentTime||0)+d));
  show();
}}
function seekTo(ev){{
  const r=scrub.getBoundingClientRect();
  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;
  const p=Math.max(0,Math.min(1,x/r.width));
  const[ws,we]=scrubWindow();
  const targetWall=ws+p*(we-ws);
  /* Recording-chain mode: target wall-time might fall in a different
     chain member than the currently-loaded one — switch VOD if so. */
  if(onRecording&&recChain){{
    const nowS=Date.now()/1000;
    const lastIdx=recChain.length-1;
    const last=recChain[lastIdx];
    /* Scrubbed past last recording's stop → go live. */
    if(last.running&&targetWall>=last.stop&&targetWall>=nowS-3){{
      goLive();return;
    }}
    const idx=recChain.findIndex(c=>c.start<=targetWall&&targetWall<c.stop);
    const targetIdx=idx>=0?idx:(targetWall<recChain[0].start?0:lastIdx);
    if(targetIdx!==recCurrentIdx){{
      switchToRecording(recChain[targetIdx],targetWall,targetIdx);
      return;
    }}
    /* Same VOD — seek within. */
    v.currentTime=Math.max(0,targetWall-recChain[targetIdx].start);
    return;
  }}
  const[s,e]=seekableRange();
  if(e<=s)return;
  /* Snap to live edge if the drag landed in the rightmost 2 % of the
     scrubbar — otherwise the cursor never quite reaches "live" because
     we clamp 1 s short and the player keeps a tiny gap visible. */
  if(p>=0.98){{goLive();return;}}
  const wallS=wallAt(s),wallE=wallAt(e);
  const clamped=Math.max(wallS,Math.min(wallE-1,targetWall));
  v.currentTime=s+(clamped-wallS);
}}
let pauseState=null;
function togglePlay(){{
  if(pauseState){{
    const ps=pauseState;pauseState=null;
    if(ps.pausedMediathekUrl){{
      switchToMediathek(ps.pausedMediathekUrl,ps.pausedWall);
    }} else if(ps.pausedHlsLive){{
      /* hls.js keeps playlist position across pause — just resume. */
      v.play().catch(()=>{{}});
    }} else {{
      v.src=ps.pausedSrc;v.load();
      v.addEventListener('loadedmetadata',()=>{{
        const d=typeof v.getStartDate==='function'?v.getStartDate():null;
        if(d&&!isNaN(d.getTime())){{
          const wallS=d.getTime()/1000;
          v.currentTime=Math.max(0,ps.pausedWall-wallS);
        }}
        v.play().catch(()=>{{}});
      }},{{once:true}});
    }}
    pp.textContent='\u23F8';
    return;
  }}
  if(v.paused||v.ended){{
    v.play().catch(()=>{{}});
    pp.textContent='\u23F8';
    return;
  }}
  /* Three pause flavours:
     - Mediathek (hls.js, non-live): destroy hls.js, resume rebuilds it.
     - DVB-C via hls.js (Chrome/Firefox): regular v.pause() keeps position.
     - DVB-C via native HLS (iOS Safari): pause() is ignored on live HLS,
       so we strip v.src and seek back via wall-time on resume. */
  const usingHlsLive=!!(hlsInst&&!onMediathek);
  pauseState={{
    pausedSrc:(onMediathek||usingHlsLive)?null:v.src,
    pausedMediathekUrl:onMediathek?hlsCurrentUrl:null,
    pausedHlsLive:usingHlsLive,
    pausedWall:wallAt(v.currentTime),
    pausedCurrent:v.currentTime
  }};
  v.pause();
  if(onMediathek&&hlsInst){{
    try{{hlsInst.destroy();}}catch(e){{}}
    hlsInst=null;hlsCurrentUrl=null;
    v.removeAttribute('src');
    v.load();
  }} else if(!usingHlsLive){{
    v.removeAttribute('src');
    v.load();
  }}
  pp.textContent='\u25B6';
  setTimeout(()=>{{if(pauseState)pp.textContent='\u25B6';}},80);
  setTimeout(()=>{{if(pauseState)pp.textContent='\u25B6';}},500);
}}

function doUnmute(){{
  v.muted=false;v.play().catch(()=>{{}});unmuteBtn.style.display='none';
}}
function tryPlay(){{
  if(!v.paused&&!v.ended)return;
  const p=v.play();
  if(p&&typeof p.then==='function'){{
    p.then(()=>{{if(v.muted)unmuteBtn.style.display='block';}})
     .catch(()=>{{
       if(!v.muted&&!unmutedByUser){{
         v.muted=true;
         v.play().then(()=>{{
           if(v.muted&&!unmutedByUser)unmuteBtn.style.display='block';
         }}).catch(()=>{{}});
       }}
     }});
  }}
}}
let unmutedByUser=false;
function onInteract(){{
  if(!unmutedByUser){{
    v.muted=false;unmutedByUser=true;unmuteBtn.style.display='none';
  }}
  /* Don't auto-resume if the user just hit pause. */
  if(pauseState)return;
  tryPlay();
}}
document.addEventListener('click',onInteract);
document.addEventListener('touchend',()=>setTimeout(onInteract,30),
                          {{passive:true}});

function loadDvbcSrc(slug){{
  onMediathek=false;
  setSource('live');
  destroyHls();
  const url=HOST+'/hls/'+slug+'/dvr.m3u8';
  v.muted=false;
  const isIos=/iPad|iPhone|iPod/.test(navigator.userAgent);
  if(!isIos && typeof Hls!=='undefined' && Hls.isSupported()){{
    const hls=new Hls({{
      maxBufferLength:30,
      backBufferLength:7200,
      renderTextTracksNatively:false,
      subtitleDisplay:false,
    }});
    hlsInst=hls;
    hlsCurrentUrl=url;
    hls.loadSource(url);
    hls.attachMedia(v);
  }} else {{
    v.src=url;v.load();
  }}
  v.addEventListener('loadedmetadata',()=>{{
    tryPlay();restoreLastPos(slug);
  }},{{once:true}});
}}
function tryMediathekLive(url){{
  return new Promise(resolve=>{{
    if(typeof Hls==='undefined'||!Hls.isSupported()){{
      resolve(false);return;
    }}
    destroyHls();
    const hls=new Hls({{forceMseHlsOnAppleDevices:true,
      maxBufferLength:30,backBufferLength:10800,
      renderTextTracksNatively:false,subtitleDisplay:false}});
    hlsInst=hls;
    hlsCurrentUrl=url;
    /* Kill any caption/subtitle tracks the player auto-selected. */
    const disableCaptions=()=>{{
      for(const t of v.textTracks)t.mode='disabled';
    }};
    v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);
    setTimeout(disableCaptions,200);
    setTimeout(disableCaptions,1500);
    let settled=false;
    const finish=ok=>{{
      if(settled)return;settled=true;
      clearTimeout(to);
      if(!ok){{
        try{{hls.destroy();}}catch(e){{}}
        hlsInst=null;hlsCurrentUrl=null;
      }}
      resolve(ok);
    }};
    const to=setTimeout(()=>{{
      console.warn('mediathek live timeout');finish(false);
    }},8000);
    hls.on(Hls.Events.ERROR,(_,data)=>{{
      if(data.fatal){{console.warn('mediathek live error',data);finish(false);}}
    }});
    const onReady=()=>{{
      v.removeEventListener('canplay',onReady);
      /* Muted autoplay to satisfy iOS gesture rules; unmute-button
         overlay appears and the user taps it to hear audio — same
         pattern as the DVB-C path. */
      v.muted=true;
      v.play().then(()=>{{
        if(unmuteBtn){{
          v.muted=true;
          unmuteBtn.style.display='inline-flex';
        }}
      }}).catch(()=>{{}});
      finish(true);
    }};
    v.addEventListener('canplay',onReady);
    v.muted=true;
    hls.loadSource(url);hls.attachMedia(v);
  }});
}}
/* Last-position per channel — persisted so "re-open live stream"
   resumes where the user left off, not at the live edge.
   Stored as wall-clock seconds + timestamp; ignored if older than 4 h
   or outside the current seekable range. */
const LASTPOS_TTL_MS=4*3600*1000;
function saveLastPos(slug){{
  if(!slug||onMediathek)return;
  const t=v.currentTime||0;
  if(t<=0)return;
  const w=wallAt(t);
  if(!w||!isFinite(w))return;
  try{{
    localStorage.setItem('lastpos_'+slug,
      JSON.stringify({{wall:w,ts:Date.now()}}));
  }}catch(e){{}}
}}
function restoreLastPos(slug){{
  let entry=null;
  try{{
    entry=JSON.parse(localStorage.getItem('lastpos_'+slug)||'null');
  }}catch(e){{}}
  if(!entry||Date.now()-entry.ts>LASTPOS_TTL_MS)return;
  let tries=20;
  const tick=()=>{{
    if(tries--<=0||current!==slug||onMediathek)return;
    const[s,e]=seekableRange();
    if(e<=s+2){{setTimeout(tick,300);return;}}
    const wallS=wallAt(s);
    if(!wallS){{setTimeout(tick,300);return;}}
    const target=s+(entry.wall-wallS);
    if(target<s+0.5||target>e-1)return;   /* out of buffer */
    v.currentTime=target;
  }};
  setTimeout(tick,900);
}}
setInterval(()=>{{if(current&&!v.paused)saveLastPos(current);}},20000);
window.addEventListener('beforeunload',()=>saveLastPos(current));
document.addEventListener('visibilitychange',()=>{{
  if(document.visibilityState==='hidden')saveLastPos(current);
}});
async function loadSrc(slug){{
  saveLastPos(current);   /* preserve previous channel position */
  current=slug;onMediathek=false;
  const ch=channels[idx];
  if(ch)chname.textContent=ch.name;
  history.replaceState(null,'','/watch/'+slug);
  loadEpg(slug);
  loadEvents(slug);
  _chaptersReady=false;
  const mtP=loadMediathekAvail(slug);
  subscribeLiveAds(slug);
  recChain=null;recCurrentIdx=-1;
  const chP=loadRecordingChain(slug);
  Promise.all([mtP,chP]).then(()=>{{
    if(slug!==current)return;
    _chaptersReady=true;renderChapters();
  }});
  showHint(slug);
  /* Public-broadcaster channels have a Mediathek live stream — try
     that first to save a DVB-C tuner + the Pi CPU. Only fall back to
     our local ffmpeg pipeline if Mediathek doesn't answer in 8 s or
     errors out fatally. Private channels never had Mediathek so they
     skip the probe entirely. */
  try{{
    const mt=await fetch(HOST+'/api/mediathek-live/'+slug)
      .then(r=>r.json()).catch(()=>null);
    if(mt&&mt.url&&current===slug){{
      if(await tryMediathekLive(mt.url)){{
        if(current!==slug)return; /* user swiped away meanwhile */
        onMediathek=true;setSource('mediathek');
        return;
      }}
    }}
  }}catch(e){{}}
  if(current!==slug)return;
  loadDvbcSrc(slug);
}}

function showHint(slug){{
  const ch=channels[idx];
  hint.textContent=ch?ch.name:slug;
  hint.classList.add('show');
  setTimeout(()=>hint.classList.remove('show'),700);
}}
function nextCh(){{if(!channels.length)return;
  idx=(idx+1)%channels.length;loadSrc(channels[idx].slug);show();}}
function prevCh(){{if(!channels.length)return;
  idx=(idx-1+channels.length)%channels.length;loadSrc(channels[idx].slug);show();}}

window.addEventListener('pagehide',()=>navigator.sendBeacon(HOST+'/stop-all'));
window.addEventListener('beforeunload',()=>navigator.sendBeacon(HOST+'/stop-all'));

function loadEpg(slug){{
  fetch(HOST+'/api/now/'+slug).then(r=>r.json()).then(d=>{{
    epgEl.textContent=d.title?(d.time+' · '+d.title):'';
  }}).catch(()=>{{epgEl.textContent='';}});
}}

// Swipe horizontally on the video to switch channels.
let startX=0,startY=0,startT=0;
document.addEventListener('touchstart',e=>{{
  if(!e.touches[0])return;
  startX=e.touches[0].clientX;startY=e.touches[0].clientY;
  startT=Date.now();
}},{{passive:true,capture:true}});
document.addEventListener('touchend',e=>{{
  if(!e.changedTouches[0])return;
  const dx=e.changedTouches[0].clientX-startX;
  const dy=e.changedTouches[0].clientY-startY;
  const dt=Date.now()-startT;
  if(Math.abs(dx)>60&&Math.abs(dx)>Math.abs(dy)*1.5&&dt<800){{
    if(dx<0)nextCh();else prevCh();
  }}
}},{{passive:true,capture:true}});
document.addEventListener('keydown',e=>{{
  if(e.key==='Escape')closePlayer();
  else if(e.key==='ArrowRight')nextCh();
  else if(e.key==='ArrowLeft')prevCh();
  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}
}});
v.addEventListener('click',()=>{{
  if(chromeBar.classList.contains('hidden'))show();
  else {{chromeBar.classList.add('hidden');topbar.classList.add('hidden');}}
}});
document.addEventListener('mousemove',show);

loadSrc(current);
fetch(HOST+'/api/channels').then(r=>r.json()).then(d=>{{
  channels=d.channels;
  idx=channels.findIndex(c=>c.slug===current);
  if(idx<0)idx=0;
}});
</script></body></html>""", 200, {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}


MEDIATHEK_LIVE = {
    # slug -> (url, dvr_window_seconds)
    "das-erste-hd":    ("https://daserste-live.ard-mcdn.de/daserste/live/hls/de/master.m3u8",    7200),
    "tagesschau24-hd": ("https://tagesschau-live.ard-mcdn.de/tagesschau/live/hls/de/master.m3u8", 7200),
    "zdf-hd":          ("https://zdf-hls-15.akamaized.net/hls/live/2016498/de/veryhigh/master.m3u8", 10800),
    "3sat-hd":         ("https://zdf-hls-18.akamaized.net/hls/live/2016501/dach/veryhigh/master.m3u8", 10800),
    "arte-hd":         ("https://artesimulcast.akamaized.net/hls/live/2030993/artelive_de/index.m3u8", 1800),
    "kika-hd":         ("https://kikageohls.akamaized.net/hls/live/2022693/livetvkika_de/master.m3u8", 7200),
}

# ARD now-next channel CRIDs for precise show-start lookup. Value is
# the base64-encoded channel id that programm-api.ard.de expects.
ARD_NOWNEXT_CRID = {
    "das-erste-hd":    "Y3JpZDovL2Rhc2Vyc3RlLmRlL2xpdmUvY2xpcC9hYmNhMDdhMy0zNDc2LTQ4NTEtYjE2Mi1mZGU4ZjY0NmQ0YzQ",
    "tagesschau24-hd": "Y3JpZDovL2Rhc2Vyc3RlLmRlL3RhZ2Vzc2NoYXUvbGl2ZXN0cmVhbQ",
    "3sat-hd":         "Y3JpZDovLzNzYXQuZGUvTGl2ZXN0cmVhbS0zc2F0",
}

# ZDF broadcaster IDs (used with their getEpg GraphQL persisted query).
# ZDFs API covers more than just ZDF — their EPG service delivers
# precise now/next times for all public-broadcast partners they host.
ZDF_BROADCASTER = {
    "zdf-hd":      "ZDF",
    "zdfinfo-hd":  "ZDFinfo",
    "zdfneo-hd":   "ZDFneo",
    "3sat-hd":     "3sat",
    "kika-hd":     "KI.KA",
    "phoenix-hd":  "PHOENIX",
    "arte-hd":     "arte",
}
ZDF_API_TOKEN = "ahBaeMeekaiy5ohsai4bee4ki6Oopoi5quailieb"
ZDF_GETEPG_HASH = "e36a71fb3206e75a82a5438737113b221e43daf0363d85f3eeceda288d158821"


def _lookup_ard_actual_start(slug, ts):
    crid = ARD_NOWNEXT_CRID.get(slug)
    if not crid:
        return None
    try:
        url = (f"https://programm-api.ard.de/nownext/api/channel"
               f"?channel={crid}&pastHours=6&futureEvents=3")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except Exception:
        return None
    from datetime import datetime
    best = None
    best_diff = 360
    for ev in data.get("events", []):
        scheduled = ev.get("startDate")
        current = ev.get("currentStartDate") or scheduled
        if not scheduled:
            continue
        try:
            sched_ts = datetime.fromisoformat(scheduled).timestamp()
        except Exception:
            continue
        diff = abs(sched_ts - ts)
        if diff < best_diff:
            try:
                best = datetime.fromisoformat(current).timestamp()
                best_diff = diff
            except Exception:
                pass
    return int(best) if best is not None else None


def _lookup_zdf_actual_start(slug, ts):
    broadcaster_id = ZDF_BROADCASTER.get(slug)
    if not broadcaster_id:
        return None
    from datetime import datetime, timezone
    try:
        base = datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None
    frm = base.replace(hour=0, minute=0, second=0, microsecond=0)
    to  = frm.replace(hour=23, minute=59, second=59)
    variables = urllib.parse.quote(json.dumps({
        "filter": {
            "broadcasterIds": [broadcaster_id],
            "from": frm.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":  to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }, separators=(",", ":")))
    extensions = urllib.parse.quote(json.dumps({
        "persistedQuery": {"version": 1, "sha256Hash": ZDF_GETEPG_HASH},
    }, separators=(",", ":")))
    url = (f"https://api.zdf.de/graphql?operationName=getEpg"
           f"&variables={variables}&extensions={extensions}")
    try:
        req = urllib.request.Request(url, headers={
            "api-auth":                  f"Bearer {ZDF_API_TOKEN}",
            "zdf-app-id":                "ngplayer_2_4",
            "x-apollo-operation-name":   "getEpg",
            "apollo-require-preflight":  "true",
            "accept":                    "application/json",
            "referer":                   "https://www.zdf.de/",
        })
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except Exception:
        return None
    best = None
    best_diff = 360
    for entry in data.get("data", {}).get("epg", []):
        bc = entry.get("broadcaster", {})
        for slot in (bc.get("now"), bc.get("next")):
            if not slot:
                continue
            scheduled = slot.get("airtimeBegin")
            effective = slot.get("effectiveAirtimeBegin") or scheduled
            if not scheduled:
                continue
            try:
                sched_ts = datetime.fromisoformat(scheduled).timestamp()
                eff_ts = datetime.fromisoformat(effective).timestamp()
            except Exception:
                continue
            diff = abs(sched_ts - ts)
            if diff < best_diff:
                best = eff_ts
                best_diff = diff
    return int(best) if best is not None else None


@app.route("/api/show-actual-start/<slug>")
def api_show_actual_start(slug):
    """Look up the ACTUAL broadcast start of the show whose EPG slot
    starts at ?ts=<epoch>. Uses ARDs programm-api or ZDFs getEpg
    persisted-query API, depending on the channel. Falls back to EPG
    if no match within ±6 min."""
    try:
        ts = int(request.args.get("ts", "0"))
    except ValueError:
        ts = 0
    if ts <= 0:
        return _cors(Response(json.dumps({"actual": ts, "source": "epg"}),
                               mimetype="application/json"))
    actual = _lookup_ard_actual_start(slug, ts)
    if actual is not None:
        return _cors(Response(json.dumps({"actual": actual, "source": "ard-nownext"}),
                               mimetype="application/json"))
    actual = _lookup_zdf_actual_start(slug, ts)
    if actual is not None:
        return _cors(Response(json.dumps({"actual": actual, "source": "zdf-getepg"}),
                               mimetype="application/json"))
    return _cors(Response(json.dumps({"actual": ts, "source": "epg"}),
                           mimetype="application/json"))


@app.route("/api/mediathek-live/<slug>")
def api_mediathek_live(slug):
    """Public HLS fallback URL (ARD Mediathek etc.) for restart beyond
    the local DVR buffer. Returns { url, window } or { url: null }."""
    info = MEDIATHEK_LIVE.get(slug)
    if info:
        url, window = info
        body = {"url": url, "window": window}
    else:
        body = {"url": None, "window": 0}
    return _cors(Response(json.dumps(body),
                           mimetype="application/json"))


@app.route("/mediathek-passthru/<slug>/master.m3u8")
def mediathek_passthru_master(slug):
    """Serve a single-variant master pointing to the highest BANDWIDTH
    rendition from upstream. Rewrites the variant URI (and its audio
    companion) through our /pl.m3u8 proxy to keep the playlist origin
    on us — sidesteps iOS cross-origin quirks. Ignoring lower variants
    means the player can't ABR-downgrade on a momentary bandwidth dip;
    that's the point — Mediathek streams top out at ~5 Mbps which any
    home connection handles, and the user has explicitly asked for max.

    Upstream URL comes from MEDIATHEK_LIVE[slug] (live-channels path)
    or, when '?u=<encoded url>' is given, that URL directly (for the
    on-demand play / recording paths where the upstream master URL
    isn't pre-known and is resolved per request)."""
    upstream_override = request.args.get("u", "")
    if upstream_override and upstream_override.startswith("http"):
        base_url = upstream_override
    else:
        info = MEDIATHEK_LIVE.get(slug)
        if not info:
            abort(404)
        base_url = info[0]
    try:
        txt = urllib.request.urlopen(base_url, timeout=8).read().decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    picked = _parse_master(base_url, txt)
    if not picked:
        # Couldn't parse — serve the raw master (legacy passthru). Better
        # than 502'ing because at least the player gets *something*.
        return Response(txt, mimetype="application/vnd.apple.mpegurl")
    video_url, audio_url, codecs = picked
    proxy = lambda u: (f"{HOST_URL}/mediathek-passthru/{slug}/pl.m3u8?"
                       f"u={urllib.parse.quote(u, safe='')}")
    strip_audio = request.args.get("no_audio") == "1"
    out = ["#EXTM3U", "#EXT-X-VERSION:3"]
    audio_attr = ""
    if audio_url and not strip_audio:
        out.append(
            f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="audio",'
            f'DEFAULT=YES,AUTOSELECT=YES,URI="{proxy(audio_url)}"')
        audio_attr = ',AUDIO="aud"'
    codecs_attr = f',CODECS="{codecs}"' if codecs else ""
    # BANDWIDTH is required by the spec but doesn't matter when there's
    # only one variant — set it high so any future ABR-aware caller
    # treats this as the top tier.
    out.append(f"#EXT-X-STREAM-INF:BANDWIDTH=99999999"
               f"{codecs_attr}{audio_attr}")
    out.append(proxy(video_url))
    return Response("\n".join(out) + "\n",
                     mimetype="application/vnd.apple.mpegurl")


@app.route("/mediathek-passthru/<slug>/pl.m3u8")
def mediathek_passthru_variant(slug):
    """Proxy a sub-playlist (video or audio) from ARD, rewriting
    segment URIs to absolute upstream URLs."""
    upstream = request.args.get("u", "")
    if not upstream or not upstream.startswith("http"):
        abort(400)
    try:
        txt = urllib.request.urlopen(upstream, timeout=8).read().decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    base = upstream.rsplit("/", 1)[0] + "/"
    out_lines = []
    for line in txt.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("http"):
            out_lines.append(base + stripped)
        else:
            out_lines.append(line)
    return Response("\n".join(out_lines) + "\n",
                     mimetype="application/vnd.apple.mpegurl")


def _parse_master(master_url, master_text):
    """Pick best-bandwidth video variant and its audio companion from a
    master playlist. Returns (video_url, audio_url_or_none, codecs)."""
    base = master_url.rsplit("/", 1)[0] + "/"
    audio_map = {}   # group-id -> list of {uri, default}
    for line in master_text.splitlines():
        if line.startswith("#EXT-X-MEDIA") and 'TYPE=AUDIO' in line:
            gid = re.search(r'GROUP-ID="([^"]+)"', line)
            uri = re.search(r'URI="([^"]+)"', line)
            if not gid or not uri:
                continue
            u = uri.group(1)
            if not u.startswith("http"):
                u = base + u
            is_default = 'DEFAULT=YES' in line
            audio_map.setdefault(gid.group(1), []).append(
                {"uri": u, "default": is_default})
    # Per group, prefer DEFAULT=YES entry, else first one
    audio_pick = {}
    for gid, entries in audio_map.items():
        dflt = next((e for e in entries if e["default"]), entries[0])
        audio_pick[gid] = dflt["uri"]
    audio_map = audio_pick
    best = None
    best_bw = -1
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            bw = int(re.search(r"BANDWIDTH=(\d+)", line).group(1)) \
                if re.search(r"BANDWIDTH=(\d+)", line) else 0
            ag = re.search(r'AUDIO="([^"]+)"', line)
            codecs = re.search(r'CODECS="([^"]+)"', line)
            if i + 1 < len(lines):
                url = lines[i + 1].strip()
                if url and not url.startswith("#") and bw > best_bw:
                    if not url.startswith("http"):
                        url = base + url
                    best_bw = bw
                    best = (url, audio_map.get(ag.group(1)) if ag else None,
                            codecs.group(1) if codecs else "")
    return best


def _rewrite_pl_as_vod(pl_url, start_ts):
    """Fetch a media playlist, slice it to segments with PDT >= start_ts,
    and return a self-contained VOD-style playlist text."""
    pl = urllib.request.urlopen(pl_url, timeout=8).read().decode()
    base = pl_url.rsplit("/", 1)[0] + "/"
    out = ["#EXTM3U", "#EXT-X-VERSION:4",
           "#EXT-X-INDEPENDENT-SEGMENTS",
           "#EXT-X-PLAYLIST-TYPE:VOD",
           "#EXT-X-TARGETDURATION:3",
           "#EXT-X-MEDIA-SEQUENCE:0"]
    pending_extinf = None
    cur_pdt = None
    started = False
    for line in pl.splitlines():
        if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            try:
                from datetime import datetime
                cur_pdt = datetime.fromisoformat(
                    line.split(":", 1)[1].strip()).timestamp()
            except Exception:
                cur_pdt = None
            continue
        if line.startswith("#EXTINF"):
            pending_extinf = line
            continue
        if line.startswith("#") or not line.strip():
            continue
        seg_url = line.strip()
        if not seg_url.startswith("http"):
            seg_url = base + seg_url
        if cur_pdt is not None and cur_pdt + 2.5 >= start_ts:
            if not started:
                tz = time.strftime('%z', time.localtime(cur_pdt))
                tz_iso = tz[:3] + ':' + tz[3:]  # "+0200" → "+02:00"
                out.append(f"#EXT-X-PROGRAM-DATE-TIME:"
                           f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(cur_pdt))}"
                           f"{tz_iso}")
                started = True
            if pending_extinf:
                out.append(pending_extinf)
            out.append(seg_url)
        pending_extinf = None
        if cur_pdt is not None:
            cur_pdt += 2
    out.append("#EXT-X-ENDLIST")
    return "\n".join(out) + "\n"


@app.route("/mediathek-clip/<slug>/master.m3u8")
def mediathek_clip_master(slug):
    """Build a VOD master playlist with video+audio variants whose URIs
    point back to our /video.m3u8 and /audio.m3u8 (which rewrite from
    the corresponding ARD playlists)."""
    info = MEDIATHEK_LIVE.get(slug)
    if not info:
        abort(404)
    base_url = info[0]
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    if start_ts <= 0:
        abort(400)
    try:
        master = urllib.request.urlopen(base_url, timeout=8).read().decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    picked = _parse_master(base_url, master)
    if not picked:
        abort(502, "no variant")
    video_url, audio_url, codecs = picked
    # Store the picked upstream URLs so the client-facing video/audio
    # endpoints know what to rewrite. Keyed by slug for simplicity.
    _clip_state[slug] = {"video": video_url, "audio": audio_url}
    vurl = f"{HOST_URL}/mediathek-clip/{slug}/video.m3u8?start={start_ts}"
    aurl = f"{HOST_URL}/mediathek-clip/{slug}/audio.m3u8?start={start_ts}"
    codec_attr = f'CODECS="{codecs}"' if codecs else ''
    lines = ["#EXTM3U", "#EXT-X-VERSION:4",
             "#EXT-X-INDEPENDENT-SEGMENTS"]
    if audio_url:
        lines += [f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Deutsch",'
                  f'DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="de",'
                  f'CHANNELS="2",URI="{aurl}"']
        lines += [f'#EXT-X-STREAM-INF:BANDWIDTH=5640800,'
                  f'AVERAGE-BANDWIDTH=5000000,{codec_attr},'
                  f'RESOLUTION=1920x1080,FRAME-RATE=50.000,'
                  f'AUDIO="aud"',
                  vurl]
    else:
        lines += [f'#EXT-X-STREAM-INF:BANDWIDTH=5640800,{codec_attr}',
                  vurl]
    return Response("\n".join(lines) + "\n",
                     mimetype="application/vnd.apple.mpegurl")


_clip_state = {}


@app.route("/mediathek-clip/<slug>/video.m3u8")
def mediathek_clip_video(slug):
    state = _clip_state.get(slug) or {}
    url = state.get("video")
    if not url:
        abort(404)
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    try:
        return Response(_rewrite_pl_as_vod(url, start_ts),
                        mimetype="application/vnd.apple.mpegurl")
    except Exception as e:
        abort(502, f"upstream: {e}")


@app.route("/mediathek-clip/<slug>/audio.m3u8")
def mediathek_clip_audio(slug):
    state = _clip_state.get(slug) or {}
    url = state.get("audio")
    if not url:
        abort(404)
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    try:
        return Response(_rewrite_pl_as_vod(url, start_ts),
                        mimetype="application/vnd.apple.mpegurl")
    except Exception as e:
        abort(502, f"upstream: {e}")


@app.route("/api/events/<slug>")
def api_events(slug):
    """EPG events for a channel within a time window (default: last 2h +
    next 1h). Used by the watch player to render chapter markers on the
    scrub bar."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"events": []}),
                               mimetype="application/json"))
    try:
        back = max(60, min(24 * 3600, int(request.args.get("back", 7200))))
        fwd  = max(0,  min(24 * 3600, int(request.args.get("fwd",  3600))))
    except ValueError:
        back, fwd = 7200, 3600
    data = fetch_epg(window_before=back, window_after=fwd)
    out = []
    for e in data["events"].get(slug, []):
        out.append({"start": e["start"], "stop": e["stop"],
                    "title": e.get("title", "")})
    return _cors(Response(json.dumps({"events": out, "now": data["now"]}),
                           mimetype="application/json"))


@app.route("/api/now/<slug>")
def api_now(slug):
    """What is currently on this channel (from EPG archive + live EPG)."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return {"title": None, "time": ""}
    now_ts = int(time.time())
    try:
        data = fetch_epg(window_before=0, window_after=300)
        for e in data["events"].get(slug, []):
            if e["start"] <= now_ts < e["stop"]:
                return {"title": e.get("title", ""),
                        "time": time.strftime("%H:%M",
                                               time.localtime(e["start"])) + "–" +
                                time.strftime("%H:%M",
                                               time.localtime(e["stop"]))}
    except Exception:
        pass
    return {"title": None, "time": ""}


@app.route("/api/recording-window/<slug>")
def api_recording_window(slug):
    """Find chained DVR entries on this channel that form a continuous
    back-buffer. Starts from the currently-recording entry (if any)
    and walks backwards through contiguous completed recordings
    (gap ≤ 180 s counts as "contiguous"). Returns the chain sorted
    oldest → newest. Empty if nothing relevant is on disk."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    ch_uuid = info.get("uuid")
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=200&sort=start&dir=DESC",
            timeout=5).read())
    except Exception:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    now_ts = int(time.time())
    CHAIN_GAP = 180   # seconds — two recordings within 3 min are "chained"
    MAX_BACK  = 6 * 3600  # how far back we'll walk (safety)
    # Collect relevant DVR entries: currently-recording OR completed
    # within the last 6 h on this channel.
    candidates = []
    for e in data.get("entries", []):
        if e.get("channel") != ch_uuid:
            continue
        start = e.get("start", 0); stop = e.get("stop", 0)
        sched = e.get("sched_status", "")
        is_running = (start <= now_ts < stop and sched == "recording")
        is_recent  = (sched == "completed"
                      and stop > now_ts - MAX_BACK
                      and stop <= now_ts)
        if not (is_running or is_recent):
            continue
        if not e.get("filename") and not is_running:
            continue   # nothing on disk yet
        candidates.append({
            "uuid": e.get("uuid"),
            "title": e.get("disp_title") or "",
            "start": start,
            "stop": stop,
            "running": is_running,
        })
    candidates.sort(key=lambda c: c["start"])
    # Walk backwards from the newest relevant entry, pulling in the
    # previous one if it's within CHAIN_GAP of the current chain-start.
    if not candidates:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    chain = [candidates[-1]]
    for cand in reversed(candidates[:-1]):
        if chain[0]["start"] - cand["stop"] > CHAIN_GAP:
            break
        chain.insert(0, cand)
    for c in chain:
        c["hls_url"] = f"{HOST_URL}/recording/{c['uuid']}/index.m3u8"
    return _cors(Response(json.dumps({"chain": chain}),
                           mimetype="application/json"))


@app.route("/api/channels")
def api_channels():
    """Ordered channel list as JSON for the Watch-player (swipe navigation)."""
    now = time.time()
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    with active_lock:
        for s, i in channels.items():
            st_snap.setdefault(s, {"watch_seconds": 0, "starts": 0})
            st_snap[s]["watch_seconds"] = st_snap[s].get("watch_seconds", 0) \
                                           + (now - i.get("started_at", now))
    with cmap_lock:
        items = list(channel_map.items())
    items.sort(key=lambda kv: (
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower(),
    ))
    return {"channels": [{"slug": s,
                          "name": info["name"],
                          "icon": _channel_logo_url(s, info.get("icon", ""))}
                         for s, info in items]}


@app.route("/record/<slug>")
def record_channel(slug):
    """Start an instant tvheadend recording for this channel."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        abort(404, "unknown channel")
    try:
        duration = int(request.args.get("duration", "7200"))
    except ValueError:
        duration = 7200
    duration = max(300, min(duration, 24 * 3600))   # 5min - 24h
    now_ts = int(time.time())
    conf = {
        "enabled": True,
        "start": now_ts,
        "stop": now_ts + duration,
        "channel": info["uuid"],
        "disp_title": info["name"] + " (" + time.strftime("%H:%M",
                                                           time.localtime(now_ts)) + ")",
        "comment": f"instant-record via gateway ({duration//60} min)",
    }
    body = urllib.parse.urlencode({"conf": json.dumps(conf)}).encode()
    req = urllib.request.Request(f"{TVH_BASE}/api/dvr/entry/create",
                                  data=body, method="POST")
    try:
        res = urllib.request.urlopen(req, timeout=10).read().decode()
        data = json.loads(res)
        return _cors(Response(json.dumps({"ok": True,
                                           "uuid": data.get("uuid"),
                                           "channel": info["name"],
                                           "duration_minutes": duration // 60}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


@app.route("/record-event/<event_id>")
def record_event(event_id):
    """Schedule a DVR entry for a specific EPG event (whole programme)."""
    body = urllib.parse.urlencode({"event_id": event_id,
                                    "config_uuid": ""}).encode()
    req = urllib.request.Request(f"{TVH_BASE}/api/dvr/entry/create_by_event",
                                  data=body, method="POST")
    try:
        res = urllib.request.urlopen(req, timeout=10).read().decode()
        data = json.loads(res)
        uuid = (data.get("uuid") or [None])[0] \
            if isinstance(data.get("uuid"), list) else data.get("uuid")
        return _cors(Response(json.dumps({"ok": True, "uuid": uuid}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


# DVB channels where the ARD Mediathek is likely to have coverage.
# We don't filter further by Mediathek publicationService because
# ARD-aired shows are produced by various regional broadcasters
# (WDR, NDR, BR, …) and appear in search under the producing
# broadcaster's name, not under "Das Erste".
ARD_SEARCH_CHANNELS = {
    "das-erste-hd", "tagesschau24-hd", "rbb-berlin-hd",
    "arte-hd", "kika-hd", "3sat-hd",
}
ZDF_SEARCH_CHANNELS = {"zdf-hd"}


def _mediathek_match(title, ch_slug, start_ts):
    """Core matcher used by both the lookup endpoint and the autorec
    follow-up loop. Returns the best match dict (same shape as the
    endpoint) or None. Title + channel slug + broadcast timestamp in,
    match-with-HLS-capable-id out."""
    if not title or not start_ts:
        return None
    if ch_slug in ARD_SEARCH_CHANNELS:
        source = "ard"
    elif ch_slug in ZDF_SEARCH_CHANNELS:
        source = "zdf"
    else:
        return None
    from datetime import datetime
    MAX_DELTA = 48 * 3600
    title_l = title.lower()
    best = None
    if source == "ard":
        qs = urllib.parse.urlencode({
            "searchString": title,
            "searchResultsPageSize": "24",
        })
        search_url = (f"https://api.ardmediathek.de/page-gateway/pages/ard/"
                      f"search?{qs}")
        data = None
        for attempt in range(2):
            try:
                data = json.loads(urllib.request.urlopen(
                    search_url, timeout=15).read())
                break
            except Exception as e:
                print(f"mediathek search (try {attempt+1}): {e}", flush=True)
        if data is None:
            return None
        for v in data.get("vodResults", []):
            svc = (v.get("publicationService") or {}).get("name", "")
            vt = (v.get("longTitle") or v.get("mediumTitle") or "").strip()
            vt_l = vt.lower()
            if title_l not in vt_l and vt_l not in title_l:
                continue
            bt = 0
            if v.get("broadcastedOn"):
                try:
                    bt = int(datetime.fromisoformat(
                        v["broadcastedOn"].replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass
            if not bt or abs(bt - start_ts) > MAX_DELTA:
                continue
            delta = abs(bt - start_ts)
            if best is None or delta < best["_delta"]:
                avail_to = 0
                if v.get("availableTo"):
                    try:
                        avail_to = int(datetime.fromisoformat(
                            v["availableTo"].replace("Z", "+00:00")).timestamp())
                    except Exception:
                        pass
                best = {
                    "_delta": delta, "source": "ard",
                    "title": vt, "channel": svc,
                    "broadcast": bt, "available_to": avail_to,
                    "duration": v.get("duration", 0),
                    "id": v.get("id"),
                    "player_url": f"https://www.ardmediathek.de/video/{v.get('id')}",
                }
    else:  # zdf
        for r in _zdf_search(title):
            rt = (r.get("title") or "").lower()
            if title_l not in rt and rt not in title_l:
                continue
            if not r["broadcast_ts"]:
                continue
            delta = abs(r["broadcast_ts"] - start_ts)
            if delta > MAX_DELTA:
                continue
            if best is None or delta < best["_delta"]:
                best = {
                    "_delta": delta, "source": "zdf",
                    "title": r["title"], "channel": "ZDF",
                    "broadcast": r["broadcast_ts"], "available_to": 0,
                    "duration": r["duration"],
                    "id": r["id"],
                    "player_url": f"https://www.zdf.de/play/{r['id']}",
                }
    if not best:
        return None
    if best["source"] == "zdf" and not best["available_to"]:
        _, vis_to = _zdf_resolve_hls(best["id"])
        if vis_to:
            best["available_to"] = vis_to
    best.pop("_delta", None)
    return best


@app.route("/api/mediathek-lookup/<event_id>")
def api_mediathek_lookup(event_id):
    """For an EPG event, try to find the same show in the ARD
    Mediathek and return its availability window + player URL."""
    title, ch_name, start_ts = "", "", 0
    # Synthetic archive key: arc_<slug>_<start> for past events that
    # were archived before we started persisting the tvheadend eid.
    if event_id.startswith("arc_"):
        try:
            _, slug_, start_s = event_id.split("_", 2)
            start_ts = int(start_s)
            with _epg_archive_lock:
                rec = _epg_archive.get((slug_, start_ts)) or {}
            title = rec.get("title", "")
            with cmap_lock:
                ch_name = channel_map.get(slug_, {}).get("name", slug_)
        except Exception:
            pass
    else:
        try:
            ev = json.loads(urllib.request.urlopen(
                f"{TVH_BASE}/api/epg/events/load?eventId={event_id}",
                timeout=6).read())
            entry = (ev.get("entries") or [{}])[0]
            title = (entry.get("title") or "").strip()
            ch_name = entry.get("channelName") or ""
            start_ts = entry.get("start", 0)
        except Exception:
            pass
        if not title:
            try:
                with _epg_archive_lock:
                    for (slug_, start), rec in _epg_archive.items():
                        if str(rec.get("event_id")) == str(event_id):
                            title = rec.get("title", "")
                            start_ts = start
                            with cmap_lock:
                                ch_name = (channel_map.get(slug_, {})
                                            .get("name", slug_))
                            break
            except Exception:
                pass
    if not title:
        return _cors(Response(json.dumps({"match": None}),
                               mimetype="application/json"))
    slug = slugify(ch_name)
    if slug not in ARD_SEARCH_CHANNELS and slug not in ZDF_SEARCH_CHANNELS:
        return _cors(Response(json.dumps({
            "match": None, "reason": "channel not covered"}),
            mimetype="application/json"))
    match = _mediathek_match(title, slug, start_ts)
    return _cors(Response(json.dumps({"match": match}),
                           mimetype="application/json"))




@app.route("/api/mediathek-schedule/<event_id>", methods=["POST"])
def api_mediathek_schedule(event_id):
    """Store a virtual recording that streams from ARD Mediathek on
    playback. Uses the lookup we already have, resolves the item's
    HLS master URL, and saves a local stub so /recordings + the
    recording player pick it up."""
    ev_title, ch_name, ev_start, ev_stop = "", "", 0, 0
    if event_id.startswith("arc_"):
        try:
            _, slug_, start_s = event_id.split("_", 2)
            ev_start = int(start_s)
            with _epg_archive_lock:
                rec = _epg_archive.get((slug_, ev_start)) or {}
            ev_title = rec.get("title", "")
            ev_stop = rec.get("stop", 0)
            with cmap_lock:
                ch_name = channel_map.get(slug_, {}).get("name", slug_)
        except Exception:
            pass
    else:
        try:
            ev = json.loads(urllib.request.urlopen(
                f"{TVH_BASE}/api/epg/events/load?eventId={event_id}",
                timeout=6).read())
            entry = (ev.get("entries") or [{}])[0]
            ev_title = (entry.get("title") or "").strip()
            ch_name = entry.get("channelName") or ""
            ev_start = entry.get("start", 0)
            ev_stop = entry.get("stop", 0)
        except Exception:
            pass
        if not ev_title:
            with _epg_archive_lock:
                for (slug_, start), rec in _epg_archive.items():
                    if str(rec.get("event_id")) == str(event_id):
                        ev_title = rec.get("title", "")
                        ev_start = start
                        ev_stop = rec.get("stop", 0)
                        with cmap_lock:
                            ch_name = (channel_map.get(slug_, {})
                                        .get("name", slug_))
                        break
    if not ev_title:
        return Response(json.dumps({"ok": False,
                                     "error": "event not found"}),
                        status=404, mimetype="application/json")
    # Re-run the lookup. Loopback HTTP is simpler than refactoring
    # the matcher into a shared helper for one more caller.
    try:
        res = urllib.request.urlopen(
            f"http://localhost:8080/api/mediathek-lookup/{event_id}",
            timeout=8).read()
        match = json.loads(res).get("match")
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": f"match: {e}"}),
                        status=500, mimetype="application/json")
    if not match or not match.get("id"):
        return Response(json.dumps({"ok": False,
                                     "error": "no mediathek match"}),
                        status=404, mimetype="application/json")
    hls_url = _resolve_mediathek_hls(match["id"],
                                       source=match.get("source", "ard"))
    if not hls_url:
        return Response(json.dumps({"ok": False,
                                     "error": "no hls url"}),
                        status=500, mimetype="application/json")
    import uuid as uuid_mod
    vuuid = "mt_" + uuid_mod.uuid4().hex[:16]
    with _mediathek_rec_lock:
        _mediathek_rec[vuuid] = {
            "title": match.get("title") or ev_title,
            "channel": ch_name,
            "start": ev_start,
            "stop": ev_stop,
            "hls_url": hls_url,
            "available_to": match.get("available_to", 0),
            "eid": event_id,
            "created_at": int(time.time()),
        }
    save_mediathek_rec()
    return _cors(Response(json.dumps({
        "ok": True, "uuid": vuuid,
        "title": _mediathek_rec[vuuid]["title"],
        "available_to": _mediathek_rec[vuuid]["available_to"],
    }), mimetype="application/json"))


@app.route("/record-series/<event_id>")
def record_series(event_id):
    """Create a tvheadend autorec rule to capture every future airing of
    this programme on the same channel. Title-regex based because
    German DVB-C doesn't ship series-link CRIDs."""
    # Resolve event → title + channel
    try:
        ev = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/epg/events/load?eventId={event_id}",
            timeout=6).read())
        entry = (ev.get("entries") or [{}])[0]
        title = entry.get("title")
        ch_uuid = entry.get("channelUuid")
        ch_name = entry.get("channelName") or ""
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": f"lookup: {e}"}),
                        status=500, mimetype="application/json")
    if not title or not ch_uuid:
        return Response(json.dumps({"ok": False,
                                     "error": "event not found"}),
                        status=404, mimetype="application/json")
    # Anchor the regex so "Tagesschau" doesn't grab "Tagesschau um 5".
    title_regex = "^" + re.escape(title) + "$"
    conf = {
        "enabled": True,
        "name": f"{title} ({ch_name})",
        "title": title_regex,
        "fulltext": False,
        "channel": ch_uuid,
        "comment": f"auto via /record-series for eid={event_id}",
    }
    # Lock to the seed event's time-of-day so a midday rerun on the
    # same channel doesn't get picked up alongside the prime-time
    # original. tvheadend autorec uses HH:MM strings and treats
    # start_window as the upper bound of the acceptable start time
    # (not a duration). Bracket the seed by -5/+15 min — drift seen
    # on kabel eins is typically forward (slot fills with promos) but
    # can be backward by 1-2 min if a preceding programme finishes
    # early. 20-min total window stays well clear of any rerun slot.
    seed_start = entry.get("start")
    if seed_start:
        lt = time.localtime(seed_start)
        seed_min = lt.tm_hour * 60 + lt.tm_min
        start_min = (seed_min - 5) % (24 * 60)
        end_min = (seed_min + 15) % (24 * 60)
        conf["start"] = f"{start_min // 60:02d}:{start_min % 60:02d}"
        conf["start_window"] = f"{end_min // 60:02d}:{end_min % 60:02d}"
    body = urllib.parse.urlencode({"conf": json.dumps(conf)}).encode()
    try:
        req = urllib.request.Request(
            f"{TVH_BASE}/api/dvr/autorec/create",
            data=body, method="POST")
        res = urllib.request.urlopen(req, timeout=10).read().decode()
        data = json.loads(res) if res else {}
        autorec_uuid = data.get("uuid")
        # tvheadend schedules matching EPG events asynchronously. Give
        # it ~2 s then count how many upcoming DVR entries the rule has
        # spawned so the client can show "N Folgen geplant".
        scheduled = 0
        try:
            time.sleep(2)
            up = json.loads(urllib.request.urlopen(
                f"{TVH_BASE}/api/dvr/entry/grid_upcoming?limit=500",
                timeout=10).read())
            scheduled = sum(1 for e in up.get("entries", [])
                            if e.get("autorec") == autorec_uuid)
        except Exception:
            pass
        return _cors(Response(json.dumps({"ok": True,
                                           "uuid": autorec_uuid,
                                           "title": title,
                                           "channel": ch_name,
                                           "scheduled": scheduled}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


@app.route("/cancel-series/<autorec_uuid>")
def cancel_series(autorec_uuid):
    """Delete an autorec rule and cancel every upcoming DVR entry it
    has spawned. Already-recorded episodes on disk are left alone —
    the user is explicitly only tearing down the "record future
    episodes" automation, not their archive."""
    cancelled = 0
    try:
        up = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid_upcoming?limit=500",
            timeout=10).read())
        for e in up.get("entries", []):
            if e.get("autorec") != autorec_uuid:
                continue
            ep_uuid = e.get("uuid")
            if not ep_uuid:
                continue
            body = urllib.parse.urlencode({"uuid": ep_uuid}).encode()
            for ep in ("/api/dvr/entry/cancel",
                       "/api/dvr/entry/remove"):
                try:
                    urllib.request.urlopen(
                        urllib.request.Request(
                            f"{TVH_BASE}{ep}",
                            data=body, method="POST"),
                        timeout=5).read()
                    cancelled += 1
                    break
                except Exception:
                    continue
    except Exception:
        pass
    try:
        body = urllib.parse.urlencode({"uuid": autorec_uuid}).encode()
        urllib.request.urlopen(
            urllib.request.Request(f"{TVH_BASE}/api/idnode/delete",
                                    data=body, method="POST"),
            timeout=10).read()
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    return _cors(Response(json.dumps({"ok": True,
                                        "cancelled": cancelled}),
                           mimetype="application/json"))


# --- EPG metadata enrichment via TVmaze (free, no API key) -----
_epg_meta = {}
_epg_meta_lock = threading.Lock()


def _load_epg_meta():
    global _epg_meta
    if not EPG_META_FILE.exists():
        return
    try:
        _epg_meta = json.loads(EPG_META_FILE.read_text())
        print(f"Loaded EPG metadata for {len(_epg_meta)} titles", flush=True)
    except Exception as e:
        print(f"load epg meta: {e}", flush=True)


def _save_epg_meta():
    try:
        tmp = EPG_META_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_epg_meta))
        tmp.replace(EPG_META_FILE)
    except Exception as e:
        print(f"save epg meta: {e}", flush=True)


def _normalize_title(t):
    """Strip episode-suffix variants so 'Show - Episode Title' looks up
    the parent show. TVmaze indexes shows, not episodes."""
    if not t:
        return ""
    # Common DE-EPG pattern: "Show - Episode Title" or "Show: Subtitle"
    for sep in (" - ", ": "):
        if sep in t:
            return t.split(sep, 1)[0].strip()
    return t.strip()


def _fetch_tmdb(title):
    """Query TMDB /search/tv. Accepts both v4 bearer tokens (JWT,
    starts with "eyJ") and v3 api_key values (32-char hex). TMDB has
    much richer DE-TV coverage than TVmaze (Galileo, Geissens, Das
    perfekte Dinner all indexed with posters). Returns None on miss.
    TMDB reports vote_average=0.0 when a show has no user ratings —
    treat that as "no rating" rather than a literal zero."""
    try:
        params = {"query": title, "language": "de-DE"}
        headers = {"Accept": "application/json"}
        if TMDB_API_KEY.startswith("eyJ"):
            headers["Authorization"] = f"Bearer {TMDB_API_KEY}"
        else:
            params["api_key"] = TMDB_API_KEY
        url = ("https://api.themoviedb.org/3/search/tv?"
               + urllib.parse.urlencode(params))
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        results = d.get("results") or []
        if not results:
            return None
        # Prefer an exact-or-prefix title match over TMDB's default
        # popularity ordering — "Das perfekte Dinner" should win over
        # spin-offs like "Das perfekte Promi-Dinner".
        lc = title.lower()
        best = next(
            (r for r in results if (r.get("name") or "").lower() == lc),
            None) or next(
            (r for r in results if (r.get("name") or "").lower().startswith(lc)),
            results[0])
        poster = best.get("poster_path")
        rating = best.get("vote_average") or 0.0
        return {
            "poster": (f"https://image.tmdb.org/t/p/w185{poster}"
                         if poster else None),
            "rating": round(rating, 1) if rating > 0 else None,
            "tmdb_id": best.get("id"),
        }
    except Exception as e:
        print(f"[epg-enrich tmdb] {title}: {e}", flush=True)
        return None


def _fetch_tvmaze(title):
    """Fallback enricher when TMDB isn't configured or misses. TVmaze
    skews US/UK so most DE reality/cooking shows miss."""
    try:
        url = ("https://api.tvmaze.com/singlesearch/shows?q="
               + urllib.parse.quote(title))
        with urllib.request.urlopen(url, timeout=8) as r:
            d = json.loads(r.read())
        img = d.get("image") or {}
        return {
            "poster": img.get("medium") or img.get("original"),
            "rating": (d.get("rating") or {}).get("average"),
            "tvmaze_id": d.get("id"),
        }
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"[epg-enrich tvmaze] {title}: {e}", flush=True)
        return None
    except Exception as e:
        print(f"[epg-enrich tvmaze] {title}: {e}", flush=True)
        return None


def _fetch_show_meta(title):
    """Try TMDB first when an API key is configured, fall back to
    TVmaze on miss. Either way return a dict stamped with fetched_at
    so we cache the negative result too and don't retry hourly."""
    out = {"fetched_at": int(time.time())}
    if TMDB_API_KEY:
        meta = _fetch_tmdb(title)
        if meta and (meta.get("poster") or meta.get("rating") is not None):
            out.update(meta)
            return out
    meta = _fetch_tvmaze(title)
    if meta:
        out.update(meta)
    return out


def _enrich_recordings_loop():
    """Hourly: scan recordings list, fetch TVmaze metadata for any
    new (or stale) title. Rate-limited to ~2 lookups/sec to be polite
    to TVmaze's free public API."""
    time.sleep(60)
    while True:
        try:
            data = json.loads(urllib.request.urlopen(
                f"{TVH_BASE}/api/dvr/entry/grid?limit=500",
                timeout=10).read())
            now = time.time()
            seen = set()
            for e in data.get("entries", []):
                key = _normalize_title(e.get("disp_title", ""))
                if not key or key in seen:
                    continue
                seen.add(key)
                with _epg_meta_lock:
                    cached = _epg_meta.get(key)
                if cached and now - cached.get("fetched_at", 0) < EPG_META_TTL_S:
                    continue
                meta = _fetch_show_meta(key)
                with _epg_meta_lock:
                    _epg_meta[key] = meta
                _save_epg_meta()
                time.sleep(0.5)  # gentle on upstream APIs
        except Exception as e:
            print(f"[epg-enrich] loop: {e}", flush=True)
        time.sleep(3600)


@app.route("/api/recording/<uuid>/watched", methods=["POST"])
def api_recording_watched(uuid):
    """Toggle the watched flag on a recording. tvheadend's `watched`
    field is read-only/computed — set the user-settable `playcount`
    instead. The auto-cleanup loop uses playcount>0 to decide what's
    eligible for deletion after WATCHED_AUTO_DELETE_DAYS days."""
    try:
        body = request.get_json(silent=True) or {}
        watched = bool(body.get("watched", True))
    except Exception:
        watched = True
    payload = urllib.parse.urlencode({
        "node": json.dumps({"uuid": uuid,
                              "playcount": 1 if watched else 0})
    }).encode()
    try:
        req = urllib.request.Request(f"{TVH_BASE}/api/idnode/save",
                                      data=payload, method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        return _cors(Response(json.dumps({"ok": True, "watched": watched}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


def _cleanup_watched_loop():
    """Delete recordings that have been marked watched (playcount>0)
    for longer than WATCHED_AUTO_DELETE_DAYS days. Runs every 6 h.
    Only touches `Completed` entries — never deletes something still
    being recorded or remuxed."""
    time.sleep(120)  # let other startup work settle
    while True:
        try:
            cutoff = time.time() - WATCHED_AUTO_DELETE_DAYS * 86400
            d = json.loads(urllib.request.urlopen(
                f"{TVH_BASE}/api/dvr/entry/grid?limit=1000",
                timeout=20).read())
            deleted = 0
            for e in d.get("entries", []):
                if (e.get("playcount", 0) > 0
                        and "Completed" in (e.get("status") or "")
                        and (e.get("stop") or 0) < cutoff):
                    try:
                        body = urllib.parse.urlencode({"uuid": e["uuid"]}).encode()
                        urllib.request.urlopen(urllib.request.Request(
                            f"{TVH_BASE}/api/dvr/entry/remove",
                            data=body, method="POST"),
                            timeout=10).read()
                        deleted += 1
                        print(f"[watched-cleanup] removed "
                              f"{e.get('disp_title','?')[:40]} "
                              f"(stopped {time.strftime('%Y-%m-%d', time.localtime(e.get('stop', 0)))})",
                              flush=True)
                    except Exception as ex:
                        print(f"[watched-cleanup] remove {e['uuid']}: {ex}",
                              flush=True)
            if deleted:
                print(f"[watched-cleanup] {deleted} recording(s) deleted",
                      flush=True)
        except Exception as ex:
            print(f"[watched-cleanup] loop: {ex}", flush=True)
        time.sleep(6 * 3600)


@app.route("/cancel-recording/<uuid>")
def cancel_recording(uuid):
    """Cancel a scheduled (or running) DVR entry, JSON response."""
    body = urllib.parse.urlencode({"uuid": uuid}).encode()
    last_err = None
    for ep in ("/api/dvr/entry/cancel", "/api/dvr/entry/remove"):
        try:
            req = urllib.request.Request(f"{TVH_BASE}{ep}",
                                          data=body, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
            return _cors(Response(json.dumps({"ok": True}),
                                   mimetype="application/json"))
        except Exception as e:
            last_err = str(e)
    return Response(json.dumps({"ok": False, "error": last_err}),
                    status=500, mimetype="application/json")


@app.route("/api/is-recording/<slug>")
def api_is_recording(slug):
    """Does this channel currently have an active or upcoming recording?"""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"active": False}),
                               mimetype="application/json"))
    ch_uuid = info["uuid"]
    now_ts = int(time.time())
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid_upcoming?limit=200",
            timeout=6).read())
    except Exception:
        return _cors(Response(json.dumps({"active": False}),
                               mimetype="application/json"))
    active = False
    title = None
    stop = 0
    for e in data.get("entries", []):
        if e.get("channel") != ch_uuid:
            continue
        s = e.get("start", 0); st = e.get("stop", 0)
        if s <= now_ts < st:
            active = True
            title = e.get("disp_title", "")
            stop = st
            break
    return _cors(Response(json.dumps({"active": active, "title": title,
                                        "stop": stop}),
                           mimetype="application/json"))


@app.route("/recordings")
def recordings_page():
    """List of recordings (ongoing + finished) with links. Entries
    spawned by the same autorec rule are collapsed into a series
    group with an episode count."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=200&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")

    now_ts = int(time.time())
    # Bucket entries by autorec UUID so we can render a collapsible
    # "series" group row + its children. Solo entries (no autorec)
    # stay on their own.
    by_autorec = {}
    for e in data.get("entries", []):
        ar = e.get("autorec") or ""
        by_autorec.setdefault(ar, []).append(e)

    def _render_row(e, in_series=False):
        uuid = e.get("uuid", "")
        title = e.get("disp_title", "?")
        start = e.get("start", 0)
        stop  = e.get("stop", 0)
        status = e.get("status", "")
        sched = e.get("sched_status", "")
        size = e.get("filesize", 0) or 0
        is_live = start <= now_ts < stop and sched == "recording"
        is_done = now_ts >= stop or "Completed" in status
        when = time.strftime("%d.%m %H:%M", time.localtime(start))
        dur_min = max(0, (stop - start) // 60)
        play_url = f"{HOST_URL}/recording/{uuid}"
        ch_icon = e.get("channel_icon") or ""
        ch_name = (e.get("channelname") or "").replace('"', "&quot;")
        ch_slug = slugify(e.get("channelname") or "")
        ch_logo_src = _channel_logo_url(ch_slug, ch_icon)
        ch_logo_html = (
            f'<img class="ch-logo" src="{ch_logo_src}" '
            f'alt="{ch_name}" title="{ch_name}" loading="lazy">'
            if ch_logo_src else ''
        )
        if is_live or is_done:
            title_cell = f'<a href="{play_url}">{title}</a>'
        else:
            title_cell = f'<span>{title}</span>'
        if is_done:
            out_dir = HLS_DIR / f"_rec_{uuid}"
            playlist = out_dir / "index.m3u8"
            scanning_lock = out_dir / ".scanning"
            # When the Mac handler is doing work, _rec_hls_procs and
            # _rec_cskip_procs are empty. Infer Mac activity from the
            # cooperative .scanning lock-file on the SMB share — fresh
            # lock + no Pi-local proc means the Mac is mid-work on
            # this recording.
            try:
                mac_active = (scanning_lock.exists() and
                              time.time() - scanning_lock.stat().st_mtime < 900)
            except Exception:
                mac_active = False
            try:
                has_endlist = (playlist.exists() and
                               "#EXT-X-ENDLIST" in playlist.read_text(errors="ignore"))
            except Exception:
                has_endlist = False
            has_txt = any(f.stat().st_size > 0
                          for f in out_dir.glob("*.txt")
                          if f.is_file())

            proc_info = _rec_hls_procs.get(uuid)
            pi_remux_running = (proc_info is not None and
                                proc_info["proc"].poll() is None)
            cskip_info = _rec_cskip_procs.get(uuid)
            pi_cskip_running = (cskip_info and
                                cskip_info["proc"].poll() is None)
            mac_remuxing = (mac_active and not pi_remux_running
                            and not has_endlist)
            mac_scanning = (mac_active and not pi_cskip_running
                            and has_endlist and not has_txt)

            if playlist.exists() and not pi_remux_running and not mac_remuxing:
                # Ready to stream → play button.
                status_cell = (f'<a class="badge play-btn" href="{play_url}" '
                               f'title="Abspielen">▶ abspielen</a>')
            elif pi_remux_running:
                total = (proc_info or {}).get("total_segs", 0)
                segs = 0
                if playlist.exists():
                    try: segs = playlist.read_text().count(".ts")
                    except Exception: pass
                pct = (int(segs * 100 / total) if total > 0 else 0)
                status_cell = (f'<span class="badge warming" '
                               f'title="Remux läuft">⏳ {pct}%</span>')
            elif mac_remuxing:
                status_cell = ('<span class="badge warming" '
                               'title="Mac remuxt">⏳ Mac</span>')
            else:
                status_cell = ('<span class="badge pending" '
                               'title="noch nicht remuxt">◌ ausstehend</span>')
            if pi_cskip_running:
                status_cell += (' <span class="badge scanning" '
                                'title="comskip analysiert Werbeblöcke">🔍</span>')
            elif mac_scanning:
                status_cell += (' <span class="badge scanning" '
                                'title="Mac comskip analysiert Werbeblöcke">🔍</span>')
        elif is_live:
            status_cell = (f'<a class="badge live" href="{play_url}" '
                           f'title="Live ansehen">● live</a>')
        else:
            status_cell = ('<span class="badge scheduled" '
                           'title="Sendung wird zur geplanten Zeit aufgenommen">⏱ geplant</span>')
        # Watched-button only makes sense for completed recordings —
        # marking a scheduled (not-yet-recorded) episode as "watched"
        # is nonsensical and the green-check next to a "—" size cell
        # confused users who didn't know what it referred to.
        watched = (e.get("playcount") or 0) > 0
        if is_done:
            watch_cell = (
                f'<a class="watch-btn{" on" if watched else ""}" '
                f'href="#" data-uuid="{uuid}" '
                f'data-watched="{1 if watched else 0}" '
                f'title="Als {"un" if watched else ""}gesehen markieren '
                f'(wird {WATCHED_AUTO_DELETE_DAYS} d nach Markierung '
                f'auto-gelöscht)">'
                f'{"✓" if watched else "○"}</a>'
            )
        else:
            watch_cell = ''
        # TVmaze enrichment: poster + rating attached to the series-
        # head row (rendered separately below). Skip on individual
        # episode rows inside a series — duplicating the same poster
        # 5× per series turns the table into visual mush. Solo
        # recordings (no autorec) keep them inline.
        if not in_series:
            with _epg_meta_lock:
                meta = _epg_meta.get(_normalize_title(title)) or {}
            poster_html = (f'<img class="rec-poster" src="{meta["poster"]}" '
                           f'alt="" loading="lazy">'
                           if meta.get("poster") else '')
            rating_html = (f' <span class="rec-rating" title="TVmaze rating">'
                           f'★ {meta["rating"]}</span>'
                           if meta.get("rating") else '')
            title_cell = ch_logo_html + poster_html + title_cell + rating_html
        del_btn = (f'<a class="del-btn" href="{HOST_URL}/recording/{uuid}/delete" '
                   f'data-title="{title.replace(chr(34),"&quot;")}" '
                   f'data-when="{when}">🗑</a>')
        tools_cell = f'<td class="row-tools">{watch_cell}{del_btn}</td>'
        if in_series:
            # Inside a series group the title is redundant (already in
            # the purple group header). Status badge doubles as the
            # play button when the recording is ready.
            return (f'<tr><td>{status_cell}</td>'
                    f'<td>{when}</td>'
                    f'<td>{dur_min} min</td>'
                    f'{tools_cell}</tr>')
        return (f'<tr><td>{status_cell}</td>'
                f'<td>{title_cell}</td>'
                f'<td>{when}</td>'
                f'<td>{dur_min} min</td>'
                f'{tools_cell}</tr>')

    rows = []
    # Solo recordings (no autorec) — render flat.
    for e in by_autorec.get("", []):
        rows.append(_render_row(e))
    # Series groups — render as <details> with a summary row and the
    # episodes inside. Sorted by the most recent episode in the group.
    series_groups = [(ar, eps) for ar, eps in by_autorec.items() if ar]
    series_groups.sort(
        key=lambda kv: max((e.get("start", 0) for e in kv[1]), default=0),
        reverse=True)
    for ar_uuid, eps in series_groups:
        group_title = eps[0].get("disp_title", "?")
        live = sum(1 for e in eps
                   if e.get("start", 0) <= now_ts < e.get("stop", 0)
                   and e.get("sched_status") == "recording")
        upcoming = sum(1 for e in eps
                       if e.get("start", 0) > now_ts and "Completed" not in e.get("status", ""))
        done = sum(1 for e in eps if now_ts >= e.get("stop", 0) or "Completed" in e.get("status", ""))
        parts = [f"{done} aufgen."]
        if live:
            parts.append(f'<span class="live-count">● {live} läuft</span>')
        parts.append(f"{upcoming} geplant")
        badge = (f'<span class="badge series">📺 Serie · '
                 f'{" · ".join(parts)}</span>')
        kill_btn = (f'<button class="series-kill" '
                    f'onclick="cancelSeries(event,\'{ar_uuid}\',\'{group_title}\',{upcoming})" '
                    f'title="Serien-Aufzeichnungs-Regel löschen">'
                    f'🗑</button>')
        # Keep group expanded if any episode needs attention (recording
        # right now, being remuxed, or comskip is running on it).
        any_active = bool(live) or any(
            e.get("uuid") in _rec_hls_procs or e.get("uuid") in _rec_cskip_procs
            for e in eps)
        open_attr = " open" if any_active else ""
        with _epg_meta_lock:
            meta = _epg_meta.get(_normalize_title(group_title)) or {}
        poster_html = (f'<img class="rec-poster" src="{meta["poster"]}" '
                       f'alt="" loading="lazy">'
                       if meta.get("poster") else '')
        rating_html = (f' <span class="rec-rating" title="TVmaze rating">'
                       f'★ {meta["rating"]}</span>'
                       if meta.get("rating") else '')
        # Channel logo from the first episode (autorec is channel-locked
        # so all episodes share it).
        first_ep = eps[0] if eps else {}
        ep_ch_icon = first_ep.get("channel_icon") or ""
        ep_ch_name = (first_ep.get("channelname") or "").replace('"', "&quot;")
        ep_ch_slug = slugify(first_ep.get("channelname") or "")
        ep_ch_logo_src = _channel_logo_url(ep_ch_slug, ep_ch_icon)
        ch_logo_html = (
            f'<img class="ch-logo" src="{ep_ch_logo_src}" '
            f'alt="{ep_ch_name}" title="{ep_ch_name}" loading="lazy">'
            if ep_ch_logo_src else ''
        )
        summary = (f'<tr class="series-head"><td colspan="5">'
                   f'<details data-ar="{ar_uuid}"{open_attr}>'
                   f'<summary>{ch_logo_html}{poster_html}{badge}'
                   f'<span class="series-title">{group_title}'
                   f' <small>({len(eps)})</small></span>'
                   f'{rating_html}{kill_btn}</summary>'
                   f'<table class="series-sub"><tbody>'
                   + "".join(_render_row(e, in_series=True) for e in sorted(
                       eps, key=lambda x: x.get("start", 0)))
                   + '</tbody></table></details></td></tr>')
        rows.append(summary)

    # Virtual Mediathek recordings — stream via hls.js on play. Sorted
    # newest-first, with an expiry reminder.
    with _mediathek_rec_lock:
        mt_entries = [(k, v) for k, v in _mediathek_rec.items()]
    mt_entries.sort(key=lambda kv: kv[1].get("created_at", 0), reverse=True)
    from datetime import datetime as _dt
    for vuuid, m in mt_entries:
        mt_title = m.get("title", "?").replace("<", "&lt;")
        when = time.strftime("%d.%m %H:%M",
                              time.localtime(m.get("start", 0)))
        dur_min = max(0, (m.get("stop", 0) - m.get("start", 0)) // 60)
        avail_to = m.get("available_to", 0)
        avail_str = ""
        if avail_to:
            try:
                avail_str = _dt.fromtimestamp(avail_to).strftime("%d.%m.%Y")
            except Exception:
                pass
        expired = avail_to and now_ts > avail_to
        # Ripped MP4 takes over once V3's background worker finishes —
        # playable forever after that, no remote HLS dependency.
        ripped_path = m.get("ripped_path", "")
        has_local = (ripped_path and
                     (HLS_DIR / f"_{vuuid}" / "file.mp4").exists())
        if has_local:
            mt_badge = '<span class="badge mediathek local">💾 Mediathek lokal</span>'
        elif expired:
            mt_badge = '<span class="badge expired">✗ abgelaufen</span>'
        else:
            mt_badge = '<span class="badge mediathek">📡 Mediathek</span>'
        if has_local:
            size_mb = (m.get("ripped_bytes", 0) or 0) / (1024 * 1024)
            avail_cell = f'<small>lokal ({size_mb:.0f} MB)</small>'
        elif avail_str and not expired:
            avail_cell = (f'<small style="color:var(--muted)">bis '
                          f'{avail_str}</small>')
        elif expired:
            avail_cell = '<small>abgelaufen</small>'
        else:
            avail_cell = ''
        if expired and not has_local:
            title_cell = f'<span>{mt_title}</span>'
        else:
            title_cell = f'<a href="{HOST_URL}/recording/{vuuid}">{mt_title}</a>'
        if avail_cell:
            title_cell += f'<br>{avail_cell}'
        rows.append(
            f'<tr><td>{mt_badge}</td>'
            f'<td>{title_cell}</td>'
            f'<td>{when}</td>'
            f'<td>{dur_min} min</td>'
            f'<td class="row-tools">'
            f'<a class="del-btn" href="{HOST_URL}/mediathek-rec/{vuuid}/delete" '
            f'data-title="{mt_title.replace(chr(34),"&quot;")}" '
            f'data-mt="1">🗑</a></td></tr>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f"body{{max-width:none;margin:0;padding:0}}"
            f".rec-header{{display:flex;align-items:center;gap:8px 14px;"
            f"padding:6px 10px;border-bottom:1px solid var(--border);"
            f"background:var(--bg);font-size:.9em}}"
            f".rec-header h1{{margin:0;font-size:1.1em;font-weight:600}}"
            # Bigger tap target on touch — same pattern as EPG header.
            f".home-link{{display:inline-flex;align-items:center;gap:8px;"
            f"padding:8px 12px;margin:-6px -8px;border-radius:6px;"
            f"text-decoration:none;color:inherit;-webkit-tap-highlight-color:transparent}}"
            f".home-link .arrow{{font-size:1.4em;line-height:1}}"
            f"@media (hover:hover){{.home-link:hover{{background:var(--stripe)}}}}"
            f".rec-body{{padding:0 10px}}"
            f".badge{{font-size:.75em;padding:2px 6px;border-radius:3px;"
            f"font-weight:600;display:inline-block}}"
            f".badge.live{{background:#e74c3c;color:#fff}}"
            f".badge.done{{background:#27ae60;color:#fff}}"
            f".badge.scheduled{{background:var(--stripe);color:var(--muted)}}"
            f".badge.ready{{background:#2980b9;color:#fff}}"
            f".badge.warming{{background:#f39c12;color:#fff}}"
            f".badge.pending{{background:var(--stripe);color:var(--muted)}}"
            f".badge.scanning{{background:#8e44ad;color:#fff;"
            f"animation:scanpulse 1.2s ease-in-out infinite}}"
            f"@keyframes scanpulse{{0%,100%{{opacity:.5}}50%{{opacity:1}}}}"
            f".badge.live{{animation:livepulse 1.5s ease-in-out infinite}}"
            f"@keyframes livepulse{{0%,100%{{opacity:1}}50%{{opacity:.6}}}}"
            f".live-count{{color:#ffeb3b;"
            f"animation:livepulse 1.5s ease-in-out infinite}}"
            f".del-modal{{position:fixed;inset:0;background:#000a;"
            f"display:flex;align-items:center;justify-content:center;z-index:100}}"
            f".del-dialog{{background:var(--bg);color:var(--fg);"
            f"padding:20px 22px;border-radius:12px;max-width:420px;"
            f"width:calc(100% - 40px);border:1px solid var(--border);"
            f"box-shadow:0 6px 20px #0008}}"
            f".del-head{{font-weight:700;font-size:1.05em;margin-bottom:10px}}"
            f".del-title{{font-weight:600;margin-bottom:6px;word-break:break-word}}"
            f".del-sub{{font-size:.85em;color:var(--muted);margin-bottom:16px}}"
            f".del-btns{{display:flex;gap:10px;justify-content:flex-end}}"
            f".del-btns button{{padding:8px 14px;border-radius:6px;"
            f"border:1px solid var(--border);font-weight:600;cursor:pointer;"
            f"font-size:.95em;background:var(--stripe);color:var(--fg)}}"
            f".del-confirm{{background:#c0392b !important;color:#fff !important;"
            f"border-color:#c0392b !important}}"
            f".del-cancel{{background:transparent !important;font-weight:400 !important}}"
            f".watch-btn{{display:inline-block;width:24px;height:24px;line-height:22px;"
            f"text-align:center;border-radius:50%;border:1px solid var(--border);"
            f"color:var(--muted);text-decoration:none;font-size:.95em;cursor:pointer}}"
            f".watch-btn.on{{background:#27ae60;color:#fff;border-color:#27ae60}}"
            f".rec-poster{{width:24px;height:34px;object-fit:cover;border-radius:3px;"
            f"vertical-align:middle;margin-right:6px;background:var(--stripe);"
            f"flex-shrink:0}}"
            f".ch-logo{{width:44px;height:32px;object-fit:contain;border-radius:3px;"
            f"vertical-align:middle;margin-right:8px;flex-shrink:0;"
            f"background:#fff;padding:2px;box-sizing:border-box}}"
            f".rec-rating{{display:inline-block;background:#f39c12;color:#000;"
            f"padding:1px 6px;border-radius:3px;font-size:.8em;font-weight:600;"
            f"margin-left:6px;vertical-align:middle;flex-shrink:0}}"
            f".row-tools{{text-align:right;white-space:nowrap;width:1%}}"
            f".row-tools .watch-btn{{margin-right:6px}}"
            f".badge.series{{background:#8e44ad;color:#fff;flex-shrink:0}}"
            f"a.badge.play-btn{{background:#2980b9;color:#fff;text-decoration:none;"
            f"font-weight:600;cursor:pointer}}"
            f"a.badge.live{{background:#e74c3c;color:#fff;text-decoration:none;"
            f"font-weight:600;cursor:pointer}}"
            f".badge.mediathek{{background:#2980b9;color:#fff}}"
            f".badge.mediathek.local{{background:#27ae60}}"
            f".badge.expired{{background:#7f8c8d;color:#fff}}"
            f"tr.series-head td{{padding:0}}"
            f"tr.series-head summary{{cursor:pointer;padding:6px 8px;"
            f"background:var(--stripe);display:flex;align-items:center;"
            f"gap:8px}}"
            f"tr.series-head summary .series-title{{flex:1;min-width:0;"
            f"overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}"
            f"tr.series-head .series-kill{{margin-left:auto;background:none;"
            f"border:0;color:var(--muted);cursor:pointer;font-size:.9em;"
            f"padding:2px 0 2px 6px}}"
            f"tr.series-head .series-kill:hover{{color:#e74c3c}}"
            f"table.series-sub{{width:100%;border-collapse:collapse}}"
            f"table.series-sub td{{padding:4px 8px}}"
            f"</style></head><body>"
            f"<div class='rec-header'>"
            f"<a class='home-link' href='{HOST_URL}/'>"
            f"<span class='arrow'>←</span><h1>Aufnahmen</h1></a>"
            f"</div>"
            f"<div class='rec-body'>"
            f"<table>"
            f"{''.join(rows) if rows else '<tr><td colspan=5>Keine Aufnahmen</td></tr>'}"
            f"</table>"
            f"</div>"
            f"<script>"
            f"if(document.querySelector('.badge.scanning,.badge.warming,.badge.live'))"
            f"  setTimeout(()=>location.reload(),15000);"
            # Persist <details> open/closed state per series across reloads.
            # Key by autorec uuid (data-ar). User toggles win over the
            # server-default 'open if any episode active'.
            f"(function(){{"
            f"  const KEY='rec-series-open';"
            f"  let saved={{}};"
            f"  try{{saved=JSON.parse(localStorage.getItem(KEY)||'{{}}');}}"
            f"  catch(e){{}}"
            f"  for(const d of document.querySelectorAll('details[data-ar]')){{"
            f"    const ar=d.dataset.ar;"
            f"    if(ar in saved)d.open=!!saved[ar];"
            f"    d.addEventListener('toggle',()=>{{"
            f"      saved[ar]=d.open;"
            f"      try{{localStorage.setItem(KEY,JSON.stringify(saved));}}"
            f"      catch(e){{}}"
            f"    }});"
            f"  }}"
            f"}})();"
            f"function showDeleteModal(title,subtitle,onConfirm){{"
            f"  const m=document.createElement('div');"
            f"  m.className='del-modal';"
            f"  m.innerHTML='<div class=\"del-dialog\">'+"
            f"    '<div class=\"del-head\">Aufnahme löschen?</div>'+"
            f"    '<div class=\"del-title\">'+title+'</div>'+"
            f"    (subtitle?'<div class=\"del-sub\">'+subtitle+'</div>':'')+"
            f"    '<div class=\"del-btns\">'+"
            f"    '<button class=\"del-cancel\">Abbrechen</button>'+"
            f"    '<button class=\"del-confirm\">🗑 Löschen</button>'+"
            f"    '</div></div>';"
            f"  document.body.appendChild(m);"
            f"  m.addEventListener('click',ev=>{{"
            f"    if(ev.target===m||ev.target.classList.contains('del-cancel')){{m.remove();return;}}"
            f"    if(ev.target.classList.contains('del-confirm')){{m.remove();onConfirm();}}"
            f"  }});"
            f"}}"
            f"document.addEventListener('click',ev=>{{"
            f"  const a=ev.target.closest('.del-btn');if(!a)return;"
            f"  ev.preventDefault();"
            f"  const title=a.dataset.title||'?';"
            f"  const sub=a.dataset.mt?'Aus Liste entfernen (falls lokale Datei existiert, wird sie gelöscht).':"
            f"    (a.dataset.when?'Geplant/aufgenommen am '+a.dataset.when:'');"
            f"  showDeleteModal(title,sub,()=>{{location.href=a.href;}});"
            f"}});"
            f"document.addEventListener('click',ev=>{{"
            f"  const a=ev.target.closest('.watch-btn');if(!a)return;"
            f"  ev.preventDefault();"
            f"  const next=a.dataset.watched==='1'?0:1;"
            f"  fetch('{HOST_URL}/api/recording/'+a.dataset.uuid+'/watched',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{watched:next===1}})}})"
            f"    .then(r=>r.json()).then(d=>{{"
            f"      if(!d.ok)return;"
            f"      a.dataset.watched=String(next);"
            f"      a.classList.toggle('on',next===1);"
            f"      a.textContent=next===1?'\\u2713':'\\u25CB';"
            f"      a.title='Als '+(next===1?'un':'')+'gesehen markieren';"
            f"    }}).catch(()=>{{}});"
            f"}});"
            f"function cancelSeries(ev,uuid,title,upcoming){{"
            f"  ev.preventDefault();ev.stopPropagation();"
            f"  const msg='Serie abbrechen?\\n\\n'+title+"
            f"'\\n\\nRegel wird gelöscht und '+upcoming+"
            f"' geplante Folge(n) verworfen. Bereits aufgenommene "
            f"Episoden bleiben erhalten.';"
            f"  if(!confirm(msg))return;"
            f"  fetch('{HOST_URL}/cancel-series/'+uuid)"
            f"    .then(r=>r.json()).then(d=>{{"
            f"      if(d.ok)location.reload();"
            f"      else alert('Fehler: '+(d.error||'?'));"
            f"    }}).catch(e=>alert('Fehler: '+e));"
            f"}}"
            f"</script>"
            f"</body></html>")
    return body


_rec_hls_lock = threading.Lock()
_rec_hls_procs = {}  # uuid -> {"proc": Popen, "started": ts, "total_segs": int}
_rec_cskip_lock = threading.Lock()
_rec_cskip_procs = {}  # uuid -> {"proc": Popen, "started": ts}


def _rec_source_path(uuid):
    """Return the file path of the DVR recording inside this container
    (via the /recordings read-only mount). None if not found.
    tvheadend's API stores filenames mojibake-encoded on this host
    (UTF-8 'ü' got round-tripped through Latin-9 → 'ÃŒ' →
    \\xc3\\x83\\xc5\\x92). The actual file on disk is plain UTF-8.
    Try the literal path first; if that misses, scan the parent dir
    and find the file whose mojibake-reversed basename matches."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=400",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                fn = e.get("filename") or ""
                if not fn:
                    return None
                if Path(fn).is_file():
                    return fn
                parent = Path(fn).parent
                if not parent.is_dir():
                    return fn
                base = Path(fn).name
                try:
                    fixed = base.encode("iso-8859-15").decode("utf-8")
                except Exception:
                    return fn
                for entry in parent.iterdir():
                    if entry.name == fixed:
                        return str(entry)
                return fn
    except Exception:
        pass
    return None


def _rec_channel_slug(uuid):
    """Look up which channel a DVR entry was recorded from, as a slug."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=400",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                return slugify(e.get("channelname") or "")
    except Exception:
        pass
    return ""


def _rec_cskip_spawn(uuid):
    """Run comskip on the original TS so we can mark commercial blocks
    on the scrub bar. Fire-and-forget; result is polled via files."""
    with _rec_cskip_lock:
        info = _rec_cskip_procs.get(uuid)
        if info and info["proc"].poll() is None:
            return
        out_dir = HLS_DIR / f"_rec_{uuid}"
        out_dir.mkdir(parents=True, exist_ok=True)
        src = _rec_source_path(uuid)
        if not src or not Path(src).exists():
            return
        # Clear any prior output txt files so we can detect new ones
        for old in out_dir.glob("*.txt"):
            try: old.unlink()
            except Exception: pass
        slug = _rec_channel_slug(uuid)
        ini = comskip_ini_for(slug)
        cmd = ["nice", "-n", "15", "comskip",
               "--ini", str(ini),
               "--output", str(out_dir),
               "--quiet", src]
        # Cooperative lock-file so the Mac-side tv-comskip.sh skips
        # this recording while we're working on it (and vice versa —
        # tv-comskip.sh writes the same file before spawning). Stale
        # locks are ignored after 15 min via mtime check on the reader.
        scanning = out_dir / ".scanning"
        try:
            scanning.write_text(f"pi {os.getpid()} {int(time.time())}")
        except Exception:
            pass
        proc = subprocess.Popen(cmd,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        _rec_cskip_procs[uuid] = {"proc": proc, "started": time.time()}
        print(f"[rec-cskip {uuid[:8]}] spawned", flush=True)
        def _cleanup_lock(p, lock):
            try: p.wait()
            except Exception: pass
            try: lock.unlink(missing_ok=True)
            except Exception: pass
        threading.Thread(target=_cleanup_lock, args=(proc, scanning),
                         daemon=True).start()


THUMB_INTERVAL = 30   # seconds between thumbnails
_rec_thumbs_lock = threading.Lock()
_rec_thumbs_running = set()   # uuids currently being processed

def _rec_thumbs_spawn(uuid):
    """Generate scrub-bar thumbnails (1 per THUMB_INTERVAL s, scaled to
    160 px wide) once per recording. Writes to _rec_<uuid>/thumbs/
    and a sentinel `.done` file when finished. Idempotent — safe to
    call on every /progress poll without spawning duplicates."""
    out_dir = HLS_DIR / f"_rec_{uuid}" / "thumbs"
    if (out_dir / ".done").exists():
        return
    with _rec_thumbs_lock:
        if uuid in _rec_thumbs_running:
            return
        _rec_thumbs_running.add(uuid)
    src = _rec_source_path(uuid)
    if not src or not Path(src).exists():
        with _rec_thumbs_lock:
            _rec_thumbs_running.discard(uuid)
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    def run():
        try:
            subprocess.run(
                ["nice", "-n", "18", "ionice", "-c", "3",
                 "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-i", src,
                 "-vf", f"fps=1/{THUMB_INTERVAL},scale=160:-2",
                 "-q:v", "6",
                 str(out_dir / "t%05d.jpg")],
                timeout=900, check=False)
            (out_dir / ".done").write_text("")
            print(f"[rec-thumbs {uuid[:8]}] done", flush=True)
        except Exception as e:
            print(f"[rec-thumbs {uuid[:8]}] {e}", flush=True)
        finally:
            with _rec_thumbs_lock:
                _rec_thumbs_running.discard(uuid)
    threading.Thread(target=run, daemon=True).start()
    print(f"[rec-thumbs {uuid[:8]}] spawned", flush=True)


def _rec_parse_comskip(out_dir):
    """Parse comskip's default .txt output → list of [start_s, stop_s].
    Adjacent blocks separated by a short sponsor card (≤25 s of "show")
    are merged — typical pattern on VOX/RTL/Pro7: Werbung · 15 s
    Präsentation-Einblendung · Werbung · Sendungsstart."""
    # Exclude *.logo.txt — comskip writes that file alongside the
    # cutlist (since the SaveLogoMaskData patch in mpeg2dec.c) and
    # filesystem ordering can put it ahead of the actual cutlist.
    txts = [p for p in out_dir.glob("*.txt")
            if not p.name.endswith(".logo.txt")]
    if not txts:
        return []
    try:
        lines = txts[0].read_text().splitlines()
    except Exception:
        return []
    fps = 25.0
    ads = []
    # Comskip's .txt occasionally writes a long run of NUL bytes
    # before the first frame-range line (witnessed on the Mac-side
    # patched build, suspect ftell/fwrite ordering on the merged-ts
    # input stream). After str.strip() those NULs stay in the line —
    # find the LAST whitespace-separated frame pair via regex so the
    # leading garbage doesn't break parsing.
    line_re = re.compile(r"(\d+)\s+(\d+)\s*$")
    for line in lines:
        line = line.strip().replace("\x00", "")
        if not line or line.startswith("-"):
            continue
        if "FRAMES AT" in line:
            try:
                fps = float(line.split()[-1]) / 100.0
                if fps <= 0:
                    fps = 25.0
            except Exception:
                pass
            continue
        m = line_re.search(line)
        if not m:
            continue
        try:
            a = float(m.group(1)) / fps
            b = float(m.group(2)) / fps
            if b - a >= 60:   # ignore stub blocks shorter than 60 s
                              # — matches comskip.ini's
                              # min_commercialbreak=60, also serves as
                              # safety net when comskip emits sub-60 s
                              # detections from edge cases (older
                              # ini, partial-window scans, etc.)
                ads.append([round(a, 2), round(b, 2)])
        except Exception:
            continue
    # Comskip's .txt sometimes contains duplicate frame pairs — dedup
    # and sort before we touch them.
    seen = set()
    dedup = []
    for a, b in ads:
        key = (round(a, 1), round(b, 1))
        if key not in seen:
            seen.add(key)
            dedup.append([a, b])
    dedup.sort(key=lambda x: x[0])
    # Merge blocks with short gaps (sponsor card / Praesentation-Einblendung).
    # 45 s catches German private-TV sponsor cards including the
    # "Werbung · Sponsor · Werbung" pattern with mid-block teasers.
    MERGE_GAP = 45.0
    merged = []
    for a, b in dedup:
        if merged and a - merged[-1][1] <= MERGE_GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def _blackframe_extend_ads(video_path, ads,
                            channel_slug=None,
                            sponsor_duration=None,
                            max_extend=None):
    """Look for blackframes after each comskip ad-end. Three cases:
       (1) two blacks found after end → pattern is
           [ad] · (black) · [sponsor] · (black) · [show] — extend to
           the latest black (sponsor end).
       (2) one black close to end → pattern is [ad] · (black) ·
           [sponsor] · hardcut → extend by sponsor_duration from the
           black (the sponsor's end isn't marked by a black).
       (3) no blackframe → no extension.
       Per-channel sponsor_duration via SPONSOR_DURATION_BY_CHANNEL."""
    if sponsor_duration is None:
        sponsor_duration = SPONSOR_DURATION_BY_CHANNEL.get(
            channel_slug or "", SPONSOR_DURATION_DEFAULT)
    if max_extend is None:
        max_extend = sponsor_duration + 10.0
    if not ads or not video_path or sponsor_duration <= 0:
        return ads
    scan_window = max(sponsor_duration + 8.0, 20.0)
    # Backward window for ad-START. Two blackframes typically precede
    # comskip's logo-loss-based detection on DE private TV:
    #   show-end (BF1) → ~25 s "Programmhinweis" sponsor card
    #   (logo still visible, looks like content) → (BF2) → real ad
    # comskip detects somewhere AFTER BF2 by 5-15 s. We snap back to
    # the EARLIEST blackframe in the window, so a 75 s window catches
    # both: BF2 (~30-40 s back) when no promo, BF1 (~50-65 s back)
    # when there is one. Safely smaller than min_show_segment_length
    # =120 s so we won't pull a blackframe from inside the show.
    START_SCAN = 75.0
    START_MAX_EXTEND = 75.0
    out = []
    for start, end in ads:
        # --- forward extension (ad-end → sponsor-end) ---
        ss = max(0, end - 1.0)
        new_end = end
        blacks_after = []
        try:
            proc = subprocess.run(
                ["ffmpeg", "-hide_banner", "-nostats",
                 "-ss", str(ss), "-i", str(video_path),
                 "-t", str(scan_window + 2),
                 "-vf", "blackdetect=d=0.04:pix_th=0.30:pic_th=0.80",
                 "-an", "-f", "null", "-"],
                capture_output=True, text=True, timeout=25)
            for line in proc.stderr.splitlines():
                if "blackdetect" in line and "black_end:" in line:
                    try:
                        rel = float(line.split("black_end:")[1].split()[0])
                        abs_t = ss + rel
                        if end < abs_t <= end + scan_window:
                            blacks_after.append(abs_t)
                    except Exception:
                        pass
        except Exception:
            pass
        extend_to = end
        if len(blacks_after) >= 2 and (blacks_after[-1] - blacks_after[0]) >= 5.0:
            extend_to = blacks_after[-1] + 0.5
        elif blacks_after and blacks_after[0] <= end + 5.0:
            extend_to = blacks_after[0] + sponsor_duration
        elif blacks_after:
            extend_to = blacks_after[-1] + 0.5
        new_end = max(new_end, min(end + max_extend, extend_to))
        # --- backward extension (ad-start → earlier blackframe) ---
        # ffmpeg fast-seek (-ss BEFORE -i) drops blackdetect output
        # for the first ~6 s after the seek-point — filter warm-up.
        # Pre-roll 12 s before our window so any blackframe at the
        # very start of the actual window survives. Blackframes still
        # filtered to [start - START_SCAN, start - 1].
        WARMUP = 12.0
        ss2 = max(0, start - START_SCAN - WARMUP)
        new_start = start
        blacks_before = []
        try:
            proc = subprocess.run(
                ["ffmpeg", "-hide_banner", "-nostats",
                 "-ss", str(ss2), "-i", str(video_path),
                 "-t", str(START_SCAN + WARMUP + 2),
                 "-vf", "blackdetect=d=0.04:pix_th=0.30:pic_th=0.80",
                 "-an", "-f", "null", "-"],
                capture_output=True, text=True, timeout=25)
            for line in proc.stderr.splitlines():
                if "blackdetect" in line and "black_start:" in line:
                    try:
                        rel = float(line.split("black_start:")[1].split()[0])
                        abs_t = ss2 + rel
                        if start - START_SCAN <= abs_t < start - 1.0:
                            blacks_before.append(abs_t)
                    except Exception:
                        pass
        except Exception:
            pass
        if blacks_before:
            # Earliest black within the scan window = true ad start.
            earliest = min(blacks_before)
            if start - earliest <= START_MAX_EXTEND:
                new_start = earliest
        # Fallback: if backward-extend found no earlier blackframe and
        # the channel is known to have a soft logo fade (comskip lags
        # by N s), shift back by at least that much.
        min_back = START_LAG_FALLBACK.get(channel_slug or "", 0)
        if min_back > 0 and new_start >= start - 1.0:
            new_start = max(0.0, start - min_back)
        out.append([round(new_start, 2), round(new_end, 2)])
    merged = []
    for a, b in out:
        if merged and a - merged[-1][1] <= 1.0:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def _rec_probe_total_segments(src_url):
    """Approximate total HLS segment count (6 s each) from the source
    duration. Used for progress UI; minor rounding is fine."""
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=nokey=1:noprint_wrappers=1", src_url],
            capture_output=True, text=True, timeout=10)
        dur = float((probe.stdout or "0").strip() or 0)
        return max(1, int(dur / 6) + 1)
    except Exception:
        return 0


def _rec_hls_spawn(uuid):
    """Spawn ffmpeg to remux TS recording into HLS VOD (copy-video, aac-audio)."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    playlist = out_dir / "index.m3u8"
    with _rec_hls_lock:
        existing = _rec_hls_procs.get(uuid)
        if existing and existing["proc"].poll() is None:
            return playlist
        out_dir.mkdir(parents=True, exist_ok=True)
        # Wipe any stale partial playlist so the "wait" loop can't
        # return a truncated one from a prior crashed run.
        try: playlist.unlink()
        except FileNotFoundError: pass
        # Prefer the on-disk file over tvheadend's /dvrfile HTTP endpoint
        # — ffmpeg 8.x's MPEG-2 decoder chokes on the HTTP stream (bails
        # after 25× "Invalid frame dimensions 0x0" with zero output),
        # while the same .ts file read directly decodes fine.
        src = _rec_source_path(uuid) or f"{TVH_BASE}/dvrfile/{uuid}"
        # Probe video codec — copy if already H.264, transcode MPEG-2 etc.
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=codec_name", "-of",
                 "default=nokey=1:noprint_wrappers=1", src],
                capture_output=True, text=True, timeout=10)
            vcodec = (probe.stdout or "").strip()
        except Exception:
            vcodec = ""
        if vcodec in SAFE_VIDEO:
            v_opts = ["-c:v", "copy"]
        else:
            v_opts = ["-vf",
                      "scale=trunc(iw*sar/2)*2:trunc(ih/2)*2,setsar=1",
                      "-c:v", "libx264", "-preset", "ultrafast",
                      "-profile:v", "main", "-pix_fmt", "yuv420p",
                      "-g", "50"]
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
               "-i", src,
               "-map", "0:v:0", "-map", "0:a:0",
               *v_opts, "-c:a", "aac", "-b:a", "128k",
               "-f", "hls",
               "-hls_time", "6",
               "-hls_list_size", "0",
               "-hls_playlist_type", "event",
               "-hls_base_url", f"/hls/_rec_{uuid}/",
               "-hls_segment_filename", str(out_dir / "seg_%05d.ts"),
               str(playlist)]
        # Run remux with low CPU priority so live playback wins the
        # scheduler if someone is watching at the same time.
        cmd = ["nice", "-n", "15"] + cmd
        proc = subprocess.Popen(cmd,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        total = _rec_probe_total_segments(src)
        _rec_hls_procs[uuid] = {"proc": proc, "started": time.time(),
                                 "total_segs": total}
        print(f"[rec-hls {uuid[:8]}] ffmpeg spawned, ~{total} segments",
              flush=True)
        # Kick off comskip right away — it reads the same .ts source
        # (read-only) so it can run concurrently with the remux and
        # commercial markers land on the scrub bar ~5 min sooner.
        # Skip when the Mac's tv-comskip.sh agent is alive: its
        # M-series CPU finishes 4-10x faster than ours and the Pi has
        # ffmpeg + 3-4 live transcodes already saturating its cores.
        if not _mac_comskip_alive():
            _rec_cskip_spawn(uuid)

        def _after_ffmpeg():
            proc.wait()
            if proc.returncode != 0:
                # Self-heal on crash: wipe the partial HLS directory so
                # the next player request re-spawns from scratch. Without
                # this we'd serve a truncated playlist + premature
                # ENDLIST, which looks like an instant-done empty clip.
                print(f"[rec-hls {uuid[:8]}] ffmpeg failed "
                      f"(rc={proc.returncode}) — wiping {out_dir.name}",
                      flush=True)
                shutil.rmtree(out_dir, ignore_errors=True)
                _rec_hls_procs.pop(uuid, None)
        threading.Thread(target=_after_ffmpeg, daemon=True).start()
    return playlist


_live_ads_lock = threading.Lock()
_live_ads = {}   # slug -> {"generated": ts, "ads": [[wall_start, wall_stop], ...]}
_live_ads_proc = {"slug": None, "started": 0}
LIVE_ADS_FILE = HLS_DIR / ".live_ads.json"
MEDIATHEK_REC_FILE = HLS_DIR / ".mediathek_recordings.json"
_mediathek_rec_lock = threading.Lock()
_mediathek_rec = {}  # uuid -> {title, channel, start, stop, hls_url,
                     #         available_to, eid, created_at}


def load_mediathek_rec():
    if not MEDIATHEK_REC_FILE.exists():
        return
    try:
        with _mediathek_rec_lock:
            _mediathek_rec.update(json.loads(MEDIATHEK_REC_FILE.read_text()))
        print(f"Loaded {len(_mediathek_rec)} mediathek recordings", flush=True)
    except Exception as e:
        print(f"load mediathek recordings: {e}", flush=True)


def save_mediathek_rec():
    try:
        with _mediathek_rec_lock:
            snap = dict(_mediathek_rec)
        MEDIATHEK_REC_FILE.write_text(json.dumps(snap))
    except Exception as e:
        print(f"save mediathek recordings: {e}", flush=True)


# V3 — pre-expiry rip of virtual Mediathek recordings to a local MP4.
# Mediathek HLS is already H.264/AAC, so ffmpeg -c copy just swaps the
# container, fast and lossless. Rip when Mediathek expiry approaches
# so the show survives the availability window.
RIP_THRESHOLD_SECONDS = 48 * 3600
RIP_LOAD_CAP = 8.0
RIP_MIN_FREE_GB = 5.0   # don't start a new rip if <5 GB free on /mnt/tv


def _mediathek_rip_file(vuuid):
    """Absolute path for the ripped MP4 of a virtual recording."""
    return HLS_DIR / f"_{vuuid}" / "file.mp4"


def _rip_mediathek(vuuid):
    """Run ffmpeg to fetch the Mediathek HLS stream into a local MP4.
    Copy-mux only (no re-encode). Records success / failure back in
    the state dict."""
    with _mediathek_rec_lock:
        entry = dict(_mediathek_rec.get(vuuid) or {})
    hls_url = entry.get("hls_url")
    if not hls_url:
        return False
    out_dir = HLS_DIR / f"_{vuuid}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "file.mp4"
    tmp_file = out_dir / "file.mp4.part"
    print(f"[mt-rip {vuuid[:10]}] starting → {out_file}", flush=True)
    base = ["nice", "-n", "15", "ffmpeg", "-y",
             "-hide_banner", "-loglevel", "warning",
             "-i", hls_url,
             "-bsf:a", "aac_adtstoasc",
             "-movflags", "+faststart",
             "-f", "mp4", str(tmp_file)]
    # First pass: pure -c copy (fastest, lossless). If that rejects the
    # stream (sometimes MP4 muxer complains about ADTS headers or
    # private-stream metadata), retry with audio re-encoded to AAC.
    attempts = [
        ["-c", "copy"],
        ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"],
    ]
    r = None
    for opts in attempts:
        cmd = base[:8] + opts + base[8:]
        # Drop any leftover .part from previous attempt
        try: tmp_file.unlink()
        except Exception: pass
        try:
            r = subprocess.run(cmd, timeout=3600, check=False,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
        except Exception as e:
            print(f"[mt-rip {vuuid[:10]}] exception: {e}", flush=True)
            return False
        if (r.returncode == 0 and tmp_file.exists()
            and tmp_file.stat().st_size >= 1_000_000):
            break
        err_short = (r.stderr or b"")[-200:].decode("utf-8", "replace").strip()
        print(f"[mt-rip {vuuid[:10]}] copy attempt failed "
              f"(rc={r.returncode}), retrying with audio re-encode: "
              f"{err_short}", flush=True)
    if r.returncode != 0 or not tmp_file.exists() \
       or tmp_file.stat().st_size < 1_000_000:
        err = (r.stderr or b"")[-500:].decode("utf-8", "replace").strip()
        print(f"[mt-rip {vuuid[:10]}] failed rc={r.returncode} {err}",
              flush=True)
        with _mediathek_rec_lock:
            if vuuid in _mediathek_rec:
                _mediathek_rec[vuuid]["rip_error"] = err[:200] or "unknown"
                _mediathek_rec[vuuid]["rip_attempt_at"] = int(time.time())
        save_mediathek_rec()
        try: tmp_file.unlink()
        except Exception: pass
        return False
    tmp_file.rename(out_file)
    size = out_file.stat().st_size
    with _mediathek_rec_lock:
        if vuuid in _mediathek_rec:
            _mediathek_rec[vuuid]["ripped_path"] = str(out_file)
            _mediathek_rec[vuuid]["ripped_at"] = int(time.time())
            _mediathek_rec[vuuid]["ripped_bytes"] = size
            _mediathek_rec[vuuid].pop("rip_error", None)
    save_mediathek_rec()
    print(f"[mt-rip {vuuid[:10]}] done {size/1e6:.1f} MB", flush=True)
    return True


def _mediathek_rip_loop():
    time.sleep(120)
    while True:
        try:
            now = time.time()
            try:
                load = os.getloadavg()[0]
            except Exception:
                load = 0
            if load <= RIP_LOAD_CAP:
                # Disk-full guard — don't start a new multi-GB rip if
                # the SSD is almost full. 5 GB floor so in-flight
                # recordings + tvheadend DVR still have room.
                try:
                    free_gb = shutil.disk_usage(HLS_DIR).free / (1024 ** 3)
                except Exception:
                    free_gb = 0
                if free_gb < RIP_MIN_FREE_GB:
                    print(f"mt-rip: only {free_gb:.1f} GB free, "
                          f"deferring", flush=True)
                    time.sleep(3600)
                    continue
                with _mediathek_rec_lock:
                    todo = [
                        u for u, v in _mediathek_rec.items()
                        if not v.get("ripped_path")
                        and v.get("available_to")
                        and (v["available_to"] - now) < RIP_THRESHOLD_SECONDS
                        and (v["available_to"] - now) > 60  # still playable
                        # don't retry a recent failure within 6 h
                        and (now - v.get("rip_attempt_at", 0)) > 6 * 3600
                    ]
                if todo:
                    # One at a time to keep disk I/O polite.
                    _rip_mediathek(todo[0])
        except Exception as e:
            print(f"mt-rip loop: {e}", flush=True)
        time.sleep(3600)


def _mediathek_autorec_once():
    """One pass: for every autorec-spawned DVR entry on a Mediathek-
    covered channel that we don't yet have a virtual for, attempt the
    match and schedule a virtual recording. The rip loop then picks
    it up before expiry."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=500",
            timeout=10).read())
    except Exception as e:
        print(f"mt-autorec fetch: {e}", flush=True)
        return
    with _mediathek_rec_lock:
        existing = {m.get("tvh_entry") for m in _mediathek_rec.values()
                    if m.get("tvh_entry")}
    import uuid as uuid_mod
    created = 0
    for e in data.get("entries", []):
        if not e.get("autorec"):
            continue
        tvh_uuid = e.get("uuid")
        if not tvh_uuid or tvh_uuid in existing:
            continue
        ch_name = e.get("channelname") or ""
        slug = slugify(ch_name)
        if (slug not in ARD_SEARCH_CHANNELS
            and slug not in ZDF_SEARCH_CHANNELS):
            continue
        title = e.get("disp_title") or e.get("title") or ""
        start = e.get("start", 0)
        stop = e.get("stop", 0)
        if not title or not start:
            continue
        match = _mediathek_match(title, slug, start)
        if not match or not match.get("id"):
            continue
        hls_url = _resolve_mediathek_hls(
            match["id"], source=match.get("source", "ard"))
        if not hls_url:
            continue
        vuuid = "mt_" + uuid_mod.uuid4().hex[:16]
        with _mediathek_rec_lock:
            _mediathek_rec[vuuid] = {
                "title": match.get("title") or title,
                "channel": ch_name,
                "start": start,
                "stop": stop,
                "hls_url": hls_url,
                "available_to": match.get("available_to", 0),
                "eid": f"autorec_{tvh_uuid}",
                "tvh_entry": tvh_uuid,
                "autorec": e.get("autorec"),
                "created_at": int(time.time()),
            }
        save_mediathek_rec()
        created += 1
        print(f"[mt-autorec] scheduled virtual for "
              f"'{title}' on {ch_name} → {vuuid}", flush=True)
    if created:
        print(f"[mt-autorec] pass complete: {created} virtual(s) added",
              flush=True)


def _mediathek_autorec_loop():
    time.sleep(300)   # let tvheadend populate its upcoming list
    while True:
        try:
            _mediathek_autorec_once()
        except Exception as e:
            print(f"mt-autorec loop: {e}", flush=True)
        time.sleep(3600)


def _zdf_search(title):
    """Search ZDF Mediathek for episodes matching `title`. Returns a
    list of dicts {id, title, broadcast_ts, available_to, duration}."""
    qs = urllib.parse.urlencode({
        "q": title, "contentTypes": "episode", "limit": "24",
    })
    url = f"https://api.zdf.de/search/documents?{qs}"
    req = urllib.request.Request(url, headers={
        "api-auth": f"Bearer {ZDF_API_TOKEN}",
    })
    from datetime import datetime
    out = []
    for attempt in range(2):
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=15).read())
            for r in data.get("http://zdf.de/rels/search/results", []):
                t = r.get("http://zdf.de/rels/target", {})
                ed = t.get("editorialDate") or ""
                bt = 0
                if ed:
                    try:
                        bt = int(datetime.fromisoformat(
                            ed.replace("Z", "+00:00")).timestamp())
                    except Exception:
                        pass
                out.append({
                    "id": t.get("id"),
                    "title": t.get("teaserHeadline") or t.get("title") or "",
                    "broadcast_ts": bt,
                    "available_to": 0,   # filled in from item details on demand
                    "duration": t.get("duration", 0),
                })
            return out
        except Exception as e:
            print(f"zdf search (try {attempt+1}): {e}", flush=True)
    return out


def _zdf_resolve_hls(canonical_id):
    """Given a ZDF search id (e.g. 'heute-journal-vom-19-april-2026-100'),
    walk content doc → ptmd template → manifest → HLS URL. Also
    returns the `visibleTo` expiry so the caller can use it as
    available_to if the search result didn't have it."""
    req = urllib.request.Request(
        f"https://api.zdf.de/content/documents/{canonical_id}.json",
        headers={"api-auth": f"Bearer {ZDF_API_TOKEN}"})
    try:
        item = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        print(f"zdf item: {e}", flush=True)
        return None, 0
    mvc = (item.get("mainVideoContent") or {}).get(
        "http://zdf.de/rels/target") or {}
    tmpl = mvc.get("http://zdf.de/rels/streams/ptmd-template", "")
    vis_to = 0
    from datetime import datetime
    if mvc.get("visibleTo"):
        try:
            vis_to = int(datetime.fromisoformat(
                mvc["visibleTo"].replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    if not tmpl:
        return None, vis_to
    ptmd_url = ("https://api.zdf.de"
                + tmpl.replace("{playerId}", "ngplayer_2_4"))
    req = urllib.request.Request(ptmd_url, headers={
        "api-auth": f"Bearer {ZDF_API_TOKEN}"})
    try:
        ptmd = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        print(f"zdf ptmd: {e}", flush=True)
        return None, vis_to
    # Walk priorityList → formitaeten → qualities → audio/tracks → uri
    import re
    hls_url = None
    for pri in ptmd.get("priorityList", []):
        for f in pri.get("formitaeten", []):
            if f.get("type") != "hls":
                continue
            for q in f.get("qualities", []):
                for a in q.get("audio", {}).get("tracks", []):
                    u = a.get("uri") or ""
                    if ".m3u8" in u:
                        hls_url = u
                        break
                if hls_url: break
            if hls_url: break
        if hls_url: break
    if not hls_url:
        # Fallback: scan the whole response for any m3u8
        m = re.search(r'https?://[^"\s]+\.m3u8[^"\s]*', json.dumps(ptmd))
        if m:
            hls_url = m.group(0)
    return hls_url, vis_to


def _resolve_mediathek_hls(item_id, source="ard"):
    """Resolve an HLS master URL for a Mediathek match. Dispatches on
    `source`: ARD uses the page-gateway item endpoint, ZDF uses the
    PTMD manifest chain. ARD's item endpoint is occasionally slow
    (>6 s); retry once on timeout."""
    if source == "zdf":
        hls, _ = _zdf_resolve_hls(item_id)
        return hls
    url = (f"https://api.ardmediathek.de/page-gateway/pages/ard/"
           f"item/{item_id}?devicetype=pc&embedded=true")
    for attempt in range(2):
        try:
            data = json.loads(
                urllib.request.urlopen(url, timeout=15).read())
            # Scan every stream/media entry for a .m3u8 URL — the order
            # isn't guaranteed across shows.
            for stream in (data.get("widgets", [{}])[0]
                               .get("mediaCollection", {})
                               .get("embedded", {})
                               .get("streams", [])):
                for m in stream.get("media", []):
                    u = m.get("url", "")
                    if ".m3u8" in u:
                        return u
            # Fallback: any forcedLabel "Auto" entry even without .m3u8
            for stream in (data.get("widgets", [{}])[0]
                               .get("mediaCollection", {})
                               .get("embedded", {})
                               .get("streams", [])):
                for m in stream.get("media", []):
                    if m.get("forcedLabel") == "Auto":
                        return m.get("url")
            return None
        except Exception as e:
            print(f"mediathek hls resolve (try {attempt+1}): {e}",
                  flush=True)
    return None


def load_live_ads():
    if not LIVE_ADS_FILE.exists():
        return
    try:
        data = json.loads(LIVE_ADS_FILE.read_text())
        # Drop entries older than 2 h — the ad blocks' wall times would
        # point outside any live buffer window anyway.
        cutoff = time.time() - 2 * 3600
        with _live_ads_lock:
            _live_ads.update({
                s: v for s, v in data.items()
                if v.get("generated", 0) > cutoff
            })
        print(f"Loaded live-ads for {len(_live_ads)} channels", flush=True)
    except Exception as e:
        print(f"load live-ads: {e}", flush=True)


def save_live_ads():
    try:
        with _live_ads_lock:
            snap = {s: dict(v) for s, v in _live_ads.items()}
        LIVE_ADS_FILE.write_text(json.dumps(snap))
    except Exception as e:
        print(f"save live-ads: {e}", flush=True)


def _live_ads_analyze(slug, window_end_seg=None, window_size=1800):
    """Concatenate HLS segments of a live channel in a sliding window,
    run comskip to detect commercial blocks, and store the result
    keyed by wall-clock seconds. `window_end_seg` names the rightmost
    segment to include (default: live edge). `window_size` is in
    segments (each ~1 s). Keeps results from previous scans that
    covered a different window (via the caller merging)."""
    ch_dir = HLS_DIR / slug
    playlist = ch_dir / "index.m3u8"
    if not playlist.exists():
        return False
    # Anchor wall time: parse the first #EXT-X-PROGRAM-DATE-TIME.
    first_pdt = None
    try:
        from datetime import datetime
        for line in playlist.read_text().splitlines():
            if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
                first_pdt = datetime.fromisoformat(
                    line.split(":", 1)[1].strip()).timestamp()
                break
    except Exception as e:
        print(f"[{slug}] adskip PDT parse: {e}", flush=True)
    if first_pdt is None:
        print(f"[{slug}] adskip skip: no PROGRAM-DATE-TIME found",
              flush=True)
        return False
    all_segs = sorted(ch_dir.glob("seg_*.ts"))
    if len(all_segs) < 120:      # at least ~2 min of segments
        return False
    # Determine scan window.
    if window_end_seg is not None:
        end_idx = next((i for i, p in enumerate(all_segs)
                        if p.name == window_end_seg), len(all_segs))
        end_idx = min(end_idx + 1, len(all_segs))
    else:
        end_idx = len(all_segs)
    start_idx = max(0, end_idx - window_size)
    segs = all_segs[start_idx:end_idx]
    latest_name = segs[-1].name if segs else all_segs[-1].name
    # For live-edge scans, short-circuit if nothing new.
    if window_end_seg is None:
        with _live_ads_lock:
            prev = _live_ads.get(slug) or {}
        if prev.get("latest_seg") == latest_name:
            return False
    first_pdt += start_idx * SEGMENT_TIME
    work = ch_dir / ".adskip"
    work.mkdir(exist_ok=True)
    for f in work.glob("*.txt"):
        try: f.unlink()
        except Exception: pass
    merged = work / "merged.ts"
    merged.unlink(missing_ok=True)
    # Concatenate into a single MPEG-TS so comskip sees a continuous
    # file. TS is self-synchronising so raw cat works — much faster
    # than ffmpeg's concat demuxer which was bottlenecked on per-file
    # parsing.
    try:
        with open(merged, "wb") as out:
            for s in segs:
                try:
                    out.write(s.read_bytes())
                except Exception:
                    pass
    except Exception as e:
        print(f"[{slug}] adskip cat: {e}", flush=True)
        return False
    if not merged.exists() or merged.stat().st_size < 1_000_000:
        return False
    # comskip — maximum niceness and tight ionice so it loses to
    # every live-TV ffmpeg whenever there's contention.
    ini = comskip_ini_for(slug)
    try:
        subprocess.run(
            ["nice", "-n", "19", "ionice", "-c", "3",
             "comskip", "--ini", str(ini), "--quiet",
             "--output", str(work), str(merged)],
            timeout=600, check=False)
    except Exception as e:
        print(f"[{slug}] adskip comskip: {e}", flush=True)
    ads_sec = _rec_parse_comskip(work)
    ads_sec = _blackframe_extend_ads(str(merged), ads_sec,
                                       channel_slug=slug)
    merged.unlink(missing_ok=True)
    # Convert frame-based seconds (relative to concat start) to wall
    ads_wall = [[round(first_pdt + a, 1), round(first_pdt + b, 1)]
                for a, b in ads_sec]
    # Keep ads from previous scans that lie OUTSIDE the current scan
    # window (either before it starts or after it ends). Drop anything
    # older than the 2 h DVR buffer.
    buffer_cutoff = time.time() - WINDOW_SECONDS
    scan_start = first_pdt
    scan_end = first_pdt + len(segs) * SEGMENT_TIME
    with _live_ads_lock:
        prev = _live_ads.get(slug, {}).get("ads", [])
    retained = [a for a in prev
                if (a[1] <= scan_start or a[0] >= scan_end)
                and a[1] > buffer_cutoff]
    merged_ads = sorted(retained + ads_wall)
    # Cross-scan merge pass: _rec_parse_comskip merges adjacent
    # detections within MERGE_GAP INSIDE one scan, but a single
    # commercial break can span a scan boundary — sub-ads detected
    # in two different scans end up as separate entries here. Re-run
    # the same merge across the combined list so the player sees
    # one block per actual commercial break instead of fragmented
    # stubs.
    MERGE_GAP = 45.0
    coalesced = []
    for a, b in merged_ads:
        if coalesced and a - coalesced[-1][1] <= MERGE_GAP:
            coalesced[-1][1] = max(coalesced[-1][1], b)
        else:
            coalesced.append([a, b])
    merged_ads = coalesced
    with _live_ads_lock:
        _live_ads[slug] = {"generated": time.time(), "ads": merged_ads,
                            "latest_seg": latest_name}
    save_live_ads()
    print(f"[{slug}] adskip: {len(ads_wall)} new, "
          f"{len(retained)} retained, {len(merged_ads)} total",
          flush=True)
    return True


# Only scan channels where commercials are plausible — public
# broadcasters have no regular ads primetime.
LIVE_ADSKIP_SLUGS = {
    "prosieben", "sat-1", "kabel-eins", "kabeleins", "prosiebenmaxx",
    "rtl", "vox", "rtlzwei", "nitro", "rtlup", "superrtl",
    "tele-5", "tele5", "sport1", "dmax", "sixx",
}


def _live_adskip_loop():
    # When LIVE_ADS_OFFLOAD is set (e.g. "mac"), the live-buffer scan
    # runs on another host that writes .live_ads.json directly. We read
    # that file on every /api/live-ads/<slug> call instead.
    if os.environ.get("LIVE_ADS_OFFLOAD"):
        print("adskip loop: disabled (LIVE_ADS_OFFLOAD set — "
              f"{os.environ.get('LIVE_ADS_OFFLOAD')} handles scanning)",
              flush=True)
        return
    # Short warm-up: threads and adopted ffmpegs settle in a few seconds.
    # The per-channel `started_at > 120s` check below is what actually
    # guarantees the buffer is long enough to bother analysing.
    time.sleep(10)
    # 1-min loadavg threshold: we have 4 cores and 5 SD-MPEG-2 streams
    # already running libx264, each eating ~0.7-1.0 core. Add comskip
    # and the playing client, and ffmpeg starves — segment writes
    # stall for 20 s, playback stutters. Hold off the scan until
    # there's real headroom.
    LOAD_CAP = 10.0
    while True:
        try:
            load = os.getloadavg()[0]
            if load > LOAD_CAP:
                time.sleep(60)
                continue
            with active_lock:
                candidates = [s for s, i in channels.items()
                              if i["process"].poll() is None
                              and s in LIVE_ADSKIP_SLUGS
                              and (time.time() - i.get("started_at", 0)) > 120]
            with _live_ads_lock:
                recent = {s for s, v in _live_ads.items()
                          if time.time() - v.get("generated", 0) < 720}
            todo = [s for s in candidates if s not in recent]
            if todo:
                slug = todo[0]
                print(f"[{slug}] adskip: analysing "
                      f"(load={load:.1f}, {len(todo)-1} more queued)",
                      flush=True)
                _live_ads_proc["slug"] = slug
                _live_ads_proc["started"] = time.time()
                try:
                    _live_ads_analyze(slug)
                finally:
                    _live_ads_proc["slug"] = None
        except Exception as e:
            print(f"adskip loop: {e}", flush=True)
        time.sleep(30)


@app.route("/api/live-ads/<slug>/scan", methods=["POST"])
def api_live_ads_scan(slug):
    """Force an ad-detection pass across the whole current live buffer
    in 25-min chunks. Each chunk's result is merged into the cache so
    older ad blocks aren't lost when the next chunk runs."""
    if os.environ.get("LIVE_ADS_OFFLOAD"):
        return _cors(Response(json.dumps({"skipped": True,
                                           "reason": "offloaded — external scanner owns live-ad detection"}),
                               mimetype="application/json"))
    if slug not in LIVE_ADSKIP_SLUGS:
        return _cors(Response(json.dumps({"skipped": True,
                                           "reason": "not in commercial list"}),
                               mimetype="application/json"))
    def run():
        try:
            ch_dir = HLS_DIR / slug
            all_segs = sorted(ch_dir.glob("seg_*.ts"))
            CHUNK = 1800
            STEP = 1500  # overlap by 5 min so blocks at chunk edges aren't missed
            end_idx = len(all_segs)
            while end_idx > 120:
                anchor = all_segs[end_idx - 1].name
                _live_ads_analyze(slug, window_end_seg=anchor,
                                   window_size=CHUNK)
                end_idx -= STEP
                if end_idx <= 0:
                    break
        except Exception as e:
            print(f"[{slug}] manual adskip: {e}", flush=True)
    threading.Thread(target=run, daemon=True).start()
    return _cors(Response(json.dumps({"started": True, "slug": slug}),
                           mimetype="application/json"))


def _live_ads_payload(slug):
    """Snapshot of {ads, generated} for a channel. Reads the JSON file
    under LIVE_ADS_OFFLOAD (the authoritative writer is another host),
    or the in-memory cache otherwise."""
    if os.environ.get("LIVE_ADS_OFFLOAD") and LIVE_ADS_FILE.exists():
        try:
            data = json.loads(LIVE_ADS_FILE.read_text())
            v = data.get(slug)
        except Exception:
            v = None
    else:
        with _live_ads_lock:
            v = _live_ads.get(slug)
    if not v:
        return {"ads": [], "generated": 0}
    return {"ads": v["ads"], "generated": int(v["generated"])}


@app.route("/api/live-ads/<slug>")
def api_live_ads(slug):
    return _cors(Response(json.dumps(_live_ads_payload(slug)),
                           mimetype="application/json"))


@app.route("/api/live-ads-stream/<slug>")
def api_live_ads_stream(slug):
    """SSE push: emit a fresh ad payload whenever .live_ads.json
    changes on disk (= the Mac scanner just saved a new scan result).
    Replaces the player's 30 s polling loop so the skip button shows
    up the moment a new ad is detected, not 0-30 s later. Heartbeat
    every 20 s keeps long-lived connections alive through Caddy /
    cellular proxies. mtime-poll cadence 1 s — file is a few kB, OS
    caches it, the cost is negligible.
    Each connection holds a waitress worker thread for its lifetime;
    with the default pool of 4 we can hold 4 concurrent player tabs
    before /api/* requests start queuing. Plenty for home use."""
    def gen():
        last_mtime = -1
        last_payload = None
        last_heartbeat = time.time()
        # Send the current state immediately on connect so the client
        # doesn't wait for the first file change to render.
        try:
            if LIVE_ADS_FILE.exists():
                last_mtime = LIVE_ADS_FILE.stat().st_mtime
            payload = _live_ads_payload(slug)
            last_payload = json.dumps(payload)
            yield f"data: {last_payload}\n\n"
        except Exception:
            pass
        while True:
            time.sleep(1)
            try:
                if LIVE_ADS_FILE.exists():
                    mt = LIVE_ADS_FILE.stat().st_mtime
                    if mt != last_mtime:
                        last_mtime = mt
                        payload = _live_ads_payload(slug)
                        msg = json.dumps(payload)
                        if msg != last_payload:
                            last_payload = msg
                            yield f"data: {msg}\n\n"
                            last_heartbeat = time.time()
                            continue
            except Exception:
                pass
            now = time.time()
            if now - last_heartbeat > 20:
                yield ":hb\n\n"
                last_heartbeat = now
    return Response(gen(),
                     mimetype="text/event-stream",
                     headers={"Cache-Control": "no-cache",
                              "X-Accel-Buffering": "no",
                              "Access-Control-Allow-Origin": "*"})


def _rec_prewarm_loop():
    """Background: remux finished DVR recordings to HLS so the player
    doesn't have to wait. Serial, low priority, skipped while live
    streams are active so we never starve live TV of CPU."""
    time.sleep(20)  # give the service a moment to settle on startup
    while True:
        try:
            _rec_prewarm_once()
        except Exception as e:
            print(f"[rec-prewarm] error: {e}", flush=True)
        time.sleep(60)


def _mac_comskip_alive():
    """True if the Mac-side tv-comskip.sh launchd agent has touched
    the heartbeat file in the last 5 min. Default-skip the Pi's
    rec-comskip path while the Mac is alive — both can do the work
    but Mac's M-series CPU is much faster and the Pi has live-stream
    ffmpeg + remuxing to keep up with."""
    try:
        hb = HLS_DIR / ".mac-comskip-alive"
        return hb.exists() and time.time() - hb.stat().st_mtime < 300
    except Exception:
        return False


def _rec_prewarm_once():
    # Load-based gate. The dominant cost in this loop is the
    # MPEG-2 -> H.264 transcode for VOD playback (libx264 ultrafast
    # eats 1.5-2.5 cores on its own). nice 15 helps prevent it from
    # preempting live transcodes, but the scheduler still has to
    # service them — once loadavg passes ~3.0 on this 4-core box
    # users notice live-stream segment stalls. Keep the gate well
    # below saturation so prewarm waits for genuine slack.
    try:
        if os.getloadavg()[0] > 3.0:
            return
    except Exception:
        pass
    # Skip while any remux is already in progress
    with _rec_hls_lock:
        for info in _rec_hls_procs.values():
            if info["proc"].poll() is None:
                return
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid_finished?limit=100",
            timeout=10).read())
    except Exception:
        return

    # Garbage-collect orphaned _rec_<uuid> dirs: tvheadend's native UI
    # deletes the .ts file but doesn't know about our HLS cache, so
    # entries deleted there leave stale dirs sitting on disk forever.
    # Build the set of currently-known DVR uuids (finished + scheduled
    # + recording), then remove any _rec_<uuid> not in it.
    known_uuids = {e["uuid"] for e in data.get("entries", []) if e.get("uuid")}
    try:
        all_data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=500", timeout=10).read())
        for e in all_data.get("entries", []):
            if e.get("uuid"):
                known_uuids.add(e["uuid"])
    except Exception:
        pass
    if known_uuids:  # only purge if we actually got a real list
        for d in HLS_DIR.glob("_rec_*"):
            if not d.is_dir(): continue
            uuid = d.name[len("_rec_"):]
            if uuid in known_uuids: continue
            # Skip if any cleanup might race with our own work
            if uuid in _rec_hls_procs or uuid in _rec_cskip_procs: continue
            print(f"[rec-prewarm] gc orphan {uuid[:8]}", flush=True)
            shutil.rmtree(d, ignore_errors=True)

    for e in data.get("entries", []):
        uuid = e.get("uuid")
        if not uuid or not e.get("filename"):
            continue
        out_dir = HLS_DIR / f"_rec_{uuid}"
        playlist = out_dir / "index.m3u8"
        if not playlist.exists():
            # Hand off to the Mac if its handler is alive — its CPU
            # remuxes MPEG-2 -> H.264 5-10x faster and the Pi has
            # ffmpeg + live transcodes already saturating its cores.
            # Lazy fallback in recording_hls() (= user actually opened
            # this recording) is unaffected — that path always spawns
            # so the user isn't stuck waiting on the Mac.
            if _mac_comskip_alive():
                continue
            print(f"[rec-prewarm] remuxing {uuid[:8]} "
                  f"({e.get('disp_title', '?')[:40]})", flush=True)
            _rec_hls_spawn(uuid)
            return  # one per cycle
        # HLS already present — fill in the ad markers if missing.
        # Hand off to the Mac entirely if its launchd agent is alive
        # (heartbeat < 5 min); only run our own comskip if the Mac is
        # offline. Cooperative '.scanning' lock-file is the secondary
        # guard against double-runs in any remaining race window.
        if not list(out_dir.glob("*.txt")):
            if _mac_comskip_alive():
                continue
            scanning = out_dir / ".scanning"
            try:
                fresh_lock = (scanning.exists()
                              and time.time() - scanning.stat().st_mtime < 900)
            except Exception:
                fresh_lock = False
            if fresh_lock:
                continue
            cskip_info = _rec_cskip_procs.get(uuid)
            if not (cskip_info and cskip_info["proc"].poll() is None):
                print(f"[rec-prewarm] comskip {uuid[:8]}", flush=True)
                _rec_cskip_spawn(uuid)
                return


def _rec_playlist_as_vod(text, is_running=False):
    """Convert an EVENT-type playlist into VOD with ENDLIST so iOS
    shows a scrub bar and starts from the beginning instead of live-edge.
    When the remux is still running, keep EVENT type and omit ENDLIST
    so iOS treats the playlist as growing and keeps polling."""
    if is_running:
        return text
    text = text.replace("#EXT-X-PLAYLIST-TYPE:EVENT",
                        "#EXT-X-PLAYLIST-TYPE:VOD")
    if "#EXT-X-PLAYLIST-TYPE" not in text:
        text = text.replace("#EXT-X-TARGETDURATION",
                            "#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION",
                            1)
    if "#EXT-X-ENDLIST" not in text:
        text = text.rstrip() + "\n#EXT-X-ENDLIST\n"
    return text


def _rec_state(uuid):
    """Internal: ffmpeg state + segment progress for a recording uuid."""
    playlist = HLS_DIR / f"_rec_{uuid}" / "index.m3u8"
    info = _rec_hls_procs.get(uuid)
    running = info is not None and info["proc"].poll() is None
    segs = 0
    if playlist.exists():
        try:
            segs = playlist.read_text().count(".ts")
        except Exception:
            pass
    # "done" means: playlist exists AND ffmpeg is not currently running.
    # If ffmpeg was never started in this process lifetime but a full
    # playlist is on disk from a prior run → also treated as done.
    done = playlist.exists() and not running
    total = (info or {}).get("total_segs", 0)
    return {"done": done, "segments": segs, "total": total,
            "running": running, "playlist": playlist}


@app.route("/recording/<uuid>/index.m3u8")
def recording_hls(uuid):
    """VOD playlist endpoint. Spawns ffmpeg in the background on first
    call; serves the growing playlist as soon as ~10 segments are
    ready (EVENT type so iOS keeps polling), then switches to VOD
    with ENDLIST once the remux finishes."""
    st = _rec_state(uuid)
    if not st["playlist"].exists() and not st["running"]:
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    if not st["playlist"].exists() or st["segments"] < 10:
        return Response(json.dumps({"done": False,
                                     "segments": st["segments"],
                                     "total": st["total"]}),
                        status=202, mimetype="application/json")
    return Response(_rec_playlist_as_vod(st["playlist"].read_text(),
                                           is_running=st["running"]),
                    mimetype="application/vnd.apple.mpegurl")


@app.route("/recording/<uuid>/ads")
def recording_ads(uuid):
    """Commercial-block markers for the scrub bar. { ads: [[s,e], …] }."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    cskip_info = _rec_cskip_procs.get(uuid)
    running = cskip_info is not None and cskip_info["proc"].poll() is None
    if not out_dir.exists():
        ads = []
    else:
        ads_cache = out_dir / "ads.json"
        txts = list(out_dir.glob("*.txt"))
        txt_mtime = max((t.stat().st_mtime for t in txts), default=0)
        if (not running and txts
                and ads_cache.exists()
                and ads_cache.stat().st_mtime >= txt_mtime):
            try:
                ads = json.loads(ads_cache.read_text())
            except Exception:
                ads = _rec_parse_comskip(out_dir)
        else:
            ads = _rec_parse_comskip(out_dir)
            if not running and ads:
                src = _rec_source_path(uuid)
                if src and Path(src).exists():
                    ads = _blackframe_extend_ads(
                        src, ads, channel_slug=_rec_channel_slug(uuid))
                try:
                    ads_cache.write_text(json.dumps(ads))
                except Exception:
                    pass
    resp = _cors(Response(json.dumps({"ads": ads, "running": running}),
                            mimetype="application/json"))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/recording/<uuid>/thumbs.json")
def recording_thumbs_manifest(uuid):
    """How many thumbs are ready and what interval they cover."""
    thumbs_dir = HLS_DIR / f"_rec_{uuid}" / "thumbs"
    done = (thumbs_dir / ".done").exists()
    count = len(list(thumbs_dir.glob("t*.jpg"))) if thumbs_dir.exists() else 0
    return _cors(Response(json.dumps({
        "interval": THUMB_INTERVAL,
        "count": count,
        "done": done,
    }), mimetype="application/json"))


@app.route("/recording/<uuid>/thumbs/<fname>")
def recording_thumb(uuid, fname):
    if not re.fullmatch(r"t\d{5}\.jpg", fname):
        abort(404)
    fp = HLS_DIR / f"_rec_{uuid}" / "thumbs" / fname
    if not fp.exists():
        abort(404)
    resp = Response(fp.read_bytes(), mimetype="image/jpeg")
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return _cors(resp)


@app.route("/recording/<uuid>/progress")
def recording_hls_progress(uuid):
    """Lightweight progress poll for the player loader. Also kicks off
    ffmpeg if it hasn't started yet (so the player can just poll)."""
    st = _rec_state(uuid)
    if not st["playlist"].exists() and not st["running"]:
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    # Make sure comskip is on its way — idempotent, safe on every
    # poll. Covers pre-fix recordings that never got a scan, and
    # cleans up 0-byte .txt leftovers from an aborted prior run that
    # would otherwise make us think comskip already finished.
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if out_dir.exists():
        usable = [t for t in out_dir.glob("*.txt")
                  if t.stat().st_size > 0]
        if not usable:
            _rec_cskip_spawn(uuid)
        if not (out_dir / "thumbs" / ".done").exists():
            _rec_thumbs_spawn(uuid)
    # Expose the playlist's mtime so the player can invalidate a
    # stored localStorage seek-position from before the recording was
    # remuxed (otherwise an old saved offset against a partial old
    # playlist makes the player land in the middle on every reload).
    pl_mtime = 0
    try:
        pl_mtime = int(st["playlist"].stat().st_mtime * 1000)
    except Exception:
        pass
    return _cors(Response(json.dumps({"done": st["done"],
                                        "segments": st["segments"],
                                        "total": st["total"],
                                        "playlist_mtime_ms": pl_mtime}),
                           mimetype="application/json"))


def _render_mediathek_player(uuid, entry):
    """HTML for a virtual Mediathek recording. If we've already ripped
    the show to a local MP4 (V3), play that directly via <video src>.
    Otherwise stream the remote HLS via hls.js."""
    title_safe = entry["title"].replace("<", "&lt;")
    upstream_hls = entry["hls_url"]
    # Route through our passthru so the player gets a single-variant
    # master pinned to the upstream's highest BANDWIDTH rendition,
    # bypassing client-side ABR downgrade. Same endpoint that serves
    # live-mediathek channels — the slug here is just routing for the
    # /pl.m3u8 sub-playlist proxy and doesn't have to match anything.
    if upstream_hls.startswith("http"):
        wrapped = (f"{HOST_URL}/mediathek-passthru/_max/master.m3u8?"
                   f"u={urllib.parse.quote(upstream_hls, safe='')}")
        hls_url = wrapped.replace("'", "%27")
    else:
        hls_url = upstream_hls.replace("'", "%27")
    ripped_path = entry.get("ripped_path", "")
    has_local = False
    if ripped_path and uuid != "preview":
        try:
            has_local = Path(ripped_path).exists()
        except Exception:
            has_local = False
    local_url = f"{HOST_URL}/mediathek-rec/{uuid}/file.mp4"
    avail_to = entry.get("available_to", 0)
    from datetime import datetime
    avail_str = ""
    if avail_to:
        try:
            avail_str = datetime.fromtimestamp(avail_to).strftime("%d.%m.%Y")
        except Exception:
            pass
    badge_label = ("Mediathek · lokal" if has_local else "ARD Mediathek")
    badge_extra = (" · bis " + avail_str if avail_str and not has_local
                   else "")
    src_badge = (f"<span class='src-badge'>{badge_label}{badge_extra}</span>")
    return (f"<!doctype html><html><head>"
            f"{PLAYER_HEAD_META}"
            f"<title>{title_safe}</title>"
            f"<script src='https://cdn.jsdelivr.net/npm/hls.js@1/"
            f"dist/hls.min.js'></script>"
            f"<style>{PLAYER_BASE_CSS}"
            f".src-badge{{display:inline-block;background:#2980b9;"
            f"color:#fff;padding:2px 8px;border-radius:10px;"
            f"font-size:.75em;margin-left:8px;vertical-align:1px}}"
            f"#unmute{{position:fixed;left:50%;top:50%;"
            f"transform:translate(-50%,-50%);z-index:30;background:#fff;"
            f"color:#000;padding:14px 22px;border-radius:30px;"
            f"font-weight:600;font-size:1em;border:0;cursor:pointer;"
            f"display:none}}"
            f"</style></head><body>"
            f"<video id='v' autoplay muted playsinline webkit-playsinline></video>"
            f"<button id='unmute'>🔊 Ton an</button>"
            f"<div id='topbar'>"
            f"<button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>"
            f"<a class='iconbtn' href='{HOST_URL}/recordings' "
            f"onclick='return closePlayer(event)' aria-label='Schließen'>✕</a>"
            f"</div>"
            f"<div id='hint'></div>"
            f"<div id='chrome'>"
            f"<div id='scrub'><div id='track'><div id='played'></div></div>"
            f"<div id='thumb'></div></div>"
            f"<div class='row'>"
            f"<button class='iconbtn' onclick='seek(-10)'>⏪</button>"
            f"<button id='pp' class='iconbtn' onclick='togglePlay()'>▶</button>"
            f"<button class='iconbtn' onclick='seek(10)'>⏩</button>"
            f"<span id='volume-wrap'>"
            f"<button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>"
            f"<input type='range' id='vol-slider' min='0' max='100' value='100'>"
            f"</span>"
            f"<span id='cur' class='time'>0:00</span>"
            f"<span class='spacer'></span>"
            f"<span id='dur' class='time'>0:00</span>"
            f"</div>"
            f"<div id='ttlrow'>{title_safe}{src_badge}</div>"
            f"</div>"
            f"<script>"
            f"const PLAYER_HOME='{HOST_URL}/recordings';"
            f"{PLAYER_BASE_JS}"
            f"const cur=document.getElementById('cur');"
            f"const dur=document.getElementById('dur');"
            f"function fmt(s){{"
            f"  if(!isFinite(s)||s<0)s=0;"
            f"  const m=Math.floor(s/60),ss=Math.floor(s%60);"
            f"  const h=Math.floor(m/60);"
            f"  return h>0?h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0')"
            f"           :m+':'+String(ss).padStart(2,'0');"
            f"}}"
            f"function togglePlay(){{"
            f"  if(v.paused||v.ended){{v.play().catch(()=>{{}});}}else{{v.pause();}}"
            f"}}"
            f"function seek(d){{"
            f"  const D=isFinite(v.duration)?v.duration:Infinity;"
            f"  v.currentTime=Math.max(0,Math.min(D-0.5,(v.currentTime||0)+d));"
            f"  show();"
            f"}}"
            f"function refresh(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const T=v.currentTime||0;"
            f"  cur.textContent=fmt(T);dur.textContent=fmt(D);"
            f"  const pct=D>0?(T/D)*100:0;"
            f"  if(!_dragging){{played.style.width=pct+'%';thumb.style.left=pct+'%';}}"
            f"}}"
            f"v.addEventListener('timeupdate',refresh);"
            f"v.addEventListener('loadedmetadata',refresh);"
            f"function seekTo(ev){{"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D>0)v.currentTime=p*D;"
            f"}}"
            f"document.addEventListener('keydown',e=>{{"
            f"  if(e.key==='Escape')closePlayer();"
            f"  else if(e.key==='ArrowRight')seek(10);"
            f"  else if(e.key==='ArrowLeft')seek(-10);"
            f"  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}"
            f"}});"
            f"v.addEventListener('click',()=>{{"
            f"  if(chromeBar.classList.contains('hidden'))show();"
            f"  else {{chromeBar.classList.add('hidden');topbar.classList.add('hidden');}}"
            f"}});"
            f"document.addEventListener('mousemove',show);"
            f"const rawUrl='{hls_url}';"
            f"const localUrl='{local_url}';"
            f"const hasLocal={'true' if has_local else 'false'};"
            f"const unmuteBtn=document.getElementById('unmute');"
            f"const disableCaptions=()=>{{"
            f"  for(const t of v.textTracks)t.mode='disabled';"
            f"}};"
            f"v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);"
            f"setTimeout(disableCaptions,200);"
            f"setTimeout(disableCaptions,1500);"
            f"function doUnmute(){{"
            f"  v.muted=false;unmuteBtn.style.display='none';"
            f"  v.play().catch(()=>{{}});"
            f"}}"
            f"unmuteBtn.onclick=doUnmute;"
            f"v.addEventListener('click',()=>{{if(v.muted)doUnmute();}});"
            f"function startPlayback(){{"
            f"  v.muted=true;"
            f"  v.play().then(()=>{{unmuteBtn.style.display='inline-flex';}})"
            f"    .catch(()=>{{unmuteBtn.style.display='inline-flex';}});"
            f"}}"
            # If we already ripped the show to MP4, play the local file
            # directly — iOS handles MP4 natively, no hls.js needed.
            f"if(hasLocal){{"
            f"  v.src=localUrl;"
            f"  v.addEventListener('loadedmetadata',startPlayback,{{once:true}});"
            f"}} else if(window.Hls&&Hls.isSupported()){{"
            f"  const hls=new Hls({{forceMseHlsOnAppleDevices:true,"
            f"    renderTextTracksNatively:false,subtitleDisplay:false}});"
            f"  hls.loadSource(rawUrl);hls.attachMedia(v);"
            f"  v.addEventListener('canplay',startPlayback,{{once:true}});"
            f"}} else if(v.canPlayType('application/vnd.apple.mpegurl')){{"
            f"  v.src=rawUrl;"
            f"  v.addEventListener('loadedmetadata',startPlayback,{{once:true}});"
            f"}} else {{"
            f"  document.body.innerHTML='<p style=color:#fff;padding:20px>"
            f"HLS wird von diesem Browser nicht unterstützt.</p>';"
            f"}}"
            f"</script></body></html>")


@app.route("/mediathek-play/<event_id>")
def mediathek_play(event_id):
    """Play a Mediathek match of an EPG event directly, without
    persisting a virtual recording. Used by the long-press modal's
    "Jetzt abspielen" shortcut for past shows you just want to watch."""
    try:
        res = urllib.request.urlopen(
            f"http://localhost:8080/api/mediathek-lookup/{event_id}",
            timeout=35).read()
        match = json.loads(res).get("match")
    except Exception as e:
        abort(502, f"lookup: {e}")
    if not match or not match.get("id"):
        abort(404, "no mediathek match")
    hls_url = _resolve_mediathek_hls(match["id"],
                                       source=match.get("source", "ard"))
    if not hls_url:
        abort(502, "no hls url")
    entry = {
        "title": match.get("title", "Mediathek"),
        "hls_url": hls_url,
        "available_to": match.get("available_to", 0),
    }
    return _render_mediathek_player("preview", entry)


@app.route("/recording/<uuid>")
def play_recording(uuid):
    """Player page for a DVR recording — wraps tvheadend's dvrfile in
    a styled <video> so iOS Safari doesn't force native fullscreen.
    Virtual mt_* UUIDs stream directly from ARD Mediathek via hls.js."""
    if uuid.startswith("mt_"):
        with _mediathek_rec_lock:
            entry = _mediathek_rec.get(uuid)
        if not entry:
            abort(404, "unknown mediathek recording")
        return _render_mediathek_player(uuid, entry)
    title = "Aufnahme"
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=400",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                title = e.get("disp_title") or title
                break
    except Exception:
        pass
    title_safe = title.replace("<", "&lt;")
    src = f"{HOST_URL}/recording/{uuid}/index.m3u8"
    html = (f"<!doctype html><html><head>"
            f"{PLAYER_HEAD_META}"
            f"<title>{title_safe}</title>"
            f"<style>{PLAYER_BASE_CSS}"
            f".pill.rec{{background:#e74c3c}}"
            f"#thumb-preview{{position:fixed;width:160px;height:90px;"
            f"border:2px solid #fff;border-radius:4px;overflow:hidden;"
            f"box-shadow:0 2px 8px #000a;z-index:20;background:#000;"
            f"opacity:0;pointer-events:none;transition:opacity .1s}}"
            f"#thumb-preview img{{width:100%;height:100%;object-fit:cover;"
            f"display:block}}"
            f"#thumb-preview.visible{{opacity:1}}"
            f"#loader{{position:fixed;inset:0;display:flex;flex-direction:column;"
            f"align-items:center;justify-content:center;background:#000c;"
            f"z-index:30;font-size:1.05em;gap:8px;transition:opacity .3s}}"
            f"#loader.hidden{{opacity:0;pointer-events:none}}"
            f".spinner{{width:40px;height:40px;border:4px solid #fff3;"
            f"border-top-color:#fff;border-radius:50%;"
            f"animation:spin 1s linear infinite}}"
            f"@keyframes spin{{to{{transform:rotate(360deg)}}}}"
            f"</style></head><body>"
            f"<video id='v' autoplay playsinline "
            f"webkit-playsinline disablepictureinpicture></video>"
            f"<div id='loader'>"
            f"<div class='spinner'></div>"
            f"<div id='lmsg'>Aufnahme wird vorbereitet…</div>"
            f"</div>"
            f"<div id='topbar'>"
            f"<button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>"
            f"<a class='iconbtn' href='{HOST_URL}/recordings' aria-label='Schließen' "
            f"onclick='return closePlayer(event)'>✕</a>"
            f"</div>"
            f"<div id='hint'></div>"
            f"<div id='chrome'>"
            f"<div id='scrub'>"
            f"<div id='track'><div id='played'></div></div>"
            f"<div id='thumb'></div>"
            f"</div>"
            f"<div class='row'>"
            f"<button class='iconbtn' onclick='seek(-10)' aria-label='-10 s'>⏪</button>"
            f"<button id='pp' class='iconbtn' onclick='togglePlay()' "
            f"aria-label='Play/Pause'>▶</button>"
            f"<button class='iconbtn' onclick='seek(10)' aria-label='+10 s'>⏩</button>"
            f"<button id='speedbtn' class='iconbtn' "
            f"onclick='cycleSpeed()' aria-label='Geschwindigkeit' "
            f"style='font-size:.75em;font-weight:600'>1×</button>"
            f"<span id='volume-wrap'>"
            f"<button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>"
            f"<input type='range' id='vol-slider' min='0' max='100' value='100'>"
            f"</span>"
            f"<span id='cur' class='time'>0:00</span>"
            f"<span class='spacer'></span>"
            f"<button id='skipad' class='pill rec' onclick='skipAd()'"
            f">Werbung ⏭</button>"
            f"<span id='dur' class='time'>0:00</span>"
            f"</div>"
            f"<div id='ttlrow'>{title_safe}</div>"
            f"</div>"
            f"<script>"
            f"const PLAYER_HOME='{HOST_URL}/recordings';"
            f"{PLAYER_BASE_JS}"
            f"const cur=document.getElementById('cur');"
            f"const dur=document.getElementById('dur');"
            f"const loader=document.getElementById('loader');"
            f"const lmsg=document.getElementById('lmsg');"
            f"const skipBtn=document.getElementById('skipad');"
            f"const speedBtn=document.getElementById('speedbtn');"
            f"const SPEEDS=[1,1.25,1.5,2,0.75];"
            f"let speedIdx=0;"
            f"function cycleSpeed(){{"
            f"  speedIdx=(speedIdx+1)%SPEEDS.length;"
            f"  v.playbackRate=SPEEDS[speedIdx];"
            f"  speedBtn.textContent=SPEEDS[speedIdx]+'×';"
            f"  show();"
            f"}}"
            f"let ads=[];"
            f"function fmt(s){{"
            f"  if(!isFinite(s)||s<0)s=0;"
            f"  const m=Math.floor(s/60),ss=Math.floor(s%60);"
            f"  const h=Math.floor(m/60);"
            f"  return h>0?h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0')"
            f"           :m+':'+String(ss).padStart(2,'0');"
            f"}}"
            f"function togglePlay(){{"
            f"  if(v.paused||v.ended){{v.play().catch(()=>{{}});}}else{{v.pause();}}"
            f"}}"
            f"function seek(d){{"
            f"  const D=isFinite(v.duration)?v.duration:Infinity;"
            f"  v.currentTime=Math.max(0,Math.min(D-0.5,(v.currentTime||0)+d));"
            f"  show();"
            f"}}"
            f"function currentAd(){{"
            f"  const t=v.currentTime||0;"
            f"  for(const a of ads){{if(t>=a[0]&&t<a[1])return a;}}"
            f"  return null;"
            f"}}"
            f"function skipAd(){{"
            f"  const a=currentAd();if(a)v.currentTime=a[1]+0.1;"
            f"}}"
            f"function renderAds(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  document.querySelectorAll('.ad-block').forEach(e=>e.remove());"
            f"  if(D<=0)return;"
            f"  for(const[s,e]of ads){{"
            f"    const left=(s/D)*100,width=Math.max(0.3,((e-s)/D)*100);"
            f"    const el=document.createElement('div');"
            f"    el.className='ad-block';"
            f"    el.style.left=left+'%';el.style.width=width+'%';"
            f"    scrub.appendChild(el);"
            f"  }}"
            f"}}"
            f"function refresh(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const T=v.currentTime||0;"
            f"  cur.textContent=fmt(T);dur.textContent=fmt(D);"
            f"  const pct=D>0?(T/D)*100:0;"
            f"  if(!_dragging){{played.style.width=pct+'%';thumb.style.left=pct+'%';}}"
            f"  skipBtn.classList.toggle('on',currentAd()!==null);"
            f"}}"
            f"v.addEventListener('timeupdate',refresh);"
            f"v.addEventListener('loadedmetadata',()=>{{refresh();renderAds();}});"
            f"v.addEventListener('durationchange',renderAds);"
            f"function seekTo(ev){{"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D>0)v.currentTime=p*D;"
            f"}}"
            f"v.addEventListener('click',()=>{{"
            f"  if(chromeBar.classList.contains('hidden'))show();"
            f"  else {{chromeBar.classList.add('hidden');"
            f"    topbar.classList.add('hidden');}}"
            f"}});"
            f"document.addEventListener('mousemove',show);"
            f"const hideLoader=()=>loader.classList.add('hidden');"
            f"v.addEventListener('playing',hideLoader);"
            f"v.addEventListener('loadedmetadata',hideLoader);"
            f"v.addEventListener('canplay',hideLoader);"
            f"const LASTPOS_KEY='recpos_{uuid}';"
            f"const LASTPOS_TTL_MS=30*24*3600*1000;"
            f"function saveRecPos(){{"
            f"  const t=v.currentTime||0;"
            f"  if(!isFinite(v.duration)||v.duration<=0||t<=0)return;"
            f"  if(t>v.duration-10){{"
            f"    try{{localStorage.removeItem(LASTPOS_KEY);}}catch(e){{}}"
            f"    return;"
            f"  }}"
            f"  try{{localStorage.setItem(LASTPOS_KEY,"
            f"    JSON.stringify({{t:t,ts:Date.now()}}));}}catch(e){{}}"
            f"}}"
            f"function restoreRecPos(){{"
            f"  let entry=null;"
            f"  try{{entry=JSON.parse(localStorage.getItem(LASTPOS_KEY)||'null');}}"
            f"  catch(e){{}}"
            f"  if(!entry||Date.now()-entry.ts>LASTPOS_TTL_MS)return;"
            # Discard the stored seek-position if the playlist on disk
            # was rebuilt after we saved it — otherwise a recording
            # remuxed in the middle of being-watched lands the player
            # at an offset that no longer corresponds to the same
            # timeline (e.g. after a Mac re-remux that filled in
            # missing early segments).
            f"  if(window._recPlaylistMtime"
            f"     && entry.ts < window._recPlaylistMtime){{"
            f"    try{{localStorage.removeItem(LASTPOS_KEY);}}catch(e){{}}"
            f"    return;"
            f"  }}"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(entry.t>0&&entry.t<D-2)v.currentTime=entry.t;"
            f"}}"
            f"setInterval(()=>{{if(!v.paused)saveRecPos();}},15000);"
            f"window.addEventListener('beforeunload',saveRecPos);"
            f"document.addEventListener('visibilitychange',()=>{{"
            f"  if(document.visibilityState==='hidden')saveRecPos();"
            f"}});"
            f"v.addEventListener('pause',saveRecPos);"
            # Race-safe: defer restoreRecPos until the first progress
            # tick has populated _recPlaylistMtime (otherwise we miss
            # the freshness check and restore a stale offset). Cap at
            # 5 s so we don't sit forever if the endpoint is broken.
            f"v.addEventListener('loadedmetadata',()=>{{"
            f"  let waited=0;"
            f"  const tryRestore=()=>{{"
            f"    if(window._recPlaylistMtime||waited>=5000){{"
            f"      restoreRecPos();return;"
            f"    }}"
            f"    waited+=200;setTimeout(tryRestore,200);"
            f"  }};"
            f"  setTimeout(tryRestore,400);"
            f"}},{{once:true}});"
            f"let srcSet=false;"
            f"function tick(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/progress').then(r=>r.json())"
            f"   .then(d=>{{"
            # Pick up the playlist mtime so restoreRecPos can compare
            # it against the saved position's timestamp and discard
            # stale offsets after a re-remux.
            f"     if(d.playlist_mtime_ms)"
            f"       window._recPlaylistMtime=d.playlist_mtime_ms;"
            # Start playback once ~10 segments (60 s at hls_time=6) are
            # on disk — iOS can stream a growing EVENT playlist and will
            # keep polling for new segments as we remux.
            f"     const ready=d.done||d.segments>=10;"
            f"     if(ready&&!srcSet){{srcSet=true;v.src='{src}';v.load();"
            f"       v.play().catch(()=>{{}});}}"
            f"     if(d.done){{"
            f"       lmsg.textContent='Fertig — wird geladen…';"
            f"     }} else {{"
            f"       const pct=d.total>0?Math.min(99,Math.round(d.segments*100/d.total)):0;"
            f"       lmsg.textContent='Aufnahme wird vorbereitet… '+"
            f"         (d.total>0?pct+' %':(d.segments+' Segmente'));"
            f"       setTimeout(tick,1500);"
            f"     }}"
            f"   }}).catch(()=>setTimeout(tick,2500));"
            f"}}"
            f"tick();"
            f"function fetchAds(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/ads').then(r=>r.json())"
            f"   .then(d=>{{"
            f"     ads=d.ads||[];renderAds();refresh();"
            f"     if(d.running)setTimeout(fetchAds,10000);"
            f"   }}).catch(()=>{{}});"
            f"}}"
            f"fetchAds();"
            f"let thumbMeta={{count:0,interval:30,done:false}};"
            f"const thumbPrev=document.createElement('div');"
            f"thumbPrev.id='thumb-preview';"
            f"const thumbImg=document.createElement('img');"
            f"thumbImg.alt='';thumbImg.decoding='async';"
            f"thumbPrev.appendChild(thumbImg);"
            f"document.body.appendChild(thumbPrev);"
            f"function fetchThumbs(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/thumbs.json').then(r=>r.json())"
            f"   .then(d=>{{thumbMeta=d;if(!d.done)setTimeout(fetchThumbs,8000);}})"
            f"   .catch(()=>setTimeout(fetchThumbs,8000));"
            f"}}"
            f"fetchThumbs();"
            f"let lastThumbIdx=0;"
            f"function showThumb(ev){{"
            f"  if(!thumbMeta.count)return;"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D<=0)return;"
            f"  const t=p*D;"
            f"  const idx=Math.min(thumbMeta.count,"
            f"    Math.max(1,Math.floor(t/thumbMeta.interval)+1));"
            f"  if(idx!==lastThumbIdx){{"
            f"    lastThumbIdx=idx;"
            f"    thumbImg.src='{HOST_URL}/recording/{uuid}/thumbs/t'+"
            f"      String(idx).padStart(5,'0')+'.jpg';"
            f"  }}"
            f"  const thumbW=160;"
            f"  let left=ev.clientX-thumbW/2;"
            f"  left=Math.max(8,Math.min(window.innerWidth-thumbW-8,left));"
            f"  thumbPrev.style.left=left+'px';"
            f"  thumbPrev.style.bottom=(window.innerHeight-r.top+10)+'px';"
            f"  thumbPrev.classList.add('visible');"
            f"}}"
            f"function hideThumb(){{thumbPrev.classList.remove('visible');}}"
            f"scrub.addEventListener('mousemove',showThumb);"
            f"scrub.addEventListener('touchmove',showThumb,{{passive:true}});"
            f"scrub.addEventListener('mouseleave',hideThumb);"
            f"scrub.addEventListener('touchend',hideThumb);"
            f"document.addEventListener('keydown',e=>{{"
            f"  if(e.key==='Escape')closePlayer();"
            f"  else if(e.key==='ArrowRight')seek(10);"
            f"  else if(e.key==='ArrowLeft')seek(-10);"
            f"  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}"
            f"}});"
            # Auto-mark recording watched once playback crosses the
            # AUTO_WATCHED_THRESHOLD fraction. Fires once per page load.
            # The cleanup loop deletes anything with playcount>0 older
            # than WATCHED_AUTO_DELETE_DAYS days.
            f"let _markedWatched=false;"
            f"v.addEventListener('timeupdate',()=>{{"
            f"  if(_markedWatched)return;"
            f"  const D=v.duration;if(!isFinite(D)||D<=0)return;"
            f"  if((v.currentTime||0)/D < {AUTO_WATCHED_THRESHOLD})return;"
            f"  _markedWatched=true;"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/watched',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{watched:true}})}}).catch(()=>{{}});"
            f"}});"
            f"</script></body></html>")
    return html


@app.route("/mediathek-rec/<uuid>/file.mp4")
def mediathek_rec_file(uuid):
    """Serve a ripped Mediathek recording. send_from_directory handles
    HTTP range requests correctly, which iOS needs for MP4 seeking."""
    out_dir = HLS_DIR / f"_{uuid}"
    if not (out_dir / "file.mp4").exists():
        abort(404)
    return send_from_directory(out_dir, "file.mp4",
                                  mimetype="video/mp4",
                                  conditional=True)


@app.route("/mediathek-rec/<uuid>/delete")
def delete_mediathek_rec(uuid):
    """Drop a virtual Mediathek recording from the local list. Also
    cleans up the ripped MP4 if one exists — the whole point of
    "delete" is freeing the disk."""
    with _mediathek_rec_lock:
        _mediathek_rec.pop(uuid, None)
    save_mediathek_rec()
    shutil.rmtree(HLS_DIR / f"_{uuid}", ignore_errors=True)
    return Response("", status=302, headers={"Location": f"{HOST_URL}/recordings"})


@app.route("/recording/<uuid>/delete")
def delete_recording(uuid):
    """Cancel/delete a DVR entry."""
    # Kill any running ffmpeg remux and comskip, remove cached HLS output
    with _rec_hls_lock:
        p = _rec_hls_procs.pop(uuid, None)
    if p and p["proc"].poll() is None:
        try: p["proc"].kill()
        except Exception: pass
    with _rec_cskip_lock:
        c = _rec_cskip_procs.pop(uuid, None)
    if c and c["proc"].poll() is None:
        try: c["proc"].kill()
        except Exception: pass

    # Delete the DVR entry FIRST, then the HLS dir. If we go in the
    # other order and the tvheadend delete silently fails, the next
    # prewarm tick still sees the entry and re-creates the HLS dir
    # we just emptied — orphan resurrects on its own.
    body = urllib.parse.urlencode({"uuid": uuid}).encode()
    for ep in ("/api/dvr/entry/cancel", "/api/dvr/entry/remove"):
        try:
            req = urllib.request.Request(f"{TVH_BASE}{ep}",
                                          data=body, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
    shutil.rmtree(HLS_DIR / f"_rec_{uuid}", ignore_errors=True)
    return Response(status=302, headers={"Location": f"{HOST_URL}/recordings"})


@app.route("/reload")
def reload_channels():
    load_favorites()
    return f"{len(channel_map)} Kanäle geladen\n"


@app.route("/prewarm/<slug>")
def prewarm_channel(slug):
    """Start ffmpeg for channel without waiting for segments — returns
    immediately. Use for background prewarming from the Watch player."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    return _cors(Response(json.dumps({"ok": True, "slug": slug}),
                           mimetype="application/json"))


@app.route("/stop/<slug>")
def stop_channel_endpoint(slug):
    """Immediately kill the ffmpeg for this channel, freeing the tuner."""
    with active_lock:
        running = slug in channels
    if running:
        stop_channel(slug)
        return _cors(Response(json.dumps({"ok": True, "stopped": slug}),
                               mimetype="application/json"))
    return _cors(Response(json.dumps({"ok": True, "stopped": None}),
                           mimetype="application/json"))


@app.route("/stop-all")
def stop_all_endpoint():
    """Free all tuners (stop every running channel)."""
    with active_lock:
        slugs = list(channels.keys())
    for s in slugs:
        stop_channel(s)
    return _cors(Response(json.dumps({"ok": True, "stopped": slugs}),
                           mimetype="application/json"))


@app.route("/static/ch-logos/<fname>")
def static_ch_logo(fname):
    """Serve a bundled / user-supplied channel logo from the repo's
    static/ch-logos/ directory. Falls back to 404 if missing — the
    recordings template probes for file existence before building the
    URL so a 404 here implies drift, not a normal miss."""
    if "/" in fname or ".." in fname:
        abort(400)
    return send_from_directory(CH_LOGO_DIR, fname,
                                 max_age=86400)


def _channel_logo_url(slug, fallback):
    """Return the best channel-logo URL for `slug`. Prefers a local
    override from static/ch-logos/<slug>.(svg|png|jpg) over the
    low-res tvheadend imagecache default. `fallback` may be a full
    URL (already-prefixed icon_public_url) or a relative path like
    "imagecache/108"."""
    if slug:
        for ext in ("svg", "png", "jpg"):
            p = CH_LOGO_DIR / f"{slug}.{ext}"
            if p.is_file():
                return f"{HOST_URL}/static/ch-logos/{slug}.{ext}"
    if not fallback:
        return ""
    if fallback.startswith("http"):
        return fallback
    return f"{HOST_URL}/{fallback.lstrip('/')}"


@app.route("/status")
def status():
    now = time.time()
    with active_lock:
        active = [{"slug": s,
                    "name": channel_map.get(s, {}).get("name", "?"),
                    "idle_seconds": int(now - i["last_seen"]),
                    "buffer_seconds": min(int(now - i.get("started_at", now)),
                                           WINDOW_SECONDS),
                    "always_warm": s in ALWAYS_WARM,
                    "codecs": codec_cache.get(s)}
                   for s, i in channels.items()]
    with codec_lock:
        probed = {s: c for s, c in codec_cache.items()}
    return {"total_channels": len(channel_map),
            "max_warm": MAX_WARM_STREAMS,
            "always_warm": sorted(ALWAYS_WARM),
            "active": active,
            "probed_codecs": probed}


_dvr_upcoming_cache = {"count": 0, "expires": 0}
_tuner_cache = {"used": None, "total": None, "expires": 0}


def tuner_status():
    """Return (used, total, epggrab) for FRITZ!Box SAT>IP tuners.
    `used` counts tuners tuned to any mux, `epggrab` is the subset
    that tvheadend is currently using for EIT collection. Cached 5 s."""
    now = time.time()
    if now < _tuner_cache["expires"]:
        return (_tuner_cache["used"], _tuner_cache["total"],
                _tuner_cache.get("epggrab", 0))
    used, total, epg = None, TUNER_TOTAL, 0
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/status/inputs", timeout=2).read())
        entries = data.get("entries", [])
        total = len(entries) or TUNER_TOTAL
        used = sum(1 for e in entries if e.get("subs", 0) > 0)
    except Exception:
        pass
    try:
        subs = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/status/subscriptions", timeout=2).read())
        epg = sum(1 for e in subs.get("entries", [])
                  if e.get("title", "").lower() == "epggrab")
    except Exception:
        pass
    _tuner_cache["used"] = used
    _tuner_cache["total"] = total
    _tuner_cache["epggrab"] = epg
    _tuner_cache["expires"] = now + 5
    return used, total, epg


_pinned_mux_cache = {"muxes": set(), "running": 0, "expires": 0}


def _mux_from_svc(svc):
    # tvheadend subscription 'service' looks like
    # "SAT>IP DVB-C Tuner #1 (...)/FritzBox DVB-C/546MHz/VOX"
    # The second-to-last slash-separated component is the mux id.
    parts = (svc or "").split("/")
    return parts[-2] if len(parts) >= 2 else None


def pinned_mux_info():
    """Return (distinct_mux_count, running_pin_count) for pinned channels
    with an active tvheadend subscription. Lets us give a mux-sharing
    bonus to the pin limit — two pins on the same transponder cost one
    tuner, not two."""
    now = time.time()
    if now < _pinned_mux_cache["expires"]:
        return len(_pinned_mux_cache["muxes"]), _pinned_mux_cache["running"]
    muxes, running = set(), 0
    with cmap_lock:
        name_to_slug = {info["name"]: s for s, info in channel_map.items()}
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/status/subscriptions", timeout=2).read())
        for e in data.get("entries", []):
            slug = name_to_slug.get(e.get("channel", ""))
            if slug and slug in ALWAYS_WARM:
                running += 1
                m = _mux_from_svc(e.get("service"))
                if m:
                    muxes.add(m)
    except Exception:
        pass
    _pinned_mux_cache["muxes"] = muxes
    _pinned_mux_cache["running"] = running
    _pinned_mux_cache["expires"] = now + 5
    return len(muxes), running


def compute_pin_limit():
    """Tuner-based pin cap. Base = total tuners minus 1 reserved for
    ad-hoc viewing minus active DVR jobs. Bonus = pins that share muxes
    with other pins (they cost a fractional tuner each). Falls back to
    the static PIN_HARD_MAX if tvheadend status is unavailable."""
    used, total, _ = tuner_status()
    total = total or TUNER_TOTAL
    dvr = active_dvr_count()
    base = max(0, total - 1 - dvr)
    distinct_muxes, running_pins = pinned_mux_info()
    mux_bonus = max(0, running_pins - distinct_muxes)
    limit = base + mux_bonus
    # Never retroactively shrink below what's already pinned.
    return max(limit, len(ALWAYS_WARM))


def active_dvr_count():
    """Count of tvheadend DVR entries that are either airing right now
    or scheduled within the next 10 min — these will hold a tuner. We
    cache for 30 s to keep the /warm-status poll lightweight."""
    now = time.time()
    if now < _dvr_upcoming_cache["expires"]:
        return _dvr_upcoming_cache["count"]
    count = 0
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid_upcoming?limit=50",
            timeout=4).read())
        horizon = int(now) + 600
        for e in data.get("entries", []):
            start = e.get("start", 0)
            stop = e.get("stop", 0)
            if stop > now and start <= horizon:
                count += 1
    except Exception:
        pass
    _dvr_upcoming_cache["count"] = count
    _dvr_upcoming_cache["expires"] = now + 30
    return count


@app.route("/api/always-warm/<slug>", methods=["POST"])
def api_always_warm(slug):
    """Toggle permanent-warm on a channel. Body: {"on": true|false}.
    Refuses new pins that would exceed the tuner-derived dynamic cap."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404)
    try:
        body = json.loads(request.get_data() or b"{}")
    except Exception:
        body = {}
    want = bool(body.get("on", True))
    if want and slug not in ALWAYS_WARM:
        pin_limit = compute_pin_limit()
        if len(ALWAYS_WARM) >= pin_limit:
            return Response(json.dumps({
                "slug": slug, "on": False, "changed": False,
                "error": "Pin-Limit erreicht (Tuner-Reserve für DVR)",
            }), status=409, mimetype="application/json")
    changed = set_always_warm(slug, want)
    return _cors(Response(json.dumps({"slug": slug, "on": want,
                                        "changed": changed}),
                           mimetype="application/json"))


@app.route("/api/warm-status")
def api_warm_status():
    """Compact per-channel warm-state for the client (channel grid)."""
    now = time.time()
    with active_lock:
        out = {s: {
            "running": True,
            "idle_seconds": int(now - i["last_seen"]),
            "buffer_seconds": min(int(now - i.get("started_at", now)),
                                    WINDOW_SECONDS),
            "always_warm": s in ALWAYS_WARM,
        } for s, i in channels.items()}
    # Add a single now-playing string per channel — ONE EPG fetch for
    # the whole grid is much cheaper than N /api/now calls from the JS.
    try:
        now_ts = int(now)
        epg = fetch_epg(window_before=0, window_after=0)
        with cmap_lock:
            slugs = list(channel_map.keys())
        for slug in slugs:
            title = None
            for ev in epg["events"].get(slug, []):
                if ev["start"] <= now_ts < ev["stop"]:
                    title = ev.get("title")
                    break
            out.setdefault(slug, {"running": False})["now"] = title
    except Exception:
        pass
    with _dormant_pins_lock:
        dormant_snapshot = set(_dormant_pins)
    for s in ALWAYS_WARM:
        out.setdefault(s, {"running": False, "always_warm": True,
                            "idle_seconds": 0, "buffer_seconds": 0})
        # setdefault only inserts the always_warm flag when the key is
        # NEW. If the channel was already in `out` (because it had a
        # 'now' title or an active session), the flag never got set —
        # the frontend then thought it wasn't pinned, hid the pin
        # icon, and wouldn't let the user toggle it. Set it explicitly.
        out[s]["always_warm"] = True
        out[s]["dormant"] = s in dormant_snapshot
    dvr_busy = active_dvr_count()
    pins_used = len(ALWAYS_WARM)
    pin_limit = compute_pin_limit()
    pin_budget = max(0, pin_limit - pins_used)
    tuners_used, tuners_total, tuners_epggrab = tuner_status()
    ffmpeg_count = 0
    try:
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                comm = (p / "comm").read_text().strip()
                status = (p / "status").read_text()
            except Exception:
                continue
            if comm != "ffmpeg":
                continue
            if "\nState:\tZ" in status:   # ignore zombies
                continue
            ffmpeg_count += 1
    except Exception:
        pass
    # Host metrics — /proc/loadavg, /proc/meminfo and the thermal zone
    # reflect the Pi 5 host even inside the container.
    load1 = None
    try:
        load1 = float(Path("/proc/loadavg").read_text().split()[0])
    except Exception:
        pass
    cpu_count = os.cpu_count() or 4
    mem_total_mb = mem_avail_mb = None
    try:
        meminfo = Path("/proc/meminfo").read_text()
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                mem_total_mb = int(line.split()[1]) // 1024
            elif line.startswith("MemAvailable:"):
                mem_avail_mb = int(line.split()[1]) // 1024
    except Exception:
        pass
    cpu_temp_c = None
    try:
        cpu_temp_c = round(int(Path(
            "/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000, 1)
    except Exception:
        pass
    disk_free_gb = disk_total_gb = None
    try:
        import shutil as _sh
        du = _sh.disk_usage(HLS_DIR)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_total_gb = round(du.total / (1024**3), 1)
    except Exception:
        pass
    return _cors(Response(json.dumps({
        "channels": out,
        "max_warm": MAX_WARM_STREAMS,
        "window_seconds": WINDOW_SECONDS,
        "pin_hard_max": pin_limit,
        "pin_dvr_reserve": dvr_busy,
        "pin_limit": pin_limit,
        "pin_budget": pin_budget,
        "tuners_used": tuners_used,
        "tuners_total": tuners_total,
        "tuners_epggrab": tuners_epggrab,
        "adskip_slug": _live_ads_proc.get("slug"),
        "ffmpeg_count": ffmpeg_count,
        "load1": load1,
        "cpu_count": cpu_count,
        "mem_total_mb": mem_total_mb,
        "mem_avail_mb": mem_avail_mb,
        "cpu_temp_c": cpu_temp_c,
        "disk_free_gb": disk_free_gb,
        "disk_total_gb": disk_total_gb,
    }), mimetype="application/json"))


def _hb_age(filename):
    """Seconds since the named heartbeat file was last touched, or None."""
    try:
        return int(time.time() - (HLS_DIR / filename).stat().st_mtime)
    except Exception:
        return None


@app.route("/api/health")
def api_health():
    """Aggregate health snapshot for the dashboard. Reads heartbeats
    from the SMB share for the Mac scanners and probes per-channel
    state from in-memory caches + the live-ads JSON."""
    now = time.time()
    # Mac-side scanners drop heartbeat files on the SMB share. None
    # = file missing entirely, age in seconds = how long since last
    # touch. Stale > 120 s ≈ "scanner died".
    mac_live_age = _hb_age(".mac-live-comskip-alive")
    mac_rec_age = _hb_age(".mac-comskip-alive")
    # Per-channel scan freshness from .live_ads.json.
    chans = []
    try:
        if LIVE_ADS_FILE.exists():
            d = json.loads(LIVE_ADS_FILE.read_text())
            for slug, v in d.items():
                gen = v.get("generated", 0)
                chans.append({
                    "slug": slug,
                    "ad_count": len(v.get("ads", [])),
                    "scan_age_s": int(now - gen) if gen else None,
                    "latest_seg": v.get("latest_seg"),
                })
    except Exception:
        pass
    chans.sort(key=lambda x: x["slug"])
    # System metrics (subset of what api_warm_status returns).
    cpu_temp_c = None
    try:
        cpu_temp_c = round(int(Path(
            "/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000, 1)
    except Exception:
        pass
    mem_avail_mb = mem_total_mb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_total_mb = int(line.split()[1]) // 1024
            elif line.startswith("MemAvailable:"):
                mem_avail_mb = int(line.split()[1]) // 1024
    except Exception:
        pass
    disk_free_gb = disk_total_gb = None
    try:
        import shutil as _sh
        du = _sh.disk_usage(HLS_DIR)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_total_gb = round(du.total / (1024**3), 1)
    except Exception:
        pass
    load1 = None
    try:
        load1 = round(os.getloadavg()[0], 2)
    except Exception:
        pass
    # Active warm streams.
    with active_lock:
        warm = sorted([s for s in channels.keys()])
    # Recording counts.
    recs_completed = recs_scheduled = recs_watched = 0
    try:
        d = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=500", timeout=5).read())
        for e in d.get("entries", []):
            if "Completed" in (e.get("status") or ""):
                recs_completed += 1
                if (e.get("playcount") or 0) > 0:
                    recs_watched += 1
            elif "Scheduled" in (e.get("status") or "") \
                    or e.get("sched_status") in ("recording",
                                                   "scheduled"):
                recs_scheduled += 1
    except Exception:
        pass
    return _cors(Response(json.dumps({
        "now": int(now),
        "mac_live_scanner_age_s": mac_live_age,
        "mac_rec_scanner_age_s": mac_rec_age,
        "channels": chans,
        "warm": warm,
        "always_warm": sorted(ALWAYS_WARM),
        "cpu_temp_c": cpu_temp_c,
        "mem_avail_mb": mem_avail_mb,
        "mem_total_mb": mem_total_mb,
        "disk_free_gb": disk_free_gb,
        "disk_total_gb": disk_total_gb,
        "load1": load1,
        "recs_completed": recs_completed,
        "recs_scheduled": recs_scheduled,
        "recs_watched": recs_watched,
    }), mimetype="application/json"))


HEALTH_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Health</title>
<style>
:root{--bg:#1a1a1a;--fg:#eee;--muted:#888;--ok:#27ae60;--warn:#f39c12;--err:#e74c3c;--card:#252525;--border:#333}
body{margin:0;padding:16px;background:var(--bg);color:var(--fg);font-family:-apple-system,sans-serif;max-width:900px;margin:0 auto}
h1{margin:0 0 16px;font-size:1.4em}
h2{margin:20px 0 10px;font-size:1.1em;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px}
.lbl{color:var(--muted);font-size:.8em;text-transform:uppercase}
.val{font-size:1.4em;font-weight:600;margin-top:4px}
.ok{color:var(--ok)} .warn{color:var(--warn)} .err{color:var(--err)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:8px;overflow:hidden;margin-top:8px}
th,td{padding:8px 12px;text-align:left;border-bottom:1px solid var(--border);font-size:.95em}
th{background:#2c2c2c;color:var(--muted);font-weight:500;text-transform:uppercase;font-size:.75em}
tr:last-child td{border:0}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:baseline}
.refresh{color:var(--muted);font-size:.8em}
a{color:#5dade2}
</style></head><body>
<h1>System Health <span class="refresh" id="refresh"></span></h1>
<div id="root">Loading…</div>
<script>
function fmtAge(s){if(s==null)return'<span class="err">offline</span>';
  if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';
  return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m';}
function classifyHb(s){if(s==null)return'err';if(s<120)return'ok';if(s<300)return'warn';return'err';}
function pct(part,whole){return whole>0?Math.round(part/whole*100):0;}
async function load(){
  const d=await fetch('/api/health').then(r=>r.json());
  const cells=[];
  cells.push(card('Mac Live-Scanner','<span class="dot '+classifyHb(d.mac_live_scanner_age_s)+'"></span>'+fmtAge(d.mac_live_scanner_age_s),classifyHb(d.mac_live_scanner_age_s)));
  cells.push(card('Mac Rec-Scanner','<span class="dot '+classifyHb(d.mac_rec_scanner_age_s)+'"></span>'+fmtAge(d.mac_rec_scanner_age_s),classifyHb(d.mac_rec_scanner_age_s)));
  cells.push(card('CPU Temp',(d.cpu_temp_c||'?')+'°C',d.cpu_temp_c>=75?'err':d.cpu_temp_c>=65?'warn':'ok'));
  cells.push(card('Load 1m',d.load1||'?',d.load1>=4?'err':d.load1>=2?'warn':'ok'));
  cells.push(card('Memory free',(d.mem_avail_mb||'?')+'M / '+(d.mem_total_mb||'?')+'M',d.mem_avail_mb<256?'err':d.mem_avail_mb<512?'warn':'ok'));
  const dPct=d.disk_free_gb&&d.disk_total_gb?pct(d.disk_free_gb,d.disk_total_gb):0;
  cells.push(card('Disk free',(d.disk_free_gb||'?')+'G / '+(d.disk_total_gb||'?')+'G',dPct<10?'err':dPct<20?'warn':'ok'));
  cells.push(card('Recordings',d.recs_completed+' fertig, '+d.recs_watched+' gesehen, '+d.recs_scheduled+' geplant'));
  cells.push(card('Warm streams',(d.warm.length?d.warm.join(', '):'—')));
  let html='<div class="grid">'+cells.join('')+'</div>';
  html+='<h2>Channel Scanner</h2><table><tr><th>Channel</th><th>Letzter Scan</th><th>Ad-Blöcke</th><th>Latest Seg</th></tr>';
  for(const c of d.channels){
    const age=c.scan_age_s;
    const cls=age==null?'err':age<900?'ok':age<1800?'warn':'err';
    html+='<tr><td>'+c.slug+'</td><td class="'+cls+'">'+fmtAge(age)+'</td><td>'+c.ad_count+'</td><td>'+(c.latest_seg||'—')+'</td></tr>';
  }
  html+='</table>';
  document.getElementById('root').innerHTML=html;
  document.getElementById('refresh').textContent='aktualisiert '+new Date().toLocaleTimeString('de-DE');
}
function card(lbl,val,cls){return '<div class="card"><div class="lbl">'+lbl+'</div><div class="val '+(cls||'')+'">'+val+'</div></div>';}
load();setInterval(load,5000);
</script></body></html>"""


@app.route("/health")
def health_page():
    return Response(HEALTH_HTML, mimetype="text/html")


def _self_exec(reason):
    # Replace this Python process in place. Child ffmpegs (spawned with
    # start_new_session=True and tracked via PID files) remain our
    # children by PID and are re-adopted by the fresh image.
    print(f"self-exec ({reason})", flush=True)
    try: sys.stdout.flush()
    except Exception: pass
    # Close inherited FDs (listening socket on :8080 above all) so the
    # new image can rebind cleanly. Keep stdin/stdout/stderr (0,1,2).
    try: os.closerange(3, 1024)
    except Exception: pass
    os.execv(sys.executable, [sys.executable, "-u", os.path.abspath(__file__)])


def _sighup_reload(signum, frame):
    _self_exec("SIGHUP")


def _file_watcher_loop():
    # Poll service.py mtime; self-exec when the mount picks up a new
    # version. Avoids `docker restart` on hot-reloads so the ffmpeg
    # buffer survives. A broken scp would crash the fresh Python
    # (container dies, cgroup kill, buffer gone) — so compile-check
    # first and skip the reload if the file doesn't parse.
    path = os.path.abspath(__file__)
    try:
        last = os.path.getmtime(path)
    except OSError:
        return
    while True:
        time.sleep(0.5)
        try:
            mt = os.path.getmtime(path)
        except OSError:
            continue
        if mt == last:
            continue
        # Wait a tick for scp to finish writing.
        time.sleep(1)
        try:
            src = open(path, "rb").read()
            compile(src, path, "exec")
        except SyntaxError as e:
            print(f"hot-reload skipped: {path} has SyntaxError "
                  f"at line {e.lineno}: {e.msg}", flush=True)
            last = mt   # don't spam the log every 2 s; wait for next edit
            continue
        except Exception as e:
            print(f"hot-reload skipped: compile failed: {e}", flush=True)
            last = mt
            continue
        _self_exec(f"{path} changed")


if __name__ == "__main__":
    HLS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        COMSKIP_INI.write_text(COMSKIP_INI_TEXT)
    except Exception as e:
        print(f"comskip.ini write: {e}", flush=True)
    # Pre-generate per-channel inis so the Mac scanner sees them on
    # the SMB share even when LIVE_ADS_OFFLOAD disables the Pi-side
    # comskip path that would otherwise lazily create them via
    # comskip_ini_for() at first call.
    for _slug in COMSKIP_INI_PER_CHANNEL:
        comskip_ini_for(_slug)
    load_codec_cache()
    load_stats()
    load_epg_archive()
    load_favorites()
    load_always_warm()
    load_live_ads()
    load_mediathek_rec()
    adopt_surviving_ffmpegs()
    signal.signal(signal.SIGHUP, _sighup_reload)
    threading.Thread(target=_file_watcher_loop, daemon=True).start()
    threading.Thread(target=idle_killer_loop, daemon=True).start()
    threading.Thread(target=prewarm_codecs, daemon=True).start()
    threading.Thread(target=epg_snapshot_loop, daemon=True).start()
    threading.Thread(target=_rec_prewarm_loop, daemon=True).start()
    threading.Thread(target=always_warm_loop, daemon=True).start()
    threading.Thread(target=_live_adskip_loop, daemon=True).start()
    threading.Thread(target=_mediathek_rip_loop, daemon=True).start()
    threading.Thread(target=_mediathek_autorec_loop, daemon=True).start()
    threading.Thread(target=ffmpeg_watchdog_loop, daemon=True).start()
    threading.Thread(target=disk_cleanup_loop, daemon=True).start()
    threading.Thread(target=state_backup_loop, daemon=True).start()
    threading.Thread(target=_cleanup_watched_loop, daemon=True).start()
    _load_epg_meta()
    threading.Thread(target=_enrich_recordings_loop, daemon=True).start()
    # waitress in production; fall back to Flask's built-in only if
    # waitress somehow isn't importable (e.g. an older image).
    try:
        from waitress import serve
        # threads=16 handles our iOS polling load comfortably; ident
        # removed so server banner doesn't leak the version.
        print("serving via waitress on 0.0.0.0:8080", flush=True)
        serve(app, host="0.0.0.0", port=8080, threads=16, ident=None)
    except ImportError:
        print("waitress not installed, falling back to flask dev server",
              flush=True)
        app.run(host="0.0.0.0", port=8080, threaded=True)
