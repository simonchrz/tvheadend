#!/usr/bin/env python3
"""HLS Gateway on-demand for tvheadend. Spawns ffmpeg per channel on request,
stops after idle timeout. Serves an HLS playlist with 2h DVR window."""
import os, re, sys, signal, json, time, shutil, subprocess, threading, urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from flask import Flask, send_from_directory, abort, Response, request

HLS_DIR        = Path("/data/hls")
TVH_BASE       = os.environ.get("TVH_BASE", "http://localhost:9981")
HOST_URL       = os.environ.get("HOST_URL", "http://raspberrypi5lan:8080")
IDLE_TIMEOUT   = int(os.environ.get("IDLE_TIMEOUT", "120"))
MAX_WARM_STREAMS = int(os.environ.get("MAX_WARM_STREAMS", "3"))
WARM_TTL_SECONDS = int(os.environ.get("WARM_TTL_SECONDS", str(2 * 3600)))
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
PIN_HARD_MAX = 3   # tuner-driven upper bound; minus active/scheduled DVR jobs
TUNER_TOTAL = int(os.environ.get("TUNER_TOTAL", "4"))   # FRITZ!Box DVB-C tuners
EPG_SNAPSHOT_INTERVAL = 600   # 10 min
EPG_ARCHIVE_KEEP_DAYS = 14

app = Flask(__name__)
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
ul.channels { list-style: none; padding: 0; }
ul.channels li {
    display: flex; align-items: center; gap: .8em;
    padding: .5em 0; border-bottom: 1px solid var(--border);
}
ul.channels .logo {
    width: 56px; height: 42px; flex: 0 0 56px;
    display: flex; align-items: center; justify-content: center;
    background: var(--logo-bg); border-radius: 6px; overflow: hidden;
}
ul.channels .logo img { max-width: 100%; max-height: 100%; }
ul.channels .meta { flex: 1; min-width: 0; }
ul.channels .meta a { font-weight: 600; }
ul.channels .meta code { display: inline-block; margin-top: 2px; }
.usage { font-size: .8em; color: var(--muted); margin-left: .4em; }
.usage.muted { opacity: .6; }
.ch-actions { font-size: .85em; color: var(--muted); }
.ch-actions a { margin-right: 2px; }

/* ===== EPG layout: two-column flex, only right column scrolls ===== */
.epg-wrap {
    display: flex;
    border: 1px solid var(--border); border-radius: 6px;
    margin: 1em 0;
    overflow: hidden;
}
.epg-channels {
    flex: 0 0 64px;
    background: var(--bg);
    border-right: 1px solid var(--border);
}
.epg-tl-scroll {
    flex: 1 1 auto;
    overflow-x: auto;
    overflow-y: hidden;
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
               background: var(--logo-bg); border-radius: 4px; padding: 2px;
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
            "-vf", "scale='trunc(iw*sar/2)*2':'trunc(ih/2)*2',setsar=1",
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
    """Only stop warm streams that have sat idle longer than the 2h
    DVR window (older content would roll off anyway) — short-term
    idleness is *desirable* (keeps DVR warm for timeshift)."""
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


def always_warm_loop():
    """Background watchdog: make sure each slug in ALWAYS_WARM has a
    live ffmpeg. Re-spawns if the process died. Nudges last_seen so
    LRU ordering keeps these channels ahead of ad-hoc viewers."""
    time.sleep(15)
    while True:
        try:
            now = time.time()
            for slug in list(ALWAYS_WARM):
                with active_lock:
                    info = channels.get(slug)
                    alive = info and info["process"].poll() is None
                if not alive:
                    print(f"[{slug}] always-warm respawn", flush=True)
                    ensure_running(slug)
                else:
                    with active_lock:
                        if slug in channels:
                            channels[slug]["last_seen"] = now
        except Exception as e:
            print(f"always-warm loop: {e}", flush=True)
        time.sleep(30)


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
    MUX_PALETTE = ["#2c3e50", "#27ae60", "#2980b9",
                   "#e91e63", "#f1c40f", "#795548"]
    mux_colour = {}
    for i, mu in enumerate(sorted(
            [m for m, ch in mux_members.items() if len(ch) > 1])):
        mux_colour[mu] = MUX_PALETTE[i % len(MUX_PALETTE)]

    rows = []
    for s, info in items:
        watch_url = f"{HOST_URL}/watch/{s}"
        dvr_url   = f"{HOST_URL}/hls/{s}/dvr.m3u8"
        icon = info.get("icon", "")
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
        rows.append(
            f'<li data-slug="{s}">'
            f'<a class="logo" href="{watch_url}">{logo}</a>'
            f'<div class="meta">'
            f'{mux_dot}'
            f'<a href="{watch_url}">{info["name"]}</a> '
            f'<span class="warm-badge" data-slug="{s}"></span> '
            f'<button class="pin-btn" data-slug="{s}" title="Dauer-warm">📌</button> '
            f'{usage}<br>'
            f'<span class="ch-actions">'
            f'<a href="{watch_url}">⚡ Live</a> · '
            f'<a href="{dvr_url}">📺 Timeshift</a>'
            f'</span></div></li>')
    tools = [
        ("Programm (EPG-Übersicht)",            f"{HOST_URL}/epg"),
        ("Aufnahmen",                           f"{HOST_URL}/recordings"),
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
        ".warm-badge{display:none;font-size:.72em;font-weight:600;"
        "padding:1px 7px;border-radius:10px;vertical-align:2px;"
        "margin-left:4px;color:#fff}"
        ".warm-badge.running{display:inline-block;background:#27ae60;cursor:pointer}"
        ".warm-badge.running.pinned{background:#2980b9;cursor:default}"
        ".pin-btn{background:none;border:0;cursor:pointer;font-size:1em;"
        "opacity:.3;padding:0 4px;vertical-align:1px;transition:opacity .15s}"
        ".pin-btn:hover{opacity:.65}"
        ".pin-btn.active{opacity:1;filter:drop-shadow(0 0 1px #2980b9)}"
        ".tuner-badge{display:inline-block;font-size:.6em;font-weight:600;"
        "padding:3px 10px;border-radius:12px;vertical-align:6px;"
        "margin-left:10px;background:#34495e;color:#fff;letter-spacing:.03em}"
        ".tuner-badge.tight{background:#e67e22}"
        ".tuner-badge.full{background:#c0392b}"
        ".mux-dot{display:inline-block;width:8px;height:8px;"
        "border-radius:50%;margin-right:6px;vertical-align:1px;"
        "cursor:help}"
    )
    js = (
        "async function refreshWarm(){"
        "  try{"
        "    const r=await fetch('/api/warm-status');"
        "    const d=await r.json();"
        "    const W=d.window_seconds||7200;"
        "    for(const el of document.querySelectorAll('.warm-badge')){"
        "      const s=el.dataset.slug;"
        "      const e=d.channels[s];"
        "      if(!e||!e.running){el.className='warm-badge';el.textContent='';}"
        "      else {"
        "        const bs=e.buffer_seconds;"
        "        const full=bs>=W-30;"
        "        let time;"
        "        if(full)time='2h';"
        "        else if(bs>=3600)time=Math.floor(bs/3600)+'h'+Math.floor((bs%3600)/60)+'m';"
        "        else if(bs>=60)time=Math.floor(bs/60)+'m';"
        "        else time=bs+'s';"
        "        el.textContent='● '+time;"
        "        el.className='warm-badge running'+(e.always_warm?' pinned':'');"
        "        el.title=e.always_warm?'Dauer-warm · Puffer '+time+(full?' (voll)':''):"
        "          'Warm-Tuner · Puffer '+time+(full?' (voll)':'')+' · Klick: Tuner freigeben';"
        "      }"
        "    }"
        "    const budget=(d.pin_budget||0);"
        "    for(const b of document.querySelectorAll('.pin-btn')){"
        "      const s=b.dataset.slug;"
        "      const e=d.channels[s];"
        "      const pinned=e&&e.always_warm;"
        "      b.classList.toggle('active',!!pinned);"
        "      if(!pinned&&budget<=0){"
        "        b.style.display='none';"
        "      } else {"
        "        b.style.display='';"
        "      }"
        "      b.title=pinned?'Dauer-warm aktiv — klick zum Deaktivieren':"
        "                     'Kanal dauer-warm halten (max '+(d.pin_limit||0)+' bei '+(d.pin_dvr_reserve||0)+' DVR-Jobs)';"
        "    }"
        "    const tb=document.getElementById('tuner-badge');"
        "    if(tb){"
        "      const u=d.tuners_used,t=d.tuners_total||4;"
        "      if(u===null||u===undefined){tb.textContent='';tb.className='tuner-badge';}"
        "      else{"
        "        tb.textContent='📡 '+u+'/'+t+' Tuner';"
        "        tb.className='tuner-badge'+(u>=t?' full':u>=t-1?' tight':'');"
        "        tb.title='DVB-C Tuner in Nutzung (FRITZ!Box SAT>IP). Kanäle auf demselben Mux teilen sich einen Tuner.';"
        "      }"
        "    }"
        "  }catch(e){}"
        "}"
        "function reorderPinned(){"
        "  const ul=document.querySelector('ul.channels');if(!ul)return;"
        "  const items=Array.from(ul.children);"
        "  const pinned=items.filter(li=>li.querySelector('.pin-btn.active'));"
        "  const rest=items.filter(li=>!li.querySelector('.pin-btn.active'));"
        "  for(const li of pinned)ul.appendChild(li);"
        "  for(const li of rest)ul.appendChild(li);"
        "}"
        "async function togglePin(btn){"
        "  const slug=btn.dataset.slug;"
        "  const on=!btn.classList.contains('active');"
        "  btn.classList.toggle('active',on);"
        "  try{"
        "    await fetch('/api/always-warm/'+slug,{method:'POST',"
        "      headers:{'content-type':'application/json'},"
        "      body:JSON.stringify({on:on})});"
        "    refreshWarm();reorderPinned();"
        "  }catch(e){}"
        "}"
        "document.addEventListener('click',e=>{"
        "  const b=e.target.closest('.pin-btn');"
        "  if(b){e.preventDefault();togglePin(b);return;}"
        "  const w=e.target.closest('.warm-badge.running');"
        "  if(w&&!w.classList.contains('pinned')){"
        "    e.preventDefault();"
        "    const slug=w.dataset.slug;"
        "    if(!slug)return;"
        "    fetch('/stop/'+slug).then(()=>refreshWarm()).catch(()=>{});"
        "  }"
        "});"
        "refreshWarm();setInterval(refreshWarm,5000);"
    )
    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}{extra_css}</style></head>"
            f"<body><h1>HLS Gateway <span id='tuner-badge' class='tuner-badge'></span></h1>"
            f"<h2>Tools</h2><ul class='tools'>{tool_rows}</ul>"
            f"<h2>Kanäle</h2><ul class='channels'>{''.join(rows)}</ul>"
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
            if info.get("icon"):
                attrs.append(f'tvg-logo="{info["icon"]}"')
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
                       "subtitle": e.get("subtitle", "")}
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
        icon = info.get("icon", "")
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
            if eid and not is_past:
                data_attrs += f' data-eid="{eid}"'
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

    grid_html = (f'<div class="epg-wrap" id="epg">'
                 f'<div class="epg-channels">{"".join(channel_col)}</div>'
                 f'<div class="epg-tl-scroll" id="tlscroll">'
                 f'<div class="epg-tl-inner" style="width:{total_px}px">'
                 f'{"".join(tl_rows)}{now_line}</div>'
                 f'</div></div>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f".auto-refresh{{display:inline-flex;align-items:center;gap:.35em;"
            f"font-size:.85em;color:var(--muted);cursor:pointer;user-select:none}}"
            f".auto-refresh input{{accent-color:#1565c0;cursor:pointer}}"
            f"</style></head><body>"
            f"<h1>Programm</h1>"
            f"<p><a href='{HOST_URL}/'>← Kanäle</a> · "
            f"Stand {time.strftime('%H:%M', time.localtime(now_ts))}</p>"
            f"<div class='epg-controls'>"
            f"<a class='btn-now' href='#' onclick='jumpNow();return false'>"
            f"▶︎ Jetzt</a>"
            f"<a class='btn-now' href='#' onclick='jumpPrime();return false'>"
            f"🕗 20:15</a>"
            f"<label class='auto-refresh' title='Seite alle 60 s neu laden'>"
            f"<input type='checkbox' id='auto-refresh'>"
            f"<span>🔄 Auto 60s</span></label>"
            f"<span style='color:var(--muted);font-size:.85em'>"
            f"<a href='?back=6&fwd=12'>6h↞</a> · "
            f"<a href='?back=12&fwd=18'>12h↞</a> · "
            f"<a href='?back=24&fwd=24'>24h↞</a></span>"
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
            f"function handleLP(el){{"
            f"  const ttl=(el.querySelector('.t')||{{}}).textContent||'diese Sendung';"
            # Disable the anchor while the confirm sits. iOS queues the
            # synthesized click behind the modal; by the time Cancel
            # dismisses, our document-level preventDefault is too late
            # and Safari navigates via href. pointer-events:none stops
            # the click entirely.
            f"  el.style.pointerEvents='none';"
            f"  const release=()=>setTimeout(()=>{{"
            f"    el.style.pointerEvents='';"
            f"  }},400);"
            f"  if(el.dataset.uuid){{"
            f"    const ok=confirm('Geplante Aufnahme entfernen?\\n\\n'+ttl);"
            f"    release();"
            f"    if(ok)fetch('{HOST_URL}/cancel-recording/'+el.dataset.uuid)"
            f"        .then(r=>r.json()).then(d=>{{"
            f"          if(d.ok){{el.classList.remove('scheduled');"
            f"            delete el.dataset.uuid;}}"
            f"        }}).catch(()=>{{}});"
            f"  }} else if(el.dataset.eid){{"
            f"    const ok=confirm('Aufnahme planen?\\n\\n'+ttl);"
            f"    release();"
            f"    if(ok)fetch('{HOST_URL}/record-event/'+el.dataset.eid)"
            f"        .then(r=>r.json()).then(d=>{{"
            f"          if(d.ok&&d.uuid){{el.classList.add('scheduled');"
            f"            el.dataset.uuid=d.uuid;}}"
            f"        }}).catch(()=>{{}});"
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
            f"document.addEventListener('click',ev=>{{"
            f"  if(lpFired&&ev.target.closest('.epg-event')){{"
            f"    ev.preventDefault();ev.stopPropagation();lpFired=false;"
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
.spacer{flex:1 1 auto}
.iconbtn{background:#fff2;color:#fff;border:0;width:34px;height:34px;
 border-radius:17px;font-size:1em;cursor:pointer;display:flex;
 align-items:center;justify-content:center;text-decoration:none;
 flex:0 0 auto;line-height:1}
.iconbtn:active{opacity:.6}
.iconbtn:disabled{background:#555;color:#bbb;cursor:default}
.pill{background:#fff2;color:#fff;border:0;padding:7px 12px;
 border-radius:16px;font-weight:600;font-size:.85em;cursor:pointer;
 display:inline-flex;align-items:center;gap:5px;flex:0 0 auto;line-height:1}
.pill:active{opacity:.7}
.pill:disabled{background:#555;color:#bbb;cursor:default}
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
const chrome=document.getElementById('chrome');
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
  chrome.classList.remove('hidden');
  topbar.classList.remove('hidden');
  clearTimeout(_chromeT);
  _chromeT=setTimeout(()=>{
    if(!v.paused){chrome.classList.add('hidden');topbar.classList.add('hidden');}
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
// Scrub-bar drag (mouse + touch). Per-mode seekTo(ev) is hoisted.
let _dragging=false;
scrub.addEventListener('mousedown',e=>{_dragging=true;seekTo(e);show();});
window.addEventListener('mousemove',e=>{if(_dragging){seekTo(e);show();}});
window.addEventListener('mouseup',()=>{_dragging=false;});
scrub.addEventListener('touchstart',e=>{_dragging=true;seekTo(e);show();},{passive:true});
scrub.addEventListener('touchmove',e=>{if(_dragging){seekTo(e);show();}},{passive:true});
scrub.addEventListener('touchend',()=>{_dragging=false;},{passive:true});
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
  width:6px;height:16px;background:#fff;border-radius:2px;
  opacity:.85;box-shadow:0 0 2px #000a;z-index:1;cursor:pointer;
  border:0;padding:0}}
.chapter::before{{content:'';position:absolute;left:-14px;right:-14px;
  top:-12px;bottom:-12px}}
.chapter.current{{background:#ffd84d;width:6px;height:18px}}
.chapter:active{{opacity:.6}}
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
  <button id='liveBtn' class='iconbtn' onclick='goLive()'
   aria-label='Zu Live'>⏭</button>
  <button id='skipad' onclick='skipCurrentAd()'>Werbung ⏭</button>
  <button class='iconbtn' onclick='prevCh()' aria-label='voriger Kanal'>◀︎</button>
  <button class='iconbtn' onclick='nextCh()' aria-label='nächster Kanal'>▶︎</button>
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
  if(which==='mediathek'){{
    const ch=channels[idx];
    const slug=(ch&&ch.slug)||current||'';
    let label='ARD Mediathek';
    if(slug.startsWith('zdf')||slug==='3sat-hd')label='ZDF Mediathek';
    else if(slug==='arte-hd')label='arte.tv';
    else if(slug==='kika-hd')label='KiKA';
    srcBadge.textContent=label;
    srcBadge.classList.add('mediathek');
    srcBadge.classList.remove('at-live');
  }} else {{
    srcBadge.textContent='Live';
    srcBadge.classList.remove('mediathek');
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
function scrubWindow(){{
  const now=Date.now()/1000;
  return [now-WINDOW,now];
}}
function scrubPct(wallTs){{
  const[ws,we]=scrubWindow();
  return Math.max(0,Math.min(100,((wallTs-ws)/(we-ws))*100));
}}
function refresh(){{
  const[s,e]=seekableRange();
  const curWall=wallAt(v.currentTime||0);
  const seekStartW=e>s?wallAt(s):curWall;
  const seekEndW  =e>s?wallAt(e):curWall;
  const availL=scrubPct(seekStartW);
  const availR=scrubPct(seekEndW);
  avail.style.left=availL+'%';
  avail.style.width=(availR-availL)+'%';
  const playL=scrubPct(seekStartW);
  const playR=scrubPct(curWall);
  played.style.left=playL+'%';
  played.style.width=Math.max(0,playR-playL)+'%';
  thumb.style.left=scrubPct(curWall)+'%';
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
v.addEventListener('progress',()=>{{refresh();renderChapters();}});
v.addEventListener('loadeddata',()=>{{refresh();renderChapters();}});
v.addEventListener('canplay',()=>{{refresh();renderChapters();}});
v.addEventListener('loadedmetadata',()=>{{
  if(!onMediathek)goLive();
  refresh();renderChapters();
}});
setInterval(()=>{{if(!v.paused)renderChapters();}},5000);
setInterval(()=>{{if(current)loadLiveAds(current);}},300000);
function goLive(){{
  if(onMediathek){{
    onMediathek=false;
    setSource('live');
    destroyHls();
    v.src=localSrc();v.load();
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
/* Safari HLS exposes getStartDate() = wall-clock time of currentTime=0
   (from #EXT-X-PROGRAM-DATE-TIME). That gives reliable mapping between
   video time and real-world time. Fallback: Date.now() - (end-ct). */
function wallAt(t){{
  if(typeof v.getStartDate==='function'){{
    const d=v.getStartDate();
    if(d&&!isNaN(d.getTime()))return d.getTime()/1000+t;
  }}
  const[,e]=seekableRange();
  return Date.now()/1000-(e-t);
}}
function posForWall(ts){{
  const[ws,we]=scrubWindow();
  if(ts<ws||ts>we)return null;
  return ((ts-ws)/(we-ws))*100;
}}
function wallForCurrent(){{return wallAt(v.currentTime||0);}}
let epgEvents=[];
let mediathekAvailable=false;
let mediathekWindow=0;
function isReachable(ev){{
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
function renderChapters(){{
  document.querySelectorAll('.chapter').forEach(el=>el.remove());
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
  fetch(HOST+'/api/mediathek-live/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    mediathekAvailable=!!d.url;
    mediathekWindow=d.window||0;
    renderChapters();
  }}).catch(()=>{{mediathekAvailable=false;mediathekWindow=0;}});
}}
/* EPG start times are when the broadcast SLOT begins, but actual
   content (after station ID / trailers) often starts 10-30 s later.
   Seek a bit before the EPG start so we never miss the opener. ZDF
   tends to have longer slot-padding than ARD, so give it more room. */
function eventLeadIn(slug){{
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
  if(wallS!==null&&targetWall>=wallS){{
    v.currentTime=Math.max(s+0.5,Math.min(e-1,targetWall-wallS));
    show();return;
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
function jumpToEvent(ev){{
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
function goShowStart(){{
  const ev=currentEvent();
  if(!ev)return;
  const[s,e]=seekableRange();
  const wallS=wallAt(s);
  const offset=ev.start-wallS;
  if(offset>=0&&e>s){{
    v.currentTime=Math.max(s+0.5,Math.min(e-1,offset));show();return;
  }}
  /* Local buffer doesn't reach back that far — try public
     Mediathek HLS as a fallback. */
  fetch(HOST+'/api/mediathek-live/'+current)
   .then(r=>r.json()).then(d=>{{
     if(!d.url){{
       hint.textContent='Sendungsanfang außerhalb Puffer';
       hint.classList.add('show');
       setTimeout(()=>hint.classList.remove('show'),2000);
       return;
     }}
     switchToMediathek(d.url,ev.start);
   }}).catch(()=>{{}});
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
function renderLiveAds(){{
  document.querySelectorAll('.ad-block').forEach(el=>el.remove());
  for(const[wStart,wStop]of liveAds){{
    const left=posForWall(wStart);
    const right=posForWall(wStop);
    if(left===null&&right===null)continue;
    const L=(left===null?0:left);
    const R=(right===null?100:right);
    if(R<=L)continue;
    const el=document.createElement('div');
    el.className='ad-block';
    el.style.left=L+'%';el.style.width=(R-L)+'%';
    scrub.appendChild(el);
  }}
}}
function currentLiveAd(){{
  const w=wallForCurrent();
  for(const[a,b]of liveAds){{if(w>=a&&w<b)return[a,b];}}
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
  const[s,e]=seekableRange();
  if(e<=s)return;
  /* Clamp target into the seekable window, then translate wall→video time */
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
  /* iOS live-HLS ignores pause(). Unconditionally strip the src to
     stop playback; remember wall-time so resume can seek back. */
  pauseState={{
    pausedSrc:onMediathek?null:v.src,
    pausedMediathekUrl:onMediathek?hlsCurrentUrl:null,
    pausedWall:wallAt(v.currentTime),
    pausedCurrent:v.currentTime
  }};
  v.pause();
  if(onMediathek&&hlsInst){{
    try{{hlsInst.destroy();}}catch(e){{}}
    hlsInst=null;hlsCurrentUrl=null;
  }}
  v.removeAttribute('src');
  v.load();
  pp.textContent='\u25B6';
  /* Defensive: some events (autoplay attempt after load-empty) may
     flip the icon back — re-set after the next event loop tick. */
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
  tryPlay();
}}
document.addEventListener('click',onInteract);
document.addEventListener('touchend',()=>setTimeout(onInteract,30),
                          {{passive:true}});

function loadSrc(slug){{
  current=slug;onMediathek=false;
  setSource('live');
  destroyHls();
  const url=HOST+'/hls/'+slug+'/dvr.m3u8';
  v.muted=false;v.src=url;v.load();
  v.addEventListener('loadedmetadata',tryPlay,{{once:true}});
  const ch=channels[idx];
  if(ch){{
    chname.textContent=ch.name;
  }}
  history.replaceState(null,'','/watch/'+slug);
  loadEpg(slug);
  loadEvents(slug);
  loadMediathekAvail(slug);
  loadLiveAds(slug);
  showHint(slug);
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
  if(chrome.classList.contains('hidden'))show();
  else {{chrome.classList.add('hidden');topbar.classList.add('hidden');}}
}});
document.addEventListener('mousemove',show);

loadSrc(current);
fetch(HOST+'/api/channels').then(r=>r.json()).then(d=>{{
  channels=d.channels;
  idx=channels.findIndex(c=>c.slug===current);
  if(idx<0)idx=0;
}});
</script></body></html>"""


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
    """Transparently proxy ARD's master playlist, rewriting variant
    URIs to point back through us. All sub-playlists + segments also
    flow through our origin to avoid iOS cross-origin quirks."""
    info = MEDIATHEK_LIVE.get(slug)
    if not info:
        abort(404)
    base_url = info[0]
    try:
        txt = urllib.request.urlopen(base_url, timeout=8).read().decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    base = base_url.rsplit("/", 1)[0] + "/"
    strip_audio = request.args.get("no_audio") == "1"
    out_lines = []
    for line in txt.splitlines():
        stripped = line.strip()
        if strip_audio and 'TYPE=AUDIO' in stripped:
            continue
        if strip_audio and 'AUDIO=' in stripped:
            stripped = re.sub(r',?AUDIO="[^"]+"', '', stripped)
        if stripped.startswith("#EXT-X-MEDIA") and "URI=" in stripped:
            def repl(m):
                u = m.group(1)
                if not u.startswith("http"):
                    u = base + u
                proxied = (f"{HOST_URL}/mediathek-passthru/{slug}/pl.m3u8?"
                           f"u={urllib.parse.quote(u, safe='')}")
                return f'URI="{proxied}"'
            out_lines.append(re.sub(r'URI="([^"]+)"', repl, stripped))
            continue
        if stripped and not stripped.startswith("#") and ".m3u8" in stripped:
            u = stripped
            if not u.startswith("http"):
                u = base + u
            proxied = (f"{HOST_URL}/mediathek-passthru/{slug}/pl.m3u8?"
                       f"u={urllib.parse.quote(u, safe='')}")
            out_lines.append(proxied)
            continue
        out_lines.append(line)
    return Response("\n".join(out_lines) + "\n",
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
                          "icon": info.get("icon", "")} for s, info in items]}


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
    """List of recordings (ongoing + finished) with links."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=200&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")

    now_ts = int(time.time())
    rows = []
    for e in data.get("entries", []):
        uuid = e.get("uuid", "")
        title = e.get("disp_title", "?")
        start = e.get("start", 0)
        stop  = e.get("stop", 0)
        status = e.get("status", "")
        size = e.get("filesize", 0) or 0
        is_live = start <= now_ts < stop and "Recording" in status
        is_done = now_ts >= stop or "Completed" in status
        if is_live:
            badge = '<span class="badge live">● LIVE</span>'
        elif is_done:
            badge = '<span class="badge done">✓ fertig</span>'
        else:
            badge = f'<span class="badge scheduled">⏱ geplant</span>'
        when = time.strftime("%d.%m %H:%M", time.localtime(start))
        dur_min = max(0, (stop - start) // 60)
        size_mb = size / (1024 * 1024) if size else 0
        size_str = f"{size_mb:.0f} MB" if size_mb > 0 else "—"
        # Only link playable entries — scheduled future recordings aren't yet.
        if is_live or is_done:
            play_url = f"{HOST_URL}/recording/{uuid}"
            title_cell = f'<a href="{play_url}">{title}</a>'
        else:
            title_cell = f'<span>{title}</span>'
        # Prewarm state: ready / in progress / not started
        if is_done:
            playlist = HLS_DIR / f"_rec_{uuid}" / "index.m3u8"
            proc_info = _rec_hls_procs.get(uuid)
            running = proc_info is not None and proc_info["proc"].poll() is None
            if playlist.exists() and not running:
                prewarm = '<span class="badge ready" title="sofort abspielbar">▶ bereit</span>'
            elif running:
                total = (proc_info or {}).get("total_segs", 0)
                segs = 0
                if playlist.exists():
                    try: segs = playlist.read_text().count(".ts")
                    except Exception: pass
                pct = (int(segs * 100 / total)
                       if total > 0 else 0)
                prewarm = (f'<span class="badge warming" '
                           f'title="Remux läuft">⏳ {pct}%</span>')
            else:
                prewarm = '<span class="badge pending" title="noch nicht remuxt">◌ ausstehend</span>'
        else:
            prewarm = ""
        rows.append(
            f'<tr><td>{badge}</td>'
            f'<td>{title_cell}</td>'
            f'<td>{when}</td>'
            f'<td>{dur_min} min</td>'
            f'<td>{size_str}</td>'
            f'<td>{prewarm}</td>'
            f'<td><a href="{HOST_URL}/recording/{uuid}/delete" '
            f'onclick="return confirm(\'Löschen?\')">🗑</a></td></tr>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f".badge{{font-size:.75em;padding:2px 6px;border-radius:3px;"
            f"font-weight:600;display:inline-block}}"
            f".badge.live{{background:#e74c3c;color:#fff}}"
            f".badge.done{{background:#27ae60;color:#fff}}"
            f".badge.scheduled{{background:var(--stripe);color:var(--muted)}}"
            f".badge.ready{{background:#2980b9;color:#fff}}"
            f".badge.warming{{background:#f39c12;color:#fff}}"
            f".badge.pending{{background:var(--stripe);color:var(--muted)}}"
            f"</style></head><body>"
            f"<h1>Aufnahmen</h1>"
            f"<p><a href='{HOST_URL}/'>← Kanäle</a></p>"
            f"<table><tr><th></th><th>Titel</th><th>Start</th>"
            f"<th>Dauer</th><th>Größe</th><th>Cache</th><th></th></tr>"
            f"{''.join(rows) if rows else '<tr><td colspan=7>Keine Aufnahmen</td></tr>'}"
            f"</table></body></html>")
    return body


_rec_hls_lock = threading.Lock()
_rec_hls_procs = {}  # uuid -> {"proc": Popen, "started": ts, "total_segs": int}
_rec_cskip_lock = threading.Lock()
_rec_cskip_procs = {}  # uuid -> {"proc": Popen, "started": ts}


def _rec_source_path(uuid):
    """Return the file path of the DVR recording inside this container
    (via the /recordings read-only mount). None if not found."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/dvr/entry/grid?limit=400",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                fn = e.get("filename") or ""
                if fn:
                    return fn
    except Exception:
        pass
    return None


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
        cmd = ["nice", "-n", "15", "comskip",
               "--output", str(out_dir),
               "--quiet", src]
        proc = subprocess.Popen(cmd,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        _rec_cskip_procs[uuid] = {"proc": proc, "started": time.time()}
        print(f"[rec-cskip {uuid[:8]}] spawned", flush=True)


def _rec_parse_comskip(out_dir):
    """Parse comskip's default .txt output → list of [start_s, stop_s]."""
    txts = list(out_dir.glob("*.txt"))
    if not txts:
        return []
    try:
        lines = txts[0].read_text().splitlines()
    except Exception:
        return []
    fps = 25.0
    ads = []
    for line in lines:
        line = line.strip()
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
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            a = float(parts[0]) / fps
            b = float(parts[1]) / fps
            if b - a >= 10:   # ignore stub blocks shorter than 10 s
                ads.append([round(a, 2), round(b, 2)])
        except Exception:
            continue
    return ads


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
        src = f"{TVH_BASE}/dvrfile/{uuid}"
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
                      "scale='trunc(iw*sar/2)*2':'trunc(ih/2)*2',setsar=1",
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

        def _after_ffmpeg():
            proc.wait()
            _rec_cskip_spawn(uuid)
        threading.Thread(target=_after_ffmpeg, daemon=True).start()
    return playlist


_live_ads_lock = threading.Lock()
_live_ads = {}   # slug -> {"generated": ts, "ads": [[wall_start, wall_stop], ...]}
_live_ads_proc = {"slug": None, "started": 0}
LIVE_ADS_FILE = HLS_DIR / ".live_ads.json"


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


def _live_ads_analyze(slug):
    """Concatenate the current HLS segments of a live channel, run
    comskip to detect commercial blocks, and store the result keyed
    by wall-clock seconds (so results stay valid as segments roll)."""
    ch_dir = HLS_DIR / slug
    playlist = ch_dir / "index.m3u8"
    if not playlist.exists():
        return False
    # Anchor wall time: parse the first #EXT-X-PROGRAM-DATE-TIME. ffmpeg
    # writes one per segment, between the EXTINF line and the .ts name,
    # so we just scan until we find the first occurrence.
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
    # Concatenating the full 1-2 h buffer blows past the 180 s ffmpeg
    # timeout (thousands of file-open syscalls even with -c copy). 30
    # min is plenty to spot a commercial pattern; we shift the PDT
    # anchor to the first retained segment (segments are 1 s each).
    WINDOW_SEGS = 1800
    segs = all_segs[-WINDOW_SEGS:] if len(all_segs) > WINDOW_SEGS else all_segs
    first_pdt += (len(all_segs) - len(segs)) * SEGMENT_TIME
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
    # comskip
    try:
        subprocess.run(
            ["nice", "-n", "15", "comskip", "--quiet",
             "--output", str(work), str(merged)],
            timeout=600, check=False)
    except Exception as e:
        print(f"[{slug}] adskip comskip: {e}", flush=True)
    ads_sec = _rec_parse_comskip(work)
    merged.unlink(missing_ok=True)
    # Convert frame-based seconds (relative to concat start) to wall
    ads_wall = [[round(first_pdt + a, 1), round(first_pdt + b, 1)]
                for a, b in ads_sec]
    with _live_ads_lock:
        _live_ads[slug] = {"generated": time.time(), "ads": ads_wall}
    save_live_ads()
    print(f"[{slug}] adskip: {len(ads_wall)} blocks found", flush=True)
    return True


# Only scan channels where commercials are plausible — public
# broadcasters have no regular ads primetime.
LIVE_ADSKIP_SLUGS = {
    "prosieben", "sat-1", "kabel-eins", "kabeleins", "prosiebenmaxx",
    "rtl", "vox", "rtlzwei", "nitro", "rtlup", "superrtl",
    "tele-5", "tele5", "sport1", "dmax", "sixx",
}


def _live_adskip_loop():
    # Short warm-up: threads and adopted ffmpegs settle in a few seconds.
    # The per-channel `started_at > 120s` check below is what actually
    # guarantees the buffer is long enough to bother analysing.
    time.sleep(10)
    while True:
        try:
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
                      f"({len(todo)-1} more queued)", flush=True)
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
    """Force an ad-detection pass on this channel's current live
    buffer (bypasses the 12-min cache). Useful for testing."""
    if slug not in LIVE_ADSKIP_SLUGS:
        return _cors(Response(json.dumps({"skipped": True,
                                           "reason": "not in commercial list"}),
                               mimetype="application/json"))
    def run():
        try:
            _live_ads_analyze(slug)
        except Exception as e:
            print(f"[{slug}] manual adskip: {e}", flush=True)
    threading.Thread(target=run, daemon=True).start()
    return _cors(Response(json.dumps({"started": True, "slug": slug}),
                           mimetype="application/json"))


@app.route("/api/live-ads/<slug>")
def api_live_ads(slug):
    with _live_ads_lock:
        v = _live_ads.get(slug)
    if not v:
        return _cors(Response(json.dumps({"ads": [], "generated": 0}),
                               mimetype="application/json"))
    return _cors(Response(json.dumps({
        "ads": v["ads"],
        "generated": int(v["generated"]),
    }), mimetype="application/json"))


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


def _rec_prewarm_once():
    # Skip while any live stream is running — CPU goes to live
    with active_lock:
        if channels:
            return
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
    for e in data.get("entries", []):
        uuid = e.get("uuid")
        if not uuid or not e.get("filename"):
            continue
        out_dir = HLS_DIR / f"_rec_{uuid}"
        playlist = out_dir / "index.m3u8"
        if not playlist.exists():
            print(f"[rec-prewarm] remuxing {uuid[:8]} "
                  f"({e.get('disp_title', '?')[:40]})", flush=True)
            _rec_hls_spawn(uuid)
            return  # one per cycle
        # HLS already present — fill in the ad markers if missing
        if not list(out_dir.glob("*.txt")):
            cskip_info = _rec_cskip_procs.get(uuid)
            if not (cskip_info and cskip_info["proc"].poll() is None):
                print(f"[rec-prewarm] comskip {uuid[:8]}", flush=True)
                _rec_cskip_spawn(uuid)
                return


def _rec_playlist_as_vod(text):
    """Convert an EVENT-type playlist into VOD with ENDLIST so iOS
    shows a scrub bar and starts from the beginning instead of live-edge."""
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
    call; returns 202 while remux is running so the player UI can keep
    showing a loader instead of blocking on a slow HTTP request."""
    st = _rec_state(uuid)
    if not st["playlist"].exists() and not st["running"]:
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    if not st["done"]:
        return Response(json.dumps({"done": False,
                                     "segments": st["segments"],
                                     "total": st["total"]}),
                        status=202, mimetype="application/json")
    return Response(_rec_playlist_as_vod(st["playlist"].read_text()),
                    mimetype="application/vnd.apple.mpegurl")


@app.route("/recording/<uuid>/ads")
def recording_ads(uuid):
    """Commercial-block markers for the scrub bar. { ads: [[s,e], …] }."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    cskip_info = _rec_cskip_procs.get(uuid)
    running = cskip_info is not None and cskip_info["proc"].poll() is None
    ads = _rec_parse_comskip(out_dir) if out_dir.exists() else []
    return _cors(Response(json.dumps({"ads": ads, "running": running}),
                           mimetype="application/json"))


@app.route("/recording/<uuid>/progress")
def recording_hls_progress(uuid):
    """Lightweight progress poll for the player loader. Also kicks off
    ffmpeg if it hasn't started yet (so the player can just poll)."""
    st = _rec_state(uuid)
    if not st["playlist"].exists() and not st["running"]:
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    return _cors(Response(json.dumps({"done": st["done"],
                                        "segments": st["segments"],
                                        "total": st["total"]}),
                           mimetype="application/json"))


@app.route("/recording/<uuid>")
def play_recording(uuid):
    """Player page for a DVR recording — wraps tvheadend's dvrfile in
    a styled <video> so iOS Safari doesn't force native fullscreen."""
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
            f"<span id='cur' class='time'>0:00</span>"
            f"<span class='spacer'></span>"
            f"<button id='skipad' class='pill rec' onclick='skipAd()' "
            f"style='display:none'>Werbung ⏭</button>"
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
            f"  played.style.width=pct+'%';"
            f"  thumb.style.left=pct+'%';"
            f"  skipBtn.style.display=currentAd()!==null?'inline-flex':'none';"
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
            f"  if(chrome.classList.contains('hidden'))show();"
            f"  else {{chrome.classList.add('hidden');"
            f"    topbar.classList.add('hidden');}}"
            f"}});"
            f"document.addEventListener('mousemove',show);"
            f"const hideLoader=()=>loader.classList.add('hidden');"
            f"v.addEventListener('playing',hideLoader);"
            f"v.addEventListener('loadedmetadata',hideLoader);"
            f"v.addEventListener('canplay',hideLoader);"
            f"let srcSet=false;"
            f"function tick(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/progress').then(r=>r.json())"
            f"   .then(d=>{{"
            f"     if(d.done){{"
            f"       if(!srcSet){{srcSet=true;v.src='{src}';v.load();"
            f"         v.play().catch(()=>{{}});}}"
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
            f"document.addEventListener('keydown',e=>{{"
            f"  if(e.key==='Escape')closePlayer();"
            f"  else if(e.key==='ArrowRight')seek(10);"
            f"  else if(e.key==='ArrowLeft')seek(-10);"
            f"  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}"
            f"}});"
            f"</script></body></html>")
    return html


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
    shutil.rmtree(HLS_DIR / f"_rec_{uuid}", ignore_errors=True)

    body = urllib.parse.urlencode({"uuid": uuid}).encode()
    for ep in ("/api/dvr/entry/cancel", "/api/dvr/entry/remove"):
        try:
            req = urllib.request.Request(f"{TVH_BASE}{ep}",
                                          data=body, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
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
    """Return (used, total) for FRITZ!Box SAT>IP tuners. tvheadend's
    /api/status/inputs lists every configured tuner; `subs > 0` marks
    the ones currently tuned to a mux (channels on the same mux share
    one tuner). Cached 5 s."""
    now = time.time()
    if now < _tuner_cache["expires"]:
        return _tuner_cache["used"], _tuner_cache["total"]
    used, total = None, TUNER_TOTAL
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TVH_BASE}/api/status/inputs", timeout=2).read())
        entries = data.get("entries", [])
        total = len(entries) or TUNER_TOTAL
        used = sum(1 for e in entries if e.get("subs", 0) > 0)
    except Exception:
        pass
    _tuner_cache["used"] = used
    _tuner_cache["total"] = total
    _tuner_cache["expires"] = now + 5
    return used, total


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
    used, total = tuner_status()
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
    for s in ALWAYS_WARM:
        out.setdefault(s, {"running": False, "always_warm": True,
                            "idle_seconds": 0, "buffer_seconds": 0})
    dvr_busy = active_dvr_count()
    pins_used = len(ALWAYS_WARM)
    pin_limit = compute_pin_limit()
    pin_budget = max(0, pin_limit - pins_used)
    tuners_used, tuners_total = tuner_status()
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
    }), mimetype="application/json"))


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
        time.sleep(2)
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
    load_codec_cache()
    load_stats()
    load_epg_archive()
    load_favorites()
    load_always_warm()
    load_live_ads()
    adopt_surviving_ffmpegs()
    signal.signal(signal.SIGHUP, _sighup_reload)
    threading.Thread(target=_file_watcher_loop, daemon=True).start()
    threading.Thread(target=idle_killer_loop, daemon=True).start()
    threading.Thread(target=prewarm_codecs, daemon=True).start()
    threading.Thread(target=epg_snapshot_loop, daemon=True).start()
    threading.Thread(target=_rec_prewarm_loop, daemon=True).start()
    threading.Thread(target=always_warm_loop, daemon=True).start()
    threading.Thread(target=_live_adskip_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080, threaded=True)
