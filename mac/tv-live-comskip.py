#!/Library/Developer/CommandLineTools/usr/bin/python3
"""Mac-side live-buffer ad detection for the Pi's HLS streams.

Drop-in replacement for the _live_adskip_loop + _live_ads_analyze
path in simonchrz/tvheadend/hls-gateway/service.py. Instead of
re-pulling the whole 30-min scan window over SMB on every cycle
(which saturates the USB-2 link to the SSD on the Pi and trips
ext4 into shutdown mode under concurrent DVB writes), this version:

  - reads the .m3u8 over SMB (one small file, no Flask round-trip)
  - fetches segments via Caddy's file_server fast path on :8443
    (bypasses Flask, so doesn't bump channels[slug]["last_seen"]
    and keep an idle ffmpeg artificially warm)
  - keeps a local /tmp segment cache per slug — only fetches
    segments that aren't already cached (after warmup, ~30 new
    segments per scan instead of all 1800)
  - concatenates from local SSD (fast), runs comskip, writes
    .live_ads.json back to SMB

Logic ported from service.py. Keep this file in lockstep:
  parse_comskip ← _rec_parse_comskip
  blackframe_extend_ads ← _blackframe_extend_ads
  analyze ← _live_ads_analyze
  main_loop ← _live_adskip_loop
"""

import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

HLS_DIR         = Path.home() / "mnt" / "pi-tv" / "hls"
COMSKIP         = Path.home() / "src" / "Comskip" / "comskip"
WORK_DIR        = Path("/tmp/tv-live-scan")
LIVE_ADS_FILE   = HLS_DIR / ".live_ads.json"
CADDY_BASE      = "https://raspberrypi5lan:8443"
SEGMENT_TIME    = 1
WINDOW_SECONDS  = 2 * 3600
ACTIVE_MTIME_S  = 60
MIN_BUFFER_SEGS = 120
WINDOW_SIZE     = 1800
RESCAN_SECONDS  = 720
LOOP_SLEEP      = 30
HTTP_TIMEOUT    = 30

# Caddy's tls-internal CA isn't in the system trust store. We're on the
# LAN, talking to a known IP — disable verification rather than wire up
# the cert for one consumer.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode   = ssl.CERT_NONE

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
# the skip button that late. Witnessed:
#   rtlzwei 12:13:38 vs 12:13:57 detection = 19 s
#   rtlzwei 12:39:19 vs 12:39:38 detection = 19 s
START_LAG_FALLBACK = {
    "rtlzwei": 20.0,
}

LIVE_ADSKIP_SLUGS = {
    "prosieben", "sat-1", "kabel-eins", "kabeleins", "prosiebenmaxx",
    "rtl", "vox", "rtlzwei", "nitro", "rtlup", "superrtl",
    "tele-5", "tele5", "sport1", "dmax", "sixx",
}


def log(msg):
    print(f"{datetime.now().strftime('%F %T')} {msg}", flush=True)


def parse_pdt(s):
    """Parse a #EXT-X-PROGRAM-DATE-TIME value → Unix timestamp.
    Python 3.9's fromisoformat() is picky: `+0200` (no colon) and
    `Z` suffix both fail; 3.11+ handles them natively. Normalize
    here so this works on /usr/bin/python3 (macOS ships 3.9)."""
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    s = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', s)
    return datetime.fromisoformat(s).timestamp()


def load_live_ads():
    if not LIVE_ADS_FILE.exists():
        return {}
    try:
        return json.loads(LIVE_ADS_FILE.read_text())
    except Exception as e:
        log(f"load live-ads: {e}")
        return {}


def save_live_ads(state):
    """Atomic write so the Pi never reads a half-written file."""
    try:
        tmp = LIVE_ADS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(LIVE_ADS_FILE)
    except Exception as e:
        log(f"save live-ads: {e}")


def parse_comskip(out_dir):
    """Parse comskip's default .txt output → list of [start_s, stop_s].
    Adjacent blocks separated by a short sponsor card (≤25 s of "show")
    are merged."""
    txts = list(out_dir.glob("*.txt"))
    if not txts:
        return []
    try:
        lines = txts[0].read_text().splitlines()
    except Exception:
        return []
    fps = 25.0
    ads = []
    # Comskip's .txt occasionally writes a long run of NUL bytes
    # before the first frame-range line. After str.strip() those NULs
    # stay — match the trailing frame pair via regex so the leading
    # garbage doesn't break parsing.
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
            if b - a >= 10:
                ads.append([round(a, 2), round(b, 2)])
        except Exception:
            continue
    seen = set()
    dedup = []
    for a, b in ads:
        key = (round(a, 1), round(b, 1))
        if key not in seen:
            seen.add(key)
            dedup.append([a, b])
    dedup.sort(key=lambda x: x[0])
    MERGE_GAP = 25.0
    merged = []
    for a, b in dedup:
        if merged and a - merged[-1][1] <= MERGE_GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def blackframe_extend_ads(video_path, ads, channel_slug=None,
                           sponsor_duration=None, max_extend=None):
    """ffmpeg blackdetect-based post-processor for comskip output.
    Three cases after each ad-end:
      (1) two blacks → [ad]·(black)·[sponsor]·(black)·[show] — extend to 2nd.
      (2) one black near end → [ad]·(black)·[sponsor]·hardcut — extend by sponsor_duration.
      (3) no black → no extension.
    Plus a backward-extension for ad-start if a black sits within 25 s before."""
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
    # We snap back to the EARLIEST blackframe in the window — 75 s
    # catches both: BF2 (~30-40 s back) when no promo, BF1 (~50-65 s
    # back) when there is one. Safely smaller than
    # min_show_segment_length=120 s so we won't pull a blackframe
    # from inside the show.
    START_SCAN = 75.0
    START_MAX_EXTEND = 75.0
    out = []
    for start, end in ads:
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


def channel_is_active(slug):
    """Cheap activity check: m3u8 mtime via SMB stat. No content read."""
    m3u = HLS_DIR / slug / "index.m3u8"
    if not m3u.exists():
        return False
    try:
        return (time.time() - m3u.stat().st_mtime) < ACTIVE_MTIME_S
    except Exception:
        return False


def parse_playlist(text):
    """Extract first PROGRAM-DATE-TIME and ordered segment names from
    an HLS m3u8. Discontinuity markers don't reset our PDT — we always
    anchor on the first PDT and assume SEGMENT_TIME-second segments
    (matches the Pi's ffmpeg config: `-hls_time 1`)."""
    first_pdt = None
    seg_names = []
    for line in text.splitlines():
        line = line.strip()
        if first_pdt is None and line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            try:
                first_pdt = parse_pdt(line.split(":", 1)[1])
            except Exception:
                pass
        elif line and not line.startswith("#"):
            seg_names.append(line)
    return seg_names, first_pdt


def fetch_segment_http(slug, name, dest):
    """Pull one segment from Caddy's file_server (no Flask = no
    last_seen bump). Returns bytes written, raises on failure."""
    url = f"{CADDY_BASE}/hls/{slug}/{name}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, context=_SSL_CTX, timeout=HTTP_TIMEOUT) as r:
        with open(dest, "wb") as f:
            shutil.copyfileobj(r, f, length=256 * 1024)
    return dest.stat().st_size


def analyze(slug, state, window_end_seg=None, window_size=WINDOW_SIZE):
    """Incremental + HTTP-fetch port of _live_ads_analyze."""
    m3u8 = HLS_DIR / slug / "index.m3u8"
    if not m3u8.exists():
        return False
    try:
        text = m3u8.read_text()
    except Exception as e:
        log(f"[{slug}] m3u8 read: {e}")
        return False

    all_segs, first_pdt = parse_playlist(text)
    if first_pdt is None:
        log(f"[{slug}] skip: no PROGRAM-DATE-TIME in playlist")
        return False
    if len(all_segs) < MIN_BUFFER_SEGS:
        return False

    if window_end_seg is not None:
        try:
            end_idx = min(all_segs.index(window_end_seg) + 1, len(all_segs))
        except ValueError:
            end_idx = len(all_segs)
    else:
        end_idx = len(all_segs)
    start_idx = max(0, end_idx - window_size)
    window = all_segs[start_idx:end_idx]
    latest_name = window[-1] if window else all_segs[-1]

    if window_end_seg is None:
        prev = state.get(slug) or {}
        if prev.get("latest_seg") == latest_name:
            return False

    window_first_pdt = first_pdt + start_idx * SEGMENT_TIME

    # ---- segment cache: only fetch what we don't already have ----
    cache_dir = WORK_DIR / slug / "segs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached = {p.name for p in cache_dir.glob("seg_*.ts")}
    needed = set(window)
    to_fetch = [n for n in window if n not in cached]
    to_delete = cached - needed

    for n in to_delete:
        try:
            (cache_dir / n).unlink()
        except Exception:
            pass

    fetched_n = 0
    fetched_bytes = 0
    fetch_t0 = time.time()
    for n in to_fetch:
        try:
            fetched_bytes += fetch_segment_http(slug, n, cache_dir / n)
            fetched_n += 1
        except Exception as e:
            log(f"[{slug}] fetch {n}: {e}")
    fetch_dt = time.time() - fetch_t0

    # ---- concat from local cache ----
    work = WORK_DIR / slug
    for f in work.glob("*.txt"):
        try:
            f.unlink()
        except Exception:
            pass
    merged = work / "merged.ts"
    merged.unlink(missing_ok=True)
    try:
        with open(merged, "wb") as out:
            for n in window:
                p = cache_dir / n
                if p.exists():
                    out.write(p.read_bytes())
    except Exception as e:
        log(f"[{slug}] cat: {e}")
        return False
    if not merged.exists() or merged.stat().st_size < 1_000_000:
        return False

    # ---- comskip + blackframe extend ----
    # Use the Pi-written .comskip.ini on the SMB share so detection
    # uses the same DE-private-TV tuning the Pi container does
    # (min_commercialbreak=60, validate_silence=1, drop AR, etc.).
    ini_path = HLS_DIR / ".comskip.ini"
    # Persistent per-channel logo template. comskip otherwise re-learns
    # the logo on every 25-min scan window — quality varies per scan
    # depending on how much logo-only material falls inside, and the
    # learning phase itself adds 10-20 s of detection lag at the start
    # of a block. Caching one good template per channel skips both.
    logo_cache_dir = HLS_DIR / ".logos"
    logo_cache = logo_cache_dir / f"{slug}.logo.txt"
    scan_t0 = time.time()
    cmd = [str(COMSKIP), "--quiet"]
    if ini_path.is_file():
        cmd += ["--ini", str(ini_path)]
    if logo_cache.is_file() and logo_cache.stat().st_size > 0:
        cmd += ["--logo", str(logo_cache)]
    cmd += ["--output", str(work), str(merged)]
    try:
        subprocess.run(cmd, timeout=600, check=False, capture_output=True)
    except Exception as e:
        log(f"[{slug}] comskip: {e}")
    ads_sec = parse_comskip(work)
    ads_sec = blackframe_extend_ads(str(merged), ads_sec, channel_slug=slug)
    # Refresh the cached logo template if comskip wrote a fresh one and
    # the scan looked successful (≥2 ad blocks ≈ logo learning worked).
    # Avoids poisoning the cache from a degenerate scan with no ads.
    fresh_logo = work / "merged.logo.txt"
    if fresh_logo.is_file() and fresh_logo.stat().st_size > 0 \
            and len(ads_sec) >= 2:
        try:
            logo_cache_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(fresh_logo, logo_cache)
        except Exception as e:
            log(f"[{slug}] logo cache write: {e}")
    merged.unlink(missing_ok=True)
    scan_dt = time.time() - scan_t0

    ads_wall = [[round(window_first_pdt + a, 1), round(window_first_pdt + b, 1)]
                for a, b in ads_sec]

    buffer_cutoff = time.time() - WINDOW_SECONDS
    scan_start = window_first_pdt
    scan_end = window_first_pdt + len(window) * SEGMENT_TIME
    prev_ads = state.get(slug, {}).get("ads", [])
    retained = [a for a in prev_ads
                if (a[1] <= scan_start or a[0] >= scan_end)
                and a[1] > buffer_cutoff]
    merged_ads = sorted(retained + ads_wall)

    state[slug] = {"generated": time.time(), "ads": merged_ads,
                   "latest_seg": latest_name}
    save_live_ads(state)
    log(f"[{slug}] {len(ads_wall)} new, {len(retained)} retained, "
        f"{len(merged_ads)} total | fetched {fetched_n}/{len(window)} segs "
        f"({fetched_bytes // (1024*1024)} MB in {fetch_dt:.1f}s), "
        f"scan {scan_dt:.1f}s")
    return True


def gc_stale_caches():
    """Drop /tmp/tv-live-scan/<slug>/ dirs whose corresponding playlist
    is no longer fresh (channel went idle). Keeps /tmp from accumulating
    after channel switches."""
    if not WORK_DIR.exists():
        return
    for d in WORK_DIR.iterdir():
        if not d.is_dir():
            continue
        slug = d.name
        if channel_is_active(slug):
            continue
        # Inactive channel — drop its segment cache
        segs = d / "segs"
        if segs.exists():
            try:
                shutil.rmtree(segs)
            except Exception:
                pass


def main_loop():
    log(f"starting; comskip={COMSKIP}, hls={HLS_DIR}, caddy={CADDY_BASE}")
    if not HLS_DIR.is_dir():
        log(f"ERROR: {HLS_DIR} not mounted — exit so launchd retries later")
        sys.exit(1)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    # Load state ONCE at startup, then keep it in-memory for the
    # lifetime of the process. Reloading from disk every iteration
    # used to race with our own atomic-rename writes on the SMB
    # share — POSIX atomic-rename semantics aren't honoured by SMB
    # (it's effectively delete+rename) and macOS's SMB client caches
    # stat metadata, so a load right after a save would sometimes
    # return {} and the per-slug cooldown would evaporate.
    # Pi-side _live_adskip_loop is disabled when LIVE_ADS_OFFLOAD is
    # set, so we're the only writer — no need to re-read.
    state = load_live_ads()

    last_gc = 0
    while True:
        try:
            if time.time() - last_gc > 600:
                gc_stale_caches()
                last_gc = time.time()
            candidates = [s for s in LIVE_ADSKIP_SLUGS
                          if channel_is_active(s)]
            recent = {s for s, v in state.items()
                      if time.time() - v.get("generated", 0) < RESCAN_SECONDS}
            todo = [s for s in candidates if s not in recent]
            if todo:
                slug = todo[0]
                log(f"[{slug}] analysing ({len(todo)-1} more queued)")
                analyze(slug, state)
        except Exception as e:
            log(f"loop: {e}")
        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main_loop()
