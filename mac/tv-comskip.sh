#!/bin/bash
# Mac-side handler for finished tvheadend recordings on the Pi:
# - Remuxes the source .ts to HLS for VOD playback (writes index.m3u8
#   + seg_NNNNN.ts under /mnt/tv/hls/_rec_<uuid>/)
# - Runs comskip to produce the cutlist .txt
# Both pieces are idempotent and gated on a cooperative .scanning
# lock-file so we never race the Pi's _rec_prewarm path.

COMSKIP="$HOME/src/Comskip/comskip"
FFMPEG="/opt/homebrew/bin/ffmpeg"
FFPROBE="/opt/homebrew/bin/ffprobe"
MOUNT="$HOME/mnt/pi-tv"
SMB_USER="simon"
SMB_PASS="fP1PhVDGUpiP79Hi"
SMB_HOST="raspberrypi5lan"
SMB_SHARE="tv"
TVH_API="http://raspberrypi5lan:9981/api"
LOG="$HOME/Library/Logs/tv-comskip.log"

exec >>"$LOG" 2>&1
echo "=== $(date '+%F %T') ==="

# Skip if Pi isn't reachable (e.g., Mac is off-LAN)
if ! curl -sSf --max-time 3 "$TVH_API/serverinfo" >/dev/null 2>&1; then
  echo "Pi unreachable — skipping"
  exit 0
fi

# Mount share if not mounted
if ! mount | grep -q "on $MOUNT ("; then
  mkdir -p "$MOUNT"
  /sbin/mount_smbfs "//$SMB_USER:$SMB_PASS@$SMB_HOST/$SMB_SHARE" "$MOUNT" \
    || { echo "mount failed"; exit 1; }
  echo "mounted"
fi

# Heartbeat so the Pi knows the Mac is handling finished recordings
# entirely (both remux + comskip). Read by _mac_comskip_alive() in
# hls-gateway/service.py with a 300s staleness window.
touch "$MOUNT/hls/.mac-comskip-alive" 2>/dev/null

python3 - <<PY
import json, os, shlex, shutil, subprocess, time, urllib.request
API = "$TVH_API"
MOUNT = os.path.expanduser("$MOUNT")
COMSKIP = os.path.expanduser("$COMSKIP")
FFMPEG = "$FFMPEG"
FFPROBE = "$FFPROBE"

SAFE_VIDEO = {"h264", "hevc"}

LOGO_CACHE = os.path.join(MOUNT, "hls", ".logos")

# tvheadend DVR entries carry channelname (e.g. "VOX"); the live
# scanner caches per channel slug (e.g. "vox"). Build name→slug from
# hls-gateway once per script run.
def load_chname_slug_map():
    out = {}
    try:
        url = "https://raspberrypi5lan:8443/api/channels"
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        data = json.loads(urllib.request.urlopen(url, timeout=10,
                                                  context=ctx).read())
        for c in data.get("channels", []):
            n = c.get("name"); s = c.get("slug")
            if n and s: out[n] = s
    except Exception as e:
        print("chname-slug map:", e, flush=True)
    return out

CHNAME_TO_SLUG = load_chname_slug_map()

def have_complete_hls(out_dir):
    pl = os.path.join(out_dir, "index.m3u8")
    if not os.path.isfile(pl):
        return False
    try:
        with open(pl) as f:
            return "#EXT-X-ENDLIST" in f.read()
    except Exception:
        return False

def have_cutlist(out_dir):
    if not os.path.isdir(out_dir):
        return False
    for f in os.listdir(out_dir):
        if f.endswith(".txt") and \
           os.path.getsize(os.path.join(out_dir, f)) > 0:
            return True
    return False

LOCAL_WORK = "/tmp/tv-rec-handler"

def remux(uuid, src, out_dir):
    # Probe video codec — copy if h264/hevc, else transcode via libx264.
    try:
        probe = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name", "-of",
             "default=nokey=1:noprint_wrappers=1", src],
            capture_output=True, text=True, timeout=15)
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
    # Stage segments + playlist locally first — SMB-flushing each
    # 2 MB segment as ffmpeg writes it caps throughput at ~1 MB/s
    # (per-write sync overhead). Local /tmp is ~10x faster, then we
    # bulk-copy the whole tree to SMB at the end which streams at
    # ~90 MB/s sustained.
    work = os.path.join(LOCAL_WORK, uuid)
    os.makedirs(work, exist_ok=True)
    # Wipe any stale partial output from a previous failed run
    for f in os.listdir(work):
        try: os.unlink(os.path.join(work, f))
        except Exception: pass
    local_playlist = os.path.join(work, "index.m3u8")
    # -nostdin so ffmpeg doesn't try to read from stdin and trip on
    # EINTR when launchd hands us a /dev/null stdin (this caused
    # "Error opening input: Interrupted system call" before).
    cmd = [FFMPEG, "-nostdin", "-hide_banner", "-loglevel", "error",
           "-y", "-i", src,
           "-map", "0:v:0", "-map", "0:a:0",
           *v_opts, "-c:a", "aac", "-b:a", "128k",
           "-f", "hls",
           "-hls_time", "6",
           "-hls_list_size", "0",
           "-hls_playlist_type", "vod",
           "-hls_base_url", f"/hls/_rec_{uuid}/",
           "-hls_segment_filename",
           os.path.join(work, "seg_%05d.ts"),
           local_playlist]
    print(f"  remuxing ({vcodec or 'unknown'} -> "
          f"{'copy' if vcodec in SAFE_VIDEO else 'libx264'}) "
          f"via local /tmp", flush=True)
    # IMPORTANT: invoke ffmpeg via 'bash -c', NOT directly via
    # subprocess.run([...]). Python's posix_spawn-based exec on
    # macOS makes ffmpeg run ~30x slower than fork+exec from bash
    # (measured: 5 segs/10s vs 165 segs/10s on the same input).
    # Suspect close_fds + libdispatch interaction; bash sidesteps
    # the whole question.
    err_path = os.path.join(LOCAL_WORK, f"{uuid}.ffmpeg.err")
    shell_cmd = " ".join(shlex.quote(a) for a in cmd) + \
                f" < /dev/null > /dev/null 2> {shlex.quote(err_path)}"
    t0 = time.time()
    r = subprocess.run(["/bin/bash", "-c", shell_cmd],
                       timeout=3600, check=False)
    dt = time.time() - t0
    err_text = ""
    try:
        with open(err_path) as ef:
            err_text = ef.read()
    except Exception:
        pass
    try: os.unlink(err_path)
    except Exception: pass
    if r.returncode != 0:
        print(f"  remux exit={r.returncode} {err_text[:200]}",
              flush=True)
        return False
    # Bulk-copy local stage to SMB. Playlist last so the player
    # doesn't see an incomplete segment list referenced by a
    # complete (ENDLIST) playlist.
    os.makedirs(out_dir, exist_ok=True)
    t1 = time.time()
    seg_count = 0
    seg_bytes = 0
    for f in sorted(os.listdir(work)):
        if not f.startswith("seg_"): continue
        sp = os.path.join(work, f)
        dp = os.path.join(out_dir, f)
        with open(sp, "rb") as si, open(dp, "wb") as di:
            while True:
                buf = si.read(4 << 20)  # 4 MB chunks
                if not buf: break
                di.write(buf)
        seg_count += 1
        seg_bytes += os.path.getsize(sp)
    # Move playlist last
    with open(local_playlist, "rb") as si, \
         open(os.path.join(out_dir, "index.m3u8"), "wb") as di:
        di.write(si.read())
    dt2 = time.time() - t1
    # Cleanup local stage
    for f in os.listdir(work):
        try: os.unlink(os.path.join(work, f))
        except Exception: pass
    try: os.rmdir(work)
    except Exception: pass
    mb_per_s = (seg_bytes / 1e6 / dt2) if dt2 > 0 else 0
    print(f"  remux done in {dt:.0f}s + smb-copy {dt2:.0f}s "
          f"({seg_count} segs, {seg_bytes/1e6:.0f} MB, "
          f"{mb_per_s:.0f} MB/s)", flush=True)
    return True

def comskip(uuid, src, out_dir, slug=None):
    ini_path = os.path.join(MOUNT, "hls", ".comskip.ini")
    cmd = [COMSKIP, "--quiet"]
    if os.path.isfile(ini_path):
        cmd += ["--ini", ini_path]
    # Use the per-channel cached logo template if we have one. Speeds
    # up + tightens detection (no per-run learning phase).
    cached_logo = os.path.join(LOGO_CACHE, f"{slug}.logo.txt") if slug else None
    if cached_logo and os.path.isfile(cached_logo) \
            and os.path.getsize(cached_logo) > 0:
        cmd += ["--logo", cached_logo]
    cmd += ["--output", out_dir, src]
    print(f"  comskip{' (cached logo)' if cached_logo and os.path.isfile(cached_logo) else ''}", flush=True)
    t0 = time.time()
    r = subprocess.run(cmd, timeout=1200, check=False,
                       capture_output=True, text=True)
    dt = time.time() - t0
    if r.returncode != 0:
        print(f"  comskip exit={r.returncode} {r.stderr[:200]}",
              flush=True)
        return False
    # Refresh per-channel logo cache from this run. A finished
    # recording is 60+ min vs the live scanner's 25-min window, so
    # the logo template trained here is strictly better — overwrite
    # unconditionally on any successful comskip exit.
    if slug:
        try:
            for f in os.listdir(out_dir):
                if f.endswith(".logo.txt") and \
                        os.path.getsize(os.path.join(out_dir, f)) > 0:
                    os.makedirs(LOGO_CACHE, exist_ok=True)
                    shutil.copy2(os.path.join(out_dir, f),
                                  os.path.join(LOGO_CACHE,
                                                f"{slug}.logo.txt"))
                    print(f"  logo cached for {slug}", flush=True)
                    break
        except Exception as e:
            print(f"  logo cache: {e}", flush=True)
    print(f"  comskip done in {dt:.0f}s", flush=True)
    return True

try:
    data = json.loads(urllib.request.urlopen(
        API + "/dvr/entry/grid?limit=200", timeout=10).read())
except Exception as e:
    print("API error:", e); raise SystemExit(0)

now = time.time()
processed = skipped = 0
for e in data.get("entries", []):
    # Accept "completed" plus "completedError" — the latter just means
    # tvheadend lost track of the file at some point (e.g. mid-rename)
    # and never re-checked.
    if e.get("sched_status") not in ("completed", "completedError"): continue
    if e.get("stop", 0) > now: continue
    fn = e.get("filename", "")
    uuid = e.get("uuid", "")
    if not fn or not uuid: continue
    mac_ts = fn.replace("/recordings/", MOUNT + "/", 1)
    out_dir = os.path.join(MOUNT, "hls", f"_rec_{uuid}")
    if not os.path.isfile(mac_ts):
        continue

    needs_remux = not have_complete_hls(out_dir)
    needs_cskip = not have_cutlist(out_dir)
    if not (needs_remux or needs_cskip):
        skipped += 1
        continue

    # Cooperative skip: if a fresh '.scanning' lock-file exists, the
    # Pi (or another Mac run) is already on this. 15-min staleness
    # threshold ignores locks from crashed scanners.
    scanning = os.path.join(out_dir, ".scanning")
    try:
        if (os.path.isfile(scanning) and
                time.time() - os.path.getmtime(scanning) < 900):
            skipped += 1
            continue
    except Exception:
        pass

    os.makedirs(out_dir, exist_ok=True)
    title = e.get("disp_title", "?")
    print(f"→ {title[:40]} ({uuid[:8]})", flush=True)
    try:
        with open(scanning, "w") as lf:
            lf.write(f"mac {os.getpid()} {int(time.time())}")
    except Exception:
        pass
    did_work = False
    try:
        if needs_remux:
            if remux(uuid, mac_ts, out_dir):
                did_work = True
        if needs_cskip:
            slug = CHNAME_TO_SLUG.get(e.get("channelname", ""))
            if comskip(uuid, mac_ts, out_dir, slug=slug):
                did_work = True
    except subprocess.TimeoutExpired:
        print("  timeout", flush=True)
    finally:
        try: os.unlink(scanning)
        except Exception: pass
    if did_work:
        processed += 1

print(f"summary: {processed} processed, {skipped} skipped")
PY
