# Comskip patches

Local patches against [erikkaashoek/Comskip](https://github.com/erikkaashoek/Comskip)
that aren't upstreamed yet but are needed for our build environment.

## `0001-ffmpeg-compat-and-logo-output.patch`

Two unrelated fixes bundled — both small enough that splitting them
isn't worth the maintenance overhead.

### (a) FFmpeg 6/7+ `ticks_per_frame` compat

Three uses of `AVCodecContext.ticks_per_frame` in `mpeg2dec.c` are
deprecated in FFmpeg 6 and removed/zeroed in stricter FFmpeg 7+/8
builds (notably **Homebrew FFmpeg 8.x on macOS** — fails to build or
crashes at runtime when the field returns 0).

Hardcodes `1` everywhere `ticks_per_frame` was used. Correct for
progressive H.264, good enough for interlaced MPEG-2 since Comskip
handles deinterlacing elsewhere.

### (b) Reliable `<basename>.logo.txt` write at end-of-analysis

Upstream `comskip.c` calls `SaveLogoMaskData()` only inside the
two-pass-logo first-pass save path AND only while LOGO is still in
`commDetectMethod` — the latter gets cleared the moment
`give_up_logo_search` expires, so live-buffer scans (where logo
training races against ad-detection) almost never produce a logo
file. And immediately after `OutputBlocks()`, the existing
`deleteLogoFile` cleanup unconditionally removes any logo file that
was created.

Patch adds a hook at the END of `BuildMasterCommList()` (after the
deleteLogoFile cleanup) that calls `SaveLogoMaskData()` whenever
`clogoMin/MaxX/Y` describe a real logo box. Result: every successful
run that actually trained a logo now writes
`<basename>.logo.txt` — and external callers can cache that file
and reuse it via `--logo` on subsequent runs to skip the learning
phase entirely.

The Pi-side build (BtbN static FFmpeg 8.1, lenient with deprecated
ABI) doesn't strictly need part (a) — its ffmpeg keeps the field for
ABI compat — but applying the whole patch keeps both sides identical
and gives the Pi the same logo-output behaviour the Mac side relies on.

### Apply

```sh
cd ~/src/Comskip                          # or wherever your clone lives
git apply /path/to/comskip/0001-ffmpeg-compat-and-logo-output.patch
./autogen.sh && ./configure && make
```

### Apply at Pi container build

Already wired into `hls-gateway/Dockerfile`. The patch file is mirrored
into `hls-gateway/comskip-patches/` (the Pi container build context is
`hls-gateway/`, so the canonical `../comskip/` location isn't reachable
from the Dockerfile — keep the two copies in sync when editing).

Why it matters on the Pi: when the Mac live-comskip agent is offline,
the Pi's `_live_ads_analyze` takes over. Without the SaveLogoMaskData
hook the Pi's comskip wouldn't write `.logo.txt` files, so the cached
per-channel logo templates in `/data/hls/.logos/` would never refresh
and the Python-side `refine_ads_by_logo()` (which depends on `--csvout`
+ a stable logo template via `--logo`) would degrade.
