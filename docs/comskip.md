# Comskip — what & where the patch matters

[erikkaashoek/Comskip](https://github.com/erikkaashoek/Comskip) is the
upstream commercial detector. Three large translation units do almost
everything:

- `comskip.c` (~16k LOC): argument parsing, ini loading, all detection
  methods (black-frame, logo, scene-change, CC, AR, silence, cutscene,
  resolution-change), commercial-block scoring, all output-format
  writers. Detection-method bitmask constants are at the top
  (`BLACK_FRAME`, `LOGO`, `SCENE_CHANGE`, `RESOLUTION_CHANGE`, `CC`,
  `AR`, `SILENCE`, `CUTSCENE`). The `frame_info` struct is the
  per-frame record accumulated during the decode pass.
- `mpeg2dec.c` (~2.6k LOC): FFmpeg-based demux/decode driver despite
  the legacy name. `VideoState` holds libav context;
  `video_packet_process` is the per-packet entry into `comskip.c`'s
  analysis layer. PID/stream selection for MPEG-TS inputs lives here.
- `video_out_*.c`: vo backends; `_dx` is `_WIN32`-gated, `_sdl` only
  links into `comskip-gui`. The headless `comskip` binary has no
  active vo on non-Windows.

## Build

GNU autotools (`./autogen.sh && ./configure && make`); deps via
`pkg-config` (`argtable2 ≥ 2.12`, FFmpeg, SDL2 for the GUI).
`--enable-static` is silently ignored on macOS — Apple doesn't allow
fully static binaries; for redistribution use `matryoshka-name-tool`.
`--enable-debug` switches CFLAGS to `-DDEBUG -ggdb3 -O0`. To override
flags entirely, pass `CFLAGS=...` to `configure` — `configure.ac` sets
`: ${CFLAGS=""}` *before* `AC_PROG_CC` so autoconf won't inject `-g
-O2`.

`PROCESS_CC` is always set in `Makefile.am` (closed-caption decoder
always linked); `DONATOR` defaults on (unlocks paid features inside
`#ifdef DONATOR`). `HARDWARE_DECODE` gates experimental hwaccel
integration (VDPAU/DXVA2/VDA/QSV) — not enabled in default builds.

Three build targets defined in `Makefile.am`:
- `comskip` — headless CLI
- `comskip-gui` — adds `video_out_sdl.c`, compiled with `-DHAVE_SDL`,
  only built when SDL2 is detected
- `votest` — `make check` smoke test for vo backend

Package version is parsed from `PACKAGE_STRING` in `comskip.h` by
`configure.ac` via `sed`, so a release bump only needs editing
`comskip.h`.

## The local patch — `comskip/0001-ffmpeg7-ticks_per_frame-compat.patch`

Fixes three uses of `AVCodecContext.ticks_per_frame` in `mpeg2dec.c`,
which is deprecated in FFmpeg 6 and removed/zeroed in stricter FFmpeg
7+/8 builds (notably **Homebrew FFmpeg 8.x on macOS** — fails to
build or crashes at runtime when the field returns 0).

The patch hardcodes `1` everywhere `ticks_per_frame` was used.
That's correct for progressive H.264, and good enough for interlaced
MPEG-2 since Comskip handles deinterlacing elsewhere.

The Pi-side build (BtbN static FFmpeg 8.1, lenient with deprecated
ABI) still works against unpatched upstream — but if a future ffmpeg
bump in that image becomes strict, the same patch will be needed
there too.

### Apply

```sh
cd ~/src/Comskip                          # or wherever your clone lives
git apply /path/to/comskip/0001-ffmpeg7-ticks_per_frame-compat.patch
./autogen.sh && ./configure && make
```

### Apply at Pi container build (optional, if upstream ever strictens)

In `hls-gateway/Dockerfile`, after the `git clone` step:

```Dockerfile
COPY comskip/0001-ffmpeg7-ticks_per_frame-compat.patch /tmp/patches/
RUN cd /tmp/comskip && git apply /tmp/patches/0001-ffmpeg7-ticks_per_frame-compat.patch
```

(currently unused — Pi builds fine without it)

## Configuration

Runtime behavior is heavily ini-driven — most CLI tunables also live
in `comskip.ini`. `hls-gateway/service.py` writes a tuned
`COMSKIP_INI_TEXT` to `/mnt/tv/hls/.comskip.ini` at startup with
private-broadcaster defaults: `min_commercialbreak=60`,
`validate_silence=1`, `detect_method=15`, etc. Mac-side
`tv-comskip.sh` and `tv-live-comskip.py` pass `--ini` pointing at the
same file so detection is consistent across both sides.
