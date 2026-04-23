# Comskip patches

Local patches against [erikkaashoek/Comskip](https://github.com/erikkaashoek/Comskip)
that aren't upstreamed yet but are needed for our build environment.

## `0001-ffmpeg7-ticks_per_frame-compat.patch`

Fixes three uses of `AVCodecContext.ticks_per_frame` in `mpeg2dec.c`,
which is deprecated in FFmpeg 6 and removed/zeroed in stricter
FFmpeg 7+/8 builds (notably **Homebrew FFmpeg 8.x on macOS** — fails
to build or crashes at runtime when the field returns 0).

The patch hardcodes `1` everywhere `ticks_per_frame` was used. That's
correct for progressive H.264, and good enough for interlaced MPEG-2
since Comskip handles deinterlacing elsewhere.

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
