FROM python:3.12-slim AS build

# Local patch on top of upstream Comskip — bundles ffmpeg 7+ ABI compat
# (ticks_per_frame=1) plus a SaveLogoMaskData hook that reliably writes
# <basename>.logo.txt at end-of-analysis. The live scanner caches
# per-channel logo templates from that file; without the patch, the
# Pi-side comskip fallback (when the Mac is offline) loses both the
# cache writes and the goodEdge-based refinement the Mac side relies on.
# Canonical patch lives at ../comskip/ in the dev repo; mirrored here
# so the build context stays self-contained on the Pi (which doesn't
# have the full repo checked out).
COPY comskip-patches/0001-ffmpeg-compat-and-logo-output.patch /tmp/patches/

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates git autoconf automake libtool build-essential \
        pkg-config \
        libargtable2-dev libavformat-dev libavcodec-dev libavutil-dev \
        libswscale-dev libswresample-dev libsdl2-dev && \
    git clone --depth 1 https://github.com/erikkaashoek/Comskip /tmp/comskip && \
    cd /tmp/comskip && \
    git apply /tmp/patches/0001-ffmpeg-compat-and-logo-output.patch && \
    ./autogen.sh && ./configure && make -j$(nproc) && \
    mkdir -p /out && cp comskip /out/comskip


FROM python:3.12-slim

# Static ffmpeg from BtbN (ffmpeg.org's linked provider). Debian
# Trixie freezes at 7.1.x; we want 8.1+ for HLS PDT fixes and the
# native AAC improvements. The -latest link rebuilds nightly so a
# `docker compose build` pulls whatever is current.
ARG FFMPEG_URL=https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n8.1-latest-linuxarm64-gpl-8.1.tar.xz

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl xz-utils ca-certificates \
        libargtable2-0 libsdl2-2.0-0 \
        libavutil59 libavcodec61 libavformat61 \
        libswscale8 libswresample5 && \
    curl -fL "$FFMPEG_URL" -o /tmp/ffmpeg.tar.xz && \
    tar -xJf /tmp/ffmpeg.tar.xz -C /tmp && \
    cp /tmp/ffmpeg-*/bin/ffmpeg  /usr/local/bin/ffmpeg  && \
    cp /tmp/ffmpeg-*/bin/ffprobe /usr/local/bin/ffprobe && \
    rm -rf /tmp/ffmpeg-* /tmp/ffmpeg.tar.xz && \
    apt-get purge -y --auto-remove curl xz-utils && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/comskip /usr/local/bin/comskip

RUN pip install --no-cache-dir flask waitress

COPY service.py /app/service.py
WORKDIR /app

EXPOSE 8080
CMD ["python", "-u", "service.py"]
