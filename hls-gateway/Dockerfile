FROM python:3.12-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates git autoconf automake libtool build-essential \
        pkg-config \
        libargtable2-dev libavformat-dev libavcodec-dev libavutil-dev \
        libswscale-dev libswresample-dev libsdl2-dev && \
    git clone --depth 1 https://github.com/erikkaashoek/Comskip /tmp/comskip && \
    cd /tmp/comskip && ./autogen.sh && ./configure && make -j$(nproc) && \
    mkdir -p /out && cp comskip /out/comskip


FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg libargtable2-0 libsdl2-2.0-0 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/comskip /usr/local/bin/comskip

RUN pip install --no-cache-dir flask

COPY service.py /app/service.py
WORKDIR /app

EXPOSE 8080
CMD ["python", "-u", "service.py"]
