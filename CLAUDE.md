# Project Overview for Claude

Raspberry Pi 5 based TV-streaming setup. DVB-C via FRITZ!Box SAT>IP →
tvheadend → hls-gateway (Flask) → iOS Safari on iPhone/iPad,
reverse-proxied through Caddy with HTTPS.

The three containers (`tvheadend`, `hls-gateway`, `caddy`) run as
`network_mode: host` Docker services. Recordings and HLS segments
live on an external USB-3 SSD at `/mnt/tv/`. A Mac on the same LAN
runs two launchd handlers (`mac/tv-comskip.sh`,
`mac/tv-live-comskip.py`) that take comskip + recording remux off
the Pi's CPU.

## Where to look

| Topic | Read |
|---|---|
| Design decisions, file/state layout, HTTP endpoints, Player UI, public-broadcaster wiring | [docs/architecture.md](docs/architecture.md) |
| Deploying code (SIGHUP, NOT compose restart), iOS Safari quirks, SSD swap / EXT4 recovery / USB cable + power-cap diagnostics | [docs/operations.md](docs/operations.md) |
| Mac-side handlers (heartbeat, .scanning lock, TCC chain, subprocess-vs-bash perf, /tmp cache, Pi-CPU profile, history) | [docs/mac-handlers.md](docs/mac-handlers.md) |
| Comskip codebase reference + the local `mpeg2dec.c` patch | [docs/comskip.md](docs/comskip.md) |
| Things deliberately not done + open backlog | [docs/history.md](docs/history.md) |

## Stack at a glance

```
   FRITZ!Box           Pi5 (host network)               iOS Safari
  ┌─────────┐    ┌──────────────────────────────┐    ┌─────────────┐
  │ SAT>IP  │───▶│ tvheadend  :9981             │    │             │
  │ tuners  │    │   ↓                          │    │             │
  └─────────┘    │ hls-gateway :8080 (Flask)    │◀───│ /watch/<x>  │
                 │   ├ live ffmpeg per channel  │    │ /epg        │
                 │   ├ rec remux + comskip      │    │ /recordings │
                 │   ├ Mediathek live + rip     │    │             │
                 │   └ EPG archive + Now-Next   │    └─────────────┘
                 │ caddy :8443 (TLS terminator) │
                 └──────────┬───────────────────┘
                            │ /mnt/tv (ext4, USB SSD)
                            │
                 ┌──────────┴────────────┐
                 │ MacBook Pro (Wi-Fi)   │
                 │   ├ tv-comskip.sh     │  finished-rec remux + comskip
                 │   └ tv-live-comskip.py│  live-buffer ad detection
                 └───────────────────────┘
```
