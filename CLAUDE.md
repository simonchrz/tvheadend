# Project Overview for Claude

Raspberry Pi 5 based TV-streaming setup. DVB-C via FRITZ!Box SAT>IP →
tvheadend → hls-gateway (Flask) → iOS Safari on iPhone/iPad,
reverse-proxied through Caddy with HTTPS.

The three containers (`tvheadend`, `hls-gateway`, `caddy`) run as
`network_mode: host` Docker services. Recordings and HLS segments
live on an NVMe SSD bind-mounted at `/mnt/tv/` (was external USB-3
SSD; migrated 2026-04 to M.2 HAT+ → NVMe).

A Mac on the same LAN runs ONE launchd daemon
(`~/bin/tv-thumbs-daemon.py`) that takes the heavy work off the
Pi's CPU via pure HTTP — no SMB. Handles three job types:

  - **HLS-remux**  : .ts → libx264 + AAC HLS bundle (PUT-per-file
                     upload, no Pi-side tar parser)
  - **Thumbnails** : 160-px scrub-bar JPGs at 1/30 fps
  - **Ad detection**: tv-detect with `--nn-backbone` + `--nn-head`
                      (the Pi-local detection path silently omitted
                      these so the entire trained NN was unused
                      until the daemon took over)

Combined ffmpeg pass produces HLS + thumbs from one .ts download;
detection downloads the .ts again for codec-aware decode + 4-worker
processing. Pi CPU during full-recording processing dropped from
~445 % (Pi-local: HLS 236 % + thumbs 109 % + detect 100 %) to ~5 %
(HTTP serve + raw-bytes write).

## Where to look

| Topic | Read |
|---|---|
| Design decisions, file/state layout, HTTP endpoints, Player UI, public-broadcaster wiring | [docs/architecture.md](docs/architecture.md) |
| Deploying code (SIGHUP, NOT compose restart), iOS Safari quirks, SSD swap / EXT4 recovery / USB cable + power-cap diagnostics | [docs/operations.md](docs/operations.md) |
| Mac-offload daemon (HTTP-based job pipeline, /api/internal/* endpoints, why HTTP not SMB, TCC trap) | [docs/mac-handlers.md](docs/mac-handlers.md) |
| Mac-daemon scripts themselves (`tv-thumbs-daemon.py`, `tv-train-head.sh`, `mount-pi-tv.sh`) — symlinked from `~/bin/` so launchd paths stay valid | [mac-daemon/](mac-daemon/) |
| **UI design tokens, components, mobile patterns** — read BEFORE editing any HTML/CSS in service.py | [DESIGN.md](DESIGN.md) |
| Things deliberately not done + open backlog | [docs/history.md](docs/history.md) |

## Python version

All three execution contexts run **Python 3.13.13** (consolidated 2026-05-01):

- **Gateway** (Pi docker container): base image `python:3.13-slim`, set in `hls-gateway/Dockerfile`
- **Train-head ML venv** (Mac): `~/ml/tv-classifier/.venv` rebuilt with `python3.13 -m venv`. All ~160 ML deps (tensorflow 2.21, torch 2.11, onnxruntime 1.25 with CoreMLExecutionProvider, sklearn 1.8, numpy 2.4, librosa 0.11) have 3.13 wheels.
- **tv-thumbs-daemon** (Mac launchd): plist pins `/opt/homebrew/bin/python3.13` explicitly. Was incidentally Xcode-bundled 3.9 before — change kills that brittleness.

3.14 was tested + rejected: tensorflow has no 3.14 wheels yet, and numba (transitive via librosa) caps at <3.13. Re-evaluate Q4 2026 once those catch up. Don't write 3.14-only syntax (PEP 750 t-strings, etc) anywhere — code must stay 3.13-portable.

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
                 ┌──────────┴──────────────────────┐
                 │ MacBook Pro (Wi-Fi)             │
                 │ ~/bin/tv-thumbs-daemon.py       │
                 │   ├ pulls /api/internal/*-pending     │
                 │   ├ runs ffmpeg/tv-detect locally     │
                 │   └ POSTs results back via HTTP PUT   │
                 │ Talks direct to gateway :8080         │
                 │ (bypasses Caddy TLS — saved ~40 % CPU │
                 │ during bulk transfers)                │
                 └───────────────────────────────────────┘
```

## Mac-offload pipeline (added 2026-04-26)

The Pi's spawn paths (`_rec_hls_spawn`, `_rec_thumbs_spawn`,
`_rec_cskip_spawn`) check the matching `XXX_OFFLOAD=mac` env and
either write a `.requested` marker file or run the local
ffmpeg/tv-detect as before. Pi-fallback timer (60–600 s) takes over
if the Mac daemon doesn't deliver, so a Mac-down outage doesn't
strand viewers.

| Endpoint | Purpose |
|---|---|
| `GET /api/internal/{thumbs,hls,detect}-pending` | List pending markers (skips in-progress recordings via tvh `sched_status`) |
| `GET /recording/<uuid>/source` | Range-streamed .ts (2-3× faster than Mac-side SMB) |
| `PUT /api/internal/hls-segment/<uuid>/<fname>` | Per-segment HLS upload (no tar parser overhead) |
| `POST /api/internal/hls-done/<uuid>` | End-of-upload signal (atomic-ish: index.m3u8 sent last, player polls for it) |
| `POST /api/internal/thumbs-uploaded/<uuid>` | Tar of JPGs (small bundle, ~600 KB) |
| `POST /api/internal/detect-started/<uuid>` | Daemon signals it picked up the job from the pool — gateway tracks in-memory (10-min stale-expiry) so /recordings can render the 🔍 "detect läuft" badge |
| `POST /api/internal/cutlist-uploaded/<uuid>` | Raw cutlist .txt body — also clears the detect-running entry |
| `GET /api/internal/detect-config/<uuid>` | Per-job config bundle (slug, show, logo URL, smoothing, boundary extends, block prior) |
| `GET /api/internal/detect-logo/<slug>` | Per-channel logo template |
| `GET /api/internal/detect-models/<head.bin\|backbone.onnx>` | Model files for daemon-side caching |
| `GET /api/internal/recording-uuids` | All known recording UUIDs (Mac daemon uses for hourly source-cache orphan GC) |
| `POST /api/recording/<uuid>/redetect` | Force a fresh tv-detect pass on this recording (truncates cutlist + writes `.detect-requested` marker) |
| `POST /api/recording/<uuid>/bumper-capture` | Save a bumper template (start or end). Body: `{start_s, end_s, kind: "start"|"end"}`. Auto-aligns the nearest user-block boundary (±30s) to the captured frame. Channel-wide invalidation triggers re-detect across all recordings of that slug. |
| `GET /api/internal/detect-bumpers/<slug>` | Returns `{templates: [...end...], start_templates: [...start...]}` — categorised template list for the daemon. End-bumpers from `.tvd-bumpers/<slug>/end/` AND legacy channel-root; start-bumpers from `.tvd-bumpers/<slug>/start/`. |
| `GET /api/internal/scheduled-events` | `[{eid, uuid, channel, start}, ...]` — DVR upcoming + recording entries keyed by EPG event id. Used by /epg's `syncScheduledFromUpcoming` JS to refresh the green-dot indicator after schedule actions without a page reload. |

## /learning page features (added 2026-04-29 / 30)

- **Aktuelle Modell-Werte (Cards)**: Block-IoU, Test-Acc, Train-Acc, Letzter Deploy mit Ampel pro Metrik (🟢/🟡/🔴 nach kalibrierten Schwellen). Drift vs 7-d Median pro IoU. Overfit-Erkennung wenn Train >> Test.
- **Failure-Mode-Analyse**: pro geprüfter Aufnahme klassifiziert in `good / runaway / washout / missed_bumper / boundary_drift / false_positive / false_negative`. Aggregation per Channel + per Show mit Stacked-Bar — zeigt deterministisch wo welche Optimierung am meisten bringt (Washout-Cluster auf einem Sender = Audio-RMS-Hebel, Missed-Bumper überall = Snap-Window-Tuning).
- **Show-Lückenerkennung**: surface Shows mit N=0/1 Reviews als `stat_floor` (Test-IoU = Rauschen), N<5 als `drift_unlock` (Per-Show-Drift-Learning aktiviert sich bei ≥5), N≥5 + viele unreviewed als `velocity` (cheap labelling source). Action-Link öffnet die erste unreviewed Aufnahme der Show direkt im Player.

## Training pipeline (`train-head.py`, runs 03:30 nightly)

- Wrapper `tv-train-head.sh` mountet SMB via `mount-pi-tv.sh` falls weg (sleep/wake kann Mounts killen — bare existence-check reicht nicht weil "Pfad existiert aber Connection tot")
- **Active flags**: `--with-logo --with-audio --with-minute-prior --with-self-training --write-pseudo-labels` → produces a 1282-dim head (5132 B = backbone 1280 + logo + audio). `--with-channel` available but disabled (memory: feature ceiling at N≈50 → wait until N≥150 reviewed).
- **Audio-RMS** (added 2026-04-30): per-second normalised RMS via ffmpeg astats, fed as the 1282nd feature in NN inference. Go side: `signals/audio_rms.go` mirrors the Python extraction; head sizes 5132 (+LOGO+AUDIO) and 5156 (+LOGO+CHAN+AUDIO) supported in `signals/nn.go`. Gateway's `detect-config` endpoint reports `with_audio: true` when the deployed head matches the audio sizes; daemon then passes `--with-audio` to tv-detect.
- **Platt-Calibration**: nach `clf.fit` wird auf hold-out Test-Set ein 1-D Sigmoid σ(A·logit + B) gefittet, Brier-Score-Diff geloggt, Sidecar `head.calibration.json` neben head.bin. Kalibrierte Probs im `--surface-uncertain` Step → Active-Learning-Targets ranken auf TATSÄCHLICHER Modell-Unsicherheit. Phase-A/B Self-Training nutzt raw Probs (Threshold-Tuning ist auf raw kalibriert).
- **Test-set sidecar** (added 2026-05-01): train-head writes `head.test-set.json` listing UUIDs in the test split. Used by gateway's prewarm-loop to scope the post-deploy bulk re-detect to ONLY the test recordings (= ~20) instead of all 134 — V1 strategy. Cuts post-deploy compute from ~3 h to ~25 min. Train + unreviewed recordings keep their old cutlists; their `ads.json` lazy-regenerates via `/recording/<uuid>/ads` on next view.
- **Per-channel bumper threshold**: `.channel-config.json` per-slug `bumper_threshold` (default 0.75; RTL/SAT-1/ProSieben at 0.85 because their large generic template sets cause false-positive snaps at the lower bar). Diagnosed via the Failure-Mode-Analyse on /learning where missed_bumper deltas split 73 % threshold-bound vs 22 % window-bound.
- **Training-active marker**: `tv-train-head.sh` writes `.training-active` on start (mtime = start), removes via shell trap on exit. /learning surfaces a banner with elapsed + ETA derived from `.training-durations.jsonl` (median of last 10 successful runs).

Why HTTP not SMB: macOS TCC restricts launchd Aqua agents from
enumerating network mounts; `os.listdir(/Users/simon/mnt/pi-tv/hls)`
returns `Operation not permitted` even though interactive shells
work fine. The original `mac/tv-comskip.sh` setup was silently
broken by this since 2026-04-25 (its log file went 0-byte). HTTP
sidesteps the TCC scope entirely.

Eager auto-invalidation: when nightly retrain bumps `head.bin`
mtime, prewarm notices, truncates every recording's cutlist .txt
(filename kept — train-head's loader needs the basename to find the
.ts source) and writes the `.detect-requested` marker so the daemon
serially re-detects everything with the fresh model. First-run case
(marker missing) just records current mtime — without this guard a
container restart would invalidate all 50+ recordings. Also drops
each recording's `ads.json` so the smart-merge regenerates against
the new auto-cutlist.

Letterbox compensation runs daemon-side: for each detect job, the
daemon does a 5 s `ffmpeg cropdetect` pass at the 60 s mark, computes
`offset = max(0, cropdetect_y - 20)`, passes `--logo-y-offset` to
tv-detect. The 20-px constant is RTL-empirical (canonical case:
Jungle Cruise on RTL Spielfilm — cropdetect=80, optimal offset=60).
Same logic in `train-head.py` so cached training features match what
inference produces.
