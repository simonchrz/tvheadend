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
| `GET /api/internal/recording-uuids` | All known recording UUIDs (Mac daemon uses for hourly source-cache orphan GC + 30-min source-cache prefetch loop) |
| `GET /api/internal/detect-pending-low` | V2 low-prio detect queue (`.detect-requested-low` marker — written for non-test-set UUIDs after head deploy). Daemon polls only when high-prio queue empty AND no detect in flight. |
| `POST/DELETE /api/internal/training-active` | tv-train-head.sh writes/clears the `.training-active` marker via HTTP (no SMB needed). Drives /learning "Training läuft seit N min" banner. |
| `POST /api/internal/training-duration` | Append one record to `.training-durations.jsonl` (Mac-side script POSTs after each run). Feeds /learning ETA median. |
| `GET /api/internal/live-config/<slug>` | Per-channel knobs for tv-live-detect: logo_smooth_s, start_lag_s, sponsor_duration_s, cached_logo_url, min/max_block_s. Single endpoint replaces 4 SMB reads per scan. |
| `POST /api/recording/<uuid>/redetect` | Force a fresh tv-detect pass on this recording (truncates cutlist + writes `.detect-requested` marker) |
| `POST /api/recording/<uuid>/bumper-capture` | Save a bumper template (start or end). Body: `{start_s, end_s, kind: "start"|"end"}`. Auto-aligns the nearest user-block boundary (±30s) to the captured frame. Channel-wide invalidation triggers re-detect across all recordings of that slug. |
| `GET /api/internal/detect-bumpers/<slug>` | Returns `{templates: [...end...], start_templates: [...start...]}` — categorised template list for the daemon. End-bumpers from `.tvd-bumpers/<slug>/end/` AND legacy channel-root; start-bumpers from `.tvd-bumpers/<slug>/start/`. |
| `GET /api/internal/scheduled-events` | `[{eid, uuid, channel, start}, ...]` — DVR upcoming + recording entries keyed by EPG event id. Used by /epg's `syncScheduledFromUpcoming` JS to refresh the green-dot indicator after schedule actions without a page reload. |

## /learning page features (added 2026-04-29 / 30)

- **Aktuelle Modell-Werte (Cards)**: Block-IoU, Test-Acc, Train-Acc, Letzter Deploy mit Ampel pro Metrik (🟢/🟡/🔴 nach kalibrierten Schwellen). Drift vs 7-d Median pro IoU. Overfit-Erkennung wenn Train >> Test.
- **Failure-Mode-Analyse**: pro geprüfter Aufnahme klassifiziert in `good / runaway / washout / missed_bumper / boundary_drift / false_positive / false_negative`. Aggregation per Channel + per Show mit Stacked-Bar — zeigt deterministisch wo welche Optimierung am meisten bringt (Washout-Cluster auf einem Sender = Audio-RMS-Hebel, Missed-Bumper überall = Snap-Window-Tuning).
- **Show-Lückenerkennung**: surface Shows mit N=0/1 Reviews als `stat_floor` (Test-IoU = Rauschen), N<5 als `drift_unlock` (Per-Show-Drift-Learning aktiviert sich bei ≥5), N≥5 + viele unreviewed als `velocity` (cheap labelling source). Action-Link öffnet die erste unreviewed Aufnahme der Show direkt im Player.

## Training pipeline (`train-head.py`, runs 03:30 nightly)

- Wrapper `tv-train-head.sh` läuft SMB-frei für Marker (POST `/api/internal/training-active` + `/training-duration`). SMB-Mount wird im Script noch geremountet falls weg — wird aber nur vom Fallback-Pfad gebraucht wenn der UUID-Cache eine .ts nicht hat (= immer seltener seit dem `~/.cache/tv-detect-daemon/source/` Prefetch-Loop)
- **Active flags**: `--with-logo --with-audio --with-minute-prior --with-self-training --write-pseudo-labels` → produces a 1282-dim head (5132 B = backbone 1280 + logo + audio). `--with-channel` available but disabled (memory: feature ceiling at N≈50 → wait until N≥150 reviewed).
- **Audio-RMS** (added 2026-04-30): per-second normalised RMS via ffmpeg astats, fed as the 1282nd feature in NN inference. Go side: `signals/audio_rms.go` mirrors the Python extraction; head sizes 5132 (+LOGO+AUDIO) and 5156 (+LOGO+CHAN+AUDIO) supported in `signals/nn.go`. Gateway's `detect-config` endpoint reports `with_audio: true` when the deployed head matches the audio sizes; daemon then passes `--with-audio` to tv-detect.
- **Platt-Calibration**: nach `clf.fit` wird auf hold-out Test-Set ein 1-D Sigmoid σ(A·logit + B) gefittet, Brier-Score-Diff geloggt, Sidecar `head.calibration.json` neben head.bin. Kalibrierte Probs im `--surface-uncertain` Step → Active-Learning-Targets ranken auf TATSÄCHLICHER Modell-Unsicherheit. Phase-A/B Self-Training nutzt raw Probs (Threshold-Tuning ist auf raw kalibriert).
- **Test-set sidecar** (added 2026-05-01): train-head writes `head.test-set.json` listing UUIDs in the test split. **V2 invalidation strategy** (2026-05-01): test-set UUIDs get `.detect-requested` (high prio, ~25 min on ~20 recs); rest get `.detect-requested-low` (background — daemon picks only when high queue empty AND no detect in flight). Without sidecar falls back to V1 = all high. cutlist-uploaded clears both markers. Daemon submits at most one bg job per cycle so a freshly-arriving manual /redetect always wins within 5s. Net result: snapshot fires after the high-prio drain (~25 min); whole corpus catches up over 1-2 days of background work.
- **Daemon source-cache prefetch** (added 2026-05-01): `tv-thumbs-daemon` runs a 30-min prefetch loop pulling missing recordings from Pi via HTTP (3 parallel, max 5 per cycle, skips if any detect in flight). Combined with `SOURCE_CACHE_MAX_GB=300` the entire Pi corpus fits locally → train-head reads from NVMe instead of SMB fallback. Single canonical local cache: `~/.cache/tv-detect-daemon/source/<uuid>.ts` (UUID-keyed). Old `~/local-tv-cache` (path-keyed) was deleted 2026-05-01 — train-head's `--daemon-cache` flag now defaults to the daemon path.
- **Per-channel bumper threshold**: `.channel-config.json` per-slug `bumper_threshold` (default 0.75; RTL/SAT-1/ProSieben at 0.85 because their large generic template sets cause false-positive snaps at the lower bar). Diagnosed via the Failure-Mode-Analyse on /learning where missed_bumper deltas split 73 % threshold-bound vs 22 % window-bound.
- **Training-active marker**: `tv-train-head.sh` POSTs to `/api/internal/training-active` on start (gateway writes file, mtime = start), DELETEs via shell trap on exit. /learning surfaces a banner with elapsed + ETA derived from `.training-durations.jsonl` (median of last 10 successful runs, also POSTed via HTTP).

## Disaster recovery: ads_user.json backup (added 2026-05-01)

The 161+ user-reviewed `ads_user.json` files are the only irreplaceable
artifact in the system — everything else (ads.json, head.bin, calibration,
priors) is derivable from labels but slow to recompute. SSD-failure on the
Pi would cost weeks of re-reviewing without backup.

**Pipeline:**

| Layer | Path |
|---|---|
| Source of truth | `raspberrypi5lan:/mnt/tv/hls` |
| Local mirror | `/Users/simon/tv-labels-backup` (git repo) |
| Off-site | `https://github.com/simonchrz/tv-labels-backup` (private) |
| Schedule | launchd daily 04:32 (= 32 min after nightly training, captures freshest head.bin) |
| Script | `~/bin/tv-backup-labels.sh` |
| Plist | `~/Library/LaunchAgents/com.user.tv-backup-labels.plist` |
| Log | `~/Library/Logs/tv-backup-labels.log` |

**What's backed up** (~2 MB total, grows ~50-100 KB/day):

- Tier 1 (irreplaceable): all `_rec_*/ads_user.json` files
- Tier 2 (regenerable but slow): `head.bin` + `head.calibration.json` + `head.test-set.json`
- Tier 3 (config snapshots): `.channel-config.json`, `.detection_learning.json`, `.block_length_prior*.json`

Uses rsync `--include`/`--exclude` so HLS segments + bumper templates +
recording .ts files stay out (they're either regenerable or huge).

**Restore** (documented in script header):

```
git clone https://github.com/simonchrz/tv-labels-backup ~/tv-labels-backup
rsync -av ~/tv-labels-backup/_rec_*/ raspberrypi5lan:/mnt/tv/hls/
rsync -av ~/tv-labels-backup/.tvd-models/ raspberrypi5lan:/mnt/tv/hls/.tvd-models/
```

Bumper templates (`.tvd-bumpers/`, ~30 MB across 350+ PNGs) are NOT in
this backup — they're trainable from completed recordings via the
auto-train flow, just slow. Logo templates (`.tvd-logos/`) are tracked in
the main `tvheadend` repo at `mac-daemon/../.tvd-logos/`.

Why HTTP not SMB: macOS TCC restricts launchd Aqua agents from
enumerating network mounts; `os.listdir(/Users/simon/mnt/pi-tv/hls)`
returns `Operation not permitted` even though interactive shells
work fine. The original `mac/tv-comskip.sh` setup was silently
broken by this since 2026-04-25 (its log file went 0-byte). HTTP
sidesteps the TCC scope entirely.

Eager auto-invalidation: when nightly retrain bumps `head.bin` mtime,
prewarm notices and applies the V2 two-tier markering described in
the training-pipeline section above. First-run case (marker missing)
just records current mtime — without this guard a container restart
would invalidate all 130+ recordings. Also drops each recording's
`ads.json` so the smart-merge regenerates against the new auto-cutlist.

Letterbox compensation runs daemon-side: for each detect job, the
daemon does a 5 s `ffmpeg cropdetect` pass at the 60 s mark, computes
`offset = max(0, cropdetect_y - 20)`, passes `--logo-y-offset` to
tv-detect. The 20-px constant is RTL-empirical (canonical case:
Jungle Cruise on RTL Spielfilm — cropdetect=80, optimal offset=60).
Same logic in `train-head.py` so cached training features match what
inference produces.
