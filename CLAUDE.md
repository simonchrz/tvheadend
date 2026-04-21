# Project Overview for Claude

Raspberry Pi 5 based TV-streaming setup. DVB-C via FRITZ!Box SAT>IP →
tvheadend → hls-gateway (Flask) → iOS Safari on iPhone/iPad, reverse-
proxied through Caddy with HTTPS.

The three containers (`tvheadend`, `hls-gateway`, `caddy`) run as
`network_mode: host` Docker services. Recordings and HLS segments live
on an external USB-3 SSD at `/mnt/tv/`.

## Key design decisions

- **Phase-1 HLS over MediaMTX / go2rtc.** iOS Safari is strict about
  HLS conformance — we tried MediaMTX and go2rtc; both failed on DVB-
  C's interlaced SEI-laden streams. Pure ffmpeg-per-channel with 1 s
  segments and a 2 h sliding DVR window works reliably.
- **Per-channel transcode rules**: H.264 HD is `-c:v copy` (AAC remux
  for audio because iOS doesn't do AC-3). MPEG-2 SD is re-encoded
  through libx264 ultrafast with explicit `setsar=1` so iOS renders
  anamorphic content at the correct 16:9 display aspect (otherwise
  SD channels show up as 4:3).
- **Warm-tuner LRU + UI pins**: switching channel does not immediately
  stop the previous stream. LRU cache (`MAX_WARM_STREAMS`) plus any
  channels pinned via the UI (📌 on the startpage, persisted to
  `.always_warm.json`) keep running so the 2 h DVR buffer stays
  populated for instant time-shift. Pin cap is **dynamic**, derived
  from real tuner usage: `TUNER_TOTAL - 1 - active_dvr` plus a mux-
  sharing bonus — two pins on the same transponder cost one tuner, so
  the cap grows by 1. Tuner count is read live from tvheadend
  `/api/status/inputs` (entries with `subs > 0`). Header badge
  `📡 n/4 Tuner` on the startpage reflects this in real time; tap on a
  green warm-badge stops the LRU channel to free its tuner. Mux
  groupings are visualised with coloured dots next to each channel.
- **Pin idle-timeout**: a pinned channel that hasn't had a viewer in
  `PIN_IDLE_HOURS` (default 6 h) is stopped and marked dormant in
  `_dormant_pins`. The always-warm loop won't respawn a dormant pin;
  `ensure_running` (called on viewer tap) clears the flag. UI shows a
  💤 icon on the pin button for dormant channels. Saves tuner +
  electricity for the "always warm just in case" use case.
- **Mediathek live as primary source for public broadcasters**:
  watch player first tries the Mediathek live HLS (via hls.js + MSE)
  for any channel in `MEDIATHEK_LIVE`. Falls back to our ffmpeg
  pipeline only if Mediathek doesn't answer in 8 s or emits a fatal
  hls.js error. Frees a DVB-C tuner per public channel someone's
  watching.
- **Virtual Mediathek recordings + pre-expiry rip**: long-press on
  an EPG event on an ARD/ZDF channel offers "Aus Mediathek
  speichern". The entry is saved in `.mediathek_recordings.json` as
  `mt_<hash>` → played back via hls.js on click. A background worker
  (`_mediathek_rip_loop`) ffmpeg-`-c copy`'s the stream to a local
  MP4 48 h before the Mediathek expiry so the show survives past
  Mediathek availability. Rate-limited by loadavg and free-disk.
  `_mediathek_autorec_loop` automatically creates these virtuals
  for every autorec-spawned DVR entry on a Mediathek channel, so
  series recordings also get the Mediathek backup.
- **Past-event Mediathek**: tapping a past EPG event routes through
  the long-press modal instead of navigating to the live stream.
  Old archive entries without an event_id get a synthetic
  `arc_<slug>_<start>` id the lookup endpoint understands, so
  yesterday's Tatort still finds its Mediathek match.
- **Hot-reload without buffer loss**: editing `service.py` triggers an
  in-container file watcher (mtime poll) that does `os.execv` on the
  Python process. ffmpegs are spawned with `start_new_session=True`
  and tracked via a PID file (`.ffmpeg.pid` per channel) so the new
  image adopts them on startup. `docker restart` still nukes them
  (cgroup teardown), so it's deliberately avoided for code changes.
- **Live commercial detection**: a background loop (`_live_adskip_loop`)
  picks one warm private-sender channel every 30 s, concatenates the
  last 30 min of TS segments (raw `cat` — self-synchronising, much
  faster than `ffmpeg -f concat`), and runs comskip. Ad blocks are
  stored keyed by wall-clock via `EXT-X-PROGRAM-DATE-TIME`, so
  rewinding into the buffer on a warm channel lets the player skip
  commercials. Re-scanned every ~12 min so fresh content gets picked
  up. Public broadcasters are excluded (no regular ad breaks).
- **Recording playback via lazy HLS-VOD remux**. tvheadend stores as
  `.ts` with `content-disposition: attachment`, which iOS treats as
  a download. We remux on demand into an HLS-VOD playlist under
  `_rec_<uuid>/` served by Caddy, with ads flagged by `comskip` on a
  background worker.
- **Mediathek fallback for seeking beyond the local buffer.** iOS
  Safari native HLS has not allowed seeking in live playlists since
  iOS 8. For ARD/ZDF/arte/KiKA we bundle `hls.js` on the client with
  `forceMseHlsOnAppleDevices:true` so MSE kicks in (requires Safari
  17.1+), and the public broadcaster HLS endpoints act as our
  longer-history source for past shows.
- **Now-Next APIs for precise show-start seeking.** Chapter-tick taps
  call `/api/show-actual-start/<slug>?ts=<epg>` which consults ARDs
  `programm-api.ard.de/nownext` REST API or ZDFs `getEpg` GraphQL
  persisted-query. Both return an `effectiveAirtimeBegin` (or
  `currentStartDate`) that can differ by seconds to minutes from the
  scheduled EPG slot. Uncovered channels fall back to EPG time + a
  per-channel lead-in.

## File layout

| Path | Role |
|---|---|
| `hls-gateway/service.py` | waitress-served Flask app (~5k LOC): HLS orchestration, EPG archive, live + recording + Mediathek players, autorec series, virtual Mediathek recordings, live ad-skip, Now-Next bridge, file-watcher hot-reload |
| `hls-gateway/Dockerfile` | two-stage: ARM64 native build of comskip from source, then runtime with static ffmpeg 8.1 (BtbN) + libargtable2 + libsdl2, `pip install flask waitress` |
| `hls-gateway/docker-compose.yml` | Mounts `/mnt/tv` + `service.py`, runs as uid 1000 (SSD writes non-root), json-file log rotation 50 MB × 3 |
| `caddy/Caddyfile` | HTTPS on :8443 with local CA, static `seg_*.ts` served via sendfile, `.m3u8` and `/recording/*` reverse-proxied to Flask, `.ts` forced to `Content-Type: video/mp2t` (Caddy auto-detects as Qt-linguist without this override) |
| `tvheadend/docker-compose.yml` | linuxserver image, `/mnt/tv` mounted for recordings |

### State files (persisted on SSD, survive container restart)

| Path | Purpose |
|---|---|
| `/mnt/tv/hls/.always_warm.json` | Pinned channels + last-viewer timestamps |
| `/mnt/tv/hls/.mediathek_recordings.json` | Virtual Mediathek recordings (metadata, HLS URL, rip status, expiry) |
| `/mnt/tv/hls/.live_ads.json` | Detected ad blocks per channel (PDT-keyed, survives hot-reload) |
| `/mnt/tv/hls/.epg_archive.jsonl` | 14-day EPG archive with auto-compaction |
| `/mnt/tv/hls/.usage_stats.json` | Start-count + watch-hours per channel |
| `/mnt/tv/hls/.codec_cache.json` | ffprobe results (H.264 vs MPEG-2) |
| `/mnt/tv/hls/<slug>/.ffmpeg.pid` | Child PID + start-time for re-adoption after hot-reload |
| `/mnt/tv/hls/<slug>/.adskip/` | comskip artefacts for live ad detection |
| `/mnt/tv/hls/_rec_<uuid>/` | Remuxed HLS-VOD of tvheadend recordings + comskip |
| `/mnt/tv/hls/_<mt_uuid>/file.mp4` | Ripped Mediathek MP4s (pre-expiry backup) |

## User-facing endpoints on Caddy (`:8443`)

```
/                            — channel grid (with mux-sharing dots + pin 📌/💤)
/watch/<slug>                — live player (2 h DVR, Mediathek fallback)
/epg                         — program grid with chapter ticks + long-press modal
/stats                       — usage ranking
/recordings                  — DVR list (series-grouped + prewarm status badges)
/recording/<uuid>            — recording player (VOD + comskip ad markers)
/mediathek-play/<event_id>   — direct-play a Mediathek match (no DVR entry)
/mediathek-rec/<uuid>/file.mp4
                             — ripped Mediathek MP4 (post-rip playback)
/record-event/<event_id>     — schedule DVR for an EPG event (OR virtual
                               Mediathek recording if event_id starts mt_/arc_)
/record-series/<event_id>    — create tvheadend autorec from event title
/cancel-series/<autorec_uuid>
                             — delete autorec + all future scheduled episodes
/cancel-recording/<uuid>     — abort/remove DVR entry (real OR virtual mt_*)
/api/channels                — favorite channels JSON
/api/now/<slug>              — currently airing on channel
/api/events/<slug>           — EPG events in a window
/api/is-recording/<slug>     — is this channel being recorded right now
/api/show-actual-start/<slug>?ts=<epg_start>
                             — real broadcast start from ARD/ZDF now-next APIs
/api/mediathek-live/<slug>   — public CDN fallback URL + DVR window size
/api/mediathek-lookup/<event_id>
                             — search ARD/ZDF Mediathek for this EPG event;
                               synthetic arc_<slug>_<start> ids supported
/api/mediathek-schedule/<event_id>
                             — create virtual Mediathek recording (mt_<hash>)
/api/live-ads/<slug>         — current ad-block ranges for warm channel
/api/live-ads/<slug>/scan    — trigger on-demand comskip rescan
/api/warm-status             — warm/pinned/dormant state for all channels
/api/always-warm/<slug>      — toggle pin for a channel (respects dynamic cap)
/status                      — all running ffmpegs + tuner usage JSON
/stop/<slug>                 — stop one stream
/stop-all                    — stop all streams (frees all tuners)
/prewarm/<slug>              — start a channel without a viewer
/mediathek-clip/<slug>/master.m3u8
                             — (legacy VOD-ifying proxy; superseded by hls.js)
```

## Player UI (Live + Recording share the look)

- Chrome fades after 3.5 s; tap video to toggle
- Top-bar: `⛶` fullscreen left, `✕` close right
- Bottom scrub bar with played (red) · buffered (grey) · full-window
  (dim). For recordings: `.ad-block` diagonal-hatch overlays from
  comskip
- Controls row: `⏮` (jump-to-show-start) · `⏪10s` · play/pause · `⏩10s`
  · `⏭` (skip-to-live, live player only) · `◀︎`/`▶︎` (channel, live
  only) · wall-clock counter or `live` badge
- Title row: channel name · colored source badge (`Live` red / `ARD
  Mediathek` blue / `ZDF Mediathek` / `arte.tv` / `KiKA`) · EPG-now
- Chapter-ticks on the scrub bar: reachable events only (filtered
  against local seekable-range and per-channel Mediathek window); tap
  jumps to `currentStartDate` if authoritative API knows, else EPG
  time ± per-channel lead-in

## iOS Safari gotchas worth remembering

- Native HLS does **not** allow pausing live streams or seeking back
  in live playlists. Workaround: hls.js + MSE (iOS 17.1+), or `src`-
  strip for "pause" (screen goes black, resume re-seeks via stored
  wall-time).
- iOS clamps `v.currentTime` to already-buffered segments. To seek
  into fresh content reliably, `hls.stopLoad() + hls.startLoad(ts)`
  first, then poll `v.seekable` until it covers the target.
- iOS reports `duration = Infinity` for live-type HLS even when a
  finite DVR window exists → use `v.seekable.end(0) - start(0)` for
  scrub-bar length, not `duration`.
- `.ts` extension confuses Caddy's MIME sniffer (Qt-linguist). Force
  `Content-Type: video/mp2t` via `header` directive.
- Self-signed CA cert must be installed on every iOS device with the
  "Full Trust" step under Settings → General → Profiles.

## Development workflow

```sh
# Hot-reload Python code. A file watcher inside the container notices
# the mtime change on the mounted service.py and triggers an in-place
# os.execv. Children (ffmpegs spawned with start_new_session=True) are
# re-adopted by PID file on the next boot, so the 2 h DVR buffer
# survives the reload. No `docker restart` needed — and crucially, a
# full container restart DOES kill the ffmpegs (cgroup teardown), so
# avoid it for code-only changes.
scp hls-gateway/service.py <user>@<pi-host>:~/hls-gateway/service.py

# Equivalent explicit trigger (if the file watcher is suppressed, e.g.
# for bulk edits): SIGHUP the container's PID 1.
ssh <user>@<pi-host> 'docker kill -s HUP hls-gateway'

# Rebuild image (e.g. Dockerfile change) — buffer will reset.
ssh <user>@<pi-host> 'cd ~/hls-gateway && docker compose build && docker compose up -d'
```

## Operations — SSD swap / device re-enumeration

The recordings SSD is USB-connected. When swapping cables or moving the drive to another port, follow this order:

```sh
# Mac (if SMB mount + offload agent are active): stop the agent + unmount first
launchctl bootout gui/$UID/com.user.tv-comskip
diskutil unmount ~/mnt/pi-tv

# Pi: stop anything touching /mnt/tv
# 1. POST /api/dvr/entry/stop for any active DVR recording (else lsof shows open write handles)
# 2. take services down — `docker compose down`, NOT stop (see gotcha below)
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose down); done
sudo systemctl stop smbd nmbd
sudo sync && sudo umount /mnt/tv
echo 1 | sudo tee /sys/block/sda/device/delete   # flush write cache + detach USB device

# --- physically swap cable / port ---

# Pi: bring back up in dependency order
sudo mount /mnt/tv           # fstab uses UUID so /dev/sdb vs /dev/sda doesn't matter
sudo systemctl start smbd nmbd
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose up -d); done

# Mac: remount + reload agent
/sbin/mount_smbfs "//simon:***@raspberrypi5lan/tv" ~/mnt/pi-tv
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.user.tv-comskip.plist
```

**Docker bind-mount staleness after device re-enumeration** — use `docker compose down && up`, **not** `docker stop && start`. When the underlying device disappears and reappears (SSD unplug → replug), containers started with `docker start` keep a stale mount-namespace reference to the old device node; every access inside the container returns `OSError: [Errno 5] Input/output error` even though `ls /mnt/tv/hls` on the host works fine. `compose down && up` recreates the container, rebuilding the mount namespace fresh. Symptom in the wild: `/recordings` returns HTTP 500 with `[Errno 5]` in the hls-gateway traceback while every `.ts` file is visibly present from the host shell. dmesg hint: EXT4 warnings referencing the *previous* device node (e.g. `device sda1` after the drive re-enumerated as `sdb1`).

**USB cable + port identification for the T5** — the Samsung T5 negotiates USB 3.2 Gen 2 (10 Gbps) only with a proper SuperSpeed A-to-C cable. Labels like "Anker 3.1A" refer to *charging amperage*, not the USB spec — those are USB 2.0 cables. Diagnose via `lsusb -t`: the T5 must attach to Bus 002 or Bus 004 (5000M root hubs) for SuperSpeed; attaching to Bus 001/003 (480M) means USB 2.0 negotiation, either wrong port or a cable without SuperSpeed pins (only 4 contacts in the USB-A plug vs. 9 for USB 3). `dmesg` distinguishes: `high-speed` = USB 2.0, `SuperSpeed` = USB 3.0. Orthogonal issue: the kernel also applies a UAS-disable quirk for the T5 (`UAS is ignored for this device, using usb-storage instead`) which costs another 30-40% throughput even after USB 3 negotiates; overrideable with `usb-storage.quirks=04e8:61f5:` on the kernel cmdline.

## Public channels currently wired

| slug | Mediathek URL | Window | Now-Next source |
|---|---|---|---|
| das-erste-hd | daserste-live.ard-mcdn.de | 2 h | ARD nownext |
| tagesschau24-hd | tagesschau-live.ard-mcdn.de | 2 h | ARD nownext |
| zdf-hd | zdf-hls-15.akamaized.net | 3 h | ZDF getEpg |
| 3sat-hd | zdf-hls-18.akamaized.net | 3 h | ZDF getEpg (+ARD nownext) |
| arte-hd | artesimulcast.akamaized.net | 30 min | ZDF getEpg |
| kika-hd | kikageohls.akamaized.net | 2 h | ZDF getEpg |

## What's intentionally NOT done

- MediaMTX / go2rtc fallback (extensively tested, abandoned — see
  project memory for details). The go2rtc docker-compose and its
  Caddy routes were removed 2026-04-21; `/home/simon/go2rtc/` still
  exists on the Pi but the container isn't running. Caddy no longer
  references `:1984`, `/live/*.m3u8`, `/live/hls/*`, `/api/ws`,
  `/api/webrtc*`, `/video-rtc.js`, or `/video-stream.js`.
- Per-channel audio silence / scene-change analysis for show boundary
  detection (comskip-for-live) — not worth the CPU and still not
  sekundengenau. We use Now-Next APIs + per-channel lead-in instead.
- ZDF GraphQL introspection beyond the two persisted queries
  (`VideoByCanonical`, `getEpg`) — those give us what we need;
  reverse-engineering more risks breakage on their next bundle push.
- Reverse-engineering arte's private OPA v3 API for past-show
  lookup. Not publicly accessible. User opted to skip for now.

## Things on the backlog (user mentioned, not built)

- Always-warm list for some specific channels independent of viewing
  behaviour (partially supported via LRU already)
- Scene-based chapter marker tuning (would need arte.tv `scene[]`
  data which is editorially curated — absent for most shows)
