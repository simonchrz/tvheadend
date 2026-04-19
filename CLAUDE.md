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
- **Warm-tuner LRU**: switching channel does not immediately stop the
  previous stream. Up to `MAX_WARM_STREAMS` (default 3, leaving 1 of 4
  FRITZ!Box tuners for DVR jobs) keep running so the 2 h DVR buffer
  stays populated for instant time-shift on return. Eviction is LRU;
  new channel request frees the oldest idle.
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
| `hls-gateway/service.py` | Flask app: HLS orchestration, EPG archive, watch player (live), recording player (VOD), Mediathek fallback with hls.js, Now-Next bridge |
| `hls-gateway/Dockerfile` | two-stage: ARM64 native build of comskip from source, then runtime with ffmpeg + libargtable2 + libsdl2 |
| `hls-gateway/docker-compose.yml` | Mounts, env, runs as uid 1000 so SSD writes don't end up root-owned |
| `caddy/Caddyfile` | HTTPS on :8443 with local CA, static `seg_*.ts` served via sendfile, `.m3u8` and `/recording/*` reverse-proxied to Flask, `.ts` forced to `Content-Type: video/mp2t` (Caddy auto-detects as Qt-linguist without this override) |
| `tvheadend/docker-compose.yml` | linuxserver image, `/mnt/tv` mounted for recordings |

## User-facing endpoints on Caddy (`:8443`)

```
/                            — channel grid
/watch/<slug>                — live player (2 h DVR, Mediathek fallback)
/epg                         — program grid with clickable chapter ticks
/recordings                  — DVR list (with prewarm status badges)
/recording/<uuid>            — recording player (VOD + comskip ad markers)
/record-event/<event_id>     — schedule DVR for an EPG event
/cancel-recording/<uuid>     — abort/remove DVR entry
/api/channels                — favorite channels JSON
/api/now/<slug>              — currently airing on channel
/api/events/<slug>           — EPG events in a window
/api/show-actual-start/<slug>?ts=<epg_start>
                             — real broadcast start from ARD/ZDF now-next APIs
/api/mediathek-live/<slug>   — public CDN fallback URL + DVR window size
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
  project memory for details)
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
