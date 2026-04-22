# Mac-side helpers

Scripts that run on a LAN-connected Mac to offload work from the Pi.
Not deployed by the stack itself; copy by hand to `~/bin/` on the Mac.

## `tv-live-comskip.py`

Live-buffer ad detection for the Pi's HLS streams. When the Pi's
`hls-gateway` is started with `LIVE_ADS_OFFLOAD=mac` (in
`hls-gateway/docker-compose.yml`), its internal `_live_adskip_loop`
goes dormant and `/api/live-ads/<slug>` re-reads `.live_ads.json`
on every call — this script is what keeps that file populated.

The interesting design choices:

- **Reads segments via Caddy `:8443` (`file_server` fast path)**, not
  via SMB and not via Flask on `:8080`. Caddy serves `.ts` files
  directly from `/mnt/tv` and never touches the hls-gateway, so we
  don't bump `channels[slug]["last_seen"]` on segment fetches and
  don't accidentally keep an idle ffmpeg artificially warm.
- **Reads `.m3u8` over SMB**, not HTTP — same reason (Flask handler
  on `/hls/<slug>/<filename>` updates `last_seen`).
- **Incremental segment cache** at `/tmp/tv-live-scan/<slug>/segs/`.
  Only fetches segments not already cached. After warmup, ~30 new
  segments per scan instead of all 1800 — 60× less network traffic
  than the naive port. Critical for stability while the SSD is on
  USB 2.
- **Writes `.live_ads.json` over SMB** with atomic-rename semantics
  (`.tmp` → `rename`). The Pi reads it on every `/api/live-ads/<slug>`
  call.

Run via launchd:
```
~/Library/LaunchAgents/com.user.tv-live-comskip.plist
```
which calls `/usr/bin/python3` with the script path. macOS TCC
requires `/usr/bin/python3` to have Full Disk Access (Settings →
Privacy & Security → Full Disk Access) so launchd-spawned reads on
the SMB mount work. See `Comskip/CLAUDE.md` for the full story.

To disable temporarily without removing the agent:
```
launchctl disable gui/$UID/com.user.tv-live-comskip
launchctl bootout  gui/$UID/com.user.tv-live-comskip
```
To re-enable:
```
launchctl enable    gui/$UID/com.user.tv-live-comskip
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.user.tv-live-comskip.plist
```

Pairs with the simpler `tv-comskip.sh` (also in `~/bin/`, not in this
repo) which handles the **finished**-recording offload — that one
just polls the tvheadend DVR API and runs `comskip` on completed
recordings.
