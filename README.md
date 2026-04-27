# TV-Streaming am Raspberry Pi 5

DVB-C-Aufnahme + Live-Streaming über HLS, gebaut für iOS Safari auf dem Pi 5.

## Stack

- **tvheadend** (LXC-Image, `linuxserver/tvheadend`) — DVB-Input via SAT>IP (FRITZ!Box 6690 Cable), EPG, DVR, Autorec
- **hls-gateway** (Flask über waitress, Python 3.12, ffmpeg 8.1 static) — on-demand HLS-Transcoding pro Kanal, EPG-Archiv, Web-UI, Watch-Player mit Scrub/Pause/Von-Anfang/Mediathek-Fallback, Recording-Player mit Werbe-Ausblendung via tv-detect (NN-basiert, ersetzt comskip seit 2026-04), virtuelle Mediathek-Aufnahmen
- **Caddy** — HTTPS + HTTP/2 mit interner CA auf `:8443` (Pi-hole hält `:80`/`:443`)
- **Mac-side daemon** (`~/bin/tv-thumbs-daemon.py` auf MacBook im LAN) — übernimmt HLS-Remux, Thumbnail-Generierung und tv-detect-Ad-Detection vom Pi via HTTP-API. Pi-CPU für Recording-Verarbeitung dropt von ~445 % auf ~5 %. Talks direkt zu Pi-Gateway auf `:8080` ohne Caddy-TLS-Hop. Lokaler `.ts`-Cache auf Mac-NVMe (LRU bei `SOURCE_CACHE_MAX_GB=60`, stündlicher Orphan-GC gegen `/api/internal/recording-uuids`) — Re-Detects nach Head-Bumps lesen aus Cache statt LAN, ~30× schneller. Daemon ruft pro Detect `ffmpeg cropdetect` auf 5 s Sample und übergibt `--logo-y-offset` an tv-detect (Letterbox-Kompensation für 16:9-in-4:3 Spielfilme).

Alle drei Pi-Container als Docker, `network_mode: host`.

## Funktionen

### Live-TV
- 2 h DVR-Fenster (timeshift), Swipe zum Kanalwechsel
- **Mediathek-Live als Primärquelle** für ARD/ZDF-Sender — spart Tuner. Fällt automatisch auf DVB-C zurück, wenn Mediathek-HLS nicht antwortet (8 s Timeout oder hls.js-Error)
- Live-Werbeerkennung (Privatsender, comskip rolling, ~12 min Takt; skippt wenn keine neuen Segmente)
- Mux-Dots auf der Kanalübersicht zeigen welche Kanäle sich einen Tuner teilen

### Aufnahmen
- **tvheadend-DVR**: nach Fertigstellung automatisch zu iOS-HLS-VOD umgepackt, Werbeblöcke per tv-detect (NN-Head + Logo + Wall-Clock-Prior), Serien-Autorec über Titel-Regex
- **Eager-Remux + Eager-Thumbs + Eager-Detect**: Pi-side prewarm-loop (alle 120 s) erkennt fertige Aufnahmen ohne HLS/Thumbs/Cutlist, schreibt `.requested`-Marker in den jeweiligen rec-dir. Mac-Daemon (`~/bin/tv-thumbs-daemon.py`) pollt `/api/internal/{thumbs,hls,detect}-pending` alle 5 s, holt .ts via HTTP, läuft ffmpeg/tv-detect lokal, postet Resultate via PUT zurück. User-Klick auf eine fertige Aufnahme = instant playback (alles schon da)
- **Virtuelle Mediathek-Aufnahmen** (ARD + ZDF): Long-press auf EPG-Event → Modal bietet „Aus Mediathek speichern", „Jetzt abspielen" (nur streamen) oder „DVR (Episode/Serie)". Funktioniert auch für vergangene Sendungen (z. B. Tatort von gestern)
- **Pre-Expiry-Rip**: Mediathek-Aufnahmen werden 48 h vor Ablauf lokal als MP4 gerippt (`-c copy`, kein Re-encode), überleben so die Mediathek-Verfügbarkeit. Rip-Worker nur aktiv bei Load < 8 und > 5 GB freier Disk
- **Series-Autorec + Mediathek-Kombi**: beim Setzen einer Serien-Aufnahme werden upcoming Episoden automatisch mit Mediathek-Matches ergänzt, damit Tuner frei bleiben

### Warm-Tuner + Pin-System
- Konfigurierbare Pins (📌 auf Startseite), zusätzlich LRU-Cache für zuletzt geschaute
- **Pin-Cap dynamisch** nach Tuner-Belegung; Mux-Sharing wird angerechnet (7 Kanäle auf 546 MHz = 1 Tuner)
- **Pin-Idle-Timeout**: 6 h ohne Viewer → Pin geht schlafen (💤-Icon), nächster Tap weckt. Spart Strom bei Dauer-Pins
- Badge pro Kanal zeigt Puffergröße (grün=LRU, blau=pinned); Klick auf grün stoppt den Tuner
- Header: `📡 n/4 Tuner` live

### EPG
- Logo-only-Kanal-Spalte, kompakte Zeitachse, Kurzsendungen mit 2-Zeilen-Wrap
- Long-press öffnet kontextuelles Modal (Episode/Serie/Mediathek je nach Kanal + Vergangenheit)
- Tap auf past event → gleiches Modal (nicht Live-Stream)
- Serien-Gruppierung im Aufnahmen-View mit Episoden-Zähler und „Serie entfernen"

### Player-Infrastruktur
- Geteilter CSS/JS-Scaffold, Doppel-Tap links/rechts seek ±10 s
- **Hot-Reload ohne Puffer-Verlust**: `scp service.py` reicht — File-Watcher triggert `os.execv`, ffmpeg-Kinder werden per PID-Datei adoptiert. Compile-Check schützt vor kaputtem `scp`
- iOS-Safari-Workarounds: `aria-label` statt `title` (Tap-Tooltip-Bug), `forceMseHlsOnAppleDevices` für hls.js, Mute-autoplay + Unmute-Overlay

## Pfade auf dem Pi

- `/mnt/tv/` — NVMe-SSD (ext4, WD SN770M auf M.2 HAT+), Aufnahmen + HLS-Segmente. Bind-Mount aus `/mnt/nvme/tv/` (separate fstab-Zeile)
- `~/hls-gateway/service.py` per Volume gemountet → Hot-Reload allein durch `scp` (siehe Entwicklung)
- `/mnt/tv/hls/` — HLS-Segmente für Live (`<slug>/`) und VOD-Aufnahmen (`_rec_<uuid>/`)
- `/mnt/tv/hls/<slug>/.ffmpeg.pid` — Kind-PID + Startzeit für Re-Adoption nach Hot-Reload
- `/mnt/tv/hls/<slug>/.adskip/` — Live-Werbeerkennung-Artefakte (tv-detect rolling)
- `/mnt/tv/hls/_rec_<uuid>/` — tvheadend-Aufnahmen umgepackt (HLS-VOD + tv-detect-Cutlist + Thumbs)
- `/mnt/tv/hls/_rec_<uuid>/.{hls,detect}-requested`, `thumbs/.requested` — Marker-Files für Mac-Daemon, je nach pending Job (HLS-Remux, Ad-Detection, Thumbnails). Mac-Daemon löscht beim Liefern via `POST /api/internal/{thumbs,cutlist,hls-done}/<uuid>`
- `/mnt/tv/hls/.tvd-models/{head.bin,backbone.onnx,head.history.json,head.uncertain.txt}` — tv-detect NN-Head (5128 B, lin-Klassifizierer auf MobileNetV2-backbone), Train-Historie, Active-Learning-Targets
- `/mnt/tv/hls/.tvd-logos/<slug>.logo.txt` — per-Channel Logo-Templates
- `/mnt/tv/hls/.{minute_prior_by_channel,channel-config,detection_learning,block_length_prior,block_length_prior_by_show}.json` — tv-detect Tuning-State (Wall-Clock-Prior, per-Channel-Knobs, Boundary-Drift-Learning, Block-Length-Prior)
- `/mnt/tv/hls/_rec_<uuid>/{ads_user.json,ads.json,pseudo_labels.json,bumpers.json}` — User-Edits, Auto-Cutlist, Self-Training-Pseudo-Labels, Bumper-Detector-Output
- `/mnt/tv/hls/_<mt_uuid>/file.mp4` — gerippte Mediathek-MP4s (V3)
- `/mnt/tv/hls/.always_warm.json` — Pin-State (persistent)
- `/mnt/tv/hls/.mediathek_recordings.json` — virtuelle Mediathek-Aufnahmen (Metadaten + HLS-URL + rip-Pfad)
- `/mnt/tv/hls/.live_ads.json` — erkannte Werbeblöcke pro Kanal (überlebt Hot-Reload)
- `/mnt/tv/hls/.epg_archive.jsonl` — EPG-Archiv (14 Tage, Auto-Compaction)
- `/mnt/tv/hls/.usage_stats.json` — Start-Count + Watch-Hours pro Kanal
- `/mnt/tv/hls/.codec_cache.json` — ffprobe-Ergebnisse (H.264 vs MPEG-2)

## Entwicklung

Nach Änderung an `service.py` reicht ein `scp`. Ein File-Watcher
im Container erkennt die Änderung und ruft `os.execv` auf, um sich an
Ort und Stelle durch die neue Version zu ersetzen. Die laufenden
ffmpeg-Prozesse (mit `start_new_session=True` gestartet und per
PID-Datei in `/mnt/tv/hls/<slug>/.ffmpeg.pid` getrackt) werden vom
neuen Prozess wieder adoptiert — der 2-h-DVR-Puffer bleibt erhalten.
Ein `docker restart` zerstört dagegen das cgroup und killt damit auch
die ffmpegs, daher dafür nicht nutzen.

```sh
scp service.py <user>@<pi-host>:~/hls-gateway/service.py
# optional explizit: docker kill -s HUP hls-gateway
```

Beim Bauen des Image (z.B. comskip-Änderung) — Puffer wird dabei
resettet:

```sh
ssh <user>@<pi-host> 'cd ~/hls-gateway && docker compose build && docker compose up -d'
```

## Ports

| Service | Port | Zweck |
|---|---|---|
| Caddy | 8443 | HTTPS + HTTP/2 |
| tvheadend | 9981 | Web-UI + API |
| tvheadend | 9982 | HTSP |
| hls-gateway | 8080 | Flask (nur intern) |
