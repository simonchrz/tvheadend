# TV-Streaming am Raspberry Pi 5

DVB-C-Aufnahme + Live-Streaming über HLS, gebaut für iOS Safari auf dem Pi 5.

## Stack

- **tvheadend** (LXC-Image, `linuxserver/tvheadend`) — DVB-Input via SAT>IP (FRITZ!Box 6690 Cable), EPG, DVR, Autorec
- **hls-gateway** (Flask über waitress, Python 3.12, ffmpeg 8.1 static) — on-demand HLS-Transcoding pro Kanal, EPG-Archiv, Web-UI, Watch-Player mit Scrub/Pause/Von-Anfang/Mediathek-Fallback, Recording-Player mit Werbe-Ausblendung via comskip, virtuelle Mediathek-Aufnahmen
- **Caddy** — HTTPS + HTTP/2 mit interner CA auf `:8443` (Pi-hole hält `:80`/`:443`)

Alle drei als Docker-Container, `network_mode: host`.

## Funktionen

### Live-TV
- 2 h DVR-Fenster (timeshift), Swipe zum Kanalwechsel
- **Mediathek-Live als Primärquelle** für ARD/ZDF-Sender — spart Tuner. Fällt automatisch auf DVB-C zurück, wenn Mediathek-HLS nicht antwortet (8 s Timeout oder hls.js-Error)
- Live-Werbeerkennung (Privatsender, comskip rolling, ~12 min Takt; skippt wenn keine neuen Segmente)
- Mux-Dots auf der Kanalübersicht zeigen welche Kanäle sich einen Tuner teilen

### Aufnahmen
- **tvheadend-DVR**: nach Fertigstellung automatisch zu iOS-HLS-VOD umgepackt, Werbeblöcke per comskip, Serien-Autorec über Titel-Regex
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

- `/mnt/tv/` — SSD (ext4, Samsung T5 USB3), Aufnahmen + HLS-Segmente
- `~/hls-gateway/service.py` per Volume gemountet → Hot-Reload allein durch `scp` (siehe Entwicklung)
- `/mnt/tv/hls/` — HLS-Segmente für Live (`<slug>/`) und VOD-Aufnahmen (`_rec_<uuid>/`)
- `/mnt/tv/hls/<slug>/.ffmpeg.pid` — Kind-PID + Startzeit für Re-Adoption nach Hot-Reload
- `/mnt/tv/hls/<slug>/.adskip/` — comskip-Artefakte für die Live-Werbeerkennung
- `/mnt/tv/hls/_rec_<uuid>/` — tvheadend-Aufnahmen umgepackt (HLS-VOD + comskip)
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
