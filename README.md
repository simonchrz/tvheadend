# TV-Streaming am Raspberry Pi 5

DVB-C-Aufnahme + Live-Streaming über HLS, gebaut für iOS Safari auf dem Pi 5.

## Stack

- **tvheadend** (LXC-Image, `linuxserver/tvheadend`) — DVB-Input via SAT>IP (FRITZ!Box 6690 Cable), EPG, DVR
- **hls-gateway** (Flask, Python 3.12) — on-demand HLS-Transcoding pro Kanal, EPG-Archiv, Web-UI, Watch-Player mit Scrub/Pause/Von-Anfang/Mediathek-Fallback, Recording-Player mit Werbe-Ausblendung via comskip
- **Caddy** — HTTPS + HTTP/2 mit interner CA auf `:8443` (Pi-hole hält `:80`/`:443`)

Alle drei als Docker-Container, `network_mode: host`.

## Funktionen

- Live-TV mit 2 h DVR-Fenster (timeshift), Swipe zum Kanalwechsel
- EPG-Grid mit klickbaren Chapter-Ticks im Player
- „Von Anfang" springt an den Sendungsstart, falls im lokalen Puffer
- **Mediathek-Fallback** für ARD/ZDF-Sender (hls.js + MSE, iOS 17.1+): überspringt Safaris Live-HLS-Einschränkungen
- **Aufnahmen** (tvheadend-DVR) werden nach Fertigstellung automatisch zu iOS-kompatiblem HLS-VOD umgepackt, Werbeblöcke per comskip erkannt und im Scrubber schraffiert markiert
- LRU-Warm-Tuner: Kanäle laufen nach Kanalwechsel weiter (DVR bleibt warm), max 3 parallel — Live-TV bekommt Priorität über Prewarm
- Pause mit src-strip (Live) bzw. Destroy/Restart-hls.js (Mediathek) — iOS-Live-HLS lässt sich nicht nativ pausieren

## Pfade auf dem Pi

- `/mnt/tv/` — SSD (ext4, Samsung T5 USB3), Aufnahmen + HLS-Segmente
- `~/hls-gateway/service.py` per Volume gemountet → Hot-Reload nach `docker compose restart`
- `/mnt/tv/hls/` — HLS-Segmente für Live (`<slug>/`) und VOD-Aufnahmen (`_rec_<uuid>/`)

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
