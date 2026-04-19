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
- **Live-Werbeerkennung** (Privatsender) — rollender comskip-Durchlauf auf den warmen Kanälen alle ~12 min, Werbeblöcke überspringbar im 2-h-Puffer
- **Warm-Tuner** mit konfigurierbaren Pins (Startseite): gepinnte Kanäle laufen dauerhaft, zusätzlich LRU-Cache für zuletzt geschaute. Pin-Cap dynamisch nach Tuner-Belegung (Mux-Sharing wird angerechnet)
- **Kanalübersicht**: Badge pro Kanal zeigt Puffergröße (grün = LRU-warm, blau = gepinnt); Klick auf grünen Badge gibt Tuner frei. Header zeigt `📡 n/4 Tuner` live
- **EPG**: Logo-only-Spalte, kompakte Zeitachse, Kurzsendungen mit 2-Zeilen-Wrap. Langes Drücken auf Sendung → Aufnahme-Dialog (geplante Sendungen mit grünem Punkt). Tap auf Live-Sendung öffnet Player; Close bringt dich zurück ins EPG (scroll-Position bleibt)
- **Hot-Reload ohne Puffer-Verlust**: `scp service.py` reicht — File-Watcher im Container triggert `os.execv`, ffmpeg-Kinder werden per PID-Datei re-adoptiert
- Pause mit src-strip (Live) bzw. Destroy/Restart-hls.js (Mediathek) — iOS-Live-HLS lässt sich nicht nativ pausieren

## Pfade auf dem Pi

- `/mnt/tv/` — SSD (ext4, Samsung T5 USB3), Aufnahmen + HLS-Segmente
- `~/hls-gateway/service.py` per Volume gemountet → Hot-Reload allein durch `scp` (siehe Entwicklung)
- `/mnt/tv/hls/` — HLS-Segmente für Live (`<slug>/`) und VOD-Aufnahmen (`_rec_<uuid>/`)
- `/mnt/tv/hls/<slug>/.ffmpeg.pid` — Kind-PID + Startzeit für Re-Adoption nach Hot-Reload
- `/mnt/tv/hls/<slug>/.adskip/` — comskip-Artefakte für die Live-Werbeerkennung
- `/mnt/tv/hls/.always_warm.json` — Pin-State (persistent)

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
