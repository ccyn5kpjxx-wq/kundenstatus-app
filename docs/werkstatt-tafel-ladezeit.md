# Werkstatttafel: Ladezeit vom 29. Oktober 2026

## Nachgewiesene Ursache

Ausgangsstand ist Livebuild `9f2ac6de11f8`. Vierzig echte Render-Anfragen
auf `/werkstatt/tafel` ergeben getrennt nach Methode:

| Methode | Anzahl | Median | Maximum |
| --- | ---: | ---: | ---: |
| GET | 21 | 264 ms | 618 ms |
| HEAD vor dem Refresh | 19 | 1133 ms | 3262 ms |

Die vorgeschaltete HEAD-Pruefung berechnete die gesamte Tafel. Der nur fuer
GET aktive Sammelcache griff dabei nicht: Eine isolierte Reproduktion mit
den echten Helfern ergab bei 100 Auftraegen 500 Servicepfad-Abfragen fuer
HEAD, gegenueber einer Sammelabfrage fuer GET. Danach erfolgte nochmals GET.
Im eingesehenen Messfenster zeigt die Serverauslastung keinen Hinweis auf
einen Kapazitaetsengpass.

## Reparatur

- HEAD prueft weiterhin den unveraenderten Zugang einschliesslich aktueller
  Werkstattcodegueltigkeit und umgeht danach nur die Daten-/Renderarbeit.
  Body und unbekannte Content-Length entfallen; Schutzheader bleiben erhalten.
- Der Refresh laeuft nur bei sichtbarer Seite, erlaubt eine gleichzeitige
  Probe und bricht sie nach acht Sekunden ab. Vor Navigation werden neue
  Eingaben, offene Dialoge und Drag-Vorgaenge erneut geprueft.
- Vorhandene visuell gleiche Gruppen-WebP und kleines Logo ersetzen die
  grossen Startbilder: 96622 statt 2406328 Bytes. Die versteckten weiteren
  Bilder und Musik laden erst bei Nutzung; Teamvorstellung und Tonwahl bleiben.

## Pruefung und Grenzen

Die getrennten Aenderungen wurden unabhaengig geprueft. Der gemeinsame
Git-Snapshot `1df6db2e` besteht sechs echte HEAD-/Rechte-/Abfragebudgettests,
zehn synthetische Browser-/Netztests und alle 63 bestehenden Tafelpruefungen.
Die alte Hintergrund-Pfaderwartung wurde auf das optimierte Bild angepasst.
Reifen- und Checklistenregressionen bestanden bereits im Backendclaim.
Testdaten sind synthetisch, dotenv und externe Python-Verbindungen gesperrt.

Die Computersteuerung stoppte automatisch, weil die Browseradresse nicht
zuverlaessig erkannt wurde. Die aktive Nutzersitzung wurde deshalb nicht
weiter gesteuert. Laufzeiten stammen aus Render-Anfragelogs; Verhaltenstests
verwenden ausschliesslich eigene synthetische Browserkontexte. Die Angaben
sind keine gemessene vollstaendige Kaltrenderzeit auf dem Nutzergeraet.

Private Nachweise: `C:/tmp/gaertner-tafel-performance-20261029/`, insbesondere
`live-request-baseline.json` und `integration/results.json`. Keine Tokens,
IP-Adressen, Kundeninhalte oder Datenbanken werden mit diesem Bericht committet.

## Bereitstellung

Der technische Livewechsel wird einmal koordiniert: laufende Hintergrund-
Importe beenden lassen, Worker und Cron pausieren, Webbuild und betroffene
Anfragen pruefen, danach Hintergrunddienste einzeln wiederherstellen.
Auto-Deploy, Zeitplan, Instanzgroesse und persistente Disk bleiben bestehen.
Die erfolgreiche Bereitstellung und Nachmessung werden getrennt erfasst.

## Liveergebnis

Runtime `08fd077f4304573600eb69934c3c24676576ec4d` ist bereitgestellt,
Webdeploy `dep-dbhl4i8ae00c73e2bbag`. Render meldete live am 29. Oktober um
14:05:35 UTC. Im angekuendigten Serverwechsel wurde um 14:05:39 noch eine
HEAD-502 protokolliert; die anschliessenden erfassten Tafelanfragen liefern
200. Eine erste kalte HEAD-Antwort benoetigte 226 ms, die vier folgenden
24, 18, 18 und 20 ms. Die Tafel-GETs nach dem Kaltstart lagen bei 106–188 ms.

Worker wurde danach einzeln fortgesetzt (Deploy `dep-dbhl84uc6qcc73fnfqeg`,
live 14:12:37 UTC); der Prozess und wiederkehrende Appzyklen sind im Log
sichtbar. Erst anschliessend wurde Cron fortgesetzt (Deploy
`dep-dbhl9b8473hc73aa0q3g`, live 14:14:52 UTC). Beide Dienste sind wieder
`not_suspended`, mit Auto-Deploy Off und unveraendertem Cronplan `*/10 * * * *`.
Es wurde kein Cronlauf oder Testversand manuell ausgeloest.

Unter wieder aktivem Worker zeigen weitere echte Anfragen HEAD 41, 19 und
125 ms sowie GET 168, 327 und 299 ms. Die letzte Anfrage erfolgt nach beiden
Wiederanlaeufen. Beide Domains liefern nach dem Wiederherstellen Health 200,
`ok=true` und Build `08fd077f4304`. Gruppenbild und Logo sind mit genau
87724 und 8898 Bytes live abrufbar.

Das sind begrenzte reale Messfenster mit bestaetigtem Zugriff, keine Garantie
einer maximalen Ladezeit fuer jedes Geraet. Der nachfolgende Berichtscommit
aendert ausschliesslich Dokumentation und erfordert keinen weiteren Deploy.
Ergaenzende lokale Nachweise: `health-assets-live.json`,
`live-request-after.json`, `live-request-after-resume.json`,
`final-service-health.json` und `maintenance-actions.jsonl` im oben genannten
privaten Nachweisordner.
