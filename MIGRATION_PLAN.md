# Wechsel auf die selbst verwaltete Homepage

Stand: 16.07.2026

## Zielbild

- `https://auto-lackierzentrum.de` zeigt ausschließlich die öffentliche Homepage.
- `https://portal.auto-lackierzentrum.de` führt zum getrennten Kunden-/Partnerportal.
- E-Mail bleibt während der gesamten DNS- und Domainumstellung erreichbar.
- Kundendaten, Datenbank und Uploads liegen niemals im reinen Homepage-Dienst.

## Bereits vorbereitet

- Render-Blueprint mit zwei getrennten Diensten (`gaertner-homepage`, `kundenstatus-portal`).
- `PUBLIC_SITE_ONLY=1` sperrt im Homepage-Dienst alle internen und formularverarbeitenden Routen.
- Homepage-Links können über `PORTAL_BASE_URL` auf den getrennten Portal-Dienst zeigen.
- `/healthz`, `robots.txt`, `sitemap.xml`, Canonicals und kontrollierte Indexierung.
- 301-Weiterleitungen für die bisherigen Kernseiten.
- Inventur der bisherigen WordPress-Sitemaps unter `migration/legacy_urls.csv`.
- Automatischer Konfigurationscheck: `scripts/production_readiness.py`.

## URL-Bestand

Die Inventur enthält 340 bestehende URLs:

- 1 Startseite bleibt bestehen.
- 9 Kern-/Rechtsseiten besitzen ein klares Ziel beziehungsweise eine vorbereitete 301-Weiterleitung.
- 330 Regio-Seiten müssen vor dem Livegang einzeln bewertet werden.

Die große Zahl weit entfernter Regio-Seiten sollte nicht ungeprüft kopiert werden. Dünne oder nahezu identische Ortsseiten können Suchmaschinenqualität verschlechtern. Relevante Seiten für das tatsächliche Servicegebiet werden mit eigenständigem lokalem Inhalt neu erstellt; irrelevante Seiten erhalten je nach vorhandenem Traffic eine passende 301-Weiterleitung oder HTTP 410. Keine pauschale Weiterleitung aller 330 URLs auf die Startseite.

## Von Webjoker anzufordern

- Auth-Code und bestätigter Transfertermin der Domain.
- Vollständiger DNS-Zonenexport.
- Liste aller Mailboxen, Aliase und Weiterleitungen.
- MX-, SPF-, DKIM- und DMARC-Einträge.
- Information, ob Nameserver, Domain und Mail gemeinsam oder getrennt verwaltet werden.
- Bestätigung, dass vor unserer Freigabe nichts abgeschaltet wird.

## E-Mail-Inventar vor der Umschaltung

| Typ | Name/Adresse | Ziel/Anbieter | geprüft |
|---|---|---|---|
| Hauptpostfach | info@auto-lackierzentrum.de | Webjoker, noch bestätigen | offen |
| Schaden | schaden@auto-lackierzentrum.de | noch bestätigen | offen |
| Rechnungen | rechnungen@auto-lackierzentrum.de | noch bestätigen | offen |
| Termine | termine@auto-lackierzentrum.de | noch bestätigen | offen |
| Reklamationen | reklamation@auto-lackierzentrum.de | noch bestätigen | offen |

Die Tabelle wird nach Eingang der Webjoker-Angaben um alle tatsächlich vorhandenen Postfächer und Aliase ergänzt. Passwörter gehören nicht in dieses Dokument.

## Sichere Reihenfolge

1. Render-Dienste aus dem Blueprint als Testumgebung erstellen.
2. `PUBLIC_SITE_INDEXABLE=0` lassen; Testadresse nicht bei Google anmelden.
3. Homepage, Team, Leistungen, Impressum und Datenschutz prüfen.
4. Portal mit Testdaten, PostgreSQL, Disk und Backups prüfen.
5. Rechtliche Freigabe für Datenschutz/Impressum dokumentieren.
6. DNS-Zone und E-Mail-Bestand von Webjoker sichern.
7. TTL mindestens 48 Stunden vor der Umschaltung auf 300 Sekunden reduzieren.
8. Portal-Subdomain einrichten und testen.
9. Produktionschecks für beide Profile erfolgreich ausführen.
10. Hauptdomain auf den Homepage-Dienst umstellen.
11. HTTPS, Formulare, E-Mail-Ein- und -Ausgang sowie Weiterleitungen testen.
12. Erst danach `PUBLIC_SITE_INDEXABLE=1` setzen und Sitemap in Google Search Console einreichen.
13. Alte Website erst nach erfolgreicher Abnahme abschalten lassen.

## Abnahmetest am Umschalttag

- Startseite, Leistungen, Team, Impressum, Datenschutz: HTTP 200.
- `/admin` auf der Hauptdomain: HTTP 404.
- `/admin` auf der Portal-Subdomain: Login erreichbar.
- Schaden-, Status- und Mietwagenlinks führen zur Portal-Subdomain.
- Testmail an jede produktive Adresse und Antworttest nach außen.
- SPF, DKIM und DMARC prüfen.
- SSL-Zertifikate für Hauptdomain und Portal-Subdomain gültig.
- 301-Weiterleitungen der Kernseiten prüfen.
- PostgreSQL, Upload-Disk und Backup-Ablage im Portal prüfen.
- Keine echten Kundendaten in Logs, Git oder Testumgebung.

## Noch blockiert durch externe Angaben

- Domain-Auth-Code und Kündigungs-/Transfertermin.
- DNS-Zonenexport und vollständige Mailbox-/Aliasliste.
- Render-/Hosting-Konto beziehungsweise Freigabe zum Anlegen der Dienste.
- Juristische Freigabe der finalen Rechtstexte.
- Entscheidung, welche der 330 Regio-Seiten tatsächlich geschäftlich relevant sind.
