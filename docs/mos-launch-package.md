# MOS: Freigabepaket für echte Kundenbuchungen

Stand 23.09.2026. **Noch kein Zahlungs-Go-live.** Ziel sind öffentliche entgeltliche Selbstfahrer-Mieten ausschließlich für **Hyundai i10 und Hyundai KONA**. C3 ist von der Direktbuchung ausgeschlossen; Fiat bleibt ebenfalls ausgeschlossen. Werkstatttarife bleiben eine manuelle Anfrage. Der Code ist auf GitHub, die öffentliche Zahlungsroute bleibt deaktiviert; keine echte Zahlung/Erstattung wurde ausgeführt.

## Implementierter Stand

- Öffentlicher Ablauf mit Fahrzeug und konkreten Europe/Berlin-Terminen, serverseitigem Cent-Angebot, sitzungsgebundener Preisreferenz, ausdrücklicher Bedingungsannahme und Checkout. Ein Angebot läuft nach 15 Minuten ab. Kein Browserpreis wird als Zahlungsbetrag übernommen.
- Ein gemeinsamer Portalbestand für Online-Holds, Admin-Anlage, Anfrageübernahme und Datumsänderung. Die bestehenden inklusiven Tagesbelegungen bleiben konservativ erhalten; exakte Übergabezeiten stehen unveränderlich im Buchungssnapshot. Damit wird keine kurzfristige Wiedervermietung am Rückgabetag versprochen.
- Getrennter Stripe-Test-/Liveadapter mit Modusprüfung. Live verlangt geprüfte Konfiguration, dauerhaften Flask-Schlüssel, HTTPS und die autoritative PostgreSQL-Datenbank. Kein Produktivbetrieb auf einer lokalen Testkopie.
- Hosted Checkout mit zwei getrennten Posten: Mietpreis inklusive MwSt. und 500 € rückzahlbare Kaution. **Dieses Kautionsverfahren ist eine Implementierungsoption, erst nach Finanz-/Betriebsfreigabe aktivierbar.** Es ist eine Zahlung mit späterer Rückerstattung, keine langfristige Kartenautorisierung. Keine Aussage über Versicherungsschutz wird aus der 1.000-€-Kunden-Selbstbeteiligung abgeleitet.
- Signaturgeprüfter Webhook plus erneute Providerabfrage bestätigt genau einen Mietvorgang. Rücksprung, doppelte Klicks und doppelte Events ersetzen keine Zahlungsprüfung. Kunden sehen nach bestätigter Zahlung sofort den Vertrag mit dem vor Zahlung gezeigten Bedingungstext. Sie können mit Finger oder Maus unterschreiben und die unveränderlich gespeicherte, unterschriebene Fassung als PDF herunterladen. Die Werkstatt sieht den Unterschriftsstatus und kann das PDF im geschützten Adminbereich herunterladen. Zusätzlich gibt es weiterhin die Textbestätigung. Die gezeichnete Unterschrift ist keine qualifizierte elektronische Signatur. Es gibt keinen automatischen E-Mail-Versand.
- Kundenstorno vor Mietbeginn; bis einschließlich 24 Stunden vorher kostenlos. Danach maximal erster gebuchter Tag, ohne Kaution als Gebührenbasis. Nach Mietbeginn keine automatische Kundenstornierung. Admin kann persönlich geprüftes Nichterscheinen erfassen.
- Stornierung, Inventarfreigabe und Erstattungsauftrag werden gemeinsam gespeichert. Offene Erstattung sperrt das Fahrzeug nicht weiter. Erstattung ist erst bei bestätigtem Providerstatus als erfolgt markiert.
- Geschützte Adminansicht für Erstattungsstatus, Minderung wegen Ersparnissen/Wiedervermietung/geringerem oder fehlendem Schaden, erneuten Providerabgleich und volle Kautionsrückzahlung nach protokollierter Fahrzeugrückgabe. Keine automatische Schadensabbuchung. Strittige/teilweise Kautionsabzüge bleiben eine manuelle Abrechnung außerhalb dieser Automatik.
- Erstattungsaufträge besitzen unveränderliche Idempotenzreferenzen und Betragsschranken. Nach unklarem Ausgang ohne Provider-ID wird außerhalb von 23 Stunden nicht erneut gesendet; erst manueller Stripe-Abgleich. Definitiv fehlgeschlagene Erstattungen benötigen gesonderte Klärung, keinen blinden neuen Zahlungsauftrag.

## Noch fehlende Tatsachen und Abnahme – aktuell NO-GO für morgen

| Freigabe | Aktueller Stand / benötigter Beleg |
|---|---|
| Versicherung beider konkreten Autos | Schriftliche Bestätigung der öffentlichen entgeltlichen Selbstfahrervermietung für i10 **und** KONA fehlt. Eine Police für einen Hyundai beweist nicht beide Zuordnungen oder diese Nutzung. Vertrags-Selbstbeteiligung und Versicherer-Selbstbehalt nicht gleichsetzen. |
| Reale Flotte und Kalender | Portal-Zuordnung inzwischen read-only gefunden (Details lokal im Hub). KONA-Stammdaten wurden in Aufgabe76 anhand der dokumentierten Übernahme und Kennzeichen/FIN korrigiert; Die Mietwagennutzung ist laut neuer Quellenprüfung des Ursprungstasks in der angenommenen Leasingvereinbarung dokumentiert; die frühere Portalnotiz ist insoweit überholt. Vollständiger Kalender und konkrete Übergabeslots weiterhin unbestätigt. |
| Betrieb / Preise / Bedingungen | Unternehmerische Freigabe von Slots, Vorlauf/maximaler Mietdauer, Tagesberechnung, Storno, Kraftstoff-/Schadenprozess und vollständigem Bedingungstext nötig. Öffentliche Preise sind aktuell noch als vorläufig beschrieben. |
| Kaution | 500 € beschlossen; vorgeschlagenes Einziehen mit Mietpreis und spätere Erstattung, Fristen, mögliche Gebühren und Buchhaltung müssen bestätigt werden. |
| Stripe / Hosting | Konto und Wallets laut Ursprungstask aktiv; Website-Key, Webhook und echte Sandbox-Abnahme fehlen. Kein echtes Checkout-/Erstattungsereignis ausgeführt. |
| Datenbank / Abnahme | SQLite-Konkurrenz-/HTTP-/Erstattungstests und ein lokaler PostgreSQL-Kaltstart mit synthetischen Checkout-Fällen bestanden. Die neue Vertragstabelle samt Textspeicherung wurde auf derselben isolierten PostgreSQL-Instanz geprüft; ein vollständiger PostgreSQL-HTTP-Signierlauf und die Deployment-Abnahme stehen noch aus. |
| Recht / Datenschutz | Finale Mietbedingungen, elektronische Unterschrift, §312j-Abschlussdarstellung in Hosted Checkout und Stripe-Datenschutzhinweis prüfen; Entwurf nicht als freigegebene AGB einsetzen. |

Es ist deshalb keine belastbare Zusage möglich, dass Kunden morgen bereits bezahlen können. Codevorbereitung allein ersetzt diese Nachweise nicht. Update 23.09.2026: Laut verifiziertem Bericht des Ursprungstasks wurde die Versicherungsanfrage um 10:57 über IONOS gesendet (ein Eintrag in Gesendet). Der frühere SMTP-Versuch war erfolglos. Eine Antwort oder Deckungsbestätigung liegt damit noch nicht vor.

## Konfiguration ohne Geheimnisse im Repository

`MOS_BOOKING_CONFIG_FILE` verweist im Portalprozess auf eine private JSON-Konfiguration. Standard ohne Datei ist deaktiviert. Ein Beispiel mit absichtlich fehlenden Freigaben:

```json
{
  "enabled": false,
  "mode": "live",
  "live_enabled": false,
  "origin": "https://kundenstatus-app.onrender.com",
  "fleet": {
    "kona": {"id": null, "expected_name": "", "daily_cents": 5900, "discount_after_days": 3, "discount_cents": 4900},
    "i10": {"id": null, "expected_name": "", "daily_cents": 3900}
  },
  "slots": [],
  "day_rule": "elapsed_24h_ceil",
  "max_days": null,
  "included_km_day": 150,
  "extra_km_cents": 25,
  "vat_included": true,
  "deposit_cents": 50000,
  "deductible_cents": 100000,
  "deposit_method": "charge_with_rent_refund_after_return",
  "terms_version": "draft:noch-nicht-freigegeben",
  "terms_text": "",
  "privacy_url": "",
  "merchant_name": "Gärtner GmbH Karosserie + Lack",
  "merchant_address": "Binauer Höhe 4, 74821 Mosbach, Deutschland",
  "merchant_email": "",
  "merchant_phone": "",
  "launch": {}
}
```

Keine Nullwerte als echte IDs ersetzen, bevor die Zuordnung geprüft ist. Slots sind konkrete zukünftige ISO-Zeitpunkte mit dem für Europe/Berlin gültigen UTC-Offset; keine erfundenen Öffnungszeiten. Eine neue Konfigurationsversion erfordert Prozessneustart, bestehende Buchungen behalten ihren gespeicherten Snapshot. `merchant_name` und `merchant_address` müssen genau den Impressumsdaten des Vermieters entsprechen. „Autovermietung MOS“ bleibt nur die Angebotsüberschrift.

Unter `launch` benötigt jede der Freigaben `business_review`, `legal_review`, `finance_review`, `privacy_review`, `sandbox_acceptance`, `postgres_acceptance` die Felder `approved_by`, `approved_at`, `evidence`. Unter `launch.insurance.kona` und `.i10` sind `verified=true`, `use=paid_self_drive`, eine echte Belegreferenz und die passende `vehicle_id` nötig. Das sind dokumentierte Betreiberprüfungen, keine automatischen Versicherungsnachweise. Niemals bloß zur Umgehung der Sperre ausfüllen.

Secrets nur im Hosting-Secretstore: `MOS_STRIPE_TEST_KEY` oder `MOS_STRIPE_LIVE_KEY` und `MOS_STRIPE_WEBHOOK_SECRET`. Unterstützt sind eingeschränkte `rk_test_`/`rk_live_` sowie `sk_test_`/`sk_live_`. Vorzugsweise eingeschränkte Keys; keine Secrets in JSON, Chat oder Git. Webhook-Secret ist vom API-Key unabhängig. [Stripe: Schlüsseltypen](https://docs.stripe.com/keys)

## Exakte Stripe-Schnittstellen

| Zweck | Methode / REST-Pfad |
|---|---|
| Checkout erzeugen | `POST /v1/checkout/sessions` |
| Status erneut prüfen | `GET /v1/checkout/sessions/{id}` |
| Offenen Checkout beenden | `POST /v1/checkout/sessions/{id}/expire` |
| Storno/Kaution erstatten | `POST /v1/refunds` mit `payment_intent`, `amount`, Idempotency-Key |
| Erstattungsstatus | `GET /v1/refunds/{id}` |

Berechtigungen zunächst gezielt **Checkout Sessions schreiben/lesen** und **Refunds schreiben/lesen**; alle nicht benötigten Ressourcen auf None. Die genaue Abhängigkeit der Stripe-Rechte mit dem Restricted-Testkey testen. Keine Auszahlungs-, Transfer-, Konto- oder Schlüsselverwaltungsrechte. Kein Publishable-Key und keine Kartendatenerfassung im Portal erforderlich. [Checkout-API](https://docs.stripe.com/api/checkout/sessions/create), [Erstattungs-API](https://docs.stripe.com/api/refunds/create)

Geplante produktive Webhook-Adresse nach Deployment auf der konfigurierten Origin: **https://kundenstatus-app.onrender.com/mieten/webhook**. Sie ist derzeit nicht als live verfügbar bestätigt. Testmodus: `{staging-origin}/mietwagen-test/webhook`. Ereignisse: `checkout.session.completed`, `checkout.session.expired`, `checkout.session.async_payment_succeeded`, `checkout.session.async_payment_failed`. Refund-Status wird über die Refund-API abgeglichen; keine ungeprüften Refund-Webhooks als Erfolg übernehmen.

Der Refund-Datensatz enthält laut API kein garantiertes `livemode`-Feld; Modus wird durch getrennten Gateway/Key, die zuvor verifizierte Session/Payment-Intent-Zuordnung und die erneut gelesene Refund-Antwort abgesichert. [Stripe: Refund-Objekt](https://docs.stripe.com/api/refunds/object)

## Deployment- und Betriebsablauf zur Freigabe

1. Änderungen reviewen; vollständige Dateien einschließlich `mos_booking/`, Templates und Stripe-Abhängigkeit übernehmen. Noch nichts veröffentlichen oder echte Konfiguration aktivieren. Datenbankmigration/Backup und Rollback auf separater PostgreSQL-Testinstanz prüfen.
2. Staging auf separater Portal-Testdatenbank mit Sandbox-Schlüssel. Testmodus ist derzeit absichtlich auf eine SQLite-Datei mit Endung `.mos-public-test.sqlite3` begrenzt; PostgreSQL-Sperrpfad zusätzlich separat abnehmen. Protokollieren: konkurrierender Admin/Checkout, verspäteter Webhook, Duplikate, Timeout, Zahlung/Abbruch-Rennen, Storno, Teilgutschrift, Kautionsrückzahlung, fehlgeschlagene und unklare Refund-Antwort.
3. Mit geeigneten Geräten tatsächlichen Apple-Pay-/Google-Pay-/Karten-Checkout, Stripe-Rechte und finale Zahlungszusammenfassung einschließlich Kaution abnehmen. Die vorherige Preisübersicht ist kein Ersatz für den rechtlich eindeutigen letzten Zahlungsbutton. Vertragsannahme und Providerabschluss müssen zum finalen Bedingungstext passen.
4. Nach vollständigen externen Belegen private Livekonfiguration, stabile sichere Cookies und Secretstore setzen. Portal läuft auf dem bestehenden autoritativen PostgreSQL-Bestand. Keine zweite Homepage-DB verwenden.
5. Live-Key/Webhook erst nach gesonderter Handlungsgenehmigung anlegen; HTTPS-Endpunkt und Signaturprüfung verifizieren. Öffentliche Buttons erst danach über `MOS_PUBLIC_LIVE_ENTRY_URL=https://kundenstatus-app.onrender.com/mieten/` im schlanken MOS-Frontend einblenden. `enabled` und `live_enabled` bleiben bis zur Abnahme false.
6. Im Betriebsplan regelmäßig `python -m flask --app app mos-booking-reconcile` ausführen. Der Befehl sendet keine Bestätigungen ohne signierten Webhook; er gleicht offene Sessions und unveränderliche Erstattungsaufträge ab. Im aktuellen Lauf wurde kein Scheduler angelegt. Fehlerausgabe und Admin-Prüfliste aktiv überwachen, unklare alte Aufträge manuell abgleichen.
7. Adminansicht: `/mieten/admin` (Test: `/mietwagen-test/admin`), geschützt durch bestehenden Admin-Login/CSRF. Kundenstatus und signierter PDF-Download sind sitzungsgebunden; bei verlorenem Browserzugang hilft die Werkstatt nach Identitätsprüfung mit der gespeicherten Vertragskopie. Automatischer Mailversand ist nicht Teil dieser Implementierung.
8. Bei Störung `enabled=false`: keine neuen Buchungen, vorhandene Holds bleiben gesperrt; konfigurierte Webhook-/Status-/Erstattungswege müssen weiterlaufen. Nicht Datenbankzeilen löschen oder unklare Zahlungen einfach freigeben. Hinter einem Proxy die tatsächliche, vertrauenswürdig konfigurierte Client-IP prüfen: die persistente Rategrenze nutzt `remote_addr`, nicht ungeprüfte Forwarded-Header.

## Verifikation

13 neue Produktionslogiktests: fehlende Freigaben, zwei erlaubte Fahrzeuge, Restricted-Key-Modus, getrennte Miet-/Kautionsposten, 24h-Stornogrenze, Minderungen, Nichterscheinen, unveränderliche Refund-Wiederholung nach Timeout, Sperre alter unklarer Aufträge, parallele Stornierungen, Kautionsrückgabe und Adminschutz. Externe Sockets sind in Tests gesperrt; sämtliche Live-API-Objekte sind ausdrücklich synthetische Mocks. Weitere Regressionen: öffentlicher Ablauf, gemeinsamer Bestand und bisheriger Prototyp. Kein Test ist ein Nachweis tatsächlicher Stripe-Zustellung oder Versicherung.


## Abschlussprüfung am 23.09.2026

Der ursprüngliche Bearbeiter hat die Arbeit gestoppt. Nach Ablauf der alten Lease wurde Aufgabe 69 ohne konkurrierenden Claim für die Abschlussprüfung übernommen. Erneut bestanden: 13 Produktions-, 8 Public-, 21 Bestands- und 28 Prototyptests (70 insgesamt), Syntaxprüfung und `git diff --check`. Die vorhandenen Smoke-/Flow-Protokolle enden erfolgreich; diese beiden Läufe wurden bei der Abschlussprüfung nicht wiederholt. Tests laufen mit `.agent-hub/booking-venv/Scripts/python.exe`; das normale Python scheitert derzeit am fehlenden Stripe-Modul.

Vor Deployment zusätzlich separat erledigen: Stripe-Abhängigkeit in den tatsächlichen Hosting-Installationspfad aufnehmen (aktuell nur `requirements-mos-booking.txt`), den historischen C3-Datensatz aus dem Test-Seeder entfernen und ältere Drei-Fahrzeug-Dokumentation auf die aktuelle Zweierflotte korrigieren. Der öffentliche Selector schließt C3 bereits aus. Diese Dateien liegen außerhalb von Aufgabe 69 und wurden bei der Abschlussprüfung nicht geändert. Vorhandene Preview-Prozesse können älteren Code zeigen und gelten nicht als Live-Abnahme. Alle Änderungen sind uncommitted; kein Push, Deployment oder echter Zahlungsauftrag.


## Folgeaufgabe 70 – reguläres Setup und Zweier-Testflotte

Die zuvor dokumentierten Setup-/Seed-Restpunkte sind behoben: `requirements.txt` bindet `requirements-mos-booking.txt` ein; damit installiert auch der vorhandene Render-Build die identischen Flask-/Stripe-Versionen. Reguläres Setup: `python -m pip install -r requirements.txt`. Die kleine Datei bleibt als eigenständiges Minimalsetup für den isolierten Prototyp nutzbar; für den Portal-Teststarter ist das vollständige Setup erforderlich. Installation und Offline-Tests benötigen keine Stripe-Secrets und aktivieren keinen Livebetrieb.

Der öffentliche Test-Starter erzeugt ausschließlich zwei synthetische Datensätze (i10/KONA), mit lokal erzeugten IDs. Alte Prototyp-/Anfrageprüfungen sind ausdrücklich historische Berichte; aktuelle Test- und Bedingungsdokumentation verwendet die Zweierflotte. Reale IDs, Live-Freigaben und externe Abnahmen bleiben unverändert offen.

Verifikation Aufgabe 70: vollständiges `pip install -r requirements.txt` in neuer lokaler Test-venv mit vorhandenen Systempaketen erfolgreich (kein vollständig leeres Build-Abbild); Stripe-/Buchungsmodule ohne Secrets importiert. 8 Public-Tests einschließlich exakter Zweier-Seed-Prüfung und 13 Produktionslogiktests bestanden; `git diff --check` bestanden. Kein echter Stripe-/PostgreSQL-/Hostinglauf.


## Aufgabe 72 – reale Bestandsprüfung und Zulaufsperre

Read-only im Portal: Beide Fahrzeugdatensätze gefunden. KONA ist trotz verfügbar wirkender Übersicht im Grundzustand noch „In Kürze verfügbar“; tatsächliche Übergabe und Zulassung sind nicht nachgewiesen. Die leasingseitige Mietwagennutzung wurde nachträglich anhand der Annahmebestätigung belegt (siehe Korrektur unten). i10-Portalpreis und geplanter öffentlicher Tarif weichen voneinander ab. Der geprüfte KONA-Vertrag ist ausdrücklich gesperrter Entwurf ohne gespeicherte Version. Keine konkreten freigegebenen Online-Zeitfenster festgestellt. Details bleiben wegen Betriebsdaten lokal in `.agent-hub/mos-bestandspruefung-20260923.md`.

Die neue Buchungslogik lehnt Zulauffahrzeuge jetzt bei Angebot und Reservierung ab. Wechsel auf Zulauf nach Checkout führt bei Zahlung zur manuellen Prüfung statt automatischer Vermietung. Zwei Regressionen vor Änderung fehlgeschlagen; danach 23 Bestands-,10 Public- und13 Produktionslogiktests bestanden (46), diff-check bestanden. Der alte Anzeigeeffekt im Admin wurde nicht im Rahmen dieses Claims geändert; separate Folgeaufgabe erforderlich. Kein Portal-Datensatz geändert, keine Livekonfiguration aktiviert.


## Aufgabe 73 – korrigierter Leasingnachweis

Der Ursprungstask hat in IONOS die Hyundai-Leasing-Annahmebestätigung vom 09.07.2026 geprüft: Anhang `Annahmebestaetigung.pdf`, Seite 3, Allane SE als Leasinggeber, passendes KONA-Modell und ausdrücklich „Fahrzeugnutzung: Mietwagen (ohne Carsharing)“. Eine Nachricht des Hyundai-Kundenservice vom 24.07.2026 bestätigt die Vertragsaktivierung. Die angenommene Mietwagennutzung ist damit dokumentiert; „fehlende leasingseitige Mietwagenfreigabe“ wird nicht länger als offener Nachweis geführt. Diese Korrektur beruht auf dem Quellenbericht des Ursprungstasks; in dieser Folgeaufgabe wurde das PDF nicht erneut geöffnet.

Das belegt weder BGV-Deckung noch tatsächliche Fahrzeugübergabe oder Zulassung. KONA bleibt im Portal auf Zulauf mit Platzhalterkennzeichen und fehlender FIN. Diese Stammdaten, Versicherungsdeckung beider Wagen, vollständiger Kalender/Slots, finale Bedingungen sowie Stripe-/PostgreSQL-End-to-End-Abnahme bleiben offen. Kein Live-Gate wurde geändert. Die BGV-Anfrage wurde laut Ursprungstask am 23.09.2026 um 10:57 über IONOS versandt und im Gesendetordner geprüft; eine Deckungsbestätigung liegt weiterhin nicht vor.


## Aufgabe74 – echte lokale PostgreSQL-Prüfung

Eine getrennte lokale PostgreSQL17.11-Testinstanz wurde eingerichtet. 23 Bestands-/Konkurrenz-/Rollback-/Refundtests mit synthetischem Anbieter bestanden; Cluster anschließend gestoppt. Kein echter Stripe-Lauf: sicher hinterlegte Testschlüssel/Webhook fehlen. Leere Portalinitialisierung hat eine DDL-Reihenfolgelücke; der Testbootstrap ordnet das synthetische Schema vorab. Details und Grenzen: [Staging-Abnahme](mos-staging-abnahme.md). Keine Livefreigabe.


## Aufgabe75 – PostgreSQL-Kaltstart behoben

Die in Aufgabe74 gefundene Vorwärtsreferenz ist durch korrektes Anlegen von `auftraege` vor abhängigen Tabellen behoben. Der Test-Runner verwendet keinen Schema-Workaround mehr: Leere PostgreSQL-Datenbank direkt initialisiert,23 Tests bestanden. Smoke/Flow auf isolierten SQLite-Daten, Syntax/diff ebenfalls bestanden. Cluster anschließend gestoppt. Echte Stripe-Sandbox weiterhin mangels sicher hinterlegter Testzugänge nicht ausgeführt.


## Aufgabe76 – KONA-Stammdaten korrigiert

Der Ursprungstask hat zusätzlich die Vertragsaktivierung vom24.07.2026 mit dokumentierter Übernahme am17.07.2026, Kennzeichen und FIN geprüft. Modell und Vertragsreferenz stimmten mit Fahrzeug3 im Portal überein. Kennzeichen, FIN, Grundzustand und Quellennotiz wurden dort autorisiert gespeichert und anschließend erneut gelesen. Die frühere Zulauf-/fehlende-Stammdaten-Feststellung ist damit überholt. Die Notiz hält fehlende BGV-Deckung und Online-Freigabe ausdrücklich fest. Tarif, Checkout-Gates und Vertragsfreigabe wurden nicht geändert. Die gespeicherten Fahrzeugdetails bleiben lokal im Hub.


## Aufgabe77 – Nutzerentscheidung und Releaseprüfung

Nutzer bestätigt i10 39€/Tag,500€ rückzahlbareKaution und1.000€ vertraglicheSelbstbeteiligung; BGV-Klärung soll nachgelagert erfolgen. i10-Portalpreis wurde von30 auf39€ korrigiert und geprüft. Damit entfällt die bisherige Tarifabweichung. Eine Versicherungsdeckung wird nicht als belegt markiert.

Stripe-Testmodus bildet jetzt Miete und500€ Testkaution als zwei getrennte Checkout-Positionen ab; Tests verifizieren647€ Gesamtbetrag für drei KONA-Tage (147€+500€) und ausgeschalteten Livemodus. Das ersetzt keine echte Stripe-Zustellung. Eingeschränkter Testschlüssel/Webhook fehlen weiterhin; gesonderte Schlüsselfreigabe angefragt. Der Code bleibt ohne explizite Zahlungskonfiguration deaktiviert.
