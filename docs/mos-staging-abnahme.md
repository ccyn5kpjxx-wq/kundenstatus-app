# MOS: getrennte PostgreSQL- und Stripe-Abnahme

Stand 23.09.2026, Aufgabe 74. Keine Livefreigabe, kein Deployment.

## Tatsächlich ausgeführt

Portable PostgreSQL 17.11 (EDB Windows-Binaries, Revision 4) aus dem von
[PostgreSQL verlinkten Anbieter](https://www.postgresql.org/download/windows/)
und dessen [Downloadseite](https://www.enterprisedb.com/download-postgresql-binaries).
Lokaler Testcluster ausschließlich auf `127.0.0.1:55439`, eigener Testbenutzer,
SCRAM-Passwort, keine Windows-Dienstinstallation und keine Cloudkosten.
Runtime, Passwort und Verbindungsdatei liegen ausschließlich im ignorierten
`.agent-hub/postgres-runtime/`. Keine Geheimnisse in Git oder Konsolenausgaben.

```powershell
.agent-hub/setup-venv/Scripts/python.exe scripts/test_mos_postgres.py --connection-file .agent-hub/postgres-runtime/connection.json
```

Der Runner akzeptiert ausschließlich den dedizierten Loopback-Testcluster,
erstellt pro Lauf eine neue `mos_acceptance_<zufall>`-Datenbank und initialisiert
sie direkt mit dem echten `init_db()` der Anwendung, ohne Schemavorbelegung.
Bestehende Datenbanken werden weder überschrieben noch gelöscht. Die Anwendung
verwendet anschließend ihren echten PostgreSQL-Adapter und ihre echten
Bestands-/Checkout-Funktionen.

**23 Tests bestanden:** 21 wiederverwendete Bestandstests einschließlich
konkurrierender Admin-/Checkout-Anlage, Anfrageübernahme und Datumsänderung,
idempotenter Webhooks, Timeout-/Abbruchfällen und Zulaufsperre. Hinzu kommen
ein PostgreSQL-Trigger-Test für vollständigen Rollback von Mietvorgang und
Webhook-Ereignis sowie Storno/Erstattung mit idempotenter Wiederholung.
Zahlungsanbieter ist hierbei der synthetische Offline-Gateway, nicht Stripe.

Ein vorhandener HTTP-Test verwendet direkt SQLite-PRAGMA und wird hier nicht
ausgeführt; der konkurrierende Anfrageübernahme-Test läuft auf PostgreSQL.
Der SQLite-Trigger-Test wurde durch einen echten PostgreSQL-Trigger-Test ersetzt.
Das ist eine lokale Datenbank-/Geschäftslogikprüfung, keine vollständige
Hosting-, Browser-, Migrations- oder Stripe-Abnahme.

## Gefundene Initialisierungslücke

Aufgabe 74 reproduzierte auf einer leeren PostgreSQL-DB den Fehler
`relation "auftraege" does not exist`. Aufgabe 75 hat die DDL-Reihenfolge in
`app.py` korrigiert: `auftraege` wird unmittelbar nach `autohaeuser` angelegt,
bevor abhängige Tabellen darauf verweisen. Kein Spalten-/Dateninhalt geändert.
Der temporäre Schema-Workaround wurde aus dem Runner entfernt. Direkte
Initialisierung einer neuen leeren PostgreSQL-Datenbank und anschließend alle
23 Tests bestanden. Zusätzlich bestanden Smoke- und Flow-Test auf isolierten
SQLite-Testdaten sowie Syntax-/Diff-Prüfung. Der Testcluster wurde danach
wieder gestoppt; keine Produktivdatenbank wurde kontaktiert.

## Stripe-Sandbox: konkrete nächste Voraussetzungen

In der geprüften lokalen Umgebung liegen weder `MOS_STRIPE_TEST_KEY` noch
`MOS_STRIPE_WEBHOOK_SECRET` vor. Deshalb wurde keine echte Stripe-API-Session,
Webhook-Zustellung oder Stripe-Erstattung ausgeführt. Vorhandene Gateway- und
Refund-Implementierung wurde nicht durch einen zweiten Integrationsweg ersetzt.

Benötigt werden ein sicher hinterlegter eingeschränkter Testschlüssel (`rk_test_`),
Checkout Sessions lesen/schreiben, Refunds lesen/schreiben und das Signatursecret
eines getrennten Test-Webhooks. Schlüsselanlage und Berechtigungen müssen separat
autorisiert erfolgen. Keine Live-Schlüssel übernehmen und keine Secrets im Chat.
Die nötigen Rechte einschließlich eventueller Stripe-Abhängigkeiten anschließend
mit dem tatsächlichen eingeschränkten Schlüssel prüfen.

Die öffentliche Testkonfiguration ist bisher absichtlich auf isolierte SQLite
begrenzt. Der echte PostgreSQL-Adapter wurde hier separat geprüft. Für einen
gemeinsamen Browser-/Stripe-/PostgreSQL-Staginglauf ist ein ausdrücklich
abgesicherter Staging-Konfigurationspfad als eigener Schritt erforderlich;
keine Live-Freigabefelder mit erfundenen Nachweisen füllen.

Abnahmekette nach sicherer Konfiguration: Test-Checkout mit Miete und 500-Euro-
Kaution, tatsächliche Signaturzustellung und genau ein Mietvorgang, doppelte/
verspätete Events, konkurrierende Buchung, Abbruch/Timeout, Storno, Refund und
Kautionsrückgabe. Karten-/Apple-Pay-/Google-Pay-Darstellung am passenden Gerät
prüfen; Aktivierung im Dashboard ist kein Zustellungs- oder Checkout-Nachweis.

BGV-Deckung beider Autos, endgültige Tarife, echte Übergabeslots und freigegebene
Bedingungen bleiben Voraussetzung für Kundenzahlungen. Die gemeldete Allane-
Mietwagennutzung und dokumentierte KONA-Übernahme ersetzen diese Nachweise nicht.

Update Aufgabe77: Öffentliche stripe_test-Angebote enthalten nun Miete plus500€ Kaution und derselbe SharedCheckout erzeugt zwei Posten auch im Testmodus. Testkennzeichnung bleibt erhalten. Lokaler Regressionstest nutzt synthetischen Gateway; echte Stripe-Sandbox mangels Schlüssel noch nicht ausgeführt. Der Offline-Starter bleibt bewusst offline und ist kein Stripe-Runner.
