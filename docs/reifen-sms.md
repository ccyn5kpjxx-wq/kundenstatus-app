# SMS-Prüfung für Reifenbestellungen

`reifen_sms.py` enthält einen echten Twilio-Verify-v2-Adapter. Import,
Konfiguration und die Tests versenden keine SMS. Eine Anbindung an die
Bestellrouten und ein aktiver Twilio-Zugang sind damit noch nicht bestätigt.

## Sichere Einrichtung

Nur im serverseitigen Secretstore der vorgesehenen Umgebung hinterlegen:

- `REIFEN_TWILIO_ACCOUNT_SID`
- `REIFEN_TWILIO_VERIFY_SERVICE_SID`

Genau einen Authentisierungsweg konfigurieren:

- Empfohlen: `REIFEN_TWILIO_API_KEY_SID` und `REIFEN_TWILIO_API_KEY_SECRET`.
  `REIFEN_TWILIO_AUTH_TOKEN` bleibt leer. Ein eingeschränkter Schlüssel braucht
  nur `twilio/verify/verification/create` und
  `twilio/verify/verification-check/create`, keine Rechte zum Verwalten von Diensten.
- Bestehender Testzugang: `REIFEN_TWILIO_AUTH_TOKEN`; beide API-Key-Felder bleiben leer.

Die Account-SID bleibt in beiden Fällen die `AC`-Kontokennung. Beim Key-Zugang
verwendet ausschließlich der HTTP-Basic-Transport die `SK`-Key-SID und das
Key-Secret. Antwortkontrollen verlangen weiterhin dasselbe AC-Konto und den
konfigurierten VA-Dienst. Fehlende Schlüsselhälften, ungültige Kennungen und
gemischte Authentisierungswege sperren die Anbindung; es gibt keinen Rückfall auf
den Account-Token. Erneute Konfiguration mit entfernten oder ungültigen Daten
entfernt auch einen zuvor konfigurierten SMS-Adapter.

Vorhandenes geschäftliches Twilio-Konto und einen passenden Verify-Service
verwenden. Es wird kein Konto oder Service automatisch angelegt. Für das
Reifenportal einen getrennten Service, SMS-Kostenbudget, erlaubte Zielregionen
und Verify-Schutzlimits prüfen. Keine Schlüssel, SMS-Codes oder Kundennummern
in Chat, Git, Screenshots oder Logs ablegen. Ein Trial-Konto kann nur zuvor
verifizierte Empfängernummern erreichen; das ist kein Nachweis des Livebetriebs.
Der aktuelle kostenlose Trial stellt unter „Try out Verify“ einen VA-Dienst für
Verification-/VerificationCheck-APItests bereit; eigene Dienste erfordern ein
Upgrade. Trial-API-Parameter und tatsächlichen Backendfluss gesondert prüfen.

Der Adapter erhält `get_db` aus dem bestehenden Portal. `init_schema(db)` muss
im normalen Initialisierungs-/Migrationsablauf ausgeführt werden; die Tabelle
`reifen_sms_challenges` gehört zur geschützten Datenbanksicherung. Sie enthält
unveränderliche Versuchskennungen, HMAC-Bindungen, die Provider-SID und Zustände,
keine Klartextnummer, Bestellung oder SMS-Codes. Die HMAC-Bindung verwendet das
Secret des ausgewählten Authentisierungswegs. Dessen Rotation oder ein Wechsel
zwischen Token und einem anderen Key-Secret macht ausstehende lokale Nachweise
unbrauchbar; sie bestätigt keine Nummer automatisch.

## Aufrufvertrag

```python
sms = TwilioVerifySMS.from_env(get_db, authorize=authorize_sms_operation)
sms.start(phone_e164, reference)
approved = sms.check(phone_e164, code, reference)
```

`authorize(operation, reference)` muss vor jedem Aufruf exakt `True` liefern.
Der aufrufende Bestellprozess prüft dabei die erlaubte Umgebung, die
authentisierte Bestell-/Sitzungszuordnung, Einwilligung zum Codeversand sowie
atomare Auftrags-, IP-, Telefonnummern- und Kostenlimits. Eine Konfiguration
oder ein allgemeines `enabled=True` allein reicht nicht. Es gibt keinen
autonomen Worker und keinen automatischen SMS-Versand beim Seitenaufruf.

Die Referenz muss unverändert an dieselbe Bestellung und Telefonnummer gebunden
bleiben. Ein Nummernwechsel verlangt eine neue lokale Referenz/Prüfung;
vorhandene Bestellfreigaben dürfen nicht übernommen werden. Der Adapter nimmt
bereits normalisierte E.164-Schreibweise entgegen und errät kein Land.

`start()` liefert `status`, `verification_id` und `reused`. Wiederholte Starts
eines laufenden Versuchs verwenden den gespeicherten Zustand ohne zweite SMS.
Unklare Netzwerkantworten werden nicht automatisch erneut versendet. Derselbe
aktive Telefonnummernnachweis kann keine zweite Bestellreferenz bedienen.
Nach zehn Minuten kann ein neuer Versuch entstehen; alte Provider-SIDs bleiben
eindeutig zugeordnet und werden nicht an andere Versuche umgehängt.

`check()` prüft die gespeicherte `VerificationSid`, nicht eine beliebige
Telefonnummer aus dem Browser. Nur eine passende Antwort mit `status=approved`
und gleicher Account-/Service-/Verification-SID, Telefonnummer und SMS-Kanal
bestätigt den Versuch. Der veraltete Wert `valid=true`, ein HTTP-Erfolg, eine
fehlende Ressource oder ein Browserparameter reichen nicht. Bereits dauerhaft
bestätigte, noch gültige lokale Versuche werden idempotent wiedererkannt.

Verify kontrolliert Codegültigkeit und Providerlimits. Zusätzlich begrenzt der
Adapter auf zehn Minuten und fünf Prüfaufrufe pro Versuch. Der Bestellprozess
behält seine eigenen strengeren Versuchs-/Resend-/Missbrauchsgrenzen und bindet
die Bestätigung serverseitig an die Bestellung. SMS-Verifikation belegt den
Zugriff auf die Nummer, nicht die persönliche Identität oder Zahlungsfähigkeit.

## Transport und Prüfung

Nur die dokumentierten HTTPS-Endpunkte unter `verify.twilio.com` werden
angesprochen. Keine Anbieter-URL aus Konfiguration/Browser, keine Redirects,
keine automatischen Wiederholungen; Verbindungs-/Lesezeitlimits und begrenzte
Antwortgröße. Fehlertexte bleiben allgemein. Für die gesperrte lokale Demo
weiterhin den ausdrücklichen Fake-Adapter verwenden, keine echten Twilio-Keys.

Offline-Prüfung: `python scripts/test_reifen_sms.py`. Die Suite sperrt echte
Netzwerkverbindungen und verwendet ausschließlich synthetische Antworten.
Der tatsächliche PostgreSQL-Betrieb und ein ausdrücklich freigegebener
End-to-End-Lauf sind durch diese Offline-Tests noch nicht belegt.

Offizielle Dokumentation, geprüft am 02.11.2026:

- [Verification starten](https://www.twilio.com/docs/verify/api/verification)
- [Verification prüfen](https://www.twilio.com/docs/verify/api/verification-check)
- [Gültigkeit und Limits](https://www.twilio.com/docs/verify/api/rate-limits-and-timeouts)
- [Verify API und Authentisierung](https://www.twilio.com/docs/verify/api)
- [API-Key-Authentisierung](https://www.twilio.com/docs/usage/requests-to-twilio)
- [Eingeschränkte API-Keys](https://www.twilio.com/docs/iam/api-keys/restricted-api-keys)
- [Offizielle Verify-Rechte](https://docs-resources.prod.twilio.com/documents/Twilio_Restricted_API_Keys_Permissions_-_Verify_Permissions.pdf)
- [Aktueller Verify-Trial](https://www.twilio.com/docs/usage/trials/try-out-verify)
