# Reifen auswählen

Jedes gültige Preisangebot erhält die Aktion „Reifen auswählen“, auch wenn der verbindliche Bestellabschluss noch geschlossen ist. Die Auswahl zeigt den konkreten Artikel, die Stückzahl und den frisch geprüften Gesamtpreis. Sie erstellt keine Bestellung und reserviert keine Ware.

Im ausgeschalteten Kaufmodus führt das vorhandene `bestell_url`-Feld auf `GET /reifen/bestellen/<token>/auswahl`. Der bestehende signierte Token enthält Artikelnummer und die fünf Suchkriterien, keine frei übergebenen Preise. Er ist 15 Minuten gültig. Die Übersicht prüft den Artikel über die gezielte SKU-Suche erneut; sie verwendet weder einen eventuell geänderten Suchrang noch einen Preis aus dem Browser. Ein Originalpreis wird durch den Token nicht reserviert.

Der neue GET benötigt keine Kundendaten, Formulare, Sitzung, Datenbankbestellung, SMS, Lieferantenreservierung oder Zahlungsanbindung. Zusätzliche Queryparameter werden abgewiesen. Antworten einschließlich Fehlern sind privat, nicht speicherbar und senden keinen Referrer. Ausschließlich dieser formularfreie Endpunkt wird vom automatischen Hinzufügen von CSRF-Feldern ausgenommen; bestehende POST-Formulare behalten ihren Schutz.

„Reifen oder Stückzahl ändern“ führt zur Suche zurück und erhält Größe einschließlich Last-/Geschwindigkeitsindex, Saison, Anzahl, Qualität und Montage. Optionale Kriterien werden nur übernommen, wenn vorhandene sichtbare Felder sie anbieten oder ausgeblendete Vorgaben bereits genau übereinstimmen. Nicht verfügbare Optionen lösen keinen automatischen Suchlauf aus. „Preis & Bestand aktualisieren“ prüft dieselbe signierte Auswahl erneut.

Test- und Live-Kaufpfade sowie die Freigaben für verbindliche Bestellungen bleiben unverändert. Der lokale echte Katalog darf den neuen Auswahlendpunkt nur über dieselbe enge GET-/Loopback-/Host-Prüfung lesen wie die Suche. Eine private Vorschau erteilt keine öffentliche Lieferanten-Datenfreigabe.

Prüfungen: `scripts/test_reifen_auswahl_ansicht.py`, `scripts/test_reifen_auswahl_revalidierung.py`, `scripts/test_reifen_suche.py` und `scripts/test_reifen_suche_ui.js`.
