# Persönliche Urlaubsseite: aktuelle Rechte

Die persönliche Seite `/werkstatt/assistent/urlaub` liest das Urlaubskonto
und anschließend die eigenen Schulpläne. Die Schulabfrage prüft den aktuellen
Mitarbeiterstatus sowie Leserecht, Rechteversion und Anmeldeversion erneut.

Wird der Zugang zwischen diesen beiden Abfragen entzogen, antwortet die Seite
kontrolliert mit HTTP 403. Bereits gelesene Urlaubsangaben werden dann nicht
gerendert. Ungültige Jahresangaben oder andere Eingabeprobleme liefern HTTP 400.
Ein gültiger Zugang erhält weiterhin die persönliche Ansicht; der bestehende
Admin-Einstieg leitet zur Verwaltung weiter.

Die gezielte Regression verwendet synthetische SQLite-Daten und die echten
Mitarbeiter- und Schulprüfungen. Sie entzieht unmittelbar nach dem erfolgreichen
Lesen des Urlaubskontos einzeln das Leserecht, die Rechteversion, die
Anmeldeversion oder die Mitarbeiteraktivität. Alle vier Varianten müssen 403
liefern, ohne die Vorlage aufzurufen oder Urlaubs-, Schul- oder Zeitdaten zu
ändern. Ergänzende Routentests prüfen 400, 403 und den gültigen persönlichen
und administrativen Einstieg.

Prüfung: `scripts/test_mitarbeiter_selfservice.py` und
`scripts/test_mitarbeiter_schule.py`, jeweils auf isolierten Testdaten. Diese
Korrektur behebt einen nachgewiesenen Fehler der Urlaubsroute. Eine Verbindung
zur separat gemeldeten globalen 502-Störung ist bisher nicht belegt.
