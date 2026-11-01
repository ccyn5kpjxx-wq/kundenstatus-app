"""Versioned draft texts for the proposed tire checkout; no live approval.

Pure data only. These texts do not enable purchasing, payments, SMS or mail.
The operator must resolve OFFENE_ENTSCHEIDUNGEN and approve an exact later
version before publication. Never replace a saved contract's text snapshot
with this module's latest version when reproducing a confirmation.
"""
from copy import deepcopy
import hashlib
import json


RECHTSTEXTE_VERSION = "entwurf:reifen-bestellung-2026-11-01-v5"
RECHTSTEXTE_STATUS = "entwurf"
RECHERCHIERTER_STAND = "2026-11-01"
ANBIETER = {
    "firma": "Gärtner GmbH Karosserie & Lack",
    "strasse": "Alte Neckarelzer Straße 18/1",
    "plz": "74821", "ort": "Mosbach", "land": "Deutschland",
    "email": "info@auto-lackierzentrum.de", "telefon": "06261 1809260",
    "geschaeftsfuehrer": "Christopher Gärtner",
    "register": "Amtsgericht Mannheim, HRB 754425", "ust_id": "DE358424941",
}
_KONTAKT = (
    "Gärtner GmbH Karosserie & Lack, Alte Neckarelzer Straße 18/1, "
    "74821 Mosbach, Deutschland; E-Mail: info@auto-lackierzentrum.de; "
    "Telefon: 06261 1809260."
)

QUELLEN = (
    ("Aktuelles Anbieterimpressum", "https://auto-lackierzentrum.de/impressum"),
    ("Bestellschaltfläche und Übersicht", "https://www.gesetze-im-internet.de/bgb/__312j.html"),
    ("Zugang einer Erklärung", "https://www.gesetze-im-internet.de/bgb/__130.html"),
    ("Elektronische Eingangsbestätigung", "https://www.gesetze-im-internet.de/bgb/__312i.html"),
    ("Vertragsbestätigung", "https://www.gesetze-im-internet.de/bgb/__312f.html"),
    ("Vorvertragliche Informationen", "https://www.gesetze-im-internet.de/bgbeg/art_246a__1.html"),
    ("EU-Gewährleistungsmitteilung und Originaldateien", "https://europa.eu/youreurope/business/selling-in-eu/consumer-contracts-guarantees/eu-legal-guarantee-notice-and-garan-label/index_en.htm"),
    ("EU-Gewährleistungsmitteilung seit 27.09.2026", "https://commission.europa.eu/topics/consumers/consumer-rights-and-complaints/sustainable-consumption_en"),
    ("Muster Widerrufsbelehrung, Fassung 2026", "https://www.gesetze-im-internet.de/bgbeg/art_253anlage_1.html"),
    ("Muster Widerrufsformular", "https://www.gesetze-im-internet.de/bgbeg/art_253anlage_2.html"),
    ("Widerrufsfrist", "https://www.gesetze-im-internet.de/bgb/__355.html"),
    ("Fristbeginn und Dienstleistungen", "https://www.gesetze-im-internet.de/bgb/__356.html"),
    ("Elektronische Widerrufsfunktion", "https://www.gesetze-im-internet.de/bgb/__356a.html"),
    ("Widerrufsfolgen", "https://www.gesetze-im-internet.de/bgb/__357.html"),
    ("Wertersatz", "https://www.gesetze-im-internet.de/bgb/__357a.html"),
    ("Ausnahmen vom Widerrufsrecht", "https://www.gesetze-im-internet.de/bgb/__312g.html"),
    ("Optionale Zusatzentgelte", "https://www.gesetze-im-internet.de/bgb/__312a.html"),
    ("Verbraucherschlichtung", "https://www.gesetze-im-internet.de/vsbg/__36.html"),
    ("Elektronischer Widerruf seit 19.06.2026", "https://vis.bayern.de/recht/verbrauchervertraege/widerrufbutton_2026.htm"),
    ("EU-OS-Plattform eingestellt", "https://consumer-redress.ec.europa.eu/site-relocation_en"),
    ("Datenschutz-Grundverordnung", "https://eur-lex.europa.eu/eli/reg/2016/679/oj?locale=de"),
    ("Stripe Datenschutz", "https://stripe.com/de/privacy"),
    ("Stripe Datenschutzrollen", "https://stripe.com/de/legal/privacy-center"),
    ("Stripe Datenverarbeitung", "https://stripe.com/de/legal/dpa"),
    ("Stripe Drittlandübermittlungen", "https://stripe.com/de/legal/dta"),
    ("Twilio Datenschutz", "https://www.twilio.com/en-us/legal/privacy"),
    ("Twilio Datenverarbeitung und Übermittlungen", "https://www.twilio.com/en-us/legal/data-protection-addendum"),
)

OFFENE_ENTSCHEIDUNGEN = (
    "Katalogprüfung: Für jeden online angebotenen Reifen muss bestätigt sein, dass er auf dem "
    "normalen Postweg als Paket zurückgesendet werden kann. Nicht paketversandfähige Ausführungen "
    "sind vom Onlinekatalog ausgeschlossen, solange die erforderlichen Rücksendekosten nicht "
    "belastbar geklärt und die Informationen angepasst sind. Es gilt die beschlossene Regel: "
    "Kunde trägt die unmittelbaren Rücksendekosten; persönliche Rückgabe in Mosbach ist möglich.",
    "Gewährleistungsmitteilung: Vor einem Liveangebot die seit 27.09.2026 erforderliche "
    "harmonisierte Mitteilung nach Anhang I der EU-Verordnung 2025/1960 hervorheben. Online "
    "das unveränderte farbige Original mit lesbarem Inhalt und QR-Code verwenden. Ein "
    "AGB-Satz oder bloßer Quellenlink ersetzt diese Darstellung nicht. Liegen konkrete "
    "Herstellerinformationen zu einer einschlägigen Haltbarkeitsgarantie vor, zusätzlich "
    "die dafür vorgesehene GARAN-Kennzeichnung und Garantiebestimmungen prüfen.",
    "Verbraucherschlichtung: Teilnahmebereitschaft, eine mögliche Verpflichtung und Beschäftigtenzahl "
    "zum 31.12.2025 prüfen; danach passende Information nach §§ 36/37 VSBG ergänzen. "
    "Keine Teilnahme oder Nichtteilnahme erfinden; keinen abgeschalteten EU-OS-Link einbauen.",
    "Annahme und Zahlung: Bestellannahme erst nach bestätigter Liefer- und gegebenenfalls "
    "Montagereservierung; bei Onlinezahlung zusätzlich bestätigter Zahlung. Ausdrückliche Annahme "
    "auf der Bestätigungsseite oder per E-Mail, deren Zugang, die zusätzliche vollständige "
    "Vertrags-E-Mail, Zahlungszeitpunkt und Rückzahlung bei Nichtannahme müssen im echten Ablauf übereinstimmen. "
    "Bearbeitungs-/Annahmefrist und Behandlung einer abgelaufenen Auswahl betrieblich festlegen.",
    "Lieferung/Abholung: Verbindlicher Bereitstellungs-/Montagetermin, maximaler Reservierungszeitraum, "
    "Abholbenachrichtigung und Rückabwicklung müssen vor jeder einzelnen Bestellung feststehen. "
    "Keine Lieferdauer oder kostenpflichtige Lager-/Stornopauschale aus diesem Text ableiten.",
    "Montage: Warenkauf und gesondert berechnete Montage transparent ausweisen; Einordnung eines "
    "kombinierten Vertrags und dazu passende Widerrufsbelehrung abschließend prüfen. Ein "
    "Montageverlangen darf keinen pauschalen Ausschluss des Warenwiderrufs bewirken.",
    "Datenschutz: Tatsächlich verwendete Stripe-/Twilio-Gesellschaften, Vertragsrollen, "
    "Auftragsverarbeitung, Unterauftragnehmer, Regionen, Transfergrundlagen, konkrete Löschfristen "
    "und gegebenenfalls Datenschutzbeauftragten ergänzen. Der Anbieterhinweis ersetzt diese Prüfung nicht.",
    "Produktionsfreigabe: Vorvertragliche Informationen, speicherbare Vertragsbestätigung und "
    "Widerrufsformular, sichtbarer elektronischer Widerruf mit Eingangsbestätigung sowie "
    "Erstattungs-/Teilwiderrufsablauf Ende zu Ende prüfen. Die entworfenen Texte sind keine Livefreigabe.",
)

_AGB = f"""1. Verkäufer und Angebot
{_KONTAKT} Vertreten durch Geschäftsführer Christopher Gärtner. Register: Amtsgericht Mannheim,
HRB 754425. Umsatzsteuer-ID: DE358424941. Diese Bedingungen betreffen den privaten Reifenkauf
mit Abholung und auf Wunsch gesondert ausgewiesener Montage in unserer Werkstatt in Mosbach.
Vertragssprache ist Deutsch. Es erfolgt keine Lieferung an eine Kundenadresse.
Online bieten wir nur Reifen an, die auf dem normalen Postweg als Paket zurückgesendet werden
können. Nicht paketversandfähige Ausführungen sind von diesem Onlineangebot ausgeschlossen.

2. Auswahl und Preis
Die Produktanzeige ist eine Einladung, eine Bestellung abzugeben. Entscheidend sind die konkret
angezeigten Reifen mit Hersteller, Modell, Größe, Last- und Geschwindigkeitsindex, Stückzahl und
den ausdrücklich gewählten Leistungen. Vor dem Abschluss sehen Sie alle Einzelpositionen und
den Gesamtpreis in Euro einschließlich Mehrwertsteuer und ausgewiesener Bezugskosten. Eine
Montage wird nur berechnet, wenn Sie diese ausdrücklich wählen. Weitere Arbeiten oder Kosten
bedürfen einer gesonderten Vereinbarung; sie werden nicht automatisch hinzugefügt.

3. Bestellung und Vertragsschluss
Sie können Ihre Eingaben vor dem Abschluss prüfen und berichtigen. Mit dem Button
„zahlungspflichtig bestellen“ geben Sie ein verbindliches Kaufangebot zu der gezeigten Übersicht
ab. Die elektronische Eingangsbestätigung bestätigt zunächst nur den Eingang und ist noch keine
Annahme. Der Vertrag kommt zustande, sobald Ihnen unsere ausdrückliche Bestellannahme auf der
Bestätigungsseite unseres Portals oder per E-Mail zugeht. Wir erklären diese Annahme erst
nach Bestätigung der Lieferreservierung und, falls gewählt, des Montageplatzes. Bei Onlinezahlung
muss zusätzlich der erfolgreiche Zahlungseingang sicher bestätigt sein. Bei Zahlung vor Ort setzt
die Annahme keine vorherige Zahlung voraus; der Betrag bleibt bis zur Abholung oder Montage offen.
Eine bloße Rückkehr von einer Zahlungsseite, ein interner Buchungsstatus oder die Einreihung
einer E-Mail in unsere Versandwarteschlange bestätigt für sich weder Zahlung noch Vertragsannahme.
Wenn sich Preis oder Leistungsumfang vor Ihrer Bestellung ändern, erhalten Sie eine neue Übersicht
und müssen erneut zustimmen. Ohne Ihre Zustimmung wird kein höherer Preis vereinbart. Können wir
nicht annehmen, erhalten Sie eine entsprechende Nachricht; bereits erhaltene Beträge werden ohne
unnötige Verzögerung zurückgezahlt. Ihre gesetzlichen Rechte bleiben unberührt.

4. Zahlung, Abholung und Montage
Die im Checkout angebotenen Zahlungsmittel sind maßgeblich. Bei Onlinezahlung läuft die Zahlung
über die angezeigte Zahlungsseite von Stripe. Bei der Wahl vor Ort ist der Gesamtbetrag bei
Abholung beziehungsweise Montage zu zahlen. Für diese Zahlungswahl fällt kein Aufschlag an.
Abholung und Montage finden an unserer oben genannten Adresse statt. Die bestätigte
Bereitstellung und gegebenenfalls der konkrete Montagezeitpunkt stehen in Ihrer Bestellannahme.
Eine angezeigte Terminvorschau allein reserviert keinen Platz. Reifen dürfen erst montiert werden,
wenn ihre Verfügbarkeit und Zuordnung zum Fahrzeug geklärt sind. Nicht enthaltene RDKS-Arbeiten,
Entsorgung oder Sonderarbeiten vereinbaren wir vor ihrer Ausführung gesondert. Bei notwendigen
Änderungen kontaktieren wir Sie; es gibt keine automatische Ersatzbereifung oder Preiserhöhung.

5. Bestätigung und Rechte
Sie erhalten die angenommene Bestellung einschließlich Produkt-/Leistungsumfang, Gesamtpreis,
Zahlungsstatus, bestätigten Terminen und den bei Bestellung geltenden Bedingungen sowie den
Widerrufsinformationen und dem Formular per E-Mail in speicherbarer Form. Gesetzliche
Mängelrechte bestehen uneingeschränkt. Ansprüche wegen Mängeln neuer Reifen verjähren regelmäßig
zwei Jahre nach der Übergabe. Eine Hersteller- oder sonstige Garantie besteht nur,
soweit sie für das konkrete Produkt ausdrücklich ausgewiesen ist. Gesetzliche Widerrufsrechte
und Ansprüche bei Pflichtverletzungen werden durch diese Bedingungen nicht verkürzt.
"""

_LIEFERUNG = """Die bestellten Reifen sind ausschließlich zur Abholung oder zur ausdrücklich
gewählten Montage bei Gärtner GmbH Karosserie & Lack, Alte Neckarelzer Straße 18/1, 74821 Mosbach,
vorgesehen. Es erfolgt keine Zustellung an die Rechnungsadresse. Eine Rechnungsadresse begründet
keine Versandoption. Im Onlineangebot sind ausschließlich paketversandfähige Reifen enthalten;
nicht paketversandfähige Ausführungen können hier nicht bestellt werden.
Den Gesamtpreis einschließlich Mehrwertsteuer, Bezugskosten und gewählter
Montage sowie die verfügbaren Zahlungsmittel sehen Sie vor dem verbindlichen Abschluss.
Ein konkreter Bereitstellungs- und gegebenenfalls Montagezeitpunkt wird erst nach bestätigter
Verfügbarkeit angenommen und in der Bestellannahme genannt. Ohne belastbare Bestätigung erfolgt
keine automatische Bestellung eines Ersatzartikels und keine verbindliche Terminzusage.
"""

_WIDERRUF_WAREN = f"""Widerruf beim Reifenkauf
Sie können den online geschlossenen Kaufvertrag ohne Begründung innerhalb von 14 Tagen widerrufen.
Die Frist setzt voraus, dass Sie ordnungsgemäß über das Widerrufsrecht informiert wurden. Für die
Frist zählt der Erhalt der Reifen durch Sie oder eine von Ihnen bestimmte Person, die nicht der
Transporteur ist. Werden Reifen derselben Bestellung getrennt übergeben, zählt der Erhalt des
letzten Reifens. Auch bei einer Abholung ist die tatsächliche Übergabe maßgeblich.

Teilen Sie Ihren Entschluss eindeutig mit, zum Beispiel per E-Mail, telefonisch oder per Brief an:
{_KONTAKT} Sie können das beigefügte Formular verwenden; das ist freiwillig. Außerdem steht Ihnen
auf unserer Website der gut sichtbare Einstieg „Vertrag widerrufen“ zur Verfügung. Dort können Sie
Name, Bestellung oder betroffene Positionen und Ihren elektronischen Bestätigungskontakt angeben
und den Widerruf anschließend bestätigen. Den Eingang bestätigen wir Ihnen unverzüglich in
speicherbarer elektronischer Form mit Inhalt, Datum und Uhrzeit. Rechtzeitiges Absenden Ihrer
Widerrufserklärung genügt zur Fristwahrung; die Onlinefunktion ersetzt andere zulässige Wege nicht.

Nach einem wirksamen Widerruf erstatten wir erhaltene Zahlungen einschließlich der gesetzlich zu
erstattenden Standardlieferkosten ohne unnötige Verzögerung, spätestens 14 Tage nach Zugang Ihres
Widerrufs. Mehrkosten einer ausdrücklich gewählten teureren Lieferung gehören nicht dazu.
Grundsätzlich erfolgt die Erstattung über Ihr ursprüngliches Zahlungsmittel; eine andere Lösung
vereinbaren wir nur ausdrücklich und ohne zusätzliche Erstattungskosten für Sie. Beim Warenkauf
dürfen wir mit der Rückzahlung warten, bis die Ware wieder da ist oder ihr Versand nachgewiesen
wurde; maßgeblich ist das frühere Ereignis. Das gilt nicht, wenn wir eine Abholung angeboten haben.

Geben oder schicken Sie die Reifen ohne unnötige Verzögerung, spätestens 14 Tage nach Ihrer
Widerrufsmitteilung, an unsere oben genannte Adresse zurück. Für eine Rücksendung genügt die
Absendung innerhalb dieser Zeit. Sie tragen die unmittelbaren Kosten der Rücksendung. Sie können
die Reifen innerhalb dieser Frist auch persönlich an unserer oben genannten Adresse in Mosbach
zurückgeben. Im Onlineangebot sind ausschließlich Reifen enthalten, die auf dem normalen Postweg
als Paket zurückgesendet werden können. Eine Vorabfreigabe oder die Verwendung unseres Formulars
ist keine Voraussetzung für Ihren Widerruf. Diese Rücksendekostenregel für einen normalen Widerruf
ändert nichts an Ihren gesetzlichen Rechten bei Mängeln oder anderen Pflichtverletzungen.
Für einen Wertverlust müssen Sie nur einstehen, wenn er durch einen Umgang entstanden ist, der
über das zur Prüfung der Ware erforderliche Maß hinausgeht, und die gesetzlichen Voraussetzungen
vorliegen. Montage oder Nutzung schließen den Warenwiderruf nicht automatisch aus.
"""

_WIDERRUF_MONTAGE = f"""Widerruf bei gesondert vereinbarter Montage
Für einen online geschlossenen, gesonderten Montage-Dienstleistungsvertrag besteht grundsätzlich
ein 14-tägiges Widerrufsrecht ab Vertragsschluss bei ordnungsgemäßer Belehrung. Erklären können Sie
den Widerruf ohne Begründung über die oben genannten Kontaktwege oder die elektronische Funktion.
Kontakt: {_KONTAKT} Eine rechtzeitig abgesandte Erklärung genügt. Erhaltene Zahlungen erstatten wir
ohne unnötige Verzögerung, spätestens 14 Tage nach Eingang des Widerrufs, grundsätzlich über das
ursprüngliche Zahlungsmittel und ohne zusätzliche Erstattungskosten.

Soll die Montage schon während dieser Frist beginnen, benötigen wir vor Beginn Ihr ausdrückliches
Verlangen. Zusätzlich müssen Sie bestätigen, dass Sie wissen: Das Widerrufsrecht für die bezahlte
Montage kann erst bei vollständig erbrachter Leistung und unter den gesetzlichen Voraussetzungen
erlöschen. Eine bloße Terminwahl, ein vorangekreuztes Feld oder eine allgemeine Zustimmung zu den
Bestellbedingungen genügt hierfür nicht. Ohne dieses Verlangen beginnen wir nicht vor Fristablauf.
Widerrufen Sie nach einem ausdrücklich verlangten frühen Beginn, kann unter den gesetzlichen
Voraussetzungen ein angemessener Betrag für den bis zur Erklärung erbrachten Leistungsanteil
anfallen. Grundlage ist das Verhältnis zur insgesamt vereinbarten Leistung, nicht eine pauschale
Stornogebühr. Ein Erlöschen bei der Montage beseitigt nicht pauschal das Widerrufsrecht für den
Reifenkauf. Bei einer verbundenen Bestellung werden die betroffenen Positionen und ihre gesetzlichen
Folgen konkret geprüft.
"""

_FORMULAR = """Muster-Widerrufsformular
(Wenn Sie den Vertrag widerrufen wollen, dann füllen Sie bitte dieses Formular aus und senden Sie es zurück.)

An
Gärtner GmbH Karosserie & Lack
Alte Neckarelzer Straße 18/1
74821 Mosbach, Deutschland
E-Mail: info@auto-lackierzentrum.de

Hiermit widerrufe(n) ich/wir (*) den von mir/uns (*) abgeschlossenen Vertrag über den Kauf der folgenden Waren (*)/die Erbringung der folgenden Dienstleistung (*)
______________________________________________________________________
______________________________________________________________________

Bestellt am (*)/erhalten am (*)
______________________________________________________________________

Name des/der Verbraucher(s)
______________________________________________________________________

Anschrift des/der Verbraucher(s)
______________________________________________________________________
______________________________________________________________________

Unterschrift des/der Verbraucher(s) (nur bei Mitteilung auf Papier)
______________________________________________________________________

Datum
______________________________________________________________________

(*) Unzutreffendes streichen.
"""

_DATENSCHUTZ = f"""Datenschutzhinweise zum geplanten Reifenbestellprozess
Verantwortlich für unsere Verarbeitung ist {_KONTAKT} Wir verarbeiten Ihre Kontakt- und
Rechnungsdaten, Reifen-/Leistungsauswahl, Terminangaben, Bestell- und Zahlungsstatus sowie
Bestätigungen, um Ihre Bestellung vorzubereiten, abzuschließen und abzuwickeln. Rechtsgrundlage
hierfür ist Artikel 6 Absatz 1 Buchstabe b DSGVO. Soweit gesetzliche Dokumentationspflichten
bestehen, gilt Buchstabe c. Zum Schutz gegen Missbrauch verarbeiten wir notwendige technische
Prüf- und Sicherheitsdaten auf Grundlage von Buchstabe f; unser Interesse ist ein zuverlässiger
und vor missbräuchlichen Bestellungen geschützter Ablauf. Eine zusätzliche Werbeeinwilligung
wird dadurch nicht erteilt.

Bei gewählter Onlinezahlung übermitteln wir die dafür notwendigen Kontakt-, Bestell-, Betrags-
und Referenzdaten an Stripe. Zahlungsdaten geben Sie auf der bereitgestellten Stripe-Zahlungsseite
ein. Unser Portal erhält Zahlungsreferenzen und Statusinformationen; vollständige Kartenangaben
gehören nicht in unser Bestellformular oder unsere Protokolle. Stripe verarbeitet je nach Dienst
als Auftragsverarbeiter und/oder eigener Verantwortlicher. Näheres einschließlich beteiligter
Gesellschaften und Betroffenenrechten: https://stripe.com/de/privacy und
https://stripe.com/de/legal/privacy-center . Bei Zahlung vor Ort wird dafür keine Onlinezahlung
an Stripe gestartet.

Für einen von Ihnen angeforderten Bestätigungscode ist Twilio vorgesehen. Dabei werden die
Mobilnummer sowie notwendige Verifikations-, Zustell- und Sicherheitsdaten verarbeitet. Die
Telefonprüfung dient dieser Bestellung und ist keine Werbeanmeldung. Bestätigungscodes werden
nicht im Klartext in unseren normalen Bestellprotokollen gespeichert. Anbieterinformationen:
https://www.twilio.com/en-us/legal/privacy . Für auftragsbezogene Verarbeitung gelten die
vereinbarten Datenschutzbedingungen; teilweise verarbeitet Twilio auch in eigener Verantwortung.

Zugriff bei uns haben nur für die Bearbeitung zuständige Personen und notwendige technische
Dienstleister. Eine Verarbeitung durch die Anbieter oder deren Dienstleister außerhalb des EWR
kann je nach Dienst und Konfiguration stattfinden. Informationen zu den jeweils vereinbarten
Übermittlungsgrundlagen und Schutzmaßnahmen erhalten Sie über unseren Kontakt. Anbietergrundlagen:
https://stripe.com/de/legal/dta und
https://www.twilio.com/en-us/legal/data-protection-addendum . Eine ausschließlich deutsche oder
europäische Verarbeitung wird hiermit nicht behauptet.

Bestell- und Nachweisdaten bleiben für die Vertragsabwicklung und danach nur so lange gespeichert,
wie gesetzliche Aufbewahrungspflichten oder die Klärung konkreter Ansprüche dies erfordern.
Bloße Verifikations- und Sicherheitsdaten werden zweckgebunden und kürzer aufbewahrt; wir geben
Ihnen auf Anfrage die für Ihren Vorgang angewendeten Fristen beziehungsweise Kriterien an.
Unter den gesetzlichen Voraussetzungen können Sie Auskunft, Berichtigung, Löschung,
Einschränkung und Datenübertragbarkeit verlangen. Gegen auf berechtigte Interessen gestützte
Verarbeitung können Sie aus Ihrer besonderen Situation Widerspruch einlegen. Einwilligungen
können, soweit eine Verarbeitung darauf beruht, für die Zukunft zurückgenommen werden. Sie können
sich bei einer Datenschutzaufsichtsbehörde beschweren. Pflichtangaben sind für Bestellung,
Rechnung und sicheren Rückkontakt nötig; ohne sie lässt sich dieser Bestellweg nicht abschließen.
Die Telefonnummernprüfung stellt keine amtliche Identitätsfeststellung dar. Eine automatisierte
Einzelfallentscheidung mit rechtlicher oder ähnlich erheblicher Wirkung ist nicht vorgesehen.
"""


def reifen_rechtstexte(*, demo=False):
    """Return an isolated JSON-safe draft snapshot with a deterministic digest.

    demo=True marks simulation copies. Neither variant is a legal/business
    release. Approval must never be inferred from nonempty text fields or hash.
    """
    if not isinstance(demo, bool):
        raise ValueError("Demo muss ausdrücklich ein boolescher Wert sein.")
    data = {
        "version": RECHTSTEXTE_VERSION.replace("entwurf:", "demo:", 1) if demo else RECHTSTEXTE_VERSION,
        "status": RECHTSTEXTE_STATUS, "freigegeben": False, "stand": RECHERCHIERTER_STAND,
        "hinweis": "Arbeitsentwurf zur Prüfung; nicht als Freigabe für einen Liveabschluss verwenden.",
        "anbieter": deepcopy(ANBIETER),
        "agb": _AGB.strip(), "lieferung": _LIEFERUNG.strip(),
        "widerruf_waren": _WIDERRUF_WAREN.strip(), "widerruf_montage": _WIDERRUF_MONTAGE.strip(),
        "widerruf": (_WIDERRUF_WAREN + "\n\n" + _WIDERRUF_MONTAGE).strip(),
        "widerrufsformular": _FORMULAR.strip(), "datenschutz": _DATENSCHUTZ.strip(),
        "bestellhinweis": "Mit „zahlungspflichtig bestellen“ geben Sie ein verbindliches Angebot ab. "
            "Der Vertrag entsteht mit Zugang unserer ausdrücklichen Bestellannahme auf der Bestätigungsseite "
            "oder per E-Mail nach bestätigter Liefer- und gegebenenfalls Montagereservierung; bei "
            "Onlinezahlung zusätzlich nach sicher bestätigter Zahlung. Die vollständige Vertragsbestätigung "
            "erhalten Sie zusätzlich per E-Mail. Bei Zahlung vor Ort bleibt der Betrag bis zur Abholung oder Montage offen.",
        "frueher_montagebeginn": "Ich verlange ausdrücklich, dass die gesondert vereinbarte Montage "
            "zum gewählten Termin schon vor dem Ende der Widerrufsfrist beginnen darf.",
        "kenntnis_montagewiderruf": "Mir ist bekannt, dass mein Widerrufsrecht für die Montage bei "
            "vollständig erbrachter Leistung unter den gesetzlichen Voraussetzungen erlischt. "
            "Ein Widerrufsrecht für den Reifenkauf wird dadurch nicht pauschal ausgeschlossen.",
        "offene_entscheidungen": list(OFFENE_ENTSCHEIDUNGEN),
        "quellen": [{"titel": title, "url": url} for title, url in QUELLEN],
    }
    if demo:
        prefix = "DEMO-ENTWURF – keine echte Bestellung, Zahlung oder Datenübermittlung.\n\n"
        for key in ("agb", "lieferung", "widerruf", "widerruf_waren", "widerruf_montage",
                    "widerrufsformular", "datenschutz", "bestellhinweis"):
            data[key] = prefix + data[key]
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    data["sha256"] = hashlib.sha256(encoded).hexdigest()
    return data


def rechtstexte_entwurf_text():
    """Human review copy from the exact same text data; no writing side effects."""
    data = reifen_rechtstexte()
    parts = ["GÄRTNER – REIFENBESTELLUNG: ARBEITSENTWURF", "Version: " + data["version"],
             "Stand: " + data["stand"], "SHA-256 (kanonischer JSON-Textstand): " + data["sha256"],
             data["hinweis"], "Freigabestatus: ENTWURF / nicht veröffentlicht."]
    for key, label in (("agb", "BESTELLBEDINGUNGEN"), ("lieferung", "LIEFERUNG / ABHOLUNG"),
                       ("widerruf", "WIDERRUFSINFORMATIONEN"), ("widerrufsformular", "FORMULAR"),
                       ("datenschutz", "DATENSCHUTZHINWEISE – PROZESSBEZOGENER ENTWURF"),
                       ("bestellhinweis", "HINWEIS VOR DEM ABSCHLUSS"),
                       ("frueher_montagebeginn", "OPTIONALE ERKLÄRUNG: FRÜHER MONTAGEBEGINN"),
                       ("kenntnis_montagewiderruf", "GETRENNTE KENNTNISBESTÄTIGUNG")):
        parts.extend((label, data[key]))
    parts.extend(("INTERN – VOR EINER VERÖFFENTLICHUNG OFFEN", *(
        f"{index}. {text}" for index, text in enumerate(data["offene_entscheidungen"], 1))))
    parts.extend(("RECHERCHE / PRIMÄRQUELLEN", *(
        item["titel"] + ": " + item["url"] for item in data["quellen"])))
    parts.append("Bestellbedingungen und Prozesshinweise sind eigene Formulierungen anhand der "
                 "genannten Normen und Anbieterinformationen. Das amtliche Muster-Widerrufsformular "
                 "aus Anlage 2 EGBGB ist mit den Anbieterangaben ausgefüllt; individuelle Vertragsangaben "
                 "bleiben als Formularfelder offen. Keine Zusage einer Rechtsprüfung oder vollständigen "
                 "rechtlichen Absicherung. Hinweise müssen zur tatsächlich freigegebenen Vertragsgestaltung passen.")
    return "\n\n".join(parts) + "\n"
