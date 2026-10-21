"""Internal, order-bound inspection records; no customer messages or status changes."""
from pathlib import Path
from contextlib import closing
import sqlite3
from auftragsarten import auftrag_leistungsart


BACKUP_TABLES = ("inspektions_checklisten", "inspektions_checkpunkte")
SCHEMA_FEATURE = "inspektionscheckliste_v1"
ERGEBNISSE = {
    "offen": "Offen", "erledigt": "Erledigt", "mangel": "Mangel",
    "nicht_zutreffend": "Nicht zutreffend",
}
VORLAGE_TITEL = "Große Inspektion (Benziner) – Werkstattvorlage"
VORLAGE_HINWEIS = (
    "Betriebliche Checkliste zum vereinbarten Arbeitsumfang, kein FIN-spezifischer VW-Wartungsplan. "
    "Fahrzeugbezogene Herstellervorgaben, Freigaben und Montagevorgaben vor der Arbeit abgleichen. "
    "Zusatzarbeiten nur nach Freigabe; Zahnriemen und Getriebeöl sind bei 150.000 km nicht pauschal fällig. "
    "Bei Wechselpositionen tatsächlich geprüft/gewechselt und Abweichungen in der Notiz festhalten. "
    "Die Checkliste setzt den Auftrag nicht automatisch auf Fertig."
)
# Stable keys and persisted wording keep later template changes from rewriting evidence.
VORLAGE = (
    ("01_fahrzeug", "Vorbereitung", "Fahrzeugidentität und Ist-Kilometerstand prüfen", "FIN, Kennzeichen, Motor- und Getriebekennung mit dem Fahrzeug abgleichen; Ist-km dokumentieren."),
    ("02_nachweise", "Vorbereitung", "Serviceanzeige und Wartungsnachweise erfassen", "Letzte Wartung und letzte Wechsel aus Nachweisen erfassen; fehlende Angaben offen benennen."),
    ("03_beanstandungen", "Vorbereitung", "Beanstandungen und Warnanzeigen vorab erfassen", "Kundenangaben, Warnanzeigen und relevante Fehlereinträge vor Arbeitsbeginn dokumentieren."),
    ("25_zusatzfaelligkeiten", "Vorbereitung", "Fahrzeugspezifische Zusatzfälligkeiten abgleichen", "Herstellervorgaben anhand FIN, Motorkennbuchstaben, Getriebekennung und Wartungshistorie auf Zahnriemen-Prüf-/Wechselvorgaben und Getriebeölservice prüfen; Fälligkeit und Freigabe dokumentieren. Aus ca. 150.000 km allein folgt keine pauschale Fälligkeit."),
    ("04_motoroel", "Wartung", "Motoröl wechseln", "Passende Herstellerfreigabe bestätigen; tatsächliche Ölnorm und eingefüllte Menge notieren."),
    ("05_oelfilter", "Wartung", "Ölfilter, Dichtungen und erforderliche Ablassschraube erneuern", "Passende Teile und Montagevorgaben beachten; tatsächlich erneuerte Teile dokumentieren."),
    ("06_zuendkerzen", "Wartung", "Zündkerzen erneuern – nach vereinbartem Umfang", "Vereinbarten Wechsel vor Anlage der Vorlage bestätigen; motorbezogene Teile und Montagevorgaben abgleichen, Wechsel dokumentieren."),
    ("07_luftfilter", "Wartung", "Luftfilter und Gehäuse prüfen; geplanten Wechsel ausführen", "Geplanten Wechsel nach vereinbartem Umfang durchführen; geprüft/gewechselt oder Abweichung notieren."),
    ("08_innenraumfilter", "Wartung", "Innenraumfilter – geplanten Wechsel ausführen", "Passende Ausführung abgleichen; tatsächlichen Wechsel oder Abweichung dokumentieren."),
    ("09_kuehlmittel", "Wartung", "Kühlmittelstand, Frostschutz und Dichtheit prüfen", "Prüfergebnis und gegebenenfalls Auffälligkeiten festhalten."),
    ("10_bremsfluessigkeit", "Wartung", "Bremsflüssigkeitsstand, Dichtheit und Fälligkeit prüfen", "Stand und Dichtheit prüfen; letztes Wechseldatum ermitteln. Fälligkeit nach Fahrzeugvorgaben prüfen; bei Freigabe wechseln und Datum dokumentieren."),
    ("11_bremsen", "Prüfung", "Bremsbeläge und -scheiben vorne und hinten prüfen", "Zustand und relevante Messwerte dokumentieren; Verschleißgrenzen fahrzeugbezogen abgleichen."),
    ("12_bremsleitungen", "Prüfung", "Bremsleitungen, Schläuche und Feststellbremse prüfen", "Zustand, Dichtheit und Funktion prüfen; Auffälligkeiten notieren."),
    ("13_reifen", "Prüfung", "Reifen, Profiltiefe, Druck, RDK und Pannenhilfe prüfen", "Zustand und Messwerte festhalten; Druck nach Fahrzeugvorgabe einstellen, RDK nach Herstellervorgabe prüfen/gegebenenfalls kalibrieren. Vorhandenes Dichtmittel auf Verfallsdatum, Kompressor und gegebenenfalls Reserverad prüfen; Ausstattung und Auffälligkeiten notieren."),
    ("14_fahrwerk", "Prüfung", "Lenkung, Gelenke, Lager, Manschetten, Federn und Dämpfer prüfen", "Zustand, Spiel und erkennbare Schäden prüfen; Antriebswellen-Gelenkschutzhüllen, Lenkungsmanschetten sowie Stabilisatorlager und -anbindungen einbeziehen. Auffälligkeiten dokumentieren."),
    ("15_antrieb", "Prüfung", "Motor, Getriebe und Antrieb prüfen", "Auf Undichtigkeiten und lose oder beschädigte Befestigungen prüfen."),
    ("26_riementrieb", "Prüfung", "Zugänglichen Nebenaggregateriemen und Riementrieb auf Sicht prüfen", "Zugänglichen Riemen, Rollen und Spanner auf erkennbare Schäden, Verschleiß und auffälligen Lauf prüfen; Befund sowie eingeschränkte Zugänglichkeit dokumentieren. Erforderliche Zusatzarbeiten abstimmen."),
    ("16_unterboden", "Prüfung", "Karosserie und Unterboden prüfen", "Korrosion und erkennbare Schäden prüfen; bekannte Reparaturbereiche wie einen instandgesetzten Schweller gesondert ansehen."),
    ("17_abgasanlage", "Prüfung", "Abgasanlage prüfen", "Zustand, Sitz, Befestigung und erkennbare Undichtigkeiten prüfen."),
    ("18_sicht_sicherheit", "Prüfung", "Beleuchtung, Hupe, Wischer, Waschanlage und Sichtfeld prüfen", "Außen- und Innenraumbeleuchtung, Hupe, Wischer und Waschanlage auf Funktion und Zustand prüfen; Frontscheibe und Sichtfeld sowie vorhandene Scheinwerferreinigungsanlage einbeziehen. Auffälligkeiten dokumentieren."),
    ("27_scheinwerfereinstellung", "Prüfung", "Scheinwerfereinstellung und Leuchtweitenregulierung prüfen", "Abblendlichteinstellung mit geeignetem Prüfgerät nach Herstellervorgaben prüfen; vorhandene Leuchtweitenregulierung einbeziehen. Ergebnis und erforderliche Korrekturen dokumentieren."),
    ("28_wasserkasten", "Prüfung", "Wasserkasten und zugängliche Abläufe prüfen", "Zugänglichen Wasserkasten und Abläufe auf Verschmutzung oder Verstopfung prüfen; im vereinbarten Umfang nach Herstellervorgaben reinigen. Auffälligkeiten dokumentieren."),
    ("29_gurte_verriegelungen", "Prüfung", "Sicherheitsgurte, Schlösser und Haubenverriegelung prüfen", "Gurte, Gurtschlösser, Türschlösser und Haubenverriegelung einschließlich Fanghaken auf Zustand und Funktion prüfen; Auffälligkeiten dokumentieren."),
    ("30_schiebedach", "Prüfung", "Schiebedach – ausstattungsabhängig prüfen", "Falls vorhanden: Funktion, Dichtungen und zugängliche Abläufe prüfen; Pflege nach Herstellervorgaben und vereinbartem Umfang. Falls nicht verbaut, Nicht zutreffend mit Begründung speichern."),
    ("31_anhaengerkupplung", "Prüfung", "Anhängerkupplung – ausstattungsabhängig prüfen", "Falls vorhanden: Befestigung, Zustand, gegebenenfalls abnehmbare Verriegelung und Steckdosenfunktion nach Herstellervorgaben prüfen. Falls nicht verbaut, Nicht zutreffend mit Begründung speichern."),
    ("19_batterie", "Prüfung", "Batterie und Anschlüsse prüfen", "Batterietest und Zustand der Anschlüsse dokumentieren."),
    ("20_oelkontrolle", "Abschluss", "Ölstand und Dichtheit nach Motorlauf kontrollieren", "Filter, Ablassschraube und montierte Abdeckungen kontrollieren; Ergebnis festhalten."),
    ("21_motorlauf", "Abschluss", "Motorlauf nach Kerzenwechsel und Warnanzeigen kontrollieren", "Motorlauf, Warnanzeigen und relevante Fehlereinträge prüfen und dokumentieren."),
    ("22_funktionskontrolle", "Abschluss", "Probefahrt / betriebliche Funktionskontrolle durchführen", "Bremsen, Lenkung, Schaltung und auffällige Geräusche prüfen; Umfang und Abweichungen notieren."),
    ("23_servicenachweis", "Abschluss", "Passende Serviceanzeigen zurücksetzen und Wartung dokumentieren", "Nur passende Anzeigen nach ausgeführten Arbeiten zurücksetzen; Wartungsnachweis ergänzen."),
    ("24_endkontrolle", "Abschluss", "Endkontrolle und Übergabe dokumentieren", "Vollständigkeit und offene Mängel dokumentieren. Bei demontierten Rädern Radschraubenanzug nach Herstellervorgabe kontrollieren und dokumentieren. Der Auftragsstatus wird separat und bewusst gesetzt."),
)


class ChecklistError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS inspektions_checklisten (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        auftrag_id INTEGER NOT NULL UNIQUE,
        vorlage_version INTEGER NOT NULL DEFAULT 1,
        titel TEXT NOT NULL, hinweis TEXT NOT NULL,
        erstellt_am TEXT NOT NULL, erstellt_von TEXT NOT NULL,
        FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS inspektions_checkpunkte (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        auftrag_id INTEGER NOT NULL,
        punkt_key TEXT NOT NULL, gruppe TEXT NOT NULL, titel TEXT NOT NULL,
        hinweis TEXT NOT NULL, sortierung INTEGER NOT NULL,
        ergebnis TEXT NOT NULL DEFAULT 'offen', notiz TEXT NOT NULL DEFAULT '',
        version INTEGER NOT NULL DEFAULT 1,
        geaendert_am TEXT NOT NULL DEFAULT '', geaendert_von TEXT NOT NULL DEFAULT '',
        UNIQUE (auftrag_id, punkt_key),
        FOREIGN KEY (auftrag_id) REFERENCES inspektions_checklisten(auftrag_id)
    )""")


def _is_inspection(auftrag):
    # Use the same canonical classification for raw locked rows and rendered views.
    return bool(auftrag) and auftrag_leistungsart(auftrag) == "inspektion"


def _write_allowed(auftrag, admin):
    return (_is_inspection(auftrag) and not auftrag.get("archiviert")
            and not auftrag.get("angebotsphase")
            and int(auftrag.get("status") or 1) in ({1, 2, 3, 4} if admin else {2, 3, 4}))


def _locked_order(db, auftrag_id, admin):
    # A real row write fences concurrent type/status/archive edits in SQLite and PG.
    # Recheck policy after taking the lock, never just against a rendered page.
    cursor = db.execute("UPDATE auftraege SET status=status WHERE id=?", (auftrag_id,))
    if cursor.rowcount != 1:
        raise ChecklistError("Auftrag nicht gefunden.", 404)
    row = db.execute("SELECT * FROM auftraege WHERE id=?", (auftrag_id,)).fetchone()
    auftrag = dict(row)
    if auftrag.get("archiviert"):
        raise ChecklistError("Der Auftrag ist archiviert.", 409)
    if not _is_inspection(auftrag):
        raise ChecklistError("Die Checkliste ist nur für Inspektionsaufträge vorgesehen.", 409)
    if not _write_allowed(auftrag, admin):
        raise ChecklistError("Der Auftrag ist für Änderungen an der Checkliste gesperrt.", 409)
    return auftrag


def create_checklist(portal, auftrag_id):
    """An explicit admin action; retries preserve all existing item evidence."""
    with portal.portal_originals_operation_lock():
        db = portal.get_db()
        try:
            _locked_order(db, auftrag_id, True)
            existing = db.execute("SELECT id FROM inspektions_checklisten WHERE auftrag_id=?", (auftrag_id,)).fetchone()
            if existing:
                db.rollback()
                return False
            now = portal.now_str()
            db.execute("""INSERT INTO inspektions_checklisten
                (auftrag_id, vorlage_version, titel, hinweis, erstellt_am, erstellt_von)
                VALUES (?, 1, ?, ?, ?, ?)""",
                (auftrag_id, VORLAGE_TITEL, VORLAGE_HINWEIS, now, "Büro"))
            for index, (key, group, title, hint) in enumerate(VORLAGE, 1):
                db.execute("""INSERT INTO inspektions_checkpunkte
                    (auftrag_id, punkt_key, gruppe, titel, hinweis, sortierung)
                    VALUES (?, ?, ?, ?, ?, ?)""", (auftrag_id, key, group, title, hint, index))
            db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now, auftrag_id))
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def save_point(portal, auftrag_id, punkt_id, version, ergebnis, notiz, *, admin=False):
    if not isinstance(version, str) or not version.isdecimal() or len(version) > 10 or int(version) < 1:
        raise ChecklistError("Bitte den aktuellen Punktstand neu laden.")
    if not isinstance(ergebnis, str) or ergebnis not in ERGEBNISSE:
        raise ChecklistError("Bitte ein gültiges Prüfergebnis auswählen.")
    if not isinstance(notiz, str) or len(notiz) > 600 or "\x00" in notiz:
        raise ChecklistError("Die Notiz darf höchstens 600 Zeichen enthalten.")
    notiz = notiz.strip()
    if ergebnis in {"mangel", "nicht_zutreffend"} and not notiz:
        raise ChecklistError("Bei Mangel oder Nicht zutreffend bitte eine kurze Begründung eintragen.")
    with portal.portal_originals_operation_lock():
        db = portal.get_db()
        try:
            _locked_order(db, auftrag_id, admin)
            point = db.execute("SELECT id FROM inspektions_checkpunkte WHERE id=? AND auftrag_id=?", (punkt_id, auftrag_id)).fetchone()
            if not point:
                raise ChecklistError("Prüfpunkt nicht gefunden.", 404)
            now = portal.now_str()
            cursor = db.execute("""UPDATE inspektions_checkpunkte
                SET ergebnis=?, notiz=?, version=version+1, geaendert_am=?, geaendert_von=?
                WHERE id=? AND auftrag_id=? AND version=?""",
                (ergebnis, notiz, now, "Büro" if admin else "Werkstatt", punkt_id, auftrag_id, int(version)))
            if cursor.rowcount != 1:
                raise ChecklistError("Dieser Prüfpunkt wurde inzwischen geändert. Bitte neu laden und den aktuellen Stand prüfen.", 409)
            db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now, auftrag_id))
            saved = dict(db.execute("SELECT * FROM inspektions_checkpunkte WHERE id=?", (punkt_id,)).fetchone())
            db.commit()
            return point_view(saved)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def point_view(row):
    point = dict(row)
    point["key"] = point.pop("punkt_key")
    point["ergebnis_label"] = ERGEBNISSE.get(point["ergebnis"], "Unbekannt")
    return point


def load_many(portal, auftraege, *, admin=False):
    """Two reads per batch, including on the board; never initializes records on GET."""
    ids = [int(a["id"]) for a in auftraege if _is_inspection(a)]
    headers, points = {}, {}
    if ids:
        db = portal.get_db()
        try:
            for start in range(0, len(ids), 400):
                batch = ids[start:start + 400]
                placeholders = ",".join("?" for _ in batch)
                for row in db.execute(f"SELECT * FROM inspektions_checklisten WHERE auftrag_id IN ({placeholders})", batch).fetchall():
                    headers[row["auftrag_id"]] = dict(row)
                for row in db.execute(f"SELECT * FROM inspektions_checkpunkte WHERE auftrag_id IN ({placeholders}) ORDER BY sortierung,id", batch).fetchall():
                    points.setdefault(row["auftrag_id"], []).append(point_view(row))
        finally:
            db.close()
    for order in auftraege:
        visible = _is_inspection(order)
        header = headers.get(order["id"], {})
        items = points.get(order["id"], [])
        writable = _write_allowed(order, admin)
        counts = {key: sum(p["ergebnis"] == key for p in items) for key in ERGEBNISSE}
        order["inspektionscheckliste"] = {
            "sichtbar": visible, "vorhanden": bool(header),
            "bearbeitbar": writable and bool(header),
            "anlegbar": bool(admin and writable and not header),
            "gesamt": len(items), "punkte": items,
            "vorlage_titel": header.get("titel", VORLAGE_TITEL),
            "hinweis": header.get("hinweis", VORLAGE_HINWEIS),
            "erstellt_am": header.get("erstellt_am", ""),
            "vorlage_punkte": [dict(key=k, gruppe=g, titel=t, hinweis=h) for k, g, t, h in VORLAGE],
            **counts,
        }
    return auftraege


def ensure_state_for_import(portal, *, export=None, imported_db=None, target=None):
    """Old archives may not silently erase inspection records introduced later."""
    if imported_db is not None:
        with closing(sqlite3.connect(Path(imported_db).resolve().as_uri() + "?mode=ro", uri=True)) as source:
            tables = {row[0] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    else:
        source_tables = (export or {}).get("tables") or {}
        if not isinstance(source_tables, dict):
            raise ValueError("Datenpaket ungültig: Tabellen fehlen.")
        for name in BACKUP_TABLES:
            if name in source_tables and not isinstance(source_tables[name], list):
                raise ValueError("Datenpaket ungültig: Inspektionscheckliste ist keine Liste.")
        tables = set(source_tables)
    missing = set(BACKUP_TABLES) - tables
    if not missing:
        return
    owned = target is None
    db = portal.get_db() if owned else target
    try:
        if any(db.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone() for name in missing):
            raise ValueError("Import gestoppt: Diese ältere Sicherung enthält die vorhandenen Inspektionschecklisten nicht. Bitte eine vollständige aktuelle Sicherung verwenden.")
    finally:
        if owned:
            db.close()
