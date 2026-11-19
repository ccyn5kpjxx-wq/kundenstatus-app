"""Select the original work orders for one job, independently of OCR and file sharing."""

from pathlib import Path
import re

from flask import Blueprint, abort, redirect, request, session, url_for


_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic"}
_NAMED_ORDER = re.compile(
    r"(?:lackier|lack|karosserie|reparatur|werkstatt|arbeits|fremd(?:arbeits|leistungs)?)"
    r"[ -]*auftrag(?=$|[\s\d#.-]|s(?:nr|nummer)\b)|\bauftrags(?:formular|blatt)\b", re.I)
_SIMPLE_ORDER = re.compile(r"(?:^|\s)auftrag(?:$|\s|[.:\d])", re.I)
_OTHER_FILE = re.compile(
    r"rechnung|lieferschein|kostenvoranschlag|kalkulation|gutachten|prüfbericht|pruefbericht|"
    r"angebot|auftragsbest(?:ät|ae?t)igung|zulassungsbescheinigung|fahrzeugschein|"
    r"abtretung|vollmacht|mahnung|gutschrift|lastschrift|kontoauszug|führerschein|"
    r"fuhrerschein|personalausweis|schaden(?:foto|bild)|fertig(?:foto|bild)|"
    r"\b(?:kva|dekra|invoice)\b|\b(?:re|rg)\s*-?\d{3,}", re.I)
_OTHER_HEADING = re.compile(
    r"^\s*(?:rechnung|lieferschein|kostenvoranschlag|(?:schaden)?gutachten|"
    r"reparaturkosten[ -]?kalkulation|auftragsbestätigung|angebot|gutschrift)"
    r"(?:\s*$|\s*[:#]|\s+(?:nr|nummer|no|[0-9]))", re.I)


def druckquelle_status(datei):
    """'automatisch' or 'gewaehlt' prints; 'auswaehlbar' requires a staff decision."""
    if (str(datei.get("kategorie") or "standard").lower() != "standard"
            or datei.get("reklamation_id") or datei.get("prozess_key")
            or datei.get("quelle") not in ("autohaus", "intern", "werkstatt")):
        return ""
    name = Path(datei.get("original_name") or "")
    filename = name.stem.replace("_", " ").replace("-", " ")
    typ = str(datei.get("dokument_typ") or "").strip()
    if name.suffix.lower() not in _SUFFIXES or _OTHER_FILE.search(filename) or _OTHER_FILE.search(typ):
        return ""
    headings = [line.strip() for line in str(datei.get("extrahierter_text") or "")[:4096].splitlines() if line.strip()][:16]
    if any(_OTHER_HEADING.match(line) for line in headings):
        return ""
    if datei.get("druck_auswahl") == 1:
        return "gewaehlt"
    if datei.get("druck_auswahl") == -1:
        return "auswaehlbar"
    # The legacy OCR classifier also labels documents that merely mention an
    # Auftragsnummer as "Reparaturauftrag". An explicit title is stronger evidence.
    if (_NAMED_ORDER.search(filename) or _SIMPLE_ORDER.search(filename)
            or any(_NAMED_ORDER.match(line) or _SIMPLE_ORDER.match(line) for line in headings)
            or (typ.casefold() == "lackierauftrag" and not headings)):
        return "automatisch"
    return "auswaehlbar"


def _rows(db, auftrag_id, datei_id=None):
    sql = """SELECT d.id, d.auftrag_id, d.original_name, d.stored_name, d.size, d.quelle,
                    d.kategorie, d.reklamation_id, d.prozess_key, d.dokument_typ, d.druck_auswahl,
                    substr(d.extrahierter_text, 1, 4096) AS extrahierter_text, d.hochgeladen_am,
                    EXISTS(SELECT 1 FROM datei_backups b WHERE b.datei_id=d.id) AS backup_available
             FROM dateien d WHERE d.auftrag_id=?"""
    params = (auftrag_id,)
    if datei_id is not None:
        sql += " AND d.id=?"
        params += (datei_id,)
    return [dict(row) for row in db.execute(sql + " ORDER BY d.hochgeladen_am, d.id", params).fetchall()]


def _visible(portal, row, tafel):
    return (portal.assistent_datei_intern_sichtbar(row)
            and (not tafel or portal.werkstatt_datei_sichtbar(row)))


def druckquellen(portal, auftrag_id, *, tafel=False):
    db = portal.get_db()
    try:
        rows = _rows(db, auftrag_id)
    finally:
        db.close()
    sources = []
    for row in rows:
        if druckquelle_status(row) not in {"automatisch", "gewaehlt"} or not _visible(portal, row, tafel):
            continue
        sources.append({**{key: row[key] for key in ("id", "original_name", "size", "hochgeladen_am")},
                        "suffix": Path(row["original_name"]).suffix.lower(),
                        "original_available": portal.upload_file_exists(row, backup_available=row["backup_available"])})
    return sources


def lade_druckquelle(portal, auftrag_id, datei_id, *, tafel=False):
    """Return original bytes after rechecking the order, selection and file access."""
    db = portal.get_db()
    try:
        rows = _rows(db, auftrag_id, datei_id)
    finally:
        db.close()
    if not rows:
        return None
    row = rows[0]
    if druckquelle_status(row) not in {"automatisch", "gewaehlt"} or not _visible(portal, row, tafel):
        return None
    path = portal.upload_file_path(row)
    try:
        if path and path.resolve().parent == Path(portal.UPLOAD_DIR).resolve() and path.is_file():
            return path.read_bytes()
    except OSError:
        pass
    return portal.load_datei_backup_bytes(row)


def register_druckquellen(portal):
    bp = Blueprint("druckquellen", __name__)
    portal.app.jinja_env.globals["druckquelle_status"] = druckquelle_status

    @bp.post("/admin/auftrag/<int:auftrag_id>/datei/<int:datei_id>/druckauswahl")
    @portal.admin_required
    def auswahl(auftrag_id, datei_id):
        if session.get("admin") is not True:
            abort(403)
        choices = request.form.getlist("auswahl")
        if choices not in (["beilegen"], ["weglassen"]):
            abort(400)
        db = portal.get_db()
        try:
            rows = _rows(db, auftrag_id, datei_id)
            if not rows or not druckquelle_status(rows[0]) or not _visible(portal, rows[0], False):
                abort(404)
            db.execute("UPDATE dateien SET druck_auswahl=? WHERE id=? AND auftrag_id=?",
                       (1 if choices == ["beilegen"] else -1, datei_id, auftrag_id))
            db.commit()
        finally:
            db.close()
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id, _anchor="auftragsunterlagen"), code=303)

    portal.app.register_blueprint(bp)
