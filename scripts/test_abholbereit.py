from pathlib import Path
import sys
import os
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Never load operational configuration or write to an existing order database.
TEMP_DIR = Path(tempfile.mkdtemp(prefix="pickup-ready-test-"))
os.environ.update({
    "RENDER": "isolated-regression-test", "DATABASE_URL": "", "REQUIRE_POSTGRES_ON_RENDER": "0",
    "DATA_DIR": str(TEMP_DIR), "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
    "UPLOAD_DIR": str(TEMP_DIR / "uploads"), "BACKUP_DIR": str(TEMP_DIR / "backups"),
    "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"), "AUTO_BACKUP_ENABLED": "0",
    "AUTO_CHANGE_BACKUP_ENABLED": "0", "OPENAI_API_KEY": "", "LEXWARE_API_KEY": "",
    "GOOGLE_APPLICATION_CREDENTIALS": "", "WHATSAPP_ACCESS_TOKEN": "", "WHATSAPP_WORKSHOP_NUMBERS": "",
    "MAIL_IMAP_PASS": "", "MAIL_SMTP_PASS": "", "SCHADEN_IMAP_PASS": "", "SCHADEN_SMTP_PASS": "",
    "SMTP_PASSWORD": "", "FLASK_SECRET_KEY": "isolated-regression-test", "ADMIN_PASS": "isolated-regression-test",
})

def deny_network(*args, **kwargs):
    raise AssertionError("Network is forbidden in pickup-ready tests")

patch("socket.socket.connect", deny_network).start()
patch("socket.socket.connect_ex", deny_network).start()
patch("socket.create_connection", deny_network).start()
_original_exists = Path.exists
with patch.object(Path, "exists", lambda p: False if p in (ROOT / ".env", ROOT / ".env.local") else _original_exists(p)):
    import app as portal  # noqa: E402
# Exercise stored notifications even when the optional WhatsApp bridge cannot send.
whatsapp = patch.object(portal, "notify_workshop_whatsapp_abholbereit", return_value=False).start()


FEHLER = []


def check(label, ok, detail=""):
    if ok:
        print(f"[OK] {label}")
    else:
        print(f"[FEHLER] {label} {detail}")
        FEHLER.append(label)


def csrf_data(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token")
    if token:
        payload["csrf_token"] = token
    return payload


def ende():
    if FEHLER:
        print(f"\n{len(FEHLER)} Test(s) fehlgeschlagen.")
        sys.exit(1)
    print("\nAbholbereit-Test erfolgreich.")


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()

    # 1) Migration: neue Spalten existieren (Postgres-sicherer ensure_column-Weg)
    db = portal.get_db()
    cols = portal.get_table_columns(db, "auftraege")
    db.close()
    check("Spalte fahrzeug_abholbereit existiert", "fahrzeug_abholbereit" in cols)
    check("Spalte fahrzeug_abholbereit_am existiert", "fahrzeug_abholbereit_am" in cols)

    # Auftrag eines Autohauses mit Hol-/Bringservice in Status 2 vorbereiten
    kandidaten = [a for a in portal.list_auftraege() if int(a.get("autohaus_id") or 0) > 0]
    if not kandidaten:
        check("Test-Auftrag mit Autohaus vorhanden", False, "keine Autohaus-Auftraege geseedet")
        return ende()
    auftrag = kandidaten[0]
    aid = auftrag["id"]
    haus_id = int(auftrag["autohaus_id"])
    autohaus = portal.get_autohaus(haus_id)
    slug = autohaus["slug"]
    db = portal.get_db()
    db.execute(
        "UPDATE auftraege SET status=2, transport_art='hol_und_bring', "
        "fahrzeug_abholbereit=0, fahrzeug_abholbereit_am='' WHERE id=?",
        (aid,),
    )
    db.commit()
    db.close()

    # Status1 is the reported production case; there must be one action in the hero.
    pc = portal.app.test_client()
    with pc.session_transaction() as session:
        session["partner_autohaus_id"] = haus_id
    for state in (1, 2):
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=?, archiviert=0, transport_art='hol_und_bring', fahrzeug_abholbereit=0, fahrzeug_abholbereit_am='' WHERE id=?", (state, aid))
        db.commit(); db.close()
        html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
        check(f"Status{state}: one pickup action in the visible hero", html.count('value="abholbereit"') == 1 and html.index('value="abholbereit"') < html.index('data-partner-tabs'))
        before = portal.get_auftrag(aid)
        notifications_before = len(portal.list_benachrichtigungen(aid))
        whatsapp.reset_mock()
        resp = pc.post(f"/partner/{slug}/auftrag/{aid}", data=csrf_data(pc, {"aktion":"abholbereit"}))
        after = portal.get_auftrag(aid)
        check(f"Status{state}: notice stored when WhatsApp fails", resp.status_code == 302 and after['fahrzeug_abholbereit'] and len(portal.list_benachrichtigungen(aid)) == notifications_before + 1 and whatsapp.call_count == 1)
        check(f"Status{state}: order data and repair state unchanged", all(after[k] == before[k] for k in ['status','kunde_name','kennzeichen','fahrzeug','transport_art','start_datum','fertig_datum','abholtermin']))
        html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
        check(f"Status{state}: confirmation replaces the action in hero", 'Als abholbereit gemeldet' in html.split('data-partner-tabs')[0] and 'value="abholbereit"' not in html)
        # Simulate a second handler that read the old snapshot before the first update.
        with patch.object(portal, 'get_auftrag', return_value=before):
            pc.post(f"/partner/{slug}/auftrag/{aid}", data=csrf_data(pc, {"aktion":"abholbereit"}))
        check(f"Status{state}: stale parallel POST does not notify twice", len(portal.list_benachrichtigungen(aid)) == notifications_before + 1 and whatsapp.call_count == 1)
        pc.post(f"/partner/{slug}/auftrag/{aid}", data=csrf_data(pc, {"aktion":"abholbereit"}))
        check(f"Status{state}: ordinary replay does not notify twice", len(portal.list_benachrichtigungen(aid)) == notifications_before + 1 and whatsapp.call_count == 1)

    for state, archived, transport in [(3,0,'hol_und_bring'),(4,0,'hol_und_bring'),(5,0,'hol_und_bring'),(1,1,'hol_und_bring'),(1,0,'standard')]:
        db=portal.get_db(); db.execute("UPDATE auftraege SET status=?, archiviert=?, transport_art=?, fahrzeug_abholbereit=0 WHERE id=?",(state,archived,transport,aid)); db.commit(); db.close()
        html=pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
        count=len(portal.list_benachrichtigungen(aid)); whatsapp.reset_mock()
        pc.post(f"/partner/{slug}/auftrag/{aid}",data=csrf_data(pc,{'aktion':'abholbereit'}))
        check(f"Unsupported case{state}/{archived}/{transport} hidden and rejected", 'value="abholbereit"' not in html and not portal.get_auftrag(aid)['fahrzeug_abholbereit'] and len(portal.list_benachrichtigungen(aid)) == count and not whatsapp.called)
    db=portal.get_db(); db.execute("UPDATE auftraege SET status=1, archiviert=0, transport_art='hol_und_bring', fahrzeug_abholbereit=0 WHERE id=?",(aid,)); db.commit(); db.close()
    snapshot=portal.get_auftrag(aid)
    db=portal.get_db(); db.execute('UPDATE auftraege SET status=3 WHERE id=?',(aid,)); db.commit(); db.close()
    count=len(portal.list_benachrichtigungen(aid)); whatsapp.reset_mock()
    with patch.object(portal,'get_auftrag',return_value=snapshot):
        pc.post(f"/partner/{slug}/auftrag/{aid}",data=csrf_data(pc,{'aktion':'abholbereit'}))
    check('Concurrent change to status3 rejects stale notice', not portal.get_auftrag(aid)['fahrzeug_abholbereit'] and len(portal.list_benachrichtigungen(aid)) == count and not whatsapp.called)
    db=portal.get_db(); db.execute("UPDATE auftraege SET status=2 WHERE id=?",(aid,)); db.commit(); db.close()
    resp=pc.post(f"/partner/{slug}/auftrag/{aid}",data={'aktion':'abholbereit'})
    check('Missing CSRF cannot set pickup-ready flag', resp.status_code == 400 and not portal.get_auftrag(aid)['fahrzeug_abholbereit'])
    other = next(h for h in portal.list_autohaeuser() if h['id'] != haus_id)
    stranger=portal.app.test_client()
    with stranger.session_transaction() as session: session['partner_autohaus_id']=other['id']
    stranger.get(f"/partner/{other['slug']}/auftrag/{aid}")
    resp=stranger.post(f"/partner/{other['slug']}/auftrag/{aid}",data=csrf_data(stranger,{'aktion':'abholbereit'}))
    check('Foreign partner cannot set pickup-ready flag', resp.status_code == 404 and not portal.get_auftrag(aid)['fahrzeug_abholbereit'])

    # 2) Partner-Seite (eingeloggt) zeigt den Button
    pc = portal.app.test_client()
    with pc.session_transaction() as session:
        session["partner_autohaus_id"] = haus_id
    pc.get(f"/partner/{slug}/auftrag/{aid}")  # setzt CSRF-Token in die Session
    html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
    check(
        "Partner-Seite zeigt Abholbereit-Button (Status 2 + Hol/Bring)",
        'value="abholbereit"' in html and ">Abholbereit</button>" in html,
    )

    # 3) POST abholbereit -> Flag + Zeitstempel gesetzt
    resp = pc.post(f"/partner/{slug}/auftrag/{aid}", data=csrf_data(pc, {"aktion": "abholbereit"}))
    a2 = portal.get_auftrag(aid)
    check(
        "POST abholbereit setzt Flag + Zeitstempel",
        resp.status_code == 302 and a2["fahrzeug_abholbereit"] is True and bool(a2["fahrzeug_abholbereit_am"]),
        f"status={resp.status_code} flag={a2['fahrzeug_abholbereit']} am={a2['fahrzeug_abholbereit_am']!r}",
    )

    # 3b) Partner-Seite zeigt jetzt Bestaetigung statt Button
    html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
    check("Partner-Seite zeigt Bestaetigung statt Button", "Als abholbereit gemeldet" in html)

    # 4) In-App-Benachrichtigung wurde angelegt
    benachrichtigungen = portal.list_benachrichtigungen(aid)
    check(
        "In-App-Benachrichtigung 'abholbereit' angelegt",
        any("abholbereit" in (str(b.get("titel", "")) + str(b.get("nachricht", ""))).lower() for b in benachrichtigungen),
    )

    # 5) Werkstatt-Tafel zeigt den Hinweis auf der Karte
    wc = portal.app.test_client()
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "TEST99")
    wc.get("/werkstatt")
    wc.post("/werkstatt", data=csrf_data(wc, {"password": "TEST99"}))
    tafel = wc.get("/werkstatt/tafel").get_data(as_text=True)
    check("Werkstatt-Tafel zeigt Abholbereit-Hinweis", "Fahrzeug ist abholbereit" in tafel)

    # 6) Wechsel auf 'In Arbeit' (3) loescht das Flag -> Hinweis verschwindet
    with wc.session_transaction() as session:
        csrf = session.get("csrf_token")
    wc.post(
        f"/werkstatt/auftrag/{aid}/status/3",
        headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
    )
    a3 = portal.get_auftrag(aid)
    check(
        "Status -> In Arbeit loescht das Abholbereit-Flag",
        a3["status"] == 3 and a3["fahrzeug_abholbereit"] is False,
        f"status={a3['status']} flag={a3['fahrzeug_abholbereit']}",
    )
    tafel2 = wc.get("/werkstatt/tafel").get_data(as_text=True)
    check("Werkstatt-Tafel zeigt den Hinweis nach 'In Arbeit' nicht mehr", "Fahrzeug ist abholbereit" not in tafel2)

    ende()


if __name__ == "__main__":
    main()
