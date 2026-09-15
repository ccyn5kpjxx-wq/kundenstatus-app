"""Disposable local demonstration with synthetic data and disabled integrations."""
from pathlib import Path
import os
import sys
import json
import tempfile
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
STORE = Path(tempfile.mkdtemp(prefix='cockpit-preview-'))
PORT = int(os.environ.get('COCKPIT_PREVIEW_PORT','5093'))
os.environ.update({'RENDER':'cockpit-preview','DATABASE_URL':'','REQUIRE_POSTGRES_ON_RENDER':'0',
    'DATA_DIR':str(STORE),'SQLITE_DB_PATH':str(STORE/'demo.db'),'UPLOAD_DIR':str(STORE/'uploads'),
    'BACKUP_DIR':str(STORE/'backups'),'DELETED_UPLOAD_DIR':str(STORE/'deleted'),
    'AUTO_BACKUP_ENABLED':'0','AUTO_CHANGE_BACKUP_ENABLED':'0','OPENAI_API_KEY':'',
    'FLASK_SECRET_KEY':os.urandom(32).hex(),'ADMIN_PASS':'local-demo-only',
    'SCHADEN_SMTP_PASS':'','LEXWARE_API_KEY':'','SESSION_COOKIE_SECURE':'0','PUBLIC_SITE_ONLY':'0','PUBLIC_BASE_URL':f'http://127.0.0.1:{PORT}',
    'CANONICAL_BASE_URL':f'http://127.0.0.1:{PORT}'})
import app as portal
from flask import session, redirect, url_for


def disabled(*args, **kwargs):
    raise ValueError('Lokale Vorschau: Externe Aktionen sind deaktiviert.')


def setup():
    portal.PUBLIC_HOSTS = set()
    portal.app.config['TEMPLATES_AUTO_RELOAD'] = True
    portal.app.jinja_env.auto_reload = True
    portal.init_db()
    portal.schedule_change_backup = lambda reason: None
    for name in ('lexware_request','send_lead_email','post_whatsapp_payload','send_email','send_mail','send_kunden_status_mail','send_email_message'):
        if hasattr(portal,name):setattr(portal,name,disabled)
    portal.notify_lead_workshop = lambda *args, **kwargs: None
    portal.LEXWARE_API_KEY = ''
    # Block network-provider access even if another integration is added later.
    import requests
    requests.sessions.Session.request = disabled
    import smtplib
    smtplib.SMTP = disabled
    smtplib.SMTP_SSL = disabled
    today=date.today()
    order_id=portal.create_auftrag('intern',kunde_name='Rainer Beispiel',kunde_email='kunde@example.test',fahrzeug='VW Tiguan',kennzeichen='DEMO-T 123',
        beschreibung='Kotflügel vorne rechts erneuern und lackieren. Tür vorne rechts beilackieren. Stoßfänger vorne lackieren.\n\nBremsenservice wird separat abgestimmt.',
        analyse='Kotflügel vorne rechts erneuern und lackieren · Tür beilackieren · Stoßfänger vorne lackieren',
        annahme_datum=today.strftime(portal.DATE_FMT),start_datum=today.strftime(portal.DATE_FMT),fertig_datum=(today+timedelta(days=2)).strftime(portal.DATE_FMT),abholtermin=(today+timedelta(days=2)).strftime(portal.DATE_FMT),fin_nummer='WVWZZZTEST00000001',kontakt_telefon='0151 00000000')
    price={'lieferant':portal.price_record('244.45','19','Beispiel-Lieferantenangebot'), 'kunde':portal.price_record('725','19','Bestätigtes Beispielangebot')}
    db=portal.get_db()
    db.execute("UPDATE auftraege SET status=3,annahme_uhrzeit='09:00',abhol_uhrzeit='16:00',preisstand_json=?,notiz_intern='Nur für das Team: Teile vor Montage auf Passform prüfen.',analyse_pruefen=1,analyse_werkstatt_geprueft=0 WHERE id=?",(json.dumps(price),order_id))
    db.execute("INSERT INTO mitarbeiter (name,aktiv,erstellt_am,geaendert_am) VALUES ('Alex Beispiel',1,?,?)",(portal.now_str(),portal.now_str()))
    db.execute("INSERT INTO benachrichtigungen (auftrag_id,quelle,titel,nachricht,gelesen,kunden_sichtbar,erstellt_am) VALUES (?,'werkstatt','Fahrzeug angenommen','Ihr Fahrzeug ist angekommen. Die Arbeiten sind eingeplant.',0,1,?)",(order_id,portal.now_str()))
    db.commit();db.close()
    portal.UPLOAD_DIR.mkdir(exist_ok=True,parents=True)
    import fitz
    for name,visible,text in [('Kundenangebot.pdf',1,'Beispielangebot: Karosseriearbeiten 725 EUR netto.'),('Lieferantenangebot-intern.pdf',0,'Interner Beispiel-Einkauf: 244,45 EUR netto.')]:
        document=fitz.open();page=document.new_page();page.insert_text((70,90),text);path=portal.UPLOAD_DIR/name;document.save(path);document.close()
        db=portal.get_db()
        db.execute("INSERT INTO dateien (auftrag_id,original_name,stored_name,mime_type,size,quelle,kategorie,hochgeladen_am,kunde_sichtbar,partner_sichtbar,sichtbarkeit_geprueft,analyse_json,dokument_zweck) VALUES (?,?,?,?,?,'intern','standard',?,?,?,1,?,'pruefen')",(order_id,name,name,'application/pdf',path.stat().st_size,portal.now_str(),visible,visible,json.dumps({'kennzeichen':'DEMO-T 123','rep_max_kosten':'244,45 EUR'})))
        db.commit();db.close()
    return order_id


if __name__=='__main__':
    DEMO_ORDER=setup()
    @portal.app.context_processor
    def demo_context():return {'cockpit_demo':True}
    @portal.app.get('/demo')
    def demo_entry():
        session['admin']=True
        return redirect(url_for('auftrag_detail',auftrag_id=DEMO_ORDER))
    @portal.app.get('/demo/kunde')
    def demo_customer():
        db = portal.get_db()
        token = db.execute('SELECT kunden_status_token FROM auftraege WHERE id=?', (DEMO_ORDER,)).fetchone()[0]
        db.close()
        return redirect(url_for('kunden_status', token=token))
    print(f'LOCAL_DEMO http://127.0.0.1:{PORT}/demo',flush=True)
    print(f'DEMO_DATA {STORE}',flush=True)
    portal.app.run(host='127.0.0.1',port=PORT,debug=False,use_reloader=False)
