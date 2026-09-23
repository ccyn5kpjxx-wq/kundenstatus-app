"""Fresh disposable portal test inventory. Never reads the operational database/env file."""
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo
import os
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def build_test_app(directory, origin='http://127.0.0.1:5085'):
    directory=Path(directory)
    os.environ.update({'RENDER':'isolated-mos-test','DATABASE_URL':'','REQUIRE_POSTGRES_ON_RENDER':'0',
        'DATA_DIR':str(directory),'SQLITE_DB_PATH':str(directory/'portal.mos-public-test.sqlite3'),
        'UPLOAD_DIR':str(directory/'uploads'),'BACKUP_DIR':str(directory/'backups'),
        'DELETED_UPLOAD_DIR':str(directory/'deleted'),'AUTO_BACKUP_ENABLED':'0','AUTO_CHANGE_BACKUP_ENABLED':'0',
        'OPENAI_API_KEY':'','LEXWARE_API_KEY':'','GOOGLE_APPLICATION_CREDENTIALS':'',
        'GOOGLE_DOC_AI_SERVICE_ACCOUNT_FILE':'','GOOGLE_DOC_AI_PROJECT_ID':'','WHATSAPP_ACCESS_TOKEN':'',
        'WHATSAPP_WORKSHOP_NUMBERS':'','MAIL_IMAP_PASS':'','MAIL_SMTP_PASS':'','SCHADEN_IMAP_PASS':'',
        'SCHADEN_SMTP_PASS':'','SMTP_PASSWORD':'','ADMIN_PASS':'local-test-disabled-admin',
        'MOS_BOOKING_CONFIG_FILE':'','MOS_STRIPE_TEST_KEY':'','MOS_STRIPE_LIVE_KEY':'',
        'MOS_STRIPE_WEBHOOK_SECRET':'',
        'FLASK_SECRET_KEY':__import__('secrets').token_urlsafe(32)})
    exists=Path.exists
    with patch.object(Path,'exists',lambda p:False if p in (ROOT/'.env',ROOT/'.env.local') else exists(p)):
        import app as portal
    db=portal.get_db();fleet={}
    for slug,name,rate in [('kona','Hyundai KONA N Line X',5900),('i10','Hyundai i10',3900)]:
        vid=db.execute('''INSERT INTO mietfahrzeuge (kennzeichen,bezeichnung,erstellt_am,geaendert_am)
            VALUES (?,?,?,?)''',('TEST-'+slug,'TESTDATENSATZ '+name,portal.now_str(),portal.now_str())).lastrowid
        fleet[slug]={'id':vid,'daily_cents':rate}
    db.commit();db.close();fleet['kona'].update(discount_after_days=3,discount_cents=4900)
    first=(datetime.now(ZoneInfo('Europe/Berlin'))+timedelta(days=2)).replace(hour=9,minute=0,second=0,microsecond=0)
    portal.app.config['MOS_PUBLIC_BOOKING']={'enabled':True,'test_configuration':True,'mode':'offline','origin':origin,
        'fleet':fleet,'slots':[(first+timedelta(days=n)).isoformat() for n in range(5)],
        'terms_version':'draft:ENTWURF-2026-09-22-01','vat_included':True,'day_rule':'elapsed_24h_ceil',
        'included_km_day':150,'extra_km_cents':25,'max_days':30,'deposit_cents':50000,'deductible_cents':100000,
        'terms_text':'TESTENTWURF, keine verbindlichen Mietbedingungen. Vorgeschlagen: 24-Stunden-Miettage, '
        '30 Minuten Rückgabekulanz, Verlängerung nur nach Bestätigung. Persönliche Übergabe mit Ausweis-/Führerscheinprüfung '
        'und Foto-/Kilometer-/Tankprotokoll. Voll/voll. Kostenlos stornieren bis 24 Stunden vor Abholung; '
        'danach oder bei Nichterscheinen höchstens erster gebuchter Miettag, unter Anrechnung ersparter Kosten '
        'und Wiedervermietung. Nachweis geringeren oder fehlenden Schadens möglich. '
        '500 Euro rückzahlbare Kaution und 1.000 Euro vertragliche Selbstbeteiligung beschlossen; '
        'Versicherung und Kautionsverfahren noch ungeprüft. Keine echte Buchung.'}
    return portal


if __name__=='__main__':
    directory=tempfile.mkdtemp(prefix='mos-public-preview-')
    portal=build_test_app(directory)
    print('NUR TESTDATEN: http://127.0.0.1:5085/mietwagen-test/ – '+directory)
    portal.app.run(host='127.0.0.1',port=5085,debug=False)
