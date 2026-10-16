"""Company-scoped fleet access and immutable vehicle receipts; no messaging/OCR."""
from __future__ import annotations

import base64
import csv
import hashlib
import hmac
from io import BytesIO, StringIO
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time
import warnings
import zipfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import wraps
from uuid import uuid4

from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

PORTAL_TABLES = ('kundenflotten_zugaenge', 'kundenflotten_belege')
PORTAL_FEATURE = 'kundenflotten_portal_v1'
MAX_BYTES = 8 * 1024 * 1024
EXPORT_MAX_DOCUMENTS = 100
EXPORT_MAX_BYTES = 32 * 1024 * 1024
DOC_KINDS = {'tank':'Tankbeleg', 'rechnung':'Rechnung', 'reparatur':'Reparaturbeleg', 'sonstiges':'Sonstiger Fahrzeugbeleg'}
MIMES = {'.jpg':'image/jpeg','.jpeg':'image/jpeg','.png':'image/png','.webp':'image/webp','.pdf':'application/pdf'}
DOC_COLUMNS = 'id,fahrzeug_id,art,titel,belegdatum,betrag_cent,quelle,sichtbar,original_name,mime_type,size,sha256,request_id,erstellt_am'


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS kundenflotten_zugaenge (
        id TEXT PRIMARY KEY, firma_id TEXT NOT NULL UNIQUE REFERENCES kundenflotten_firmen(id),
        benutzername TEXT NOT NULL UNIQUE, passwort_hash TEXT NOT NULL,
        aktiv INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL DEFAULT 1,
        erstellt_am TEXT NOT NULL, geaendert_am TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS kundenflotten_belege (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
        art TEXT NOT NULL, titel TEXT NOT NULL, belegdatum TEXT NOT NULL,
        betrag_cent INTEGER, quelle TEXT NOT NULL, sichtbar INTEGER NOT NULL DEFAULT 0,
        original_name TEXT NOT NULL, mime_type TEXT NOT NULL, size INTEGER NOT NULL,
        sha256 TEXT NOT NULL, original_base64 TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
        erstellt_am TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_flottenbeleg_fahrzeug ON kundenflotten_belege(fahrzeug_id,id)')


def stamp():
    return datetime.now().isoformat(timespec='seconds')


def clean(value, maximum=200):
    value=str(value or '').strip()
    if len(value)>maximum or any(ord(c)<32 for c in value):
        raise ValueError('Bitte die Eingabe prüfen; sie ist zu lang oder enthält ungültige Zeichen.')
    return value


def receipt_filters(args):
    values={key:clean(args.get(key),120 if key=='q' else 10) for key in ('q','belegart','von','bis')}
    if any(len(args.getlist(key))>1 for key in values): raise ValueError('Bitte jeden Filter nur einmal angeben.')
    if values['belegart'] and values['belegart'] not in DOC_KINDS: raise ValueError('Bitte eine gültige Belegart wählen.')
    for key in ('von','bis'):
        value=values[key]
        if value:
            try:
                if date.fromisoformat(value).isoformat()!=value: raise ValueError()
            except ValueError: raise ValueError('Bitte ein gültiges Datum im Format JJJJ-MM-TT angeben.') from None
    if values['von'] and values['bis'] and values['von']>values['bis']: raise ValueError('Das Enddatum muss am oder nach dem Startdatum liegen.')
    return values


def filtered_receipts(documents, filters):
    return [doc for doc in documents if
            (not filters['belegart'] or doc['art']==filters['belegart']) and
            (not filters['von'] or doc['belegdatum']>=filters['von']) and
            (not filters['bis'] or doc['belegdatum']<=filters['bis']) and
            (not filters['q'] or filters['q'].casefold() in (doc['titel']+' '+doc['original_name']).casefold())]


def receipt_csv(documents, *, archive_names=None, vehicle=None):
    def safe(value):
        text=str(value if value is not None else '')
        # Quoting alone does not stop spreadsheet formulas, including leading whitespace.
        return "'"+text if text.lstrip().startswith(('=','+','-','@')) or any(ord(c)<32 for c in text) else text
    out=StringIO(newline='')
    writer=csv.writer(out,delimiter=';')
    writer.writerow(['Beleg-ID','Kennzeichen','Fahrzeug','Belegdatum','Belegart','Bezeichnung','Betrag brutto EUR','Quelle','Originaldatei','Datei im ZIP','SHA256'])
    for doc in documents:
        cents=doc['betrag_cent']
        amount='' if cents is None else f'{cents//100},{cents%100:02d}'
        writer.writerow([safe(v) for v in (doc['id'],(vehicle or {}).get('kennzeichen',''),(vehicle or {}).get('modell',''),doc['belegdatum'],DOC_KINDS.get(doc['art'],''),doc['titel'],amount,
            'Werkstatt' if doc['quelle']=='werkstatt' else 'Firma',doc['original_name'],(archive_names or {}).get(doc['id'],''),doc['sha256'])])
    return out.getvalue().encode('utf-8-sig')


class FleetPortal:
    def __init__(self, p, fleet):
        self.p, self.fleet=p, fleet

    def access(self, company):
        with self.fleet.db() as db:
            self.fleet.row(db,'kundenflotten_firmen',company)
            row=db.execute('SELECT id,benutzername,aktiv,version FROM kundenflotten_zugaenge WHERE firma_id=?',(company,)).fetchone()
            return dict(row) if row else None

    def configure_access(self, company, data):
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            self.fleet.row(db,'kundenflotten_firmen',company)
            old=db.execute('SELECT * FROM kundenflotten_zugaenge WHERE firma_id=?',(company,)).fetchone()
            if data.get('aktion')=='sperren':
                if old:
                    db.execute('UPDATE kundenflotten_zugaenge SET aktiv=0,version=version+1,geaendert_am=? WHERE firma_id=?',(stamp(),company))
                return
            username=clean(data.get('benutzername'),100).lower()
            password=str(data.get('passwort') or '')
            if not re.fullmatch(r'[a-z0-9][a-z0-9@._+\-]{2,99}',username):
                raise ValueError('Benutzername: 3–100 Zeichen, Kleinbuchstaben, Ziffern oder @ . _ + - verwenden.')
            if len(password)<12 or len(password)>128 or password!=data.get('passwort_wiederholen'):
                raise ValueError('Bitte ein Passwort mit 12–128 Zeichen zweimal übereinstimmend eingeben.')
            if db.execute('SELECT id FROM kundenflotten_zugaenge WHERE benutzername=? AND firma_id<>?',(username,company)).fetchone():
                raise ValueError('Dieser Benutzername ist bereits vergeben.')
            hashed=generate_password_hash(password)
            if old:
                db.execute('UPDATE kundenflotten_zugaenge SET benutzername=?,passwort_hash=?,aktiv=1,version=version+1,geaendert_am=? WHERE firma_id=?',(username,hashed,stamp(),company))
            else:
                db.execute('INSERT INTO kundenflotten_zugaenge VALUES(?,?,?,?,1,1,?,?)',(uuid4().hex,company,username,hashed,stamp(),stamp()))

    def identity(self):
        key=session.get('flotte_zugang')
        version=session.get('flotte_version')
        started=session.get('flotte_login_am')
        if not isinstance(key,str) or type(version) is not int or type(started) not in (int,float) or not 0<=time.time()-started<8*3600:
            return None
        with self.fleet.db() as db:
            row=db.execute('SELECT id,firma_id,benutzername,aktiv,version FROM kundenflotten_zugaenge WHERE id=?',(key,)).fetchone()
        return dict(row) if row and row['aktiv']==1 and row['version']==version else None

    def safe_company(self, company):
        firm, vehicles=self.fleet.company(company)
        cards=[]
        for vehicle in vehicles:
            cases=self.safe_cases(company,vehicle['id'])
            cards.append({key:vehicle[key] for key in ('id','kennzeichen','modell','km','offen')} | {'cases':cases})
        return {'name':firm['name']},cards

    def safe_cases(self, company, vehicle):
        public=('id','title','vehicle','plate','status','label','description','updated','stage','steps')
        return [{k:case[k] for k in public} for case in self.fleet.linked_cases(company,vehicle)]

    def documents(self, company, vehicle, *, internal=False):
        with self.fleet.db() as db:
            row=self.fleet.row(db,'kundenflotten_fahrzeuge',vehicle)
            if row['firma_id']!=company: raise LookupError()
            query=f'SELECT {DOC_COLUMNS} FROM kundenflotten_belege WHERE fahrzeug_id=?'
            if not internal: query+=' AND sichtbar=1'
            return [dict(row) for row in db.execute(query+' ORDER BY belegdatum DESC,id DESC',(vehicle,)).fetchall()]

    def upload(self, company, vehicle, data, files, *, internal=False, authorize=None):
        selected=[f for f in files if f and f.filename]
        if len(selected)!=1: raise ValueError('Bitte genau einen Beleg auswählen oder fotografieren.')
        file=selected[0]
        name=secure_filename(file.filename or '')[:180]
        suffix=Path(name).suffix.lower()
        if suffix not in MIMES: raise ValueError('Bitte JPG, PNG, WebP oder PDF hochladen; je Beleg höchstens 8 MB.')
        _, errors=self.p.validate_schadenaufnahme_uploads([file],require_file=True,max_file_mb=8,max_total_mb=8)
        if errors: raise ValueError(' '.join(errors))
        file.stream.seek(0)
        raw=file.stream.read(MAX_BYTES+1)
        if not raw or len(raw)>MAX_BYTES: raise ValueError('Der Beleg ist leer oder größer als 8 MB.')
        try:
            if suffix=='.pdf':
                import fitz
                with fitz.open(stream=raw,filetype='pdf') as pdf:
                    if pdf.needs_pass or not 1<=pdf.page_count<=200: raise ValueError()
            else:
                from PIL import Image
                with warnings.catch_warnings():
                    warnings.simplefilter('error',Image.DecompressionBombWarning)
                    with Image.open(BytesIO(raw)) as image:
                        if image.format not in {'JPEG','PNG','WEBP'} or image.width*image.height>30_000_000: raise ValueError()
                        expected={'JPEG':'image/jpeg','PNG':'image/png','WEBP':'image/webp'}[image.format]
                        if expected!=MIMES[suffix]: raise ValueError()
                        image.verify()
        except ImportError:
            raise ValueError('Die Dateiprüfung ist zurzeit nicht verfügbar. Bitte die Werkstatt informieren.') from None
        except Exception:
            raise ValueError('Die Datei ist beschädigt, geschützt oder kein unterstütztes Foto/PDF. Bitte neu aufnehmen oder exportieren.') from None
        kind=clean(data.get('art'),30)
        title=clean(data.get('titel'),160) or DOC_KINDS.get(kind,'')
        day=clean(data.get('belegdatum'),10)
        try:
            parsed=date.fromisoformat(day)
            if parsed>date.today() or parsed.year<2000: raise ValueError()
        except ValueError: raise ValueError('Bitte ein gültiges Belegdatum bis heute angeben.') from None
        day=parsed.isoformat()
        amount=clean(data.get('betrag'),16)
        try:
            number=Decimal(amount.replace(',','.')) if amount else None
            if number is not None and (not number.is_finite() or not 0<=number<=1000000 or number!=number.quantize(Decimal('0.01'))): raise ValueError()
            cents=int(number*100) if number is not None else None
        except (InvalidOperation,ValueError): raise ValueError('Bitte den Bruttobetrag mit höchstens zwei Nachkommastellen angeben.') from None
        receipt=clean(data.get('request_id'),32)
        if kind not in DOC_KINDS or not re.fullmatch(r'[a-f0-9]{32}',receipt): raise ValueError('Bitte Belegart wählen und das Formular neu laden.')
        origin='werkstatt' if internal else 'firma'
        visible=int(not internal or data.get('sichtbar')=='1')
        digest=hashlib.sha256(raw).hexdigest()
        values=(vehicle,kind,title,day,cents,origin,visible,name,MIMES[suffix],len(raw),digest)
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            if not internal and authorize is not None and not authorize(): raise PermissionError()
            owner=self.fleet.row(db,'kundenflotten_fahrzeuge',vehicle)
            if owner['firma_id']!=company: raise LookupError()
            previous=db.execute(f'SELECT {DOC_COLUMNS} FROM kundenflotten_belege WHERE request_id=?',(receipt,)).fetchone()
            if previous:
                fields=('fahrzeug_id','art','titel','belegdatum','betrag_cent','quelle','sichtbar','original_name','mime_type','size','sha256')
                if tuple(previous[k] for k in fields)!=values: raise ValueError('Das Formular wurde bereits für einen anderen Beleg verwendet. Bitte neu laden.')
                return previous['id']
            result=db.execute('''INSERT INTO kundenflotten_belege(fahrzeug_id,art,titel,belegdatum,betrag_cent,quelle,sichtbar,
                original_name,mime_type,size,sha256,original_base64,request_id,erstellt_am) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(*values,base64.b64encode(raw).decode('ascii'),receipt,stamp()))
            return result.lastrowid

    def original(self, company, vehicle, number, *, internal=False):
        with self.fleet.db() as db:
            row=db.execute('''SELECT b.* FROM kundenflotten_belege b JOIN kundenflotten_fahrzeuge v ON v.id=b.fahrzeug_id
                WHERE b.id=? AND b.fahrzeug_id=? AND v.firma_id=?''',(number,vehicle,company)).fetchone()
        if not row or (not internal and row['sichtbar']!=1): raise LookupError()
        try:
            raw=base64.b64decode(row['original_base64'],validate=True)
            if len(raw)!=row['size'] or not 0<len(raw)<=MAX_BYTES or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(),row['sha256']): raise ValueError()
        except (ValueError,TypeError): raise ValueError('Der Originalbeleg konnte nicht vollständig geprüft werden. Bitte die Werkstatt informieren.') from None
        return dict(row),raw


def ensure_originals_for_import(*, get_db, reference_map, export=None, imported_db=None, target=None):
    """A restore must not lose immutable receipts or roll back access revocation."""
    own=target is None
    db=target if target is not None else get_db()
    source=None
    try:
        if imported_db is not None:
            source=sqlite3.connect(Path(imported_db).resolve().as_uri()+'?mode=ro',uri=True)
            source.row_factory=sqlite3.Row
        for table,fields in [('kundenflotten_fahrzeuge',('id','firma_id')),
                             ('kundenflotten_zugaenge',('id','firma_id','benutzername','passwort_hash','aktiv','version')),
                             ('kundenflotten_belege',tuple(DOC_COLUMNS.split(',')))]:
            selected=DOC_COLUMNS if table=='kundenflotten_belege' else ','.join(fields)
            current=[dict(r) for r in db.execute(f'SELECT {selected} FROM {table}').fetchall()]
            if not current: continue
            refs=reference_map() if table=='kundenflotten_belege' and source is None and export else {}
            if source is not None:
                incoming=[dict(r) for r in source.execute(f'SELECT {selected} FROM {table}').fetchall()]
            else: incoming=(export or {}).get('tables',{}).get(table,[])
            if not isinstance(incoming,list) or any(not isinstance(r,dict) for r in incoming): raise ValueError()
            indexed={r.get('id'):r for r in incoming}
            for row in current:
                replacement=indexed.get(row['id'])
                if not replacement or any(replacement.get(k)!=row[k] for k in fields): raise ValueError()
                if table=='kundenflotten_belege':
                    reference=refs.get((table,row['id'],'original_base64'))
                    if reference is not None:
                        if reference.get('sha256')!=row['sha256'] or reference.get('size')!=row['size']: raise ValueError()
                    else:
                        encoded=source.execute('SELECT original_base64 FROM kundenflotten_belege WHERE id=?',(row['id'],)).fetchone()[0] if source is not None else replacement.get('original_base64')
                        raw=base64.b64decode(encoded or '',validate=True)
                        if len(raw)!=row['size'] or hashlib.sha256(raw).hexdigest()!=row['sha256']: raise ValueError()
    except (ValueError,KeyError,TypeError,sqlite3.Error):
        raise ValueError('Import gestoppt: Vorhandene Flottenbelege oder der aktuelle Firmen-Zugangsstatus würden verloren gehen. Bitte eine vollständige aktuelle Sicherung verwenden.') from None
    finally:
        if source is not None: source.close()
        if own: db.close()


def register_flottenportal(p, fleet):
    service=FleetPortal(p,fleet)
    p.flottenportal=service
    p.app.config.setdefault('FLOTTENPORTAL_ENABLED',os.environ.get('FLOTTENPORTAL_ENABLED','0')=='1')
    bp=Blueprint('flottenportal',__name__)

    def csrf_token():
        if not session.get('csrf_token'): session['csrf_token']=secrets.token_urlsafe(32)
        return session['csrf_token']

    @bp.before_request
    def limits_and_csrf():
        if not p.app.config.get('FLOTTENPORTAL_ENABLED'): abort(404)
        request.max_content_length=MAX_BYTES+256*1024
        if request.method=='POST':
            expected=session.get('csrf_token')
            supplied=request.form.get('csrf_token') or ''
            if not expected or not hmac.compare_digest(str(expected),str(supplied)): abort(400)

    @bp.after_request
    def private(response):
        response.headers.update({'Cache-Control':'private, no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff','X-Robots-Tag':'noindex, nofollow, noarchive'})
        response.vary.add('Cookie')
        return response

    def protected(fn=None, *, machine=False):
        def decorate(view):
            @wraps(view)
            def wrapped(*args,**kwargs):
                who=service.identity()
                if not who:
                    for key in ('flotte_zugang','flotte_version','flotte_login_am'): session.pop(key,None)
                    if machine: return jsonify(error='Anmeldung erforderlich.'),403
                    return redirect(url_for('flottenportal.login')) if request.method=='GET' else abort(403)
                return view(who,*args,**kwargs)
            return wrapped
        return decorate(fn) if fn is not None else decorate

    def confirm_identity(who):
        current=service.identity()
        if not current or any(current[k]!=who[k] for k in ('id','firma_id','version')): abort(403)

    def view_key(who):
        return hashlib.sha256(f'{who["id"]}:{who["firma_id"]}:{who["version"]}'.encode()).hexdigest()

    @bp.context_processor
    def page_identity():
        who=service.identity()
        return {'portal_view':view_key(who) if who else ''}

    def filters():
        try: return receipt_filters(request.args)
        except ValueError as exc: abort(400,description=str(exc))

    def company_page(company):
        firm,vehicles=service.safe_company(company)
        return render_template('flottenportal_uebersicht.html',company=firm,vehicles=vehicles,csrf_token=csrf_token())

    def vehicle_page(company,vehicle,*,internal=False):
        selected=filters()
        try:
            data=fleet.vehicle(company,vehicle)
            documents=filtered_receipts(service.documents(company,vehicle,internal=internal),selected)
            cases=service.safe_cases(company,vehicle)
        except LookupError: abort(404)
        return render_template('flottenportal_fahrzeug.html',company={'name':data['company']['name'],'id':company},vehicle={k:data['vehicle'][k] for k in ('id','kennzeichen','modell','km','km_datum')},plans=[{k:plan[k] for k in ('titel','art','faellig_datum','faellig_km','due')} for plan in data['plans'] if plan['status']=='offen'],cases=cases,documents=documents,filters=selected,kinds=DOC_KINDS,internal=internal,request_id=uuid4().hex,today=date.today().isoformat(),csrf_token=csrf_token())

    @bp.route('/flotte/anmelden',methods=['GET','POST'])
    def login():
        error=''
        status=200
        if request.method=='POST':
            limited,_=p.login_rate_limit_status('flottenportal','login')
            if limited:
                error,status='Zu viele Fehlversuche. Bitte später erneut versuchen.',429
            else:
                username=str(request.form.get('benutzername') or '')[:100].strip().lower()
                password=str(request.form.get('passwort') or '')
                with fleet.db() as db: row=db.execute('SELECT * FROM kundenflotten_zugaenge WHERE benutzername=?',(username,)).fetchone()
                # Dummy hash also spends password-check time for an unknown user.
                good=check_password_hash(row['passwort_hash'] if row else dummy_hash,password[:129])
                if not row or row['aktiv']!=1 or len(password)>128 or not good:
                    p.record_failed_login('flottenportal','login')
                    error,status='Die Anmeldung hat nicht geklappt. Bitte Zugangsdaten prüfen.',401
                else:
                    p.clear_login_attempts('flottenportal','login')
                    session.clear()
                    session.update(flotte_zugang=row['id'],flotte_version=row['version'],flotte_login_am=time.time(),csrf_token=secrets.token_urlsafe(32))
                    response=redirect(url_for('flottenportal.index'),code=303)
                    for scope in ('admin','partner'): p.clear_remember_login_cookie(response,scope)
                    return response
        return render_template('flottenportal_login.html',error=error,csrf_token=csrf_token()),status

    @bp.post('/flotte/abmelden')
    def logout():
        session.clear()
        return redirect(url_for('flottenportal.login'),code=303)

    @bp.get('/flotte')
    @protected
    def index(who): return company_page(who['firma_id'])

    @bp.get('/flotte/fahrzeug/<vehicle>')
    @protected
    def vehicle(who,vehicle): return vehicle_page(who['firma_id'],vehicle)

    @bp.get('/flotte/status')
    @protected(machine=True)
    def company_status(who):
        with p.portal_originals_operation_lock():
            try: _,vehicles=service.safe_company(who['firma_id'])
            except LookupError: abort(404)
            confirm_identity(who)
        return jsonify(vehicles=vehicles,view=view_key(who))

    @bp.get('/flotte/fahrzeug/<vehicle>/status')
    @protected(machine=True)
    def vehicle_status(who,vehicle):
        with p.portal_originals_operation_lock():
            try:
                # Ownership is checked even when there are no linked cases.
                with fleet.db() as db:
                    if fleet.row(db,'kundenflotten_fahrzeuge',vehicle)['firma_id']!=who['firma_id']: abort(404)
                cases=service.safe_cases(who['firma_id'],vehicle)
            except LookupError: abort(404)
            confirm_identity(who)
        return jsonify(cases=cases,view=view_key(who))

    @bp.get('/flotte/fahrzeug/<vehicle>/belege/export/<format>')
    @protected(machine=True)
    def receipt_export(who,vehicle,format):
        if format not in ('csv','zip'): abort(404)
        selected=filters()
        with p.portal_originals_operation_lock():
            confirm_identity(who)
            try:
                documents=filtered_receipts(service.documents(who['firma_id'],vehicle),selected)
                with fleet.db() as db:
                    car=fleet.row(db,'kundenflotten_fahrzeuge',vehicle)
                    car={key:car[key] for key in ('kennzeichen','modell')}
            except LookupError: abort(404)
            if len(documents)>EXPORT_MAX_DOCUMENTS or sum(d['size'] for d in documents)>EXPORT_MAX_BYTES:
                abort(413,description='Bitte den Filter eingrenzen: höchstens 100 Belege und 32 MB pro Export.')
            if format=='zip':
                out=BytesIO()
                names={}
                total=0
                try:
                    with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_STORED) as archive:
                        for doc in documents:
                            row,raw=service.original(who['firma_id'],vehicle,doc['id'])
                            # Avoid a mixed manifest if a concurrent writer changed metadata.
                            if any(row[k]!=doc[k] for k in DOC_COLUMNS.split(',')): raise ValueError()
                            total+=len(raw)
                            if total>EXPORT_MAX_BYTES: abort(413)
                            name=f'{doc["id"]:06d}_'+(secure_filename(doc['original_name'])[:180] or 'Beleg')
                            names[doc['id']]=name
                            archive.writestr(name,raw)
                        archive.writestr('Beleguebersicht.csv',receipt_csv(documents,archive_names=names,vehicle=car))
                except (LookupError,ValueError): abort(409,description='Ein Originalbeleg ist nicht verfügbar oder beschädigt. Der Export wurde vollständig abgebrochen.')
                out.seek(0)
                mime='application/zip'
            else:
                out=BytesIO(receipt_csv(documents,vehicle=car))
                mime='text/csv; charset=utf-8'
            confirm_identity(who)
        filename=secure_filename(car['kennzeichen'])[:60] or 'Fahrzeug'
        response=send_file(out,mimetype=mime,download_name=f'Belege-{filename}-{date.today().isoformat()}.{format}',as_attachment=True,max_age=0)
        response.headers['Content-Security-Policy']="default-src 'none'; sandbox"
        return response

    def upload(company,vehicle,internal):
        try:
            if set(request.files)-{'datei','kamera'}: abort(400)
            service.upload(company,vehicle,request.form,request.files.getlist('datei')+request.files.getlist('kamera'),internal=internal,authorize=None if internal else lambda: service.identity())
            flash('Beleg gespeichert und dem Fahrzeug zugeordnet.','success')
        except LookupError: abort(404)
        except PermissionError: abort(403)
        except ValueError as exc: flash(str(exc),'error')
        return redirect(url_for('flottenportal.admin_documents',company=company,vehicle=vehicle) if internal else url_for('flottenportal.vehicle',vehicle=vehicle),code=303)

    @bp.post('/flotte/fahrzeug/<vehicle>/beleg')
    @protected
    def customer_upload(who,vehicle): return upload(who['firma_id'],vehicle,False)

    def original(company,vehicle,number,internal):
        try: row,raw=service.original(company,vehicle,number,internal=internal)
        except LookupError: abort(404)
        except ValueError: abort(409)
        response=send_file(BytesIO(raw),mimetype=row['mime_type'],download_name=row['original_name'],as_attachment=True,max_age=0)
        response.headers['Content-Security-Policy']="default-src 'none'; sandbox"
        return response

    @bp.get('/flotte/fahrzeug/<vehicle>/beleg/<int:number>')
    @protected
    def customer_original(who,vehicle,number): return original(who['firma_id'],vehicle,number,False)

    @bp.route('/admin/flottenportal/firma/<company>/zugang',methods=['GET','POST'])
    @p.admin_required
    def admin_access(company):
        try:
            firm,_=fleet.company(company)
            if request.method=='POST':
                try: service.configure_access(company,request.form); flash('Zugang gespeichert. Es wurde keine Einladung versendet.','success')
                except ValueError as exc: flash(str(exc),'error')
                return redirect(url_for('flottenportal.admin_access',company=company),code=303)
            return render_template('flottenportal_zugang.html',company=firm,access=service.access(company),internal=True,csrf_token=csrf_token())
        except LookupError: abort(404)

    @bp.get('/admin/flottenportal/firma/<company>/fahrzeug/<vehicle>')
    @p.admin_required
    def admin_documents(company,vehicle): return vehicle_page(company,vehicle,internal=True)

    @bp.post('/admin/flottenportal/firma/<company>/fahrzeug/<vehicle>/beleg')
    @p.admin_required
    def admin_upload(company,vehicle): return upload(company,vehicle,True)

    @bp.get('/admin/flottenportal/firma/<company>/fahrzeug/<vehicle>/beleg/<int:number>')
    @p.admin_required
    def admin_original(company,vehicle,number): return original(company,vehicle,number,True)

    dummy_hash=generate_password_hash(secrets.token_urlsafe(24))
    p.app.register_blueprint(bp)
    return service
