"""Authenticated mailbox workspace. Reads use PEEK; writes require explicit enablement."""
import base64
import email
from email import policy
from email.message import EmailMessage
from email.utils import getaddresses, formatdate, make_msgid
from html.parser import HTMLParser
import io
import html
from pathlib import Path
import imaplib
import re
import ssl
import smtplib
import uuid
from contextlib import contextmanager
from flask import Blueprint, render_template, request, jsonify, send_file, abort, current_app

MAX_MESSAGE = 25 * 1024 * 1024


def seen_flag(metadata):
    if isinstance(metadata, str):
        metadata = metadata.encode('ascii', 'replace')
    flags = re.search(rb'\bFLAGS \(([^)]*)\)', metadata, re.I)
    return bool(flags and any(flag.lower() == b'\\seen' for flag in flags[1].split()))


def folder_label(value):
    def decode(m):
        if not m[1]: return '&'
        try: return base64.b64decode(m[1].replace(',', '/') + '=' * (-len(m[1]) % 4)).decode('utf-16-be')
        except Exception: return m[0]
    return re.sub(r'&([^-]*)-', decode, value)


def quote(value):
    if not isinstance(value, str) or any(c in value for c in '\r\n\x00'): raise ValueError('Ungültiger Ordner oder Suchtext.')
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


class PlainHTML(HTMLParser):
    def __init__(self): super().__init__(); self.parts=[]; self.hidden=0
    def handle_starttag(self, tag, attrs):
        if tag in ('script','style'): self.hidden+=1
        elif tag in ('p','br','div','li','tr'): self.parts.append('\n')
    def handle_endtag(self, tag):
        if tag in ('script','style') and self.hidden: self.hidden-=1
    def handle_data(self,data):
        if not self.hidden: self.parts.append(data)


def message_view(raw, uid, flags=''):
    msg=email.message_from_bytes(raw,policy=policy.default)
    body=msg.get_body(preferencelist=('plain','html')) if msg.is_multipart() else msg
    text=''
    if body and body.get_content_maintype()=='text':
        try: text=body.get_content()
        except Exception: text=(body.get_payload(decode=True) or b'').decode('utf8','replace')
        if body.get_content_subtype()=='html':
            parser=PlainHTML(); parser.feed(text); text=''.join(parser.parts)
    attachments=[]
    for index,part in enumerate(msg.walk()):
        if part.get_filename() or part.get_content_disposition()=='attachment':
            attachments.append({'index':index,'name':part.get_filename() or 'Anhang','type':part.get_content_type()})
    return {'uid':str(uid),'subject':str(msg.get('Subject','(Ohne Betreff)')),'sender':str(msg.get('From','')),
            'to':str(msg.get('To','')),'date':str(msg.get('Date','')),'unread':not seen_flag(flags),
            'body':text,'attachments':attachments,'reply_to':str(msg.get('Reply-To',msg.get('From',''))),
            'message_id':str(msg.get('Message-ID',''))}


class Mailbox:
    def __init__(self, config): self.config=config
    @contextmanager
    def connect(self):
        c=self.config()
        if not c.get('configured'): raise ValueError('Postfach ist noch nicht verbunden.')
        if not c.get('ssl'): raise ValueError('Verschlüsselter IMAP-Zugang erforderlich.')
        client=imaplib.IMAP4_SSL(c['host'],c['port'],ssl_context=ssl.create_default_context(),timeout=c.get('timeout',20))
        try:
            client.login(c['user'],c['password'])
            status,capabilities=client.capability()
            if status=='OK' and capabilities:client.capabilities=tuple(capabilities[0].split())
            yield client
        finally:
            try:client.logout()
            except Exception:pass
    def folders(self,client):
        status,rows=client.list()
        if status!='OK':raise ValueError('Ordner konnten nicht geladen werden.')
        items=[]
        for row in rows or []:
            if not isinstance(row,bytes):continue
            match=re.match(rb'\((.*?)\)\s+(?:"[^"]*"|NIL)\s+(.+)$',row)
            if not match:continue
            flags=match[1].decode('ascii','replace')
            if '\\Noselect' in flags:continue
            name=match[2].decode('ascii','replace')
            if name.startswith('"') and name.endswith('"'):name=name[1:-1].replace('\\"','"').replace('\\\\','\\')
            items.append({'id':name,'label':folder_label(name),'flags':flags})
        return items
    def select(self,client,folder,readonly=True,validity=None):
        # Only select a mailbox actually advertised by this account.
        if folder not in {x['id'] for x in self.folders(client)}:raise ValueError('Ordner nicht gefunden.')
        status,data=client.select(quote(folder),readonly=readonly)
        if status!='OK':raise ValueError('Ordner kann nicht geöffnet werden.')
        values=client.response('UIDVALIDITY')[1];version=values[0].decode() if values and values[0] else ''
        if validity is not None and str(validity)!=version:raise ValueError('Postfach wurde geändert. Bitte neu laden.')
        return version
    def raw(self,client,uid):
        if not re.fullmatch(r'[1-9][0-9]*',str(uid)):raise ValueError('Ungültige Nachricht.')
        status,meta=client.uid('FETCH',str(uid),'(RFC822.SIZE)')
        sizes=[int(x) for p in meta or [] if isinstance(p,bytes) for x in re.findall(rb'RFC822.SIZE (\d+)',p)]
        if status!='OK' or not sizes:raise ValueError('Nachricht nicht mehr vorhanden.')
        if max(sizes)>MAX_MESSAGE:raise ValueError('Diese Nachricht ist größer als 25 MB. Bitte in IONOS öffnen.')
        status,rows=client.uid('FETCH',str(uid),'(FLAGS BODY.PEEK[])')
        for row in rows or []:
            if status=='OK' and isinstance(row,tuple):return row[1],row[0].decode('ascii','replace')
        raise ValueError('Nachricht konnte nicht geladen werden.')
    def unread(self, client, uid):
        status, rows = client.uid('FETCH', str(uid), '(UID FLAGS)')
        if status != 'OK':
            raise ValueError('Lesestatus konnte nicht bestätigt werden. Bitte aktualisieren.')
        for row in rows or []:
            meta = row[0] if isinstance(row, tuple) else row
            if not isinstance(meta, bytes):
                continue
            found = re.search(rb'\bUID (\d+)\b', meta, re.I)
            if found and found[1].decode() == str(uid) and re.search(rb'\bFLAGS \([^)]*\)', meta, re.I):
                return not seen_flag(meta)
        raise ValueError('Nachricht nicht mehr vorhanden oder Lesestatus unbekannt. Bitte aktualisieren.')
    def listing(self,folder,page=1,search=''):
        with self.connect() as c:
            folders=self.folders(c);version=self.select(c,folder)
            if search:
                # IMAP UTF-8 charset for non-ASCII search; no interpolated search commands.
                status,data=c.uid('SEARCH','CHARSET','UTF-8','OR','SUBJECT',quote(search).encode('utf8'),'FROM',quote(search).encode('utf8'))
            else:status,data=c.uid('SEARCH',None,'ALL')
            if status!='OK':raise ValueError('Suche konnte nicht ausgeführt werden.')
            ids=(data[0].split() if data and data[0] else [])[::-1];page=max(1,int(page));chosen=ids[(page-1)*30:page*30]
            messages=[]
            if chosen:
                status,rows=c.uid('FETCH',b','.join(chosen),'(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)])')
                if status!='OK':raise ValueError('Mailliste konnte nicht geladen werden.')
                for row in rows or []:
                    if not isinstance(row,tuple):continue
                    m=re.search(rb'UID (\d+)',row[0])
                    if m:messages.append(message_view(row[1],m[1].decode(),row[0].decode('ascii','replace')))
                messages.sort(key=lambda x:int(x['uid']),reverse=True)
            return {'folders':folders,'messages':messages,'total':len(ids),'page':page,'validity':version,'folder':folder}


def register_mailbox(app, admin_required, imap_config, smtp_config, get_db):
    bp=Blueprint('mailbox',__name__);service=Mailbox(imap_config)
    def enabled():return bool(current_app.config.get('MAILBOX_WRITE_ENABLED',False))
    def move_enabled():return bool(current_app.config.get('MAILBOX_MOVE_ENABLED',False)) or enabled()
    def flags_enabled():return bool(current_app.config.get('MAILBOX_FLAGS_ENABLED',False))
    def writable():
        if not enabled():abort(403,description='Lokale Vorschau: Änderungen am echten Postfach und Versand sind ausgeschaltet.')
    @bp.errorhandler(ValueError)
    def invalid(exc):return jsonify(error=str(exc)),400
    @bp.errorhandler(imaplib.IMAP4.error)
    def imap_error(exc):return jsonify(error='Postfachzugriff fehlgeschlagen. Bitte Verbindung prüfen.'),502
    @bp.errorhandler(OSError)
    def network_error(exc):return jsonify(error='Mailserver momentan nicht erreichbar. Bitte erneut versuchen.'),502
    @bp.get('/admin/mail')
    @admin_required
    def index():return render_template('mailbox.html',mail_address=imap_config().get('user',''),mail_write=enabled(),mail_move=move_enabled(),mail_flags=flags_enabled())
    @bp.get('/admin/mail/messages')
    @admin_required
    def listing():return jsonify(service.listing(request.args.get('folder','INBOX'),request.args.get('page',1,type=int),request.args.get('q','')[:200]))
    @bp.get('/admin/mail/message/<int:uid>')
    @admin_required
    def message(uid):
        with service.connect() as c:
            service.select(c,request.args.get('folder','INBOX'),validity=request.args.get('validity'))
            raw,flags=service.raw(c,uid);return jsonify(message_view(raw,uid,flags))
    @bp.get('/admin/mail/message/<int:uid>/attachment/<int:index>')
    @admin_required
    def attachment(uid,index):
        with service.connect() as c:
            service.select(c,request.args.get('folder','INBOX'),validity=request.args.get('validity'));raw,_=service.raw(c,uid)
        parts=list(email.message_from_bytes(raw,policy=policy.default).walk())
        if index>=len(parts) or not (parts[index].get_filename() or parts[index].get_content_disposition()=='attachment'):abort(404)
        part=parts[index];return send_file(io.BytesIO(part.get_payload(decode=True) or b''),as_attachment=True,download_name=part.get_filename() or 'Anhang',mimetype='application/octet-stream')
    @bp.post('/admin/mail/message/<int:uid>/read')
    @admin_required
    def mark_read(uid):
        if not flags_enabled():abort(403,description='Gelesen-/Ungelesen-Abgleich ist ausgeschaltet.')
        folder=request.form.get('folder','');version=request.form.get('validity','');read=request.form.get('read')
        if uid < 1 or read not in ('0','1') or not re.fullmatch(r'[1-9][0-9]*',version):
            raise ValueError('Ungültiger Lesestatus oder Postfachstand. Bitte aktualisieren.')
        with service.connect() as c:
            version=service.select(c,folder,readonly=False,validity=version)
            service.unread(c,uid)  # STORE may otherwise succeed for a nonexistent UID.
            status,_=c.uid('STORE',str(uid),'+FLAGS.SILENT' if read=='1' else '-FLAGS.SILENT',r'(\Seen)')
            if status!='OK':raise ValueError('Markierung fehlgeschlagen. Bitte aktualisieren.')
            unread=service.unread(c,uid)
            if unread != (read=='0'):
                raise ValueError('IONOS hat die Markierung nicht bestätigt. Bitte aktualisieren.')
        return jsonify(ok=True,uid=str(uid),folder=folder,validity=version,unread=unread)
    @bp.post('/admin/mail/message/<int:uid>/move')
    @admin_required
    def move(uid):
        if not move_enabled():abort(403,description='Verschieben ist ausgeschaltet.')
        with service.connect() as c:
            folder=request.form['folder'];folders=service.folders(c)
            target=request.form.get('target','')
            if target=='__trash__':
                target=next((f['id'] for f in folders if '\\Trash' in f['flags'] or f['label'].lower() in ('trash','papierkorb')),None)
            if not target or target not in {f['id'] for f in folders}:raise ValueError('Zielordner nicht gefunden.')
            if target==folder:raise ValueError('Nachricht liegt bereits in diesem Ordner.')
            if b'MOVE' not in c.capabilities:raise ValueError('Sicheres Verschieben wird vom Mailserver nicht unterstützt. Bitte IONOS verwenden.')
            service.select(c,folder,readonly=False,validity=request.form['validity'])
            status,_=c.uid('MOVE',str(uid),quote(target))
            if status!='OK':raise ValueError('Nachricht konnte nicht verschoben werden.')
        return jsonify(ok=True,message='Nachricht verschoben. Im Zielordner kannst du sie wieder zurückverschieben.')

    def outgoing():
        msg=EmailMessage();cfg=smtp_config()
        recipients=getaddresses([request.form.get('to','')])
        if not recipients or any(not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+',addr) for _,addr in recipients):raise ValueError('Bitte gültige Empfänger angeben.')
        msg['From']=cfg['from_address'];msg['To']=', '.join(addr for _,addr in recipients)
        msg['Subject']=request.form.get('subject','')[:250];msg['Date']=formatdate(localtime=True);msg['Message-ID']=make_msgid(domain=cfg['from_address'].split('@')[-1])
        if request.form.get('reply_id'):msg['In-Reply-To']=request.form['reply_id'];msg['References']=request.form['reply_id']
        body=request.form.get('body','')[:100000]
        signature_text='Christopher Gärtner\nGründer & Geschäftsführer\nKarosserie & Lack Gärtner GmbH\nBinauer Höhe 4 · 74821 Mosbach-Lohrbach\nTelefon: +49 152 27706694\nE-Mail: info@auto-lackierzentrum.de\nInternet: www.auto-lackierzentrum.de'
        msg.set_content(body+'\n\n'+signature_text)
        signature=render_template('_mail_signature.html')
        images=[('/static/logo-transparent.png','signature-logo','image','png'),('/static/homepage/portrait.webp','signature-portrait','image','webp')]
        for url,cid,_,_ in images:signature=signature.replace(url,'cid:'+cid)
        msg.add_alternative('<div style="white-space:pre-wrap">'+html.escape(body)+'</div><br>'+signature,subtype='html')
        for url,cid,maintype,subtype in images:
            payload=(Path(current_app.static_folder)/url.removeprefix('/static/')).read_bytes()
            msg.get_payload()[-1].add_related(payload,maintype=maintype,subtype=subtype,cid='<'+cid+'>',disposition='inline')
        total=0
        for file in request.files.getlist('attachments'):
            if not file.filename:continue
            payload=file.read(MAX_MESSAGE+1);total+=len(payload)
            if total>18*1024*1024:raise ValueError('Anhänge dürfen zusammen höchstens 18 MB groß sein.')
            msg.add_attachment(payload,maintype='application',subtype='octet-stream',filename=file.filename)
        return msg,cfg
    def append_special(msg,flag):
        with service.connect() as c:
            folders=service.folders(c);target=next((f['id'] for f in folders if flag in f['flags']),None)
            aliases={'\\Sent':('sent','sent items','gesendet'),'\\Drafts':('drafts','entwürfe')}
            target=target or next((f['id'] for f in folders if f['label'].lower() in aliases[flag]),None)
            if not target:raise ValueError('Zielordner fehlt. Nachricht wurde nicht im Postfach abgelegt.')
            status,_=c.append(quote(target),r'(\Draft)' if flag=='\\Drafts' else r'(\Seen)',None,msg.as_bytes(policy=policy.SMTP))
            if status!='OK':raise ValueError('Nachricht konnte nicht im Ordner abgelegt werden.')
    @bp.post('/admin/mail/draft')
    @admin_required
    def draft():
        writable();msg,_=outgoing();append_special(msg,'\\Drafts');return jsonify(ok=True,message='Entwurf im IONOS-Postfach gespeichert.')
    @bp.post('/admin/mail/send')
    @admin_required
    def send():
        writable();msg,cfg=outgoing()
        token=request.form.get('send_token','')
        try:uuid.UUID(token)
        except ValueError:raise ValueError('Ungültiger Versandvorgang.')
        db=get_db()
        try:
            db.execute('CREATE TABLE IF NOT EXISTS mailbox_send_guard (token TEXT PRIMARY KEY, status TEXT NOT NULL)')
            cursor=db.execute("INSERT INTO mailbox_send_guard(token,status) VALUES (?, 'sending') ON CONFLICT(token) DO NOTHING",(token,));db.commit()
            if cursor.rowcount!=1:return jsonify(error='Dieser Versand wurde bereits gestartet. Bitte Gesendet prüfen; nicht erneut senden.'),409
        finally:db.close()
        if not cfg.get('smtp_configured'):raise ValueError('SMTP noch nicht eingerichtet.')
        if not (cfg.get('smtp_ssl') or cfg.get('smtp_tls')):raise ValueError('Verschlüsselter SMTP-Zugang erforderlich.')
        try:
            smtp=(smtplib.SMTP_SSL(cfg['smtp_host'],cfg['smtp_port'],context=ssl.create_default_context(),timeout=30) if cfg['smtp_ssl'] else smtplib.SMTP(cfg['smtp_host'],cfg['smtp_port'],timeout=30))
            with smtp:
                if not cfg['smtp_ssl']:smtp.starttls(context=ssl.create_default_context())
                smtp.login(cfg['smtp_user'],cfg['_smtp_password']);refused=smtp.send_message(msg)
            if refused:raise ValueError('Nicht alle Empfänger angenommen. Vor erneutem Versand prüfen.')
        except Exception:
            return jsonify(error='Versandstatus unklar. Bitte Gesendet prüfen, bevor du erneut sendest.'),502
        db=get_db()
        try:db.execute("UPDATE mailbox_send_guard SET status='accepted' WHERE token=?",(token,));db.commit()
        finally:db.close()
        try:append_special(msg,'\\Sent')
        except Exception:return jsonify(ok=True,message='Vom Mailserver angenommen; Kopie in Gesendet konnte nicht gespeichert werden. Nicht erneut senden.')
        return jsonify(ok=True,message='Vom Mailserver angenommen und in Gesendet gespeichert.')
    app.register_blueprint(bp)
    return service

