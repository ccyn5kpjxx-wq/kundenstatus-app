"""Authenticated mail API/MIME contract; synthetic messages, no network."""
import io
import tempfile
import unittest
import uuid
from functools import wraps
from pathlib import Path
from unittest.mock import patch, MagicMock

from flask import Flask, abort, session
from mailbox_client import register_mailbox

ROOT = Path(__file__).resolve().parent


class SendRoutes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.app = Flask(__name__, template_folder=str(ROOT/'templates'), static_folder=str(ROOT/'static'))
        self.app.secret_key = 'synthetic-test'
        self.app.config.update(MAILBOX_SEND_ENABLED=True, MAILBOX_WRITE_ENABLED=False, MAILBOX_OUTBOX_DIR=self.temp.name)
        self.cfg = dict(from_address='test@example.invalid', smtp_user='test@example.invalid', display_name='Christopher Gärtner')
        def auth(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                if not session.get('admin'): abort(403)
                return fn(*args, **kwargs)
            return wrapper
        register_mailbox(self.app, auth, lambda:dict(user='test@example.invalid'), lambda:self.cfg, lambda:None)
        self.client = self.app.test_client()
        with self.client.session_transaction() as s:s['admin']=True
        self.queue = MagicMock(); self.queue.status.return_value=None
        self.queue.send.return_value=dict(state='sent', message='Gesendet', token='synthetic')
        self.mock = patch('mailbox_outbox.MailOutbox', return_value=self.queue).start(); self.addCleanup(patch.stopall)
        self.data = dict(to='Alex Beispiel <customer@example.invalid>', subject='Besichtigung', body='Guten Tag, hier ist die Antwort.', send_token=str(uuid.uuid4()))

    def post(self, **kwargs):
        return self.client.post('/admin/mail/send', data={**self.data, **kwargs})

    def test_mime_signature_reply_references_and_attachment(self):
        response=self.post(reply_id='<parent@example.invalid>', references='<root@example.invalid> <parent@example.invalid>', quoted_body='Frühere Rückfrage', attachments=(io.BytesIO(b'%PDF synthetic'), 'Angebot.pdf'))
        self.assertEqual(response.status_code,200,response.json)
        msg=self.queue.send.call_args.args[1]
        self.assertEqual(msg['From'].addresses[0].addr_spec,'test@example.invalid')
        self.assertEqual(msg['From'].addresses[0].display_name,'Christopher Gärtner')
        self.assertEqual(msg['Reply-To'],'test@example.invalid')
        self.assertEqual(msg['References'],'<root@example.invalid> <parent@example.invalid>')
        self.assertEqual(msg['In-Reply-To'],'<parent@example.invalid>')
        plain=msg.get_body(preferencelist=('plain',)).get_content()
        self.assertLess(plain.index('Christopher'),plain.index('Frühere Rückfrage'))
        markup=msg.get_body(preferencelist=('html',)).get_content()
        self.assertIn('cid:signature-portrait',markup)
        self.assertIn('cid:signature-logo',markup)
        self.assertEqual(sum(p.get_content_maintype()=='image' for p in msg.walk()),2)
        attachments=list(msg.iter_attachments())
        self.assertEqual(attachments[0].get_content_type(),'application/pdf')
        self.assertEqual(attachments[0].get_payload(decode=True),b'%PDF synthetic')

    def test_validation_preserves_all_text_or_rejects_without_send(self):
        for bad in (dict(subject='x'*251),dict(subject='x\r\nBcc: other@example.invalid'),dict(body=''),dict(body='x'*100001),dict(to='bad'),dict(references='a\nInjected: yes')):
            with self.subTest(bad=list(bad)):
                self.assertEqual(self.post(**bad).status_code,400)
        self.queue.send.assert_not_called()
        response=self.post(body='<script>alert(1)</script>')
        self.assertEqual(response.status_code,200)
        markup=self.queue.send.call_args.args[1].get_body(preferencelist=('html',)).get_content()
        self.assertIn('&lt;script&gt;',markup)

    def test_size_and_attachment_names(self):
        self.assertEqual(self.post(attachments=(io.BytesIO(b'x'*(18*1024*1024+1)),'large.pdf')).status_code,400)
        self.queue.send.assert_not_called()
        self.assertEqual(self.post(attachments=(io.BytesIO(b'a'),'C:\\fakepath\\Bild.png')).status_code,200)
        msg=self.queue.send.call_args.args[1]
        self.assertEqual(list(msg.iter_attachments())[0].get_filename(),'Bild.png')

    def test_gate_auth_and_identity(self):
        self.cfg['from_address']='other@example.invalid'
        self.assertEqual(self.post().status_code,400)
        self.cfg['from_address']='test@example.invalid'
        self.app.config['MAILBOX_SEND_ENABLED']=False
        self.assertEqual(self.post().status_code,403)
        self.app.config['MAILBOX_SEND_ENABLED']=True
        self.assertEqual(self.client.post('/admin/mail/draft',data=self.data).status_code,403)
        with self.client.session_transaction() as s:s.clear()
        for path in ('/admin/mail/outbox','/admin/mail/outbox/'+self.data['send_token'],'/admin/mail/outbox/'+self.data['send_token']+'/eml'):
            self.assertEqual(self.client.get(path).status_code,403)
        self.assertEqual(self.post().status_code,403)
        self.queue.send.assert_not_called()

    def test_eml_attachment_survives_mime_roundtrip(self):
        import email
        from email import policy
        original=b'Subject: attached\r\n\r\nOriginal message'
        self.assertEqual(self.post(attachments=(io.BytesIO(original),'original.eml')).status_code,200)
        msg=email.message_from_bytes(self.queue.send.call_args.args[1].as_bytes(),policy=policy.default)
        self.assertEqual(list(msg.iter_attachments())[0].get_payload(decode=True),original)

    def test_existing_send_status_precedes_config_and_payload_validation(self):
        self.queue.status.return_value=dict(state='uncertain',token=self.data['send_token'])
        self.cfg.clear()
        result=self.post(to='',body='')
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.json['state'],'uncertain')
        self.queue.send.assert_not_called()

    def test_history_status_copy_and_download_do_not_resend(self):
        self.queue.recent.return_value=[dict(state='uncertain')]
        self.assertEqual(len(self.client.get('/admin/mail/outbox').json['items']),1)
        token=self.data['send_token']
        self.assertEqual(self.client.get('/admin/mail/outbox/'+token).json['state'],'not_found')
        self.queue.retry_copy.return_value=dict(state='sent')
        self.assertEqual(self.client.post('/admin/mail/outbox/'+token+'/copy').json['state'],'sent')
        self.queue.retry_copy.assert_called_once_with(token)
        self.queue.payload.return_value=b'Subject: synthetic\r\n\r\nbody'
        result=self.client.get('/admin/mail/outbox/'+token+'/eml')
        self.assertEqual(result.status_code,200)
        self.assertIn('attachment',result.headers['Content-Disposition'])
        self.queue.send.assert_not_called()
        self.assertEqual(self.client.get('/admin/mail/outbox/invalid').status_code,400)


class RealAppIntegration(unittest.TestCase):
    def test_real_routes_csrf_config_and_no_leads(self):
        import ast, os, socket, sys, types
        with tempfile.TemporaryDirectory() as tmp:
            module=types.ModuleType('synthetic_mail_app');module.__file__=str(ROOT/'app.py')
            tree=ast.parse((ROOT/'app.py').read_text(encoding='utf8'))
            skip={'init_db','start_hourly_backups','start_lexware_auto_sync','start_google_ads_auto_sync'}
            tree.body=[n for n in tree.body if not (
                isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id in skip
                or isinstance(n,ast.For) and isinstance(n.target,ast.Name) and n.target.id=='env_file')]
            for n in tree.body:
                if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='BASE' for t in n.targets):
                    n.value=ast.Call(func=ast.Attribute(value=ast.Name(id='pathlib',ctx=ast.Load()),attr='Path',ctx=ast.Load()),args=[ast.Constant(tmp)],keywords=[])
            ast.fix_missing_locations(tree)
            env=dict(SYSTEMROOT=os.environ.get('SYSTEMROOT',''),PATH=os.environ.get('PATH',''),TEMP=tmp,TMP=tmp,
                     RENDER='1',DATA_DIR=tmp,SQLITE_DB_PATH=str(Path(tmp)/'test.db'),UPLOAD_DIR=str(Path(tmp)/'uploads'),
                     BACKUP_DIR=str(Path(tmp)/'backups'),MAIL_IMAP_HOST='imap.ionos.de',MAIL_IMAP_USER='test@example.invalid',
                     MAIL_IMAP_PASS='synthetic-password',FLASK_SECRET_KEY='synthetic-secret',ADMIN_PASS='synthetic-admin',
                     AUTO_BACKUP_ENABLED='0',AUTO_CHANGE_BACKUP_ENABLED='0')
            with patch.dict(os.environ,env,clear=True), patch.dict(sys.modules,{'synthetic_mail_app':module}), patch('socket.create_connection',side_effect=AssertionError('Real network forbidden')):
                exec(compile(tree,str(ROOT/'app.py'),'exec'),module.__dict__)
                for name in ('seed_akquise_autohaeuser','seed_standard_mitarbeiter','seed_default_autohaeuser','seed_default_versicherungen','seed_default_auftraege','seed_default_werkstatt_news'):
                    setattr(module,name,lambda db:None)
                module.app.config.update(TESTING=True)
                module.init_db()
                self.assertTrue(module.app.config['MAILBOX_SEND_ENABLED'])
                self.assertFalse(module.app.config['MAILBOX_WRITE_ENABLED'])
                self.assertEqual(Path(module.app.config['MAILBOX_OUTBOX_DIR']),Path(tmp)/'mail_outbox')
                cfg=module.get_werkstatt_smtp_config()
                self.assertEqual(cfg['smtp_host'],'smtp.ionos.de');self.assertEqual(cfg['_smtp_password'],'synthetic-password')
                with patch.dict(os.environ,{'MAIL_SMTP_HOST':'other.example.invalid'}):
                    self.assertFalse(module.get_werkstatt_smtp_config()['smtp_configured'])
                client=module.app.test_client()
                self.assertIn(client.get('/admin/mail/outbox').status_code,(302,401,403))
                with client.session_transaction() as state:state.update(admin=True,csrf_token='synthetic-csrf')
                self.assertEqual(client.post('/admin/mail/send').status_code,400)
                with patch('mailbox_outbox.MailOutbox') as outbox, patch.object(module,'schedule_change_backup') as backup:
                    outbox.return_value.status.return_value=dict(state='sent',token='synthetic')
                    result=client.post('/admin/mail/send',data={'csrf_token':'synthetic-csrf','send_token':str(uuid.uuid4())})
                    self.assertEqual(result.status_code,200,result.get_data(as_text=True)[:300])
                    self.assertEqual(result.json['state'],'sent')
                    backup.assert_not_called();outbox.return_value.send.assert_not_called()
                from contextlib import closing
                with closing(module.get_db()) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM auftraege').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT count(*) FROM leads').fetchone()[0],0)


if __name__=='__main__':unittest.main()
