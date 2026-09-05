import unittest
from unittest.mock import patch
from functools import wraps
from flask import Flask,session,abort
from mailbox_client import Mailbox,message_view,quote,folder_label,register_mailbox

class Client:
 def list(self):return 'OK',[b'(\\HasNoChildren) "/" "INBOX"',b'(\\Drafts) "/" "Drafts"']
 def select(self,folder,readonly=True):assert readonly;return 'OK',[b'1']
 def response(self,key):return key,[b'7']
 def uid(self,cmd,*args):
  if cmd=='SEARCH':return 'OK',[b'5']
  if cmd=='FETCH' and args[-1]=='(RFC822.SIZE)':return 'OK',[b'1 (RFC822.SIZE 90)']
  return 'OK',[(b'1 (UID 5 FLAGS ())',b'From: test@example.org\r\nSubject: Hello\r\n\r\nTest')]

class Tests(unittest.TestCase):
 def test_folder_quote(self):
  with self.assertRaises(ValueError):quote('INBOX\r\nDELETE x')
  self.assertEqual(folder_label('Entw&APw-rfe'),'Entwürfe')
 def test_safe_html(self):
  m=message_view(b'Content-Type: text/html\r\n\r\n<script>bad()</script><p>Hello</p><img src="https://tracker.invalid">',1)
  self.assertIn('Hello',m['body']);self.assertNotIn('bad()',m['body']);self.assertNotIn('tracker',m['body'])
 def test_uidvalidity(self):
  with self.assertRaises(ValueError):Mailbox(lambda:{}).select(Client(),'INBOX',validity='6')
 def test_unknown_folder(self):
  with self.assertRaises(ValueError):Mailbox(lambda:{}).select(Client(),'secret')
 def test_readonly_routes(self):
  app=Flask(__name__);app.secret_key='test'
  def auth(f):
   @wraps(f)
   def inner(*a,**k):
    if not session.get('admin'):abort(403)
    return f(*a,**k)
   return inner
  register_mailbox(app,auth,lambda:{},lambda:{},lambda:None)
  c=app.test_client()
  self.assertEqual(c.get('/admin/mail/messages').status_code,403)
  with c.session_transaction() as s:s['admin']=True
  with patch('mailbox_client.smtplib.SMTP') as send:
   for url in ['/admin/mail/send','/admin/mail/draft','/admin/mail/message/5/read','/admin/mail/message/5/move']:
    self.assertEqual(c.post(url).status_code,403)
   send.assert_not_called()
 def test_attachment_parse(self):
  from email.message import EmailMessage
  m=EmailMessage();m.set_content('Hello');m.add_attachment(b'pdf',maintype='application',subtype='pdf',filename='test.pdf')
  self.assertEqual(message_view(m.as_bytes(),5)['attachments'][0]['name'],'test.pdf')

class SendTests(unittest.TestCase):
 def test_send_once_and_sent_copy(self):
  import sqlite3,tempfile,uuid
  from contextlib import contextmanager
  from pathlib import Path
  with tempfile.TemporaryDirectory() as tmp:
   app=Flask(__name__);app.config['MAILBOX_WRITE_ENABLED']=True
   cfg=dict(smtp_configured=True,smtp_ssl=True,smtp_host='example.org',smtp_port=465,smtp_user='test',_smtp_password='test',from_address='test@example.org')
   service=register_mailbox(app,lambda f:f,lambda:{},lambda:cfg,lambda:sqlite3.connect(Path(tmp)/'guard.db'))
   class Fake(Client):
    def list(self):return 'OK',[b'(\\Sent) "/" "Sent"',b'(\\Drafts) "/" "Drafts"']
    def append(self,*args):return 'OK',[]
   @contextmanager
   def connection():yield Fake()
   service.connect=connection
   with patch('mailbox_client.smtplib.SMTP_SSL') as smtp:
    smtp.return_value.send_message.return_value={}
    c=app.test_client();data={'to':'recipient@example.org','subject':'Test','body':'Hello','send_token':str(uuid.uuid4())}
    self.assertEqual(c.post('/admin/mail/send',data=data).status_code,200)
    self.assertEqual(c.post('/admin/mail/send',data=data).status_code,409)
    self.assertEqual(smtp.return_value.send_message.call_count,1)
    sent=smtp.return_value.send_message.call_args.args[0]
    self.assertIn('Christopher Gärtner',sent.get_body(preferencelist=('plain',)).get_content())
    markup=sent.get_body(preferencelist=('html',)).get_content()
    self.assertIn('cid:signature-portrait',markup)
    self.assertEqual(sum(1 for part in sent.walk() if part.get_content_maintype()=='image'),2)
    result=c.post('/admin/mail/draft',data=data);self.assertEqual(result.status_code,200,result.json)

class MoveTests(unittest.TestCase):
 def test_recoverable_move_only(self):
  from contextlib import contextmanager
  calls=[]
  class Fake(Client):
   capabilities=(b'MOVE',)
   def list(self):return 'OK',[b'() "/" "INBOX"',b'(\\Trash) "/" "Trash"']
   def select(self,folder,readonly=True):self.readonly=readonly;return 'OK',[]
   def uid(self,*args):calls.append(args);return 'OK',[]
  app=Flask(__name__);app.config['MAILBOX_MOVE_ENABLED']=True
  service=register_mailbox(app,lambda f:f,lambda:{},lambda:{},lambda:None)
  @contextmanager
  def conn():yield Fake()
  service.connect=conn
  c=app.test_client();result=c.post('/admin/mail/message/5/move',data={'folder':'INBOX','validity':'7','target':'__trash__'})
  self.assertEqual(result.status_code,200,result.json);self.assertEqual(calls,[('MOVE','5','"Trash"')])
  self.assertEqual(c.post('/admin/mail/send').status_code,403)
  self.assertEqual(c.post('/admin/mail/draft').status_code,403)

if __name__=='__main__':unittest.main()


