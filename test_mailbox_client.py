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





class ReadFlagTests(unittest.TestCase):
 def setUp(self):
  from contextlib import contextmanager
  self.app=Flask(__name__);self.app.secret_key='synthetic';self.app.config['MAILBOX_FLAGS_ENABLED']=True
  self.app.config['MAILBOX_WRITE_ENABLED']=False
  owner=self
  class FlagClient(Client):
   def __init__(self):
    self.flags={b'\\Answered',b'$Forwarded'};self.calls=[];self.missing=False;self.store_error=False;self.readback_error=False;self.noop=False
   def select(self,folder,readonly=True):self.calls.append(('SELECT',folder,readonly));return 'OK',[b'1']
   def uid(self,cmd,*args):
    self.calls.append((cmd,*args))
    meta=b'1 (UID 5 FLAGS ('+b' '.join(sorted(self.flags))+b'))'
    if cmd=='STORE':
     if self.store_error:return 'NO',[b'refused']
     if not self.noop:
      if args[1]=='+FLAGS.SILENT':self.flags.add(b'\\Seen')
      else:self.flags.discard(b'\\Seen')
     return 'OK',[None]
    if cmd=='FETCH' and args[-1]=='(UID FLAGS)':
     if self.missing:return 'OK',[None]
     if self.readback_error and any(x[0]=='STORE' for x in self.calls):return 'NO',[b'failed']
     return 'OK',[meta]
    if cmd=='FETCH' and args[-1]=='(RFC822.SIZE)':return 'OK',[b'1 (UID 5 RFC822.SIZE 90)']
    return 'OK',[(meta,b'From: test@example.invalid\r\nSubject: Test\r\n\r\nBody')]
  self.imap=FlagClient()
  def auth(fn):
   @wraps(fn)
   def wrapper(*args,**kwargs):
    if not session.get('admin'):abort(403)
    return fn(*args,**kwargs)
   return wrapper
  service=register_mailbox(self.app,auth,lambda:{},lambda:{},lambda:None)
  @contextmanager
  def connection():yield owner.imap
  service.connect=connection
  self.client=self.app.test_client()
  with self.client.session_transaction() as s:s['admin']=True
 def post(self,read='1',**extra):
  return self.client.post('/admin/mail/message/5/read',data={'folder':'INBOX','validity':'7','read':read,**extra})
 def test_separate_flags_switch_does_not_enable_mail_send_or_drafts(self):
  self.assertEqual(self.post().status_code,200)
  for endpoint in ('send','draft'):
   self.assertEqual(self.client.post('/admin/mail/'+endpoint).status_code,403)
  self.app.config['MAILBOX_FLAGS_ENABLED']=False
  self.app.config['MAILBOX_WRITE_ENABLED']=True
  self.assertEqual(self.post().status_code,403)
 def test_authenticated_route_only(self):
  with self.client.session_transaction() as s:s.clear()
  self.assertEqual(self.post().status_code,403);self.assertEqual(self.imap.calls,[])
 def test_both_directions_verified_and_other_flags_preserved(self):
  for read in ('1','1','0'):
   response=self.post(read)
   self.assertEqual(response.status_code,200,response.json)
   self.assertEqual(response.json,dict(ok=True,uid='5',folder='INBOX',validity='7',unread=read=='0'))
   self.assertTrue({b'\\Answered',b'$Forwarded'}.issubset(self.imap.flags))
  stores=[x for x in self.imap.calls if x[0]=='STORE']
  self.assertEqual(stores,[('STORE','5','+FLAGS.SILENT',r'(\Seen)'),('STORE','5','+FLAGS.SILENT',r'(\Seen)'),('STORE','5','-FLAGS.SILENT',r'(\Seen)')])
 def test_invalid_values_never_select_or_store(self):
  for read in ('', 'true', 'false', '2', 'read'):
   self.assertEqual(self.post(read).status_code,400)
  for validity in ('', '0','7\r\n','wrong'):
   self.assertEqual(self.post(validity=validity).status_code,400)
  self.assertEqual(self.imap.calls,[])
 def test_changed_validity_and_unknown_folder_never_store(self):
  self.assertEqual(self.post(validity='6').status_code,400)
  self.assertEqual(self.post(folder='secret').status_code,400)
  self.assertFalse(any(x[0]=='STORE' for x in self.imap.calls))
 def test_missing_uid_cannot_report_false_success(self):
  self.imap.missing=True
  self.assertEqual(self.post().status_code,400)
  self.assertFalse(any(x[0]=='STORE' for x in self.imap.calls))
 def test_store_error_or_unverified_readback_is_not_success(self):
  for failure in ('store_error','readback_error','noop'):
   with self.subTest(failure=failure):
    self.setUp();setattr(self.imap,failure,True)
    self.assertEqual(self.post().status_code,400)
    self.assertEqual(sum(x[0]=='STORE' for x in self.imap.calls),1)
 def test_reads_and_attachments_use_peek_and_do_not_mark_seen(self):
  response=self.client.get('/admin/mail/message/5?folder=INBOX&validity=7')
  self.assertEqual(response.status_code,200);self.assertTrue(response.json['unread'])
  self.assertIn(('FETCH','5','(FLAGS BODY.PEEK[])'),self.imap.calls)
  self.assertIn(('SELECT','"INBOX"',True),self.imap.calls)
  self.assertFalse(any(x[0]=='STORE' for x in self.imap.calls))
 def test_seen_flag_is_exact_and_case_insensitive(self):
  for flags,unread in ((r'FLAGS (\Seen)',False),(r'flags (\seen \Answered)',False),(r'FLAGS (\SeenExtra)',True),(r'FLAGS ($Forwarded)',True)):
   with self.subTest(flags=flags):self.assertEqual(message_view(b'Subject: Test\r\n\r\nText',5,flags)['unread'],unread)

if __name__=='__main__':unittest.main()
