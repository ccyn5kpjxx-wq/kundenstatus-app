"""Real public-host guard and explicit inquiry handler, isolated synthetic DB.

No app import, portal DB, SMTP, supplier, payment or network operation.
"""
import ast
from contextlib import closing
from datetime import date,datetime
import hmac,json,pathlib,re,secrets,socket,sqlite3,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from flask import Flask,abort,request,session,redirect,url_for,jsonify
from werkzeug.datastructures import MultiDict
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from shop_service_requests import ShopOfferError,parse_shop_offer_context,MAX_CART_REQUEST_BYTES,CART_REQUEST_PATH


def draft(count=2):
    return {'version':1,'created_at':1700000000000,'message':'Please check my selected articles.',
            'full_text':'Unverbindliche Shopauswahl\n\n'+'\n\n'.join(
                f'{i}. 4 × SYNTHETIC ARTICLE {i} · exact size and variant\nLieferziel angefragt: Werkstatt Mosbach'
                for i in range(1,count+1)),'position_count':count,'omitted_count':max(0,count-2)}


def form_context(value=None):
    return MultiDict({'anliegen':'allgemein','shop_anfrage':'1',
                      'shop_auswahl':json.dumps(value if value is not None else draft(),ensure_ascii=False)})


class ParserChecks(unittest.TestCase):
    def test_ordinary_form_unchanged_and_full_40_positions_preserved(self):
        self.assertIsNone(parse_shop_offer_context(MultiDict({'anliegen':'allgemein'})))
        value=draft(40);result=parse_shop_offer_context(form_context(value))
        self.assertEqual(result['full_text'],value['full_text']);self.assertEqual(result['position_count'],40)
        self.assertTrue(result['requires_manual_review']);self.assertFalse(result['quote_confirmed'])
        self.assertNotIn('price',result);self.assertNotIn('order',result)

    def test_missing_marker_payload_wrong_concern_and_duplicate_fields_rejected(self):
        for changes in ({'shop_anfrage':''},{'shop_auswahl':''},{'anliegen':'reifenservice'}):
            value=form_context()
            for key,replacement in changes.items():value[key]=replacement
            with self.subTest(changes=changes),self.assertRaises(ShopOfferError):parse_shop_offer_context(value)
        for key in ('shop_anfrage','shop_auswahl','anliegen'):
            value=form_context();value.add(key,value[key])
            with self.subTest(duplicate=key),self.assertRaises(ShopOfferError):parse_shop_offer_context(value)

    def test_malformed_typed_or_excessive_context_fails_fixed_safe(self):
        variations=[{**draft(),'position_count':True},{**draft(),'position_count':41},
            {**draft(),'created_at':True},{**draft(),'version':True},{**draft(),'omitted_count':3},
            {**draft(),'full_text':'x\x00y'},{**draft(),'full_text':'x\ud800'},
            {**draft(),'full_text':'ü'*20000},{**draft(),'message':'x'*2001},
            {**draft(),'price':0},{**draft(),'full_text':''}]
        for value in variations:
            with self.subTest(keys=list(value)),self.assertRaisesRegex(ShopOfferError,'^invalid_shop_offer_context$'):
                parse_shop_offer_context(form_context(value))
        for raw in ('{"version":1,"version":1}','NaN','['*20000+'0'+']'*20000):
            value=form_context();value['shop_auswahl']=raw
            with self.assertRaises(ShopOfferError):parse_shop_offer_context(value)


class ActualIntakeChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tree=ast.parse((ROOT/'app.py').read_text(encoding='utf-8-sig'))

    def setUp(self):
        for target,name in ((socket,'create_connection'),(socket,'getaddrinfo'),(socket.socket,'connect')):
            guard=patch.object(target,name,side_effect=AssertionError('Network forbidden'));guard.start();self.addCleanup(guard.stop)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.dbpath=Path(self.tmp.name)/'synthetic-leads.sqlite3'
        self.db_calls=0
        def db_connect():
            self.db_calls+=1;db=sqlite3.connect(self.dbpath);db.row_factory=sqlite3.Row;return db
        columns=('quelle status autohaus_id auftrag_id source_email_id kunde_name kontakt_telefon kunde_email fahrzeug kennzeichen '
                 'schadenart beschreibung naechste_aktion naechster_kontakt_am geschaetzter_wert notiz verloren_grund datei_original_name '
                 'datei_stored_name datei_mime_type datei_size erstellt_am geaendert_am website herkunft_typ herkunft_id kunden_status_token '
                 'kunden_status_aktiv portal_json angebot_status').split()
        with closing(sqlite3.connect(self.dbpath)) as db,db:db.execute('CREATE TABLE leads(id INTEGER PRIMARY KEY,'+','.join('"'+c+'" TEXT' for c in columns)+')')
        self.notices=Mock();self.backups=Mock();self.attempts=Mock();self.rate=Mock(return_value=(False,0))
        self.app=Flask(__name__,root_path=str(ROOT));self.app.secret_key='SYNTHETIC-ONLY'
        self.app.config.update(SERVER_NAME='shop.example.test',PREFERRED_URL_SCHEME='https')
        def render(**kwargs):
            return jsonify({'form':dict(kwargs.get('formdata') or {}),'errors':kwargs.get('errors',[]),
                            'submitted':kwargs.get('gesendet',False)}),kwargs.get('status_code',200)
        def get_lead(identifier):
            db=db_connect()
            try:row=dict(db.execute('SELECT * FROM leads WHERE id=?',(identifier,)).fetchone())
            finally:db.close()
            return {**row,'kunden_status_url':'https://shop.example.test/status/SYNTHETIC'}
        self.ns={'__name__':__name__,'request':request,'session':session,'abort':abort,'redirect':redirect,'url_for':url_for,
            'sys':sys,'hmac':hmac,'json':json,'pathlib':pathlib,'secrets':secrets,'re':re,'date':date,'datetime':datetime,
            'PUBLIC_SITE_ONLY':False,'PUBLIC_HOSTS':{'shop.example.test'},'PUBLIC_BASE_URL':'https://shop.example.test',
            'PORTAL_BASE_URL':'https://portal.example.test','CSRF_FIELD_NAME':'_csrf_token',
            'progress_csrf_exempt':lambda _:False,'csrf_recovery_response':lambda:None,
            'clean_text':lambda x:str(x or '').strip(),'render_website_anfrage':render,'public_form_rate_limit_status':self.rate,
            'record_public_form_attempt':self.attempts,'login_wait_label':lambda n:str(n),
            'WEBSITE_ANLIEGEN':{'allgemein':'Allgemeine Beratung'},'WEBSITE_KURZANFRAGEN':{},
            'WEBSITE_BESICHTIGUNGSARTEN':{'werkstatt':'Werkstatt'},'WEBSITE_MIETWAGEN_FLOTTE':{},'HOMEPAGE_LEISTUNGEN':{},
            'WEBSITE_MECHANIK_KATEGORIEN':{},'WEBSITE_PFLEGE_KATEGORIEN':{},'WEBSITE_REIFEN_LEISTUNGEN':{},
            'WEBSITE_ANFRAGE_MAX_DATEI_MB':5,'WEBSITE_ANFRAGE_MAX_GESAMT_MB':25,
            'validate_website_lead_uploads':lambda *a,**k:([],[]),'parse_email_recipients':lambda s:[s],
            'get_db':db_connect,'get_lead':get_lead,'now_str':lambda:'17.11.2026 00:00',
            'normalize_lead_website':lambda x:x,'normalize_lead_quelle':lambda x:x,'normalize_lead_status':lambda x:x,
            'normalize_schadenart':lambda x:x,'normalize_lead_herkunft_typ':lambda x:x,'positive_money_amount':lambda x:0,
            'format_date':lambda x:x or '', 'add_lead_portal_event':Mock(),
            'schedule_change_backup':self.backups,'sende_website_lead_benachrichtigung':self.notices,
            'notify_lead_workshop':self.notices,'lead_notice':self.notices,
            'ReifenAnfrageBereitsVorhanden':type('ReifenAnfrageBereitsVorhanden',(Exception,),{}),
            'ReifenSlotBelegt':type('ReifenSlotBelegt',(Exception,),{})}
        names=('is_public_site_request','restrict_public_site_service','protect_csrf','create_lead','website_anfrage')
        functions=[]
        for name in names:
            fn=next(n for n in self.tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
            import copy
            fn=copy.deepcopy(fn);fn.decorator_list=[];functions.append(fn)
        exec(compile(ast.fix_missing_locations(ast.Module(body=functions,type_ignores=[])),str(ROOT/'app.py'),'exec'),self.ns)
        self.app.before_request(self.ns['restrict_public_site_service']);self.app.before_request(self.ns['protect_csrf'])
        self.app.add_url_rule('/anfrage','website_anfrage',self.ns['website_anfrage'],methods=['GET','POST'])
        self.app.add_url_rule('/private-probe','admin_private_probe',lambda:'PRIVATE',methods=['GET','POST'])
        self.client=self.app.test_client()
        with self.client.session_transaction() as session_:session_['_csrf_token']='SYNTHETIC-CSRF'

    def payload(self,count=2):
        return {**form_context(draft(count)).to_dict(),'_csrf_token':'SYNTHETIC-CSRF',
                'name':'SYNTHETIC CUSTOMER','telefon':'01234567890','nachricht':'Please check exact vehicle fitment.'}

    def lead_rows(self):
        db=sqlite3.connect(self.dbpath);db.row_factory=sqlite3.Row
        try:return [dict(row) for row in db.execute('SELECT * FROM leads')]
        finally:db.close()

    def test_public_host_get_opens_real_handler_without_saving_reserving_or_notifying(self):
        response=self.client.get(CART_REQUEST_PATH)
        self.assertEqual(response.status_code,200);self.assertEqual(response.json['form']['anliegen'],'allgemein')
        self.assertFalse(response.json['submitted']);self.assertEqual(self.db_calls,0);self.notices.assert_not_called();self.backups.assert_not_called()
        self.assertEqual(self.client.get('/private-probe').status_code,404)

    def test_stateless_public_host_still_blocks_intake_get_and_post(self):
        self.ns['PUBLIC_SITE_ONLY']=True
        self.assertEqual(self.client.get(CART_REQUEST_PATH).status_code,404)
        self.assertEqual(self.client.post('/anfrage',data=self.payload()).status_code,404)
        self.assertEqual(self.lead_rows(),[]);self.notices.assert_not_called()

    def test_post_requires_existing_csrf_before_any_lead_or_notice(self):
        data=self.payload();data.pop('_csrf_token')
        self.assertEqual(self.client.post('/anfrage',data=data).status_code,400)
        self.assertEqual(self.lead_rows(),[]);self.attempts.assert_not_called();self.notices.assert_not_called()

    def test_honeypot_and_rate_limit_do_not_create_a_lead_or_notify(self):
        data=self.payload();data['website']='SYNTHETIC BOT'
        self.assertEqual(self.client.post('/anfrage',data=data).status_code,302)
        self.assertEqual(self.lead_rows(),[]);self.notices.assert_not_called()
        self.rate.return_value=(True,60)
        self.assertEqual(self.client.post('/anfrage',data=self.payload()).status_code,429)
        self.assertEqual(self.lead_rows(),[]);self.notices.assert_not_called()

    def test_invalid_contact_preserves_full_context_for_editable_retry_without_submission(self):
        data=self.payload(40);data['name']='X';data['telefon']=''
        response=self.client.post('/anfrage',data=data)
        self.assertEqual(response.status_code,400);self.assertEqual(response.json['form']['shop_auswahl'],data['shop_auswahl'])
        self.assertFalse(response.json['submitted']);self.assertEqual(self.lead_rows(),[]);self.notices.assert_not_called()

    def test_explicit_valid_post_durably_preserves_every_position_as_unconfirmed_lead_only(self):
        response=self.client.post('/anfrage',data=self.payload(40))
        self.assertEqual(response.status_code,302)
        rows=self.lead_rows();self.assertEqual(len(rows),1);row=rows[0];context=json.loads(row['portal_json'])['shop_anfrage']
        self.assertEqual(context['full_text'],draft(40)['full_text']);self.assertIn('40. 4 × SYNTHETIC ARTICLE 40',row['beschreibung'])
        self.assertIn('Fahrzeugpassung',row['beschreibung']);self.assertFalse(context['quote_confirmed']);self.assertTrue(context['requires_manual_review'])
        self.assertIsNone(row['auftrag_id']);self.assertIn('vor einer Zahlung',row['naechste_aktion'])
        self.assertTrue(self.notices.called);self.backups.assert_called_once_with('website-anfrage-erfolgreich')

    def test_invalid_cart_context_is_rejected_before_persistence(self):
        data=self.payload();data.pop('shop_anfrage')
        response=self.client.post('/anfrage',data=data)
        self.assertEqual(response.status_code,400);self.assertEqual(self.lead_rows(),[]);self.notices.assert_not_called()

if __name__=='__main__':unittest.main()
