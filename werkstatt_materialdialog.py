"""Persistent, evidence-bound material requests; transport and dispatch stay opt-in.

Image labels are proposals. Only direct signed employee text supplies purchase
intent/quantity/urgency; independently reviewed commercial terms supply prices.
No Flask session is fabricated and no model output grants purchase permission.
"""
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import os
import re
import secrets
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests
from werkstatt_materialkanal import _json, _fingerprint, _note, _row_id, _key, _BorrowedConnection

TABLES = ('einkauf_material_dialoge', 'einkauf_material_texte', 'einkauf_material_rueckfragen')
UNITS = {'karton':'Karton', 'kartons':'Karton', 'rolle':'Rolle', 'rollen':'Rolle', 'packung':'Packung',
         'packungen':'Packung', 'gebinde':'Gebinde', 'dose':'Dose', 'dosen':'Dose', 'flasche':'Flasche',
         'flaschen':'Flasche', 'stück':'Stück', 'stueck':'Stück'}
WORDS = {'ein':'1','eine':'1','einen':'1','einem':'1','zwei':'2','drei':'3','vier':'4','fünf':'5','fuenf':'5',
         'sechs':'6','sieben':'7','acht':'8','neun':'9','zehn':'10'}
QUANTITY_PATTERN = r'\b(\d{1,8}(?:[.,]\d{1,6})?|' + '|'.join(WORDS) + r')\s+(' + '|'.join(sorted(UNITS, key=len, reverse=True)) + r')\b'


def article_query(text):
    """Remove request grammar, retaining SKU, colour and dimensional evidence."""
    value = re.sub(QUANTITY_PATTERN,' ',_note(text),flags=re.I)
    value = re.sub(r'\b(?:bitte|bestellen|bestelle|bestell|nachbestellen|nachbestelle|dringend|sofort|nicht|regulär|regulaer|normal|wöchentlich|woechentlich|ich|möchte|moechte|brauche|benötige|benoetige|mir|wir|uns|erst|am|montag)\b',' ',value,flags=re.I)
    return re.sub(r'\s+',' ',value).strip(' ,;.!:')[:150]


def parse_request(text, question=''):
    """Small conservative grammar, never extracts quantity from a product label."""
    text = _note(text)
    value = text.casefold()
    fields = {}
    if re.search(r'\b(abbrechen|stornieren)\b|\bnicht(?:\s+mehr)?\s+bestell\w*|\bkeine?\s+bestellung\b|\bbestell\w*\s+(?:bitte\s+)?nicht(?:\s+mehr)?\s*[.!]?$', value):
        return {'cancelled': True}
    conditional = bool(re.search(r'\b(vielleicht|eventuell|falls|wenn|beispiel|angenommen)\b', value))
    if conditional or '?' in value:
        return fields
    if re.search(r'\b(?:bestellen|bestelle|bestell|nachbestellen|nachbestelle)\b', value):
        fields['order_requested'] = True
    urgency = set()
    for match in re.finditer(r'\b(?:dringend|sofort)\b', value):
        prefix = value[max(0,match.start()-45):match.start()]
        urgency.add(not bool(re.search(r'\b(?:nicht|keinesfalls|keine)\b(?:\s+\w+){0,3}\s*$',prefix)))
    if re.search(r'\b(?:regulär|regulaer|normal|wöchentlich|woechentlich|erst\s+(?:am\s+)?montag)\b',value):
        urgency.add(False)
    if len(urgency)==1:
        fields['urgent'] = urgency.pop()
    elif len(urgency)>1:
        fields['urgent_unclear'] = True
    elif not urgency and question == 'urgent' and re.fullmatch(r'\s*(?:ja|nein)[.!]?\s*', value):
        fields['urgent'] = value.strip(' .!') == 'ja'
    matches = []
    for match in re.finditer(QUANTITY_PATTERN, value):
        prefix = value[max(0, match.start()-35):match.start()]
        if re.search(r'(?:\bve|inhalt|packungsinhalt|enthält|enthaelt)\s*[:=]?\s*$', prefix):
            continue
        amount = Decimal(WORDS.get(match[1], match[1]).replace(',', '.'))
        if amount > 0:
            matches.append((format(amount.normalize(), 'f'), UNITS[match[2]]))
    if len(set(matches)) == 1:
        fields['quantity'], fields['unit'] = matches[0]
    return fields


class MaterialDialog:
    def __init__(self, portal):
        self.p = portal
        self.channel = portal.material_channel
        self.clock = self.channel.clock
        portal.app.config.setdefault('MATERIAL_WHATSAPP_REPLIES_ENABLED',
            getattr(portal, 'MATERIAL_WHATSAPP_REPLIES_ENABLED', os.environ.get('MATERIAL_WHATSAPP_REPLIES_ENABLED', '').lower() in {'1','true','yes'}))
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS einkauf_material_dialoge (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL UNIQUE,
                    revision INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'open',
                    fields_json TEXT NOT NULL DEFAULT '{}', review_json TEXT NOT NULL DEFAULT '{}',
                    analysis_json TEXT NOT NULL DEFAULT '{}', analysis_state TEXT NOT NULL DEFAULT 'pending',
                    analysis_lease TEXT NOT NULL DEFAULT '', analysis_until DOUBLE PRECISION NOT NULL DEFAULT 0,
                    snapshot_json TEXT NOT NULL DEFAULT '{}', snapshot_hash TEXT NOT NULL DEFAULT '',
                    missing_json TEXT NOT NULL DEFAULT '[]', dispatch_id TEXT NOT NULL DEFAULT '',
                    dispatch_state TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
                    created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL);
                CREATE TABLE IF NOT EXISTS einkauf_material_texte (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, phone_number_id TEXT NOT NULL, wamid TEXT NOT NULL,
                    canonical_hash TEXT NOT NULL, sender_id INTEGER NOT NULL, sender_revision INTEGER NOT NULL,
                    employee_id INTEGER NOT NULL, rights_version INTEGER NOT NULL, reply_to TEXT NOT NULL DEFAULT '',
                    body TEXT NOT NULL, forwarded INTEGER NOT NULL DEFAULT 0, source_at TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', draft_id INTEGER, bound_revision INTEGER,
                    error_code TEXT NOT NULL DEFAULT '', created_at DOUBLE PRECISION NOT NULL,
                    UNIQUE(phone_number_id,wamid));
                CREATE TABLE IF NOT EXISTS einkauf_material_rueckfragen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, draft_id INTEGER NOT NULL, revision INTEGER NOT NULL,
                    field TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
                    provider_id TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
                    created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL,
                    UNIQUE(draft_id,revision));
            ''')

    def _source(self, db, draft, lock=False):
        source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?', (draft['message_id'],)).fetchone()
        if not source or source['state'] not in {'ready','text_ready'}:
            raise PermissionError('Das ursprüngliche persönliche Foto ist nicht verfügbar.')
        self.channel._active(db, source, lock=lock)
        if lock:
            db.execute('UPDATE einkauf_material_dialoge SET updated_at=updated_at WHERE id=?', (draft['id'],))
        return dict(source)

    def _draft(self, db, draft_id, revision=None, lock=False):
        _row_id(draft_id)
        row = db.execute('SELECT * FROM einkauf_material_dialoge WHERE id=?', (draft_id,)).fetchone()
        if not row:
            raise ValueError('Materialvorgang nicht gefunden.')
        if lock:
            self._source(db, row, lock=True)
            row = db.execute('SELECT * FROM einkauf_material_dialoge WHERE id=?', (draft_id,)).fetchone()
        if revision is not None and (_row_id(revision) != row['revision']):
            raise ValueError('Materialvorgang wurde geändert. Bitte neu laden.')
        return dict(row)

    def _accepted(self, db, draft):
        if draft['dispatch_id']:
            return True
        # Also close the small gap after durable enqueue but before order_attempt.
        manager = getattr(self.p, 'workshop_orders', None)
        if manager and getattr(manager, 'dispatch', None):
            return bool(db.execute('SELECT id FROM assistent_bestellanforderungen WHERE request_id=?', ('material:' + str(draft['id']),)).fetchone())
        return False

    def _proof(self, source, kind='image', source_id=None):
        return {'kind': kind, 'id': source_id or source['id'], 'wamid': source['wamid'],
                'source_at': source['source_at'], 'employee_id': source['employee_id']}

    def _merge(self, fields, parsed, proof):
        for key, value in parsed.items():
            previous = fields.get(key)
            if previous and previous['proof']['source_at'] > proof['source_at']:
                continue
            if key == 'urgent_unclear':
                previous = fields.get('urgent')
                if not previous or previous['proof']['source_at'] <= proof['source_at']:
                    fields.pop('urgent',None)
            elif key == 'urgent':
                fields.pop('urgent_unclear',None)
            fields[key] = {'value': value, 'proof': proof}

    def ensure_draft(self, message_id):
        with self.db() as db:
            source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?', (_row_id(message_id),)).fetchone()
            if not source or source['state'] not in {'ready','text_ready'}:
                raise ValueError('Fotoeingang ist noch nicht vollständig.')
            return self._ensure(db,source)

    def _ensure(self, db, source, text=None):
        self.channel._active(db,source,lock=True)
        fields = {}
        if not source['forwarded']:
            parsed = parse_request(source['caption'])
            if parsed.get('urgent') is True and not parsed.get('cancelled'):
                parsed['order_requested'] = True
            self._merge(fields,parsed,self._proof(text,'text',text['id']) if text else self._proof(source))
        db.execute('''INSERT INTO einkauf_material_dialoge(message_id,fields_json,created_at,updated_at)
            VALUES(?,?,?,?) ON CONFLICT(message_id) DO NOTHING RETURNING id''',(source['id'],_json(fields),self.clock(),self.clock())).fetchall()
        draft = dict(db.execute('SELECT * FROM einkauf_material_dialoge WHERE message_id=?',(source['id'],)).fetchone())
        if text:
            db.execute("UPDATE einkauf_material_texte SET state='applied',draft_id=?,bound_revision=1 WHERE id=?",(draft['id'],text['id']))
        self._refresh(db,draft)
        return self._view(db,draft['id'])

    def _text_source(self, db, text):
        employee = self.channel._employee(db,text['employee_id'])
        intake = copy.copy(self.p.workshop_intake)
        intake.p = SimpleNamespace(get_db=lambda:_BorrowedConnection(db))
        group = intake.create({'supplier':'Lieferant ungeklärt','source_key':'whatsapp-text:'+text['phone_number_id']+':'+hashlib.sha256(text['wamid'].encode()).hexdigest(),
            'external_ref':'Persönlicher Materialwunsch per Text: '+text['body'][:400], 'source_at':text['source_at'],
            'already_ordered':False,'original_author':employee['name'],
            'lines':[{'product':'Materialwunsch – Artikelzuordnung prüfen','quantity':None,'urgent':None,'category':'ungeklaert'}]})
        db.execute('''INSERT INTO einkauf_material_nachrichten(phone_number_id,wamid,canonical_hash,sender_id,sender_revision,
            employee_id,employee_name,rights_version,media_id,mime,expected_sha256,caption,forwarded,source_at,received_at,state,intake_id,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'text_ready',?,?) ON CONFLICT(phone_number_id,wamid) DO NOTHING RETURNING id''',
            (text['phone_number_id'],text['wamid'],text['canonical_hash'],text['sender_id'],text['sender_revision'],text['employee_id'],employee['name'],
             text['rights_version'],'','text/plain',hashlib.sha256(text['body'].encode()).hexdigest(),text['body'],0,text['source_at'],self.clock(),group['id'],self.clock())).fetchall()
        source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE phone_number_id=? AND wamid=?',(text['phone_number_id'],text['wamid'])).fetchone()
        if source['mime'] != 'text/plain' or source['canonical_hash'] != text['canonical_hash']:
            raise ValueError('Diese Nachrichtenkennung gehört bereits zu einer anderen Quelle.')
        return source

    def _today(self):
        return datetime.fromtimestamp(self.clock(),ZoneInfo('Europe/Berlin')).date().isoformat()

    @staticmethod
    def _hit_identity(hit):
        return {key:' '.join(str(hit.get(key,'')).casefold().split()) for key in ('lieferant','artikelnummer','groesse','farbe','gebinde','ve')}

    def _reuse_review(self, db, hit):
        identity = self._hit_identity(hit)
        rows = [(row['id'],json.loads(row['review_json'])) for row in db.execute(
            "SELECT id,review_json FROM einkauf_material_dialoge WHERE review_json<>'{}'").fetchall()]
        candidates = sorted(((key,review) for key,review in rows if review.get('match_identity')==identity
            and review.get('reviewed_by')=='admin'),key=lambda item:(item[1].get('reviewed_at',''),item[0]),reverse=True)
        if candidates:
            key, review = candidates[0]
            if review.get('verified_until','') >= self._today():
                supplier = self.p.workshop_orders.resolve_supplier(review['supplier_id'])
                if supplier and supplier.get('verified') and supplier['recipient']==review['recipient']:
                    return dict(review,reused_from=key)
        return {}

    def receive_text(self, db, event, sender, employee):
        fingerprint = _fingerprint(event)
        old = db.execute('SELECT canonical_hash FROM einkauf_material_texte WHERE phone_number_id=? AND wamid=?', (event['phone_number_id'],event['wamid'])).fetchone()
        if old:
            if old['canonical_hash'] != fingerprint:
                raise ValueError('Textnachrichtenkennung mit anderem Inhalt wiederholt.')
            return False
        inserted = db.execute('''INSERT INTO einkauf_material_texte
            (phone_number_id,wamid,canonical_hash,sender_id,sender_revision,employee_id,rights_version,reply_to,body,forwarded,source_at,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(phone_number_id,wamid) DO NOTHING RETURNING id''',
            (event['phone_number_id'],event['wamid'],fingerprint,sender['id'],sender['revision'],employee['id'],employee['version'],
             event['reply_to'],event['body'],int(event['forwarded']),event['source_at'],self.clock())).fetchone()
        if not inserted:
            stored = db.execute('SELECT canonical_hash FROM einkauf_material_texte WHERE phone_number_id=? AND wamid=?', (event['phone_number_id'],event['wamid'])).fetchone()
            if not stored or stored['canonical_hash'] != fingerprint:
                raise ValueError('Textnachrichtenkennung mit anderem Inhalt wiederholt.')
        return bool(inserted)

    def _refresh(self, db, draft):
        if self._accepted(db, draft):
            return
        fields, review = json.loads(draft['fields_json']), json.loads(draft['review_json'])
        selected = fields.get('selected_article',{}).get('value')
        if selected and review and review.get('match_identity') != self._hit_identity(selected):
            review = {}
            db.execute("UPDATE einkauf_material_dialoge SET review_json='{}' WHERE id=?",(draft['id'],))
        if not review and fields.get('selected_article'):
            review = self._reuse_review(db,fields['selected_article']['value'])
            if review:
                db.execute('UPDATE einkauf_material_dialoge SET review_json=? WHERE id=?',(_json(review),draft['id']))
        values = {key:item['value'] for key,item in fields.items()}
        missing = []
        if values.get('cancelled') is True:
            state, payload = 'cancelled', {}
        else:
            for key in ('order_requested','quantity','unit','urgent'):
                if key not in values or key == 'order_requested' and values[key] is not True:
                    missing.append(key)
            if not review:
                missing += ['article','price']
            elif review.get('verified_until','') < self._today():
                missing.append('price')
            elif values.get('unit') and values['unit'].casefold() != review['unit'].casefold():
                missing.append('unit_conflict')
            payload = {}
            if not missing:
                payload = {key:review[key] for key in ('supplier_id','recipient','article_number','product_name','variant','unit',
                    'unit_price_cents','shipping_cents','extra_costs_cents','price_source')}
                total = int((Decimal(values['quantity']) * review['unit_price_cents']).to_integral_value(rounding=ROUND_CEILING)) + review['shipping_cents'] + review['extra_costs_cents']
                payload.update(order_requested=True,quantity=values['quantity'],urgent=values['urgent'],max_total_cents=total,
                               price_verified=True,price_basis='gross',currency='EUR')
                if not 0 < total <= 25000:
                    missing.append('budget')
            state = 'open' if missing else 'approved'
        snapshot = {'actor':'mitarbeiter:' + str(self._source(db,draft)['employee_id']), 'request_key':'material:' + str(draft['id']), 'payload':payload}
        db.execute('''UPDATE einkauf_material_dialoge SET state=?,snapshot_json=?,snapshot_hash=?,missing_json=?,updated_at=? WHERE id=?''',
            (state,_json(snapshot),_fingerprint(snapshot),_json(missing),self.clock(),draft['id']))
        db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded' WHERE draft_id=? AND revision<>? AND state='queued'", (draft['id'],draft['revision']))
        if missing and state == 'open':
            first = missing[0]
            questions = {'order_requested':'Soll dieses Material bestellt werden? Bitte ausdrücklich „bestellen“ schreiben.',
                'quantity':'Wie viel möchtest du bestellen? Bitte Menge und Bestelleinheit nennen, etwa „ein Karton“.',
                'unit':'Welche Bestelleinheit meinst du: Karton, Rolle oder Packung?', 'urgent':'Ist die Bestellung dringend?',
                'article':'Welcher genaue Artikel und welche Variante sind gemeint? Die Werkstattleitung prüft die Zuordnung.',
                'price':'Die Werkstattleitung muss Bruttopreis, Versand und Nebenkosten anhand einer aktuellen Quelle prüfen.',
                'unit_conflict':'Genannte Bestelleinheit und geprüfter Artikel passen noch nicht zusammen. Bitte klären.',
                'budget':'Der Gesamtbetrag liegt außerhalb des freigegebenen Rahmens. Die Werkstattleitung muss übernehmen.'}
            if first == 'article':
                hits = json.loads(draft['analysis_json']).get('treffer',[])
                if len(hits)==1:
                    hit=hits[0]
                    questions['article']='Meinst du '+str(hit.get('produkt_name','Artikel'))+' '+str(hit.get('groesse',''))+' '+str(hit.get('farbe',''))+'?'
                elif hits:
                    questions['article']='Welche Variante: '+', '.join(str(h.get('groesse') or h.get('produkt_name')) for h in hits[:5])+'?'
            body = 'M-' + str(draft['id']) + ' R' + str(draft['revision']) + ': ' + questions[first] + ' Antworte bitte zitiert oder mit diesem Vorgangscode.'
            db.execute('''INSERT INTO einkauf_material_rueckfragen(draft_id,revision,field,body,created_at,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(draft_id,revision) DO NOTHING RETURNING id''',
                (draft['id'],draft['revision'],first,body,self.clock(),self.clock())).fetchall()

    def _view(self, db, draft_id):
        row = self._draft(db,draft_id)
        source = db.execute('SELECT employee_id,employee_name,intake_id,mime FROM einkauf_material_nachrichten WHERE id=?',(row['message_id'],)).fetchone()
        row.update(employee_id=source['employee_id'] if source else None, employee_name=source['employee_name'] if source else '',
                   intake_id=source['intake_id'] if source else None, source_kind='text' if source and source['mime']=='text/plain' else 'image')
        for source,target in (('fields_json','fields'),('review_json','review'),('analysis_json','analysis'),('missing_json','missing_fields')):
            row[target] = json.loads(row.pop(source))
        row.pop('snapshot_json'); row.pop('snapshot_hash'); row.pop('analysis_lease')
        row['code'] = 'M-' + str(row['id']) + ' R' + str(row['revision'])
        row['questions'] = [dict(q) for q in db.execute('SELECT id,revision,field,body,state,error_code FROM einkauf_material_rueckfragen WHERE draft_id=? ORDER BY id DESC LIMIT 8',(draft_id,)).fetchall()]
        return row

    def status(self, draft_id):
        with self.db() as db:
            return self._view(db,draft_id)

    def list(self, limit=50):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Ungültige Listengröße.')
        with self.db() as db:
            ids = db.execute('SELECT id FROM einkauf_material_dialoge ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
            return [self._view(db,row['id']) for row in ids]

    def process_text(self):
        with self.db() as db:
            text = db.execute("SELECT * FROM einkauf_material_texte WHERE state='queued' ORDER BY id LIMIT 1").fetchone()
            if not text:
                return None
            text = dict(text)
            changed = db.execute("UPDATE einkauf_material_texte SET state='processing' WHERE id=? AND state='queued'",(text['id'],)).rowcount
            if changed != 1:
                return None
            try:
                self.channel._active(db,text,lock=True)
                if text['forwarded']:
                    raise ValueError('Weiterleitung ist keine persönliche Bestellantwort.')
                reference = re.search(r'\bM-(\d+)\s+R(\d+)\b',text['body'],re.I)
                question = None
                if reference:
                    draft = self._draft(db,int(reference[1]),int(reference[2]),lock=True)
                    question = db.execute('SELECT * FROM einkauf_material_rueckfragen WHERE draft_id=? AND revision=?',(draft['id'],draft['revision'])).fetchone()
                elif text['reply_to']:
                    question = db.execute("SELECT * FROM einkauf_material_rueckfragen WHERE provider_id=? AND state='sent'",(text['reply_to'],)).fetchone()
                    if question:
                        draft = self._draft(db,question['draft_id'],question['revision'],lock=True)
                    else:
                        origin = db.execute('''SELECT d.id FROM einkauf_material_dialoge d JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                            WHERE n.phone_number_id=? AND n.wamid=?''',(text['phone_number_id'],text['reply_to'])).fetchone()
                        if not origin:
                            raise ValueError('Antwort gehört noch zu keinem bekannten Foto.')
                        draft = self._draft(db,origin['id'],lock=True)
                else:
                    parsed = parse_request(text['body'])
                    if not parsed.get('order_requested') and parsed.get('urgent') is not True:
                        raise ValueError('Bitte den konkreten Vorgang zitieren oder ausdrücklich einen neuen Artikel bestellen.')
                    view = self._ensure(db,self._text_source(db,text),text)
                    return {'id':text['id'],'state':'applied','draft_id':view['id']}
                source = self._source(db,draft)
                if any(source[key] != text[key] for key in ('sender_id','sender_revision','employee_id','rights_version','phone_number_id')):
                    raise PermissionError('Antwort gehört einem anderen persönlichen Fotoeingang.')
                if self._accepted(db,draft) or draft['state'] == 'cancelled':
                    raise ValueError('Bestellung ist bereits übergeben oder abgebrochen; einen neuen Vorgang beginnen.')
                body = re.sub(r'\bM-\d+\s+R\d+\s*[:,-]?','',text['body'],flags=re.I).strip()
                parsed = parse_request(body,question['field'] if question else '')
                if parsed.get('urgent') is True and not parsed.get('cancelled'):
                    parsed['order_requested'] = True
                analysis = json.loads(draft['analysis_json'])
                hits = analysis.get('treffer',[])
                hit = None
                if question and question['field']=='article' and body.casefold().strip(' .!')=='ja' and len(hits)==1 and not analysis.get('treffer_gekuerzt'):
                    hit = hits[0]
                else:
                    label = re.sub(r'\s+','',body.casefold().strip(' .!'))
                    matching = [h for h in hits if label and label in {re.sub(r'\s+','',str(h.get(k,'')).casefold()) for k in ('groesse','artikelnummer','produkt_name')}]
                    if len(matching)==1 and not analysis.get('treffer_gekuerzt'):
                        hit = matching[0]
                if hit:
                    parsed['selected_article'] = hit
                if not parsed:
                    # A bound but unclear correction must stop an earlier approval.
                    db.execute("UPDATE einkauf_material_dialoge SET state='review',error_code='antwort_unverstaendlich',updated_at=? WHERE id=?",(self.clock(),draft['id']))
                    raise ValueError('Antwort ist nicht eindeutig. Bitte konkrete Menge/Einheit oder Dringlichkeit nennen.')
                fields = json.loads(draft['fields_json'])
                self._merge(fields,parsed,self._proof(text,'text',text['id']))
                review = json.loads(draft['review_json'])
                if parsed.get('selected_article') and review.get('match_identity') != self._hit_identity(parsed['selected_article']):
                    db.execute("UPDATE einkauf_material_dialoge SET review_json='{}' WHERE id=?",(draft['id'],))
                db.execute("UPDATE einkauf_material_dialoge SET fields_json=?,revision=revision+1,error_code='' WHERE id=?",(_json(fields),draft['id']))
                db.execute("UPDATE einkauf_material_texte SET state='applied',draft_id=?,bound_revision=? WHERE id=?",(draft['id'],draft['revision'],text['id']))
                draft = self._draft(db,draft['id'])
                self._refresh(db,draft)
                return {'id':text['id'],'state':'applied','draft_id':draft['id']}
            except (ValueError,PermissionError):
                db.execute("UPDATE einkauf_material_texte SET state='review',error_code='antwort_oder_berechtigung_klaeren' WHERE id=?",(text['id'],))
                return {'id':text['id'],'state':'review'}

    def analyze(self, draft_id):
        lease = secrets.token_hex(16)
        with self.db() as db:
            draft = self._draft(db,draft_id,lock=True)
            source = self._source(db,draft)
            if draft['analysis_state'] == 'done' or draft['analysis_until'] > self.clock():
                return self._view(db,draft_id)
            db.execute("UPDATE einkauf_material_dialoge SET analysis_state='processing',analysis_lease=?,analysis_until=? WHERE id=?",(lease,self.clock()+120,draft_id))
        photos = copy.copy(self.p.assistant_material_photos)
        @contextmanager
        def guarded_photo_db():
            with self.db() as db:
                current = self._draft(db,draft_id,lock=True)
                if current['analysis_lease'] != lease or current['analysis_until'] < self.clock() or current['state'] == 'cancelled':
                    raise PermissionError('Fotoauslese wurde geändert oder abgebrochen.')
                yield db
        photos.db = guarded_photo_db
        try:
            if source['mime'] == 'text/plain':
                query = article_query(source['caption'])
                lookup = self.p.cockpit_data.articles(query) if query else {'varianten':[]}
                hits = []
                for item in lookup.get('varianten',[])[:8]:
                    if item.get('artikelnummer') and item.get('lieferant'):
                        hit = {key:item.get(key,'') for key in ('produkt_name','artikelnummer','lieferant','groesse','farbe','gebinde','ve')}
                        hit.update(id=_fingerprint(hit)[:32],pruefen=True,bestellbar=False)
                        hits.append(hit)
                result={'status':'pruefen','merkmale':{},'treffer':hits,'treffer_gekuerzt':bool(lookup.get('varianten_gekuerzt') or lookup.get('abdeckung',{}).get('begrenzt'))}
            else:
                result = photos.analyze({'actor':'mitarbeiter:' + str(source['employee_id']),'lesen':True,'einkaufen':True},source['assistant_photo_id'])
            with self.db() as db:
                current = self._draft(db,draft_id,lock=True)
                if current['analysis_lease'] != lease or current['analysis_until'] < self.clock():
                    raise PermissionError('Fotoauslese wurde inzwischen übernommen.')
                state = 'done' if result['status'] == 'pruefen' else 'failed'
                db.execute("UPDATE einkauf_material_dialoge SET analysis_json=?,analysis_state=?,analysis_lease='',analysis_until=0,revision=revision+1 WHERE id=?",(_json(result),state,draft_id))
                self._refresh(db,self._draft(db,draft_id))
            return self.status(draft_id)
        except (PermissionError,ValueError):
            with self.db() as db:
                db.execute("UPDATE einkauf_material_dialoge SET analysis_state='failed',analysis_lease='',analysis_until=0,error_code='fotoauslese_oder_berechtigung_klaeren' WHERE id=? AND analysis_lease=?",(draft_id,lease))
            return self.status(draft_id)

    def apply_admin_review(self, draft_id, revision, payload, actor='admin'):
        if actor != 'admin' or not isinstance(payload,dict) or payload.get('reviewed') is not True:
            raise PermissionError('Werkstattleitung muss Artikel und aktuelle Preisbedingungen ausdrücklich prüfen.')
        keys = {'supplier_id','article_number','product_name','variant','unit','unit_price_cents','shipping_cents','extra_costs_cents','price_source','reviewed','verified_until'}
        if set(payload) != keys:
            raise ValueError('Artikel, Lieferant, Brutto-Einheitspreis, Versand, Nebenkosten und aktuelle Preisquelle vollständig angeben.')
        review = {}
        try:
            review['verified_until'] = datetime.strptime(payload['verified_until'],'%Y-%m-%d').date().isoformat()
        except (ValueError,TypeError):
            raise ValueError('Gültigkeit der geprüften Einkaufskonditionen als Datum angeben.') from None
        if review['verified_until'] < self._today():
            raise ValueError('Die Einkaufskonditionen sind bereits abgelaufen.')
        for key in ('supplier_id','article_number','product_name','variant','unit','price_source'):
            value = payload[key]
            if not isinstance(value,str) or not value.strip() or len(value)>300 or _note(value,300) != value.strip():
                raise ValueError('Geprüfte Artikel- und Preisquelle eindeutig angeben.')
            review[key] = value.strip()
        review['unit'] = UNITS.get(review['unit'].casefold(),review['unit'])
        for key in ('unit_price_cents','shipping_cents','extra_costs_cents'):
            if type(payload[key]) is not int or not 0 <= payload[key] <= 100000000:
                raise ValueError('Bruttopreis und alle Nebenkosten ausdrücklich in Cent bestätigen; nichts schätzen.')
            review[key] = payload[key]
        supplier = self.p.workshop_orders.resolve_supplier(review['supplier_id'])
        if not supplier or supplier.get('verified') is not True:
            raise ValueError('Bestelladresse dieses Lieferanten ist nicht geprüft.')
        review.update(recipient=supplier['recipient'],reviewed_by='admin',reviewed_at=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat())
        with self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            if self._accepted(db,draft) or draft['state']=='cancelled':
                raise ValueError('Bereits übergebenen oder abgebrochenen Vorgang nicht ändern.')
            selected = json.loads(draft['fields_json']).get('selected_article',{}).get('value')
            if selected:
                if selected.get('artikelnummer') != review['article_number'] or selected.get('lieferant','').casefold() != supplier.get('name','').casefold():
                    raise ValueError('Bestätigter Artikel und geprüfter Lieferant passen nicht zusammen.')
                review['match_identity'] = self._hit_identity(selected)
            db.execute('UPDATE einkauf_material_dialoge SET review_json=?,revision=revision+1 WHERE id=?',(_json(review),draft_id))
            self._refresh(db,self._draft(db,draft_id))
            return self._view(db,draft_id)

    def guard_order(self, db, draft_id, revision=None, actor=None, request_key=None, intent=None):
        draft = self._draft(db,draft_id,revision,lock=True)
        source = self._source(db,draft)
        if draft['state'] not in {'approved','accepted'}:
            raise PermissionError('Materialbedarf ist noch nicht vollständig bestätigt.')
        snapshot = json.loads(draft['snapshot_json'])
        if _fingerprint(snapshot) != draft['snapshot_hash'] or snapshot['request_key'] != 'material:' + str(draft_id):
            raise PermissionError('Bestellfreigabe wurde verändert.')
        if actor is not None and actor != snapshot['actor'] or request_key is not None and request_key != snapshot['request_key']:
            raise PermissionError('Bestellfreigabe gehört einem anderen Vorgang.')
        fields = json.loads(draft['fields_json'])
        review = json.loads(draft['review_json'])
        if review.get('verified_until','') < self._today() or review.get('reviewed_by') != 'admin':
            raise PermissionError('Aktuell geprüfte Einkaufskonditionen fehlen.')
        selected = fields.get('selected_article',{}).get('value')
        if selected and review.get('match_identity') != self._hit_identity(selected):
            raise PermissionError('Gewählte Variante und geprüfte Einkaufskonditionen stimmen nicht überein.')
        for key in ('quantity','unit','urgent','order_requested'):
            if snapshot['payload'].get(key) != fields.get(key,{}).get('value'):
                raise PermissionError('Bestellfreigabe passt nicht zu den persönlichen Angaben.')
        for key in ('supplier_id','recipient','article_number','product_name','variant','unit','unit_price_cents','shipping_cents','extra_costs_cents','price_source'):
            if snapshot['payload'].get(key) != review.get(key):
                raise PermissionError('Bestellfreigabe passt nicht zu den geprüften Konditionen.')
        for key in ('order_requested','quantity','unit','urgent') + (('selected_article',) if selected else ()):
            field = fields.get(key,{})
            proof = field.get('proof',{})
            if proof.get('employee_id') != source['employee_id']:
                raise PermissionError('Persönlicher Nachrichtenbeleg fehlt.')
            if proof.get('kind') == 'image':
                if proof.get('id') != source['id'] or source['forwarded']:
                    raise PermissionError('Weiterleitung ist keine Bestellfreigabe.')
            elif proof.get('kind') == 'text':
                evidence = db.execute('SELECT * FROM einkauf_material_texte WHERE id=?',(proof.get('id'),)).fetchone()
                if not evidence or evidence['state']!='applied' or evidence['draft_id']!=draft_id or evidence['forwarded'] or evidence['employee_id']!=source['employee_id']:
                    raise PermissionError('Zugeordneter Nachrichtenbeleg fehlt.')
            else:
                raise PermissionError('Signierter Nachrichtenbeleg fehlt.')
        manager = self.p.workshop_orders
        normalized = manager.dispatch._intent(snapshot['payload'],snapshot['request_key'])
        if intent is not None and _json(normalized) != _json(intent):
            raise PermissionError('Bestellinhalt passt nicht zur unveränderten Freigabe.')
        rights = db.execute('SELECT limit_cent FROM assistent_rechte WHERE mitarbeiter_id=?',(source['employee_id'],)).fetchone()
        if not rights or not 0 < normalized['max_total_cents'] <= min(25000,manager.cap(),rights['limit_cent']):
            raise PermissionError('Persönlicher oder betrieblicher Brutto-Kostenrahmen überschritten.')
        supplier = manager.resolve_supplier(normalized['supplier_id'])
        if not supplier or not supplier.get('verified') or supplier['recipient'] != normalized['recipient']:
            raise PermissionError('Lieferant oder Bestelladresse wurde geändert.')
        return dict(snapshot,draft_id=draft_id,revision=draft['revision'],fingerprint=draft['snapshot_hash'])

    def approved_order(self, draft_id, revision):
        with self.db() as db:
            return self.guard_order(db,draft_id,revision)

    def recheck(self, draft_id, revision):
        """Re-evaluate unchanged evidence; never dispatch or retry uncertain mail."""
        with self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            if self._accepted(db,draft) or draft['state']=='cancelled':
                raise ValueError('Übergebenen oder abgebrochenen Vorgang nicht erneut freigeben.')
            if draft['error_code']=='antwort_unverstaendlich':
                raise ValueError('Die unklare Mitarbeiterantwort zuerst über denselben Vorgang klären.')
            db.execute("UPDATE einkauf_material_dialoge SET revision=revision+1,error_code='' WHERE id=?",(draft_id,))
            self._refresh(db,self._draft(db,draft_id))
            return self._view(db,draft_id)

    def order_attempt(self, draft_id, revision, result):
        if not isinstance(result,dict):
            raise ValueError('Bestellstatus fehlt.')
        with self.db() as db:
            draft = self._draft(db,draft_id,revision)
            order_id = str(result.get('id') or '')
            if not order_id:
                durable = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE request_id=?',('material:'+str(draft_id),)).fetchone()
                order_id = draft['dispatch_id'] or (str(durable['id']) if durable else '')
            if draft['dispatch_id'] and draft['dispatch_id'] != order_id:
                raise ValueError('Bestellreferenz ist bereits fest gespeichert.')
            state = 'accepted' if order_id else 'review'
            db.execute('UPDATE einkauf_material_dialoge SET state=?,dispatch_id=?,dispatch_state=?,error_code=?,updated_at=? WHERE id=?',
                (state,order_id,str(result.get('state','blocked'))[:50],'' if order_id else 'bestelluebergabe_offen',self.clock(),draft_id))

    def send_question(self):
        with self.db() as db:
            db.execute("UPDATE einkauf_material_rueckfragen SET state='uncertain',error_code='sendestatus_unklar_nicht_erneut_senden' WHERE state='sending' AND updated_at<?",(self.clock()-120,))
        if self.p.app.config.get('MATERIAL_WHATSAPP_REPLIES_ENABLED') is not True:
            return None
        with self.db() as db:
            question = db.execute("SELECT * FROM einkauf_material_rueckfragen WHERE state='queued' ORDER BY id LIMIT 1").fetchone()
            if not question:
                return None
            question = dict(question)
            try:
                draft = self._draft(db,question['draft_id'],question['revision'],lock=True)
                source = self._source(db,draft)
                if draft['state'] != 'open':
                    raise ValueError('Vorgang benötigt keine Rückfrage mehr.')
                last = db.execute('SELECT MAX(source_at) AS stamp FROM einkauf_material_texte WHERE sender_id=? AND phone_number_id=?', (source['sender_id'],source['phone_number_id'])).fetchone()
                stamp = max(source['source_at'],last['stamp'] or source['source_at'])
                if not 0 <= self.clock()-datetime.fromisoformat(stamp).timestamp() < 23*3600:
                    raise ValueError('Direktes Antwortfenster ist abgelaufen.')
                sender = db.execute('SELECT phone_e164 FROM einkauf_material_absender WHERE id=?',(source['sender_id'],)).fetchone()
                if db.execute("UPDATE einkauf_material_rueckfragen SET state='sending',updated_at=? WHERE id=? AND state='queued'",(self.clock(),question['id'])).rowcount!=1:
                    return None
            except (PermissionError,ValueError):
                db.execute("UPDATE einkauf_material_rueckfragen SET state='review',error_code='rueckfrage_oder_berechtigung_klaeren' WHERE id=?",(question['id'],))
                return {'id':question['id'],'state':'review'}
        # Commit 'sending' before POST. A crash/timeout is deliberately never retried.
        config = self.channel._config()
        state, provider_id = 'uncertain',''
        try:
            with self.db() as db:
                self._draft(db,draft['id'],draft['revision'],lock=True)
            response = self.channel.transport.post('https://graph.facebook.com/'+config['version']+'/'+source['phone_number_id']+'/messages',
                headers={'Authorization':'Bearer '+config['token']},json={'messaging_product':'whatsapp','recipient_type':'individual',
                'to':sender['phone_e164'],'type':'text','context':{'message_id':source['wamid']},'text':{'preview_url':False,'body':question['body']}},
                timeout=(5,15),allow_redirects=False,stream=True)
            data = json.loads(self.channel._response_bytes(response,64*1024))
            ids = data.get('messages',[]) if isinstance(data,dict) else []
            provider_id = ids[0].get('id') if len(ids)==1 and isinstance(ids[0],dict) else ''
            if _key(provider_id,400) and provider_id.startswith('wamid.'):
                state = 'sent'
        except (requests.RequestException,ValueError,PermissionError,TypeError):
            pass
        with self.db() as db:
            db.execute("UPDATE einkauf_material_rueckfragen SET state=?,provider_id=?,updated_at=?,error_code=? WHERE id=? AND state='sending'",
                (state,provider_id if state=='sent' else '',self.clock(),'' if state=='sent' else 'sendestatus_unklar_nicht_erneut_senden',question['id']))
        return {'id':question['id'],'state':state}

    def process_next(self):
        if not self.channel._config()['enabled']:
            return None
        # A revoked old item must not permanently starve valid later requests.
        for _ in range(5):
            with self.db() as db:
                source = db.execute('''SELECT n.id FROM einkauf_material_nachrichten n LEFT JOIN einkauf_material_dialoge d ON d.message_id=n.id
                    WHERE n.state='ready' AND d.id IS NULL ORDER BY n.id LIMIT 1''').fetchone()
            if not source:
                break
            try:
                self.ensure_draft(source['id'])
                break
            except (PermissionError,ValueError):
                with self.db() as db:
                    db.execute("UPDATE einkauf_material_nachrichten SET state='review',error_code='dialog_berechtigung_klaeren' WHERE id=? AND state='ready' AND NOT EXISTS (SELECT 1 FROM einkauf_material_dialoge WHERE message_id=?)",(source['id'],source['id']))
        draft = None
        for _ in range(5):
            with self.db() as db:
                draft = db.execute("SELECT id FROM einkauf_material_dialoge WHERE state NOT IN ('accepted','cancelled','review') AND (analysis_state='pending' OR (analysis_state='processing' AND analysis_until<?)) ORDER BY id LIMIT 1",(self.clock(),)).fetchone()
            if not draft:
                break
            try:
                self.analyze(draft['id'])
                break
            except (PermissionError,ValueError):
                with self.db() as db:
                    db.execute("UPDATE einkauf_material_dialoge SET state='review',analysis_state='failed',error_code='fotoauslese_oder_berechtigung_klaeren' WHERE id=? AND (analysis_state='pending' OR analysis_until<?)",(draft['id'],self.clock()))
        text = self.process_text()
        with self.db() as db:
            approved = db.execute("SELECT id,revision FROM einkauf_material_dialoge WHERE state='approved' ORDER BY updated_at,id LIMIT 1").fetchone()
        manager = getattr(self.p,'workshop_orders',None)
        if approved and callable(getattr(manager,'submit_material_request',None)):
            try:
                result = manager.submit_material_request(approved['id'],approved['revision'])
                self.order_attempt(approved['id'],approved['revision'],result)
            except (PermissionError,ValueError):
                result = {'state':'blocked'}
                self.order_attempt(approved['id'],approved['revision'],result)
            return {'id':approved['id'],'state':result['state']}
        question = self.send_question()
        return question or text or ({'id':draft['id'],'state':'analyzed'} if draft else None)


def register_material_dialog(p):
    if 'werkstatt_materialdialog' in p.app.extensions:
        return p.app.extensions['werkstatt_materialdialog']
    service = MaterialDialog(p)
    p.material_dialog = service
    p.material_dialog_init_schema = service.init_schema
    p.app.extensions['werkstatt_materialdialog'] = service
    return service
