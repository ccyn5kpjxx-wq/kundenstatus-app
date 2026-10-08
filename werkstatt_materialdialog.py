"""Persistent, evidence-bound material requests; transport and dispatch stay opt-in.

Image labels are proposals. Direct signed employee text supplies purchase
intent/quantity; unstated urgency follows the owner's Monday rule. Independently
reviewed commercial terms supply prices.
No Flask session is fabricated and no model output grants purchase permission.
"""
from contextlib import contextmanager
import copy
from functools import wraps
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import os
import re
import secrets
import time
import unicodedata
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests
from werkstatt_materialkanal import _json, _fingerprint, _note, _row_id, _key, _BorrowedConnection
from werkstatt_materialbestellung import portal_request_details

TABLES = ('einkauf_material_dialoge', 'einkauf_material_texte', 'einkauf_material_rueckfragen')
UNITS = {'karton':'Karton', 'kartons':'Karton', 'rolle':'Rolle', 'rollen':'Rolle', 'packung':'Packung',
         'packungen':'Packung', 'pack':'Packung', 'packs':'Packung', 'gebinde':'Gebinde', 'dose':'Dose', 'dosen':'Dose', 'flasche':'Flasche',
         'flaschen':'Flasche', 'stück':'Stück', 'stueck':'Stück'}
WORDS = {'ein':'1','eine':'1','einen':'1','einem':'1','zwei':'2','drei':'3','vier':'4','fünf':'5','fuenf':'5',
         'sechs':'6','sieben':'7','acht':'8','neun':'9','zehn':'10'}
QUANTITY_PATTERN = r'(?<![\w.,/+−-])(\d{1,8}(?:[.,]\d{1,6})?|' + '|'.join(WORDS) + r')(?:\s*[x×]\s*|\s+)(' + '|'.join(sorted(UNITS, key=len, reverse=True)) + r')\b'
COUNTED_ITEM_PATTERN = r'(?<![\w.,/+−-])([1-9]\d{0,7})\s*[x×]\s*(?P<product>[^\W\d_][\w/-]{2,})(?!\w)'
EMPLOYEE_FIELDS = {'order_requested', 'quantity', 'unit', 'urgent', 'article', 'unit_conflict', 'possible_duplicate'}
INTERNAL_FIELDS = {'supplier_review', 'price', 'budget', 'unit_review', 'parts_inquiry', 'article_correction'}
EXTERNAL_STATES = {'external_pending', 'external_sent'}


def _request_text(text):
    """Normalize a unit typo for parsing, keeping the stored original untouched."""
    return re.sub(r'\bstűck\b','Stück',unicodedata.normalize('NFC',_note(text)),flags=re.I)


def _bare_quantity(text):
    value = _request_text(text).casefold().strip(' .!')
    if not re.fullmatch(r'\d{1,8}(?:[.,]\d{1,6})?',value):
        return None
    amount = Decimal(value.replace(',','.'))
    return format(amount.normalize(),'f') if amount>0 else None


def _photo_piece_count(text):
    value = _request_text(text).casefold().strip()
    match = re.fullmatch(r'([1-9][0-9]{0,7})(?:(?:\s+|\s*[,;]\s*)(?:dringend|sofort|nicht\s+(?:dringend|sofort)|normal|regulär|regulaer))?',value)
    return match[1] if match else None


def _photo_count(text, source, fields):
    """The owner's piece-count rule applies only to personal image messages."""
    count = _photo_piece_count(text)
    if not count or not source['mime'].startswith('image/') or source['forwarded']:
        return {}
    result = {'quantity':count}
    if not fields.get('unit'):
        result['unit'] = 'Stück'
    return result


def _counted_items(text):
    """Only an explicit count before a named item, never a dimension or VE."""
    excluded = set(UNITS) | set(WORDS) | {'mm','cm','m','mtr','meter','millimeter','zentimeter',
        'liter','litre','ml','kg','gramm','ve','inhalt','mal','pro','und','oder','zwölf','zwoelf',
        'dreizehn','vierzehn','fünfzehn','fuenfzehn','zwanzig','dreißig','dreissig','hundert',
        'bitte','bestellen','dringend','sofort','normal','nicht','kein','keine','ja','nein',
        'prüfen','pruefen','kontrollieren','ansehen','anschauen','testen','benutzt','benutzen',
        'verwendet','verwenden','genutzt','nutzen','verbraucht','verbrauchen','gefunden',
        'gezählt','gezaehlt','zählen','zaehlen','fotografieren','gescannt','scannen',
        'geöffnet','geoeffnet','öffnen','oeffnen','reparieren','reinigen','lackieren','polieren','schleifen','zeigen'}
    return [match for match in re.finditer(COUNTED_ITEM_PATTERN,text,flags=re.I)
            if match['product'].casefold() not in excluded
            and not re.search(r'(?:[+−-]|[0-9]\s*[/.,])\s*$',text[:match.start()])]


def article_query(text):
    """Remove request grammar, retaining SKU, colour and dimensional evidence."""
    value = _request_text(text)
    for match in reversed(_counted_items(value)):
        value = value[:match.start()] + value[match.start('product'):]
    value = re.sub(QUANTITY_PATTERN,' ',value,flags=re.I)
    value = re.sub(r'\b(?:bitte|bestellen|bestelle|bestell|nachbestellen|nachbestelle|dringend|sofort|nicht|regulär|regulaer|normal|wöchentlich|woechentlich|ich|möchte|moechte|brauche|benötige|benoetige|mir|wir|uns|erst|am|montag)\b',' ',value,flags=re.I)
    return re.sub(r'\s+',' ',value).strip(' ,;.!:')[:150]


def _named_unit_request(text, parsed):
    """A single nominal material line may start a personal text request."""
    if (not parsed.get('quantity') or not parsed.get('unit') or parsed.get('cancelled')
            or parsed.get('urgent_unclear') or not re.fullmatch(r'[1-9][0-9]{0,7}',parsed['quantity'])):
        return False
    value = _request_text(text).casefold().strip()
    if len(value)>160 or any(char in value for char in '?\n\r;'):
        return False
    # These describe stock, source material, work or conversation, not a
    # requested purchase count. Check the entire message before trimming.
    forbidden = (r'\b(?:ve|inhalt|packungsinhalt|enthält|enthaelt|etikett|aufdruck|katalog|rechnung|beleg|'
        r'artikelnummer|teilenummer|sku|art[.-]?nr|bestand|lager|vorhanden|übrig|uebrig|geliefert|erhalten|'
        r'angekommen|gekauft|benutzt|benutzen|benutze|verwendet|verwenden|verwende|genutzt|nutzen|nutze|'
        r'verbraucht|verbrauchen|verbrauche|prüfen|pruefen|prüfe|pruefe|kontrollieren|kontrolliere|'
        r'ansehen|anschauen|testen|teste|gefunden|gezählt|gezaehlt|zählen|zaehlen|'
        r'auffüllen|auffuellen|auffülle|auffuelle|füllen|fuellen|tragen|trage|anziehen|anziehe|'
        r'reparieren|repariere|reinigen|reinige|putzen|putze|lackieren|lackiere|polieren|poliere|'
        r'schleifen|schleife|fotografieren|fotografiere|scannen|scan|gescannt|öffnen|oeffnen|öffne|oeffne|'
        r'verteilen|verteile|holen|hole|bringen|bringe|nehmen|nehme|zeigen|zeige|entsorgen|entsorge|'
        r'und|oder|auch|kein|keine|keinen|keines|keinesfalls|vielleicht|eventuell|falls|wenn|beispiel|'
        r'angenommen|habe|haben|hat|ist|sind|war|wurde|wird|soll|sollen|muss|müssen|muessen|'
        r'gestern|bereits|schon|danke|hallo)\b')
    if re.search(forbidden,value):
        return False
    # Only these simple request modifiers can surround the nominal line.
    value = re.sub(r'\b(?:nicht\s+(?:dringend|sofort)|erst\s+(?:am\s+)?montag|bitte|dringend|sofort|regulär|regulaer|normal)\b',' ',value)
    if re.search(r'\b(?:nicht|ich|du|wir|ihr|mir|uns)\b',value):
        return False
    matches = list(re.finditer(QUANTITY_PATTERN,value,re.I))
    if len(matches)!=1:
        return False
    match = matches[0]
    amount = Decimal(WORDS.get(match[1],match[1]).replace(',','.'))
    if amount!=Decimal(parsed['quantity']) or UNITS[match[2]]!=parsed['unit']:
        return False
    before,after = (part.strip(' ,.!:') for part in (value[:match.start()],value[match.end():]))
    if bool(before)==bool(after):
        return False
    name = before or after
    return (len(name.split())<=6 and bool(re.fullmatch(r'[\wÄÖÜäöüß /.,-]+',name))
            and bool(re.search(r'(?<!\w)[^\W\d_]{3,}(?!\w)',name)))


def parse_request(text, question=''):
    """Small conservative grammar, never extracts quantity from a product label."""
    text = _request_text(text)
    value = text.casefold()
    fields = {}
    if re.search(r'\b(abbrechen|stornieren)\b|\bnicht(?:\s+mehr)?\s+bestell\w*|\bkeine?\s+bestellung\b|\bbestell\w*\s+(?:bitte\s+)?nicht(?:\s+mehr)?\s*[.!]?$', value):
        return {'cancelled': True}
    conditional = bool(re.search(r'\b(vielleicht|eventuell|falls|wenn|beispiel|angenommen)\b', value))
    if conditional or '?' in value:
        return fields
    if (re.search(r'\b(?:vorhanden|übrig|uebrig|bestand|geliefert|erhalten|angekommen|gekauft|benutzt|benutzen|verwendet|verwenden|genutzt|nutzen|verbraucht|verbrauchen|prüfen|pruefen|kontrollieren|ansehen|anschauen|testen|gefunden|gezählt|gezaehlt)\b|\bauf\s+lager\b',value)
            and not re.search(r'\b(?:bestellen|bestelle|bestell|nachbestellen|nachbestelle)\b',value)):
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
        if re.search(r'(?:\bve|inhalt|packungsinhalt|enthält|enthaelt)\s*[:=]?\s*$|\b(?:nicht|kein|keine)\s*$|(?:[+−-]|[0-9]\s*[/.,])\s*$', prefix):
            continue
        amount = Decimal(WORDS.get(match[1], match[1]).replace(',', '.'))
        if amount > 0:
            matches.append((format(amount.normalize(), 'f'), UNITS[match[2]]))
    counted = _counted_items(value)
    if len(counted)==1 and not matches:
        match = counted[0]
        prefix = value[max(0,match.start()-35):match.start()]
        if not re.search(r'(?:\bve|inhalt|packungsinhalt|enthält|enthaelt)\s*[:=]?\s*$|\b(?:nicht|kein|keine)\s*$',prefix):
            matches.append((str(int(match[1])),'Stück'))
    elif counted:
        # Multiple item counts or a competing explicit unit need clarification.
        matches = []
    if len(set(matches)) == 1:
        fields['quantity'], fields['unit'] = matches[0]
    elif question=='quantity' and _bare_quantity(text):
        fields['quantity'] = _bare_quantity(text)
    elif question=='unit' and value.strip(' .!') in UNITS:
        fields['unit'] = UNITS[value.strip(' .!')]
    return fields


@contextmanager
def _originals_operation(portal):
    if getattr(portal, 'material_order_portal', None) is not None:
        with portal.portal_originals_operation_lock():
            yield
    else:
        yield


def _originals_guard(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        with _originals_operation(self.p):
            return method(self, *args, **kwargs)
    return guarded


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
        if draft['state'] in EXTERNAL_STATES or draft['dispatch_id']:
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
            if key in {'quantity','unit','urgent','selected_article'} and previous and previous['value']!=value:
                fields.pop('duplicate_confirmation',None)
            if key == 'urgent_unclear':
                previous = fields.get('urgent')
                if not previous or previous['proof']['source_at'] <= proof['source_at']:
                    fields.pop('urgent',None)
            elif key == 'urgent':
                fields.pop('urgent_unclear',None)
            fields[key] = {'value': value, 'proof': proof}
        # Explicit employee quantity in the dedicated material-order channel is
        # the purchase request. The owner's default is the Monday collection;
        # keep that policy distinguishable from a literally stated urgency.
        if not fields.get('cancelled',{}).get('value'):
            quantity = fields.get('quantity')
            if quantity and fields.get('unit') and 'order_requested' not in fields:
                fields['order_requested'] = {'value':True,'proof':dict(quantity['proof'],
                    basis='quantity_in_personal_material_channel')}
            if fields.get('order_requested',{}).get('value') is True and not fields.get('urgent_unclear') and 'urgent' not in fields:
                fields['urgent'] = {'value':False,'proof':dict(fields['order_requested']['proof'],
                    basis='owner_default_monday_14')}

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
            details = portal_request_details(source)
            parsed = ({'quantity':str(details['menge']), 'unit':'Stück', 'urgent':details['dringend'],
                       'order_requested':details['vorgang']=='bestellung', 'vorgang':details['vorgang'],
                       'beschreibung':details['beschreibung']} if details else parse_request(source['caption']))
            if parsed.get('urgent') is True and not parsed.get('cancelled'):
                if not details or details['vorgang']=='bestellung':
                    parsed['order_requested'] = True
            proof = self._proof(text,'text',text['id']) if text else self._proof(source)
            self._merge(fields,parsed,proof)
            if not details:
                self._merge(fields,_photo_count(source['caption'],source,fields),dict(proof,basis='owner_photo_piece_count'))
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

    @staticmethod
    def _exact_photo_hit(draft, analysis):
        """Select only an unambiguous exact label match, never its price."""
        labels,hits = analysis.get('merkmale',{}),analysis.get('treffer',[])
        if analysis.get('code_widerspruch') or analysis.get('code_erkennung',{}).get('status') == 'mehrdeutig':
            return None
        if draft['analysis_state']!='done' or len(hits)!=1 or analysis.get('treffer_gekuerzt'):
            return None
        hit = hits[0]
        compact = lambda value: re.sub(r'\s+','',str(value or '').casefold()).replace(',','.')
        product,code = compact(labels.get('produkt')),compact(labels.get('artikelnummer'))
        exact_code = bool(code and code==compact(hit.get('artikelnummer')))
        if code and hit.get('artikelnummer') and not exact_code:
            return None
        dimensions = [(compact(labels.get(left)),compact(hit.get(right))) for left,right in (('breite','groesse'),('farbe','farbe'))]
        if any(label and actual and label!=actual for label,actual in dimensions):
            return None
        exact_name_variant = (len(product)>=6 and product==compact(hit.get('produkt_name'))
            and product!=compact(labels.get('marke'))
            and any(label for label,actual in dimensions) and all(not label or label==actual for label,actual in dimensions))
        if hit.get('artikelnummer') and hit.get('lieferant') and (exact_code or exact_name_variant):
            return hit
        return None

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

    def _duplicate_signature(self, fields, review, draft=None):
        if fields.get('vorgang',{}).get('value')=='anfrage':
            return None
        values = {key:fields.get(key,{}).get('value') for key in ('quantity','unit','urgent')}
        if not values['quantity'] or not values['unit'] or type(values['urgent']) is not bool:
            return None
        item = fields.get('selected_article',{}).get('value',{})
        supplier,article = item.get('lieferant'),item.get('artikelnummer')
        variant = {key:' '.join(str(item.get(key,'')).casefold().split()) for key in ('groesse','farbe','gebinde','ve')} if item else {
            'variant':' '.join(str(review.get('variant','')).casefold().split())}
        if not article and review.get('article_number'):
            contact = self.p.workshop_orders.resolve_supplier(review.get('supplier_id'))
            supplier,article = (contact or {}).get('name'),review['article_number']
        if draft and draft['state'] in EXTERNAL_STATES:
            claim = json.loads(draft['snapshot_json'])
            if _fingerprint(claim)==draft['snapshot_hash'] and claim.get('draft_id')==draft['id']:
                supplier,article = claim.get('supplier_name'),claim.get('article_number')
                if not item:
                    variant = {'variant':' '.join(str(claim.get('variant','')).casefold().split())}
        if not supplier or not article:
            return None
        return dict(values,supplier=str(supplier).casefold().strip(),article=str(article).casefold().strip(),variant=variant)

    def _duplicate_of(self, db, draft, source, signature):
        if not signature:
            return None
        rows = db.execute('''SELECT d.* FROM einkauf_material_dialoge d
            JOIN einkauf_material_nachrichten n ON n.id=d.message_id
            WHERE n.employee_id=? AND d.id<>? AND d.state<>'cancelled'
            AND d.created_at>=? AND d.created_at<=?
            AND (d.id<? OR d.state IN ('accepted','external_pending','external_sent') OR d.dispatch_id<>''
                OR EXISTS (SELECT 1 FROM assistent_bestellanforderungen o WHERE o.request_id=('material:' || CAST(d.id AS TEXT))))
            ORDER BY d.id''',(source['employee_id'],draft['id'],draft['created_at']-600,draft['created_at']+600,draft['id'])).fetchall()
        for row in rows:
            original = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?',(row['message_id'],)).fetchone()
            details = portal_request_details(original) if original else None
            if details and details['vorgang']=='anfrage':
                continue
            other = self._duplicate_signature(json.loads(row['fields_json']),json.loads(row['review_json']),row)
            if not other or any(other[key]!=signature[key] for key in ('supplier','article','quantity','unit')):
                continue
            # Missing structured variant data on an older manual order cannot
            # exempt the same SKU from review. Only evidenced differences split it.
            if any(signature['variant'].get(key) and other['variant'].get(key)
                    and signature['variant'][key]!=other['variant'][key] for key in ('groesse','farbe','gebinde','ve')):
                continue
            return {'id':row['id'],'signature':_fingerprint(signature)}
        return None

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
        source = self._source(db,draft)
        photo_source = source['mime'].startswith('image/')
        details = portal_request_details(source)
        if details:
            proof = self._proof(source)
            for key in ('vorgang','beschreibung'):
                fields[key] = {'value':details[key], 'proof':proof}
            if details['vorgang']=='anfrage':
                # The immutable source mode always wins over answers, matching
                # articles, reused prices and manually changed dialog fields.
                fields['order_requested'] = {'value':False, 'proof':proof}
                fields.pop('possible_duplicate',None)
                fields.pop('duplicate_confirmation',None)
                cancelled = fields.get('cancelled',{}).get('value') is True
                state = ('cancelled' if cancelled else 'open' if draft['analysis_state'] in {'pending','processing'} else 'review')
                missing = [] if cancelled else ['parts_inquiry']
                snapshot = {'actor':'mitarbeiter:'+str(source['employee_id']), 'request_key':'material:'+str(draft['id']), 'payload':{}}
                db.execute('''UPDATE einkauf_material_dialoge SET fields_json=?,state=?,snapshot_json=?,snapshot_hash=?,missing_json=?,updated_at=? WHERE id=?''',
                    (_json(fields),state,_json(snapshot),_fingerprint(snapshot),_json(missing),self.clock(),draft['id']))
                db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded' WHERE draft_id=? AND state='queued'",(draft['id'],))
                return
        analysis = json.loads(draft['analysis_json'])
        manual = fields.get('manual_article',{}).get('value')
        correction = bool(details and details.get('artikelkorrektur') is True)
        if manual or correction:
            # A later employee reply or reanalysis must not silently replace an
            # admin identity or reuse another article's commercial approval.
            changed = fields.pop('selected_article',None) is not None
            if review and (not manual or manual['product_name'] != review.get('product_name')
                    or manual['article_number'] and manual['article_number'] != review.get('article_number')
                    or manual['variant'] and manual['variant'] != review.get('variant')):
                review = {}
                db.execute("UPDATE einkauf_material_dialoge SET review_json='{}' WHERE id=?",(draft['id'],))
            if changed:
                db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(_json(fields),draft['id']))
        selected_field = fields.get('selected_article',{})
        if selected_field.get('proof',{}).get('basis')=='exact_photo_catalog_match':
            current_hit = self._exact_photo_hit(draft,analysis)
            if not current_hit or self._hit_identity(current_hit)!=self._hit_identity(selected_field['value']):
                fields.pop('selected_article',None)
                review = {}
                db.execute("UPDATE einkauf_material_dialoge SET fields_json=?,review_json='{}' WHERE id=?",(_json(fields),draft['id']))
        if not correction and not fields.get('manual_article') and not fields.get('selected_article') and not fields.get('cancelled',{}).get('value'):
            hit = self._exact_photo_hit(draft,analysis)
            source = self._source(db,draft)
            if hit and source['mime']!='text/plain' and not source['forwarded']:
                fields['selected_article'] = {'value':hit,'proof':dict(self._proof(source),basis='exact_photo_catalog_match')}
                db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(_json(fields),draft['id']))
        selected = fields.get('selected_article',{}).get('value')
        if selected and review and review.get('match_identity') != self._hit_identity(selected):
            review = {}
            db.execute("UPDATE einkauf_material_dialoge SET review_json='{}' WHERE id=?",(draft['id'],))
        if not correction and not manual and not review and fields.get('selected_article'):
            review = self._reuse_review(db,fields['selected_article']['value'])
            if review:
                db.execute('UPDATE einkauf_material_dialoge SET review_json=? WHERE id=?',(_json(review),draft['id']))
        values = {key:item['value'] for key,item in fields.items()}
        signature = self._duplicate_signature(fields,review,draft)
        duplicate = values.get('possible_duplicate')
        if not duplicate or duplicate.get('signature')!=_fingerprint(signature):
            fields.pop('duplicate_confirmation',None)
            duplicate = self._duplicate_of(db,draft,self._source(db,draft),signature)
            if duplicate:
                fields['possible_duplicate'] = {'value':duplicate,'proof':{'kind':'system','basis':'same_employee_article_quantity_within_10_minutes'}}
            else:
                fields.pop('possible_duplicate',None)
            db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(_json(fields),draft['id']))
        labels, hits = analysis.get('merkmale',{}), analysis.get('treffer',[])
        # Recognizing a label is not a catalog match or a price approval. It only
        # means the employee need not repeat an already readable product name.
        # A single conflict-filtered catalog candidate plus printed dimensions
        # gives the workshop enough evidence to review internally. It is not a
        # SKU selection and cannot reuse commercial terms or authorize dispatch.
        catalog_review_hint = (draft['analysis_state']=='done' and len(hits)==1
            and not analysis.get('treffer_gekuerzt')
            and all(labels.get(key) for key in ('marke','materialtyp','masse'))
            and all(hits[0].get(key) for key in ('artikelnummer','lieferant','produkt_name')))
        identified = bool(correction or fields.get('manual_article') or selected or catalog_review_hint or (draft['analysis_state']=='done' and labels.get('produkt')
            and str(labels.get('produkt')).casefold().strip()!=str(labels.get('marke','')).casefold().strip()
            and (labels.get('marke') or labels.get('artikelnummer') or labels.get('breite') or labels.get('farbe'))
            and (not hits or len(hits)==1 and not analysis.get('treffer_gekuerzt'))))
        missing = []
        if values.get('cancelled') is True:
            state, payload = 'cancelled', {}
        else:
            if correction and not manual:
                missing.append('article_correction')
            for key in ('order_requested','quantity','unit','urgent'):
                if key not in values or key == 'order_requested' and values[key] is not True:
                    missing.append(key)
            if not review:
                missing += ['supplier_review' if identified else 'article','price']
            elif review.get('verified_until','') < self._today():
                missing.append('price')
            elif values.get('unit') and values['unit'].casefold() != review['unit'].casefold():
                missing.append('unit_review' if photo_source and values['unit']=='Stück' else 'unit_conflict')
            if duplicate and fields.get('duplicate_confirmation',{}).get('value')!=duplicate:
                missing.append('possible_duplicate')
            payload = {}
            if not missing or missing==['possible_duplicate']:
                payload = {key:review[key] for key in ('supplier_id','recipient','article_number','product_name','variant','unit',
                    'unit_price_cents','shipping_cents','extra_costs_cents','price_source')}
                total = int((Decimal(values['quantity']) * review['unit_price_cents']).to_integral_value(rounding=ROUND_CEILING)) + review['shipping_cents'] + review['extra_costs_cents']
                payload.update(order_requested=True,quantity=values['quantity'],urgent=values['urgent'],max_total_cents=total,
                               price_verified=True,price_basis='gross',currency='EUR')
                if not 0 < total <= 25000:
                    missing.append('budget')
            staff_missing = [key for key in missing if key in EMPLOYEE_FIELDS]
            state = ('review' if not staff_missing and draft['analysis_state'] not in {'pending','processing'} else 'open') if missing else 'approved'
        snapshot = {'actor':'mitarbeiter:' + str(self._source(db,draft)['employee_id']), 'request_key':'material:' + str(draft['id']), 'payload':payload}
        db.execute('''UPDATE einkauf_material_dialoge SET state=?,snapshot_json=?,snapshot_hash=?,missing_json=?,updated_at=? WHERE id=?''',
            (state,_json(snapshot),_fingerprint(snapshot),_json(missing),self.clock(),draft['id']))
        db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded' WHERE draft_id=? AND revision<>? AND state='queued'", (draft['id'],draft['revision']))
        if missing and state in {'open','review'}:
            first = ('quantity' if photo_source and 'quantity' in missing else
                     next((key for key in missing if key in EMPLOYEE_FIELDS),'internal_review'))
            if first in {'article','internal_review'} and draft['analysis_state'] in {'pending','processing'}:
                return
            questions = {'order_requested':'Soll dieses Material bestellt werden? Bitte ausdrücklich „bestellen“ schreiben.',
                'quantity':'Wie viel möchtest du bestellen? Bitte Menge und Bestelleinheit nennen, etwa „ein Karton“.',
                'unit':'Welche Bestelleinheit meinst du: Karton, Rolle oder Packung?', 'urgent':'Ist die Bestellung dringend?',
                'article':'Der Artikel ist noch nicht eindeutig lesbar. Bitte Artikelnamen und Variante vom Etikett nennen.',
                'unit_conflict':'Genannte Bestelleinheit und geprüfter Artikel passen noch nicht zusammen. Bitte klären.',
                'possible_duplicate':('Gleicher Artikel und gleiche Menge wurden bereits in M-'+str((duplicate or {}).get('id',''))+
                    ' angefordert. Möchtest du zusätzlich bestellen? Bitte mit Ja oder Nein antworten. Noch nicht erneut bestellt.'),
                'internal_review':'Die interne Lieferanten- und Preisprüfung ist noch offen. Die Werkstattleitung prüft die Zuordnung und vollständigen aktuellen Kosten. Noch nicht bestellt.'}
            if photo_source:
                unit_name = {'Karton':'Kartons','Rolle':'Rollen','Packung':'Packungen','Dose':'Dosen','Flasche':'Flaschen'}.get(values.get('unit'),values.get('unit') or 'Stück')
                questions['quantity']='Wie viele '+unit_name+' brauchst du? Eine Zahl genügt.'
            if 'unit_review' in missing:
                questions['internal_review']='Die Stückzahl ist erfasst. Die Werkstattleitung prüft das passende Gebinde und die aktuellen Kosten. Noch nicht bestellt.'
            elif 'budget' in missing:
                questions['internal_review']='Der Gesamtbetrag liegt außerhalb des freigegebenen Rahmens von 250 Euro. Die Werkstattleitung muss übernehmen. Noch nicht bestellt.'
            elif review and 'price' in missing:
                questions['internal_review']='Die bisherigen Einkaufskonditionen sind abgelaufen. Die Werkstattleitung prüft die aktuellen vollständigen Kosten. Noch nicht bestellt.'
            if first == 'unit_conflict':
                questions[first]='Du hast '+str(values['unit'])+' angegeben; die geprüften Konditionen gelten je '+review['unit']+'. Welche Bestelleinheit meinst du?'
            if first == 'article':
                if len(hits)==1:
                    hit=hits[0]
                    questions['article']='Meinst du '+str(hit.get('produkt_name','Artikel'))+' '+str(hit.get('groesse',''))+' '+str(hit.get('farbe',''))+'?'
                elif hits:
                    questions['article']='Welche Variante: '+', '.join(str(h.get('groesse') or h.get('produkt_name')) for h in hits[:5])+'?'
            details = []
            product = selected or review
            if product:
                name = ' '.join(str(product.get(key,'')) for key in ('produkt_name','product_name','groesse','farbe','variant')).strip()
                if name:
                    details.append('Artikel: '+name+'.')
            elif any(labels.get(key) for key in ('produkt','marke','materialtyp','masse','breite','farbe')):
                parts = []
                for key in ('marke','produkt','materialtyp','masse','breite','farbe'):
                    label = str(labels.get(key,'')).strip()
                    if label and label.casefold() not in ' '.join(parts).casefold():
                        parts.append(label)
                details.append('Auf dem Foto erkannt: '+' '.join(parts)+'.')
            known = []
            if values.get('quantity'):
                known.append(str(values['quantity'])+' '+str(values.get('unit','')).strip())
            if type(values.get('urgent')) is bool:
                known.append('dringend' if values['urgent'] else 'nicht dringend')
            if known:
                details.append('Erfasst: '+', '.join(known)+'.')
            body = 'M-' + str(draft['id']) + ' R' + str(draft['revision']) + ': ' + ' '.join(details+[questions[first]])
            if first != 'internal_review':
                body += ' Antworte bitte zitiert oder mit diesem Vorgangscode.'
            else:
                last = db.execute("""SELECT body FROM einkauf_material_rueckfragen
                    WHERE draft_id=? AND field='internal_review' AND state IN ('queued','sending','sent','uncertain')
                    ORDER BY id DESC LIMIT 1""",(draft['id'],)).fetchone()
                # Admin rechecking unchanged evidence must not repeat a status
                # already delivered or with an uncertain/in-flight outcome.
                strip_code = lambda text: re.sub(r'^M-\d+\s+R\d+:\s*','',text)
                if last and strip_code(last['body']) == strip_code(body):
                    return
            db.execute('''INSERT INTO einkauf_material_rueckfragen(draft_id,revision,field,body,created_at,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(draft_id,revision) DO NOTHING RETURNING id''',
                (draft['id'],draft['revision'],first,body,self.clock(),self.clock())).fetchall()

    def _view(self, db, draft_id):
        row = self._draft(db,draft_id)
        source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?',(row['message_id'],)).fetchone()
        details = portal_request_details(source) if source else None
        row.update(employee_id=source['employee_id'] if source else None, employee_name=source['employee_name'] if source else '',
                   intake_id=source['intake_id'] if source else None, file_id=source['file_id'] if source else None,
                   source_kind='text' if source and source['mime']=='text/plain' else 'image',
                   article_correction=bool(details and details.get('artikelkorrektur') is True),
                   vorgang=details['vorgang'] if details else 'bestellung', beschreibung=details['beschreibung'] if details else '')
        for source,target in (('fields_json','fields'),('review_json','review'),('analysis_json','analysis'),('missing_json','missing_fields')):
            row[target] = json.loads(row.pop(source))
        snapshot = json.loads(row.pop('snapshot_json'))
        fingerprint = row.pop('snapshot_hash')
        row['external_order'] = snapshot if (row['state'] in EXTERNAL_STATES
            and snapshot.get('kind') == 'manual_external_order' and _fingerprint(snapshot) == fingerprint) else {}
        row.pop('analysis_lease')
        row['code'] = 'M-' + str(row['id']) + ' R' + str(row['revision'])
        row['questions'] = [dict(q) for q in db.execute('SELECT id,revision,field,body,state,error_code FROM einkauf_material_rueckfragen WHERE draft_id=? ORDER BY id DESC LIMIT 8',(draft_id,)).fetchall()]
        row['employee_reply_required'] = row['state']=='open' and any(key in EMPLOYEE_FIELDS for key in row['missing_fields'])
        row['internal_review_pending'] = row['state']=='review' and not row['error_code'] and any(key in INTERNAL_FIELDS for key in row['missing_fields'])
        row['duplicate_of'] = row['fields'].get('possible_duplicate',{}).get('value',{}).get('id')
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

    @_originals_guard
    def process_text(self, text_id=None):
        if text_id is not None:
            _row_id(text_id)
        with self.db() as db:
            text = db.execute("SELECT * FROM einkauf_material_texte WHERE state='queued' AND id=?", (text_id,)).fetchone() if text_id else db.execute("SELECT * FROM einkauf_material_texte WHERE state='queued' AND phone_number_id<>'portal:personal' ORDER BY id LIMIT 1").fetchone()
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
                    short_request = not article_query(text['body']) and (
                        parsed.get('order_requested') is True or parsed.get('quantity') and parsed.get('unit'))
                    bare_quantity = _bare_quantity(text['body'])
                    photo_count = _photo_piece_count(text['body'])
                    bare_unit = parse_request(text['body'],question='unit').get('unit') if not parsed else None
                    # Short intent/quantity can answer one recent personal photo;
                    # never pick the newest of several possible requests.
                    origins = []
                    if short_request or bare_quantity or photo_count or bare_unit:
                        origins = db.execute('''SELECT d.id,n.state FROM einkauf_material_nachrichten n
                            LEFT JOIN einkauf_material_dialoge d ON d.message_id=n.id
                            WHERE (d.id IS NULL OR d.state NOT IN ('accepted','cancelled','external_pending','external_sent'))
                            AND n.forwarded=0 AND n.mime<>'text/plain' AND n.sender_id=? AND n.sender_revision=?
                            AND n.employee_id=? AND n.rights_version=? AND n.phone_number_id=?
                            AND n.received_at>=? AND n.source_at>=? AND n.source_at<=?
                            AND NOT EXISTS (SELECT 1 FROM assistent_bestellanforderungen o WHERE o.request_id=('material:' || CAST(d.id AS TEXT)))
                            ORDER BY n.id LIMIT 2''',
                            (text['sender_id'],text['sender_revision'],text['employee_id'],text['rights_version'],
                             text['phone_number_id'],self.clock()-15*60,
                             datetime.fromtimestamp(self.clock()-15*60,timezone.utc).isoformat(),text['source_at'])).fetchall()
                        if len(origins)!=1:
                            raise ValueError('Antwort gehört nicht zu genau einem aktuellen Foto. Bitte das konkrete Foto zitieren.')
                        if not origins[0]['id'] or origins[0]['state']!='ready':
                            if origins[0]['state'] not in {'queued','processing','ready'}:
                                raise ValueError('Fotoeingang muss intern geklärt werden; Menge noch nicht übernehmen.')
                            db.execute("UPDATE einkauf_material_texte SET state='queued' WHERE id=?",(text['id'],))
                            return {'id':text['id'],'state':'waiting_for_photo'}
                    if origins:
                        draft = self._draft(db,origins[0]['id'],lock=True)
                        question = db.execute('SELECT * FROM einkauf_material_rueckfragen WHERE draft_id=? AND revision=?',
                                              (draft['id'],draft['revision'])).fetchone()
                        if bare_quantity and not photo_count and (not question or question['field']!='quantity'
                                or 'quantity' not in json.loads(draft['missing_json'])):
                            raise ValueError('Reine Zahl gehört noch zu keiner eindeutigen Mengenfrage. Bitte Menge und Einheit nennen.')
                        if bare_unit and (not question or question['field']!='unit'
                                or 'unit' not in json.loads(draft['missing_json'])):
                            raise ValueError('Bestelleinheit gehört noch zu keiner eindeutigen Einheitenfrage.')
                    else:
                        named_request = _named_unit_request(text['body'],parsed)
                        implicit_request = named_request if parsed.get('quantity') and parsed.get('unit') else parsed.get('urgent') is True
                        if (not parsed.get('order_requested') and not implicit_request) or not article_query(text['body']):
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
                fields = json.loads(draft['fields_json'])
                photo_count = _photo_count(body,source,fields)
                numeric_photo_reply = source['mime'].startswith('image/') and (photo_count or _bare_quantity(body)
                    or re.fullmatch(r'[+−-]?\s*[0-9][0-9.,]*(?:[eE][+−-]?[0-9]+)?',body))
                if numeric_photo_reply and not photo_count:
                    parsed.pop('quantity',None)
                if question and question['field']=='possible_duplicate' and body.casefold().strip(' .!') in {'ja','nein'}:
                    marker = json.loads(draft['fields_json']).get('possible_duplicate',{}).get('value')
                    if marker:
                        parsed = {'duplicate_confirmation':marker} if body.casefold().strip(' .!')=='ja' else {'cancelled':True}
                if parsed.get('urgent') is True and not parsed.get('cancelled'):
                    parsed['order_requested'] = True
                analysis = json.loads(draft['analysis_json'])
                hits = analysis.get('treffer',[])
                hit = None
                existing_article = json.loads(draft['fields_json']).get('selected_article',{})
                label_acknowledgement = (existing_article.get('proof',{}).get('basis')=='exact_photo_catalog_match'
                    and len(hits)==1 and self._hit_identity(existing_article.get('value',{}))==self._hit_identity(hits[0]))
                if numeric_photo_reply:
                    pass  # A piece count is never a numeric SKU/variant selection.
                elif ((question and question['field']=='article') or label_acknowledgement) and body.casefold().strip(' .!')=='ja' and len(hits)==1 and not analysis.get('treffer_gekuerzt'):
                    hit = hits[0]
                else:
                    label = re.sub(r'\s+','',body.casefold().strip(' .!'))
                    matching = [h for h in hits if label and label in {re.sub(r'\s+','',str(h.get(k,'')).casefold()) for k in ('groesse','artikelnummer','produkt_name')}]
                    if len(matching)==1 and not analysis.get('treffer_gekuerzt'):
                        hit = matching[0]
                if hit:
                    parsed['selected_article'] = hit
                if not parsed and not photo_count:
                    # A bound but unclear correction must stop an earlier approval.
                    db.execute("UPDATE einkauf_material_dialoge SET state='review',error_code='antwort_unverstaendlich',updated_at=? WHERE id=?",(self.clock(),draft['id']))
                    raise ValueError('Antwort ist nicht eindeutig. Bitte konkrete Menge/Einheit oder Dringlichkeit nennen.')
                proof = self._proof(text,'text',text['id'])
                self._merge(fields,parsed,proof)
                self._merge(fields,photo_count,dict(proof,basis='owner_photo_piece_count'))
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

    def reanalyze_photo(self, draft_id, revision):
        """Explicit revision-bound reread; discard stale selections, never send."""
        _row_id(revision)
        return self.analyze(draft_id,refresh=True,revision=revision)

    def analyze(self, draft_id, *, refresh=False, revision=None):
        if type(refresh) is not bool or refresh and revision is None:
            raise ValueError('Erneute Fotoauslese benötigt die aktuelle Revision.')
        lease = secrets.token_hex(16)
        unchanged = ('revision','fields_json','review_json','analysis_json','snapshot_json','snapshot_hash','state','error_code','dispatch_id')
        # Keep restore/dispatch atomic with each DB phase, never with vision.
        with _originals_operation(self.p), self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            source = self._source(db,draft)
            if self._accepted(db,draft) or draft['state']=='cancelled':
                if refresh:
                    raise ValueError('Übergebenes oder abgebrochenes Foto nicht erneut auslesen.')
                return self._view(db,draft_id)
            if refresh and (source['mime']=='text/plain' or source['forwarded'] or draft['analysis_until']>self.clock()):
                raise ValueError('Nur ein persönliches Foto ohne laufende Auslese erneut prüfen.')
            if refresh and (draft['error_code']=='antwort_unverstaendlich' or db.execute(
                    "SELECT id FROM einkauf_material_rueckfragen WHERE draft_id=? AND state='sending'",(draft_id,)).fetchone()):
                raise ValueError('Offene unklare Antwort oder laufenden Nachrichtenversand zuerst klären.')
            if not refresh and (draft['analysis_state'] == 'done' or draft['analysis_until'] > self.clock()):
                return self._view(db,draft_id)
            if refresh:
                fields = json.loads(draft['fields_json'])
                fields.pop('selected_article',None)
                fields.pop('duplicate_confirmation',None)
                db.execute("""UPDATE einkauf_material_dialoge SET fields_json=?,review_json='{}',analysis_json='{}',
                    snapshot_json='{}',snapshot_hash='',state='open',error_code='',revision=revision+1 WHERE id=?""",(_json(fields),draft_id))
                db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded' WHERE draft_id=? AND state='queued'",(draft_id,))
            db.execute("UPDATE einkauf_material_dialoge SET analysis_state='processing',analysis_lease=?,analysis_until=? WHERE id=?",(lease,self.clock()+120,draft_id))
            expected = self._draft(db,draft_id)
        photos = copy.copy(self.p.assistant_material_photos)
        source_keys = ('id','canonical_hash','expected_sha256','assistant_photo_id','employee_id',
                       'rights_version','intake_id','file_id','mime','caption','forwarded')
        def current_analysis(db):
            # _draft(lock=True) revalidates/locks current employee rights via _source.
            current = self._draft(db,draft_id,lock=True)
            latest_source = self._source(db,current)
            if (self._accepted(db,current) or current['analysis_lease'] != lease
                    or current['analysis_until'] < self.clock() or current['state'] == 'cancelled'
                    or any(latest_source[key]!=source[key] for key in source_keys)):
                raise PermissionError('Fotoauslese wurde geändert oder abgebrochen.')
            if refresh and any(current[key]!=expected[key] for key in unchanged):
                raise PermissionError('Materialvorgang wurde während der erneuten Auslese geändert.')
            return current
        @contextmanager
        def guarded_photo_db():
            with _originals_operation(self.p), self.db() as db:
                current_analysis(db)
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
                result = photos.analyze({'actor':'mitarbeiter:' + str(source['employee_id']),'lesen':True,'einkaufen':True},
                                        source['assistant_photo_id'],**({'refresh':True} if refresh else {}))
            with _originals_operation(self.p), self.db() as db:
                current_analysis(db)
                state = 'done' if result['status'] == 'pruefen' else 'failed'
                db.execute("UPDATE einkauf_material_dialoge SET analysis_json=?,analysis_state=?,analysis_lease='',analysis_until=0,revision=revision+1 WHERE id=?",(_json(result),state,draft_id))
                self._refresh(db,self._draft(db,draft_id))
            return self.status(draft_id)
        except (PermissionError,ValueError):
            with _originals_operation(self.p), self.db() as db:
                db.execute("""UPDATE einkauf_material_dialoge SET analysis_state='failed',analysis_lease='',analysis_until=0,
                    error_code=CASE WHEN revision=? THEN 'fotoauslese_oder_berechtigung_klaeren' ELSE error_code END
                    WHERE id=? AND analysis_lease=? AND state NOT IN ('accepted','cancelled','external_pending','external_sent')
                    AND NOT EXISTS (SELECT 1 FROM assistent_bestellanforderungen o WHERE o.request_id=?)""",
                    (expected['revision'],draft_id,lease,'material:'+str(draft_id)))
            return self.status(draft_id)

    @staticmethod
    def _external_text(value, maximum=500):
        if (not isinstance(value,str) or not value.strip() or len(value)>maximum
                or any(ord(char)<32 for char in value) or _note(value,maximum)!=value.strip()):
            raise ValueError('Die Angaben zur einmaligen externen Bestellung vollständig und eindeutig eintragen.')
        return value.strip()

    def _external_audit(self, db, action, draft_id, revision, snapshot):
        db.execute('INSERT INTO assistent_audit(actor,auftrag_id,aktion,details,zeit) VALUES(?,?,?,?,?)',
            ('admin',None,action,_json({'draft_id':draft_id,'revision':revision,
             'reservation_id':snapshot['reservation_id'],'snapshot_hash':_fingerprint(snapshot),
             'recipient':snapshot['recipient'],'subject':snapshot['subject'],
             'max_total_cents':snapshot['max_total_cents']}),
             datetime.fromtimestamp(self.clock(),timezone.utc).isoformat()))

    def reserve_external(self, draft_id, revision, payload, actor='admin'):
        """Freeze one explicitly authorized external mail; never enqueue or price it.

        This transaction shares the dialog lock with automatic dispatch. A claim
        has no reset/retry operation: an uncertain external send remains claimed.
        """
        keys = {'supplier_id','recipient','product_name','article_number','variant','subject',
                'recipient_source','authorization_note','max_total_cents','confirmed'}
        if actor!='admin' or not isinstance(payload,dict) or payload.get('confirmed') is not True:
            raise PermissionError('Die Werkstattleitung muss diese einzelne externe Bestellung ausdrücklich bestätigen.')
        if set(payload)!=keys:
            raise ValueError('Lieferant, genauer Artikel, Empfänger, Betreff und verbindliche Gesamtobergrenze fehlen.')
        claim = {key:self._external_text(payload[key]) for key in keys-{'confirmed','max_total_cents'}}
        from werkstatt_bestellungen import _email
        claim['recipient'] = _email(claim['recipient'])
        cap = payload['max_total_cents']
        if type(cap) is not int or not 0<cap<=25000:
            raise ValueError('Die verbindliche Gesamtobergrenze muss höchstens 250 Euro brutto betragen.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            source = self._source(db,draft)
            details = portal_request_details(source)
            if details and details['vorgang']=='anfrage':
                raise ValueError('Eine Teileanfrage ist keine Bestellung und kann nicht für Bestellversand reserviert werden.')
            if self._accepted(db,draft) or draft['state']=='cancelled':
                raise ValueError('Dieser Vorgang ist bereits übergeben, extern reserviert oder abgebrochen.')
            fields = json.loads(draft['fields_json'])
            self._check_manual_identity(fields, details, claim)
            values = {key:fields.get(key,{}).get('value') for key in ('quantity','unit','urgent','order_requested')}
            duplicate = fields.get('possible_duplicate',{}).get('value')
            if not duplicate:
                duplicate = self._duplicate_of(db,draft,source,self._duplicate_signature(fields,json.loads(draft['review_json']),draft))
            if duplicate and fields.get('duplicate_confirmation',{}).get('value')!=duplicate:
                raise ValueError('Mögliche Doppelbestellung: Vorgang erneut prüfen und persönlich als zusätzliche Bestellung bestätigen lassen.')
            if (draft['error_code']=='antwort_unverstaendlich' or fields.get('cancelled')
                    or values['order_requested'] is not True or type(values['urgent']) is not bool
                    or not isinstance(values['quantity'],str) or not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,6})?',values['quantity'])
                    or Decimal(values['quantity'])<=0 or values['unit'] not in set(UNITS.values())):
                raise ValueError('Persönlicher Bestellwunsch, Menge, Einheit oder Dringlichkeit sind noch nicht eindeutig.')
            for key in tuple(values) + (('duplicate_confirmation',) if duplicate else ()):
                proof = fields.get(key,{}).get('proof',{})
                if proof.get('employee_id')!=source['employee_id']:
                    raise PermissionError('Persönlicher Nachrichtenbeleg fehlt.')
                if proof.get('kind')=='image':
                    if proof.get('id')!=source['id'] or source['forwarded']:
                        raise PermissionError('Weiterleitung ist keine Bestellfreigabe.')
                elif proof.get('kind')=='text':
                    evidence = db.execute('SELECT * FROM einkauf_material_texte WHERE id=?',(proof.get('id'),)).fetchone()
                    if (not evidence or evidence['state']!='applied' or evidence['draft_id']!=draft_id
                            or evidence['forwarded'] or evidence['employee_id']!=source['employee_id']):
                        raise PermissionError('Zugeordneter Nachrichtenbeleg fehlt.')
                else:
                    raise PermissionError('Signierter Nachrichtenbeleg fehlt.')
            manager = self.p.workshop_orders
            rights = db.execute('SELECT limit_cent FROM assistent_rechte WHERE mitarbeiter_id=?',(source['employee_id'],)).fetchone()
            if not rights or cap>min(25000,manager.cap(),rights['limit_cent']):
                raise PermissionError('Persönlicher oder betrieblicher Brutto-Kostenrahmen überschritten.')
            supplier = manager.resolve_supplier(claim['supplier_id'])
            # Single-order, evidenced admin confirmation does not globally verify
            # this contact or manufacture current commercial terms.
            if not supplier or supplier['recipient'].casefold()!=claim['recipient'].casefold():
                raise ValueError('Empfänger stimmt nicht mit dem ausgewählten Lieferantenkontakt überein.')
            selected = fields.get('selected_article',{}).get('value')
            if selected and (selected.get('artikelnummer')!=claim['article_number']
                    or selected.get('lieferant','').casefold()!=supplier['name'].casefold()):
                raise ValueError('Persönlich bestätigter Artikel und externe Einzelbestellung passen nicht zusammen.')
            claim.update(kind='manual_external_order',reservation_id=secrets.token_hex(16),draft_id=draft_id,
                reserved_revision=draft['revision'],reserved_by='admin',
                reserved_at=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat(),
                employee_id=source['employee_id'],supplier_name=supplier['name'],
                quantity=values['quantity'],unit=values['unit'],urgent=values['urgent'],max_total_cents=cap)
            amount = format(Decimal(cap)/100,'.2f').replace('.',',')
            claim['body'] = (f"{'Dringende Bestellung' if claim['urgent'] else 'Bestellung'} M-{draft_id}\n\n"
                f"Hiermit bestellen wir {claim['quantity']} {claim['unit']} {claim['product_name']}, {claim['variant']}.\n"
                f"Lieferantenartikelnummer: {claim['article_number']}.\n\n"
                f"Verbindlicher Höchstgesamtbetrag: {amount} EUR einschließlich Mehrwertsteuer, Versand und aller Nebenkosten. "
                'Bei Überschreitung dieses Gesamtbetrags den Auftrag nicht ausführen. Keine Ersatzartikel liefern. '
                'Bitte den konkreten Gesamtpreis und den Liefertermin bestätigen.')
            db.execute("""UPDATE einkauf_material_dialoge SET state='external_pending',revision=revision+1,
                snapshot_json=?,snapshot_hash=?,analysis_lease='',analysis_until=0,missing_json='[]',error_code='',updated_at=? WHERE id=?""",
                (_json(claim),_fingerprint(claim),self.clock(),draft_id))
            db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded',updated_at=? WHERE draft_id=? AND state='queued'",(self.clock(),draft_id))
            self._external_audit(db,'material_external_reserved',draft_id,draft['revision']+1,claim)
            return self._view(db,draft_id)

    def record_external_sent(self, draft_id, revision, payload, actor='admin'):
        """Record a witnessed external send, including after rights revocation."""
        keys = {'reservation_id','recipient','subject','sent_at','send_evidence','confirmed'}
        if actor!='admin' or not isinstance(payload,dict) or payload.get('confirmed') is not True:
            raise PermissionError('Den tatsächlichen externen Versand muss die Werkstattleitung belegen.')
        if set(payload)!=keys:
            raise ValueError('Reservierung, Empfänger, Betreff, Sendezeit und Versandnachweis vollständig angeben.')
        data = {key:self._external_text(payload[key],1000 if key=='send_evidence' else 500) for key in keys-{'confirmed'}}
        try:
            sent = datetime.fromisoformat(data['sent_at'])
            if sent.tzinfo is None:
                raise ValueError()
            data['sent_at'] = sent.astimezone(timezone.utc).isoformat()
        except (TypeError,ValueError):
            raise ValueError('Die Sendezeit mit Zeitzone angeben.') from None
        with self.p.portal_originals_operation_lock(), self.db() as db:
            # Lock without requiring still-active employee rights: this is an
            # admin's truthful record of an already completed external action.
            db.execute('UPDATE einkauf_material_dialoge SET updated_at=updated_at WHERE id=?',(_row_id(draft_id),))
            draft = self._draft(db,draft_id)
            claim = json.loads(draft['snapshot_json'])
            if (draft['state'] not in EXTERNAL_STATES or claim.get('kind')!='manual_external_order'
                    or _fingerprint(claim)!=draft['snapshot_hash']):
                raise ValueError('Keine unveränderte externe Reservierung vorhanden.')
            if any(data[key]!=claim.get(key) for key in ('reservation_id','recipient','subject')):
                raise ValueError('Versandnachweis gehört nicht zu dieser reservierten E-Mail.')
            if draft['state']=='external_sent':
                if all(data[key]==claim.get(key) for key in ('sent_at','send_evidence')):
                    return self._view(db,draft_id)
                raise ValueError('Der Versandnachweis ist bereits fest gespeichert.')
            self._draft(db,draft_id,revision)
            if sent.timestamp()<datetime.fromisoformat(claim['reserved_at']).timestamp() or sent.timestamp()>self.clock()+60:
                raise ValueError('Sendezeit muss nach der Reservierung liegen und darf nicht in der Zukunft liegen.')
            claim.update(sent_at=data['sent_at'],send_evidence=data['send_evidence'],recorded_by='admin',
                recorded_at=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat())
            db.execute("UPDATE einkauf_material_dialoge SET state='external_sent',revision=revision+1,snapshot_json=?,snapshot_hash=?,updated_at=? WHERE id=?",
                (_json(claim),_fingerprint(claim),self.clock(),draft_id))
            self._external_audit(db,'material_external_sent',draft_id,draft['revision']+1,claim)
            return self._view(db,draft_id)

    @_originals_guard
    def assign_article(self, draft_id, revision, payload, actor='admin'):
        """Record identity only. Never reuse terms or submit an order."""
        if actor != 'admin':
            raise PermissionError('Nur die Werkstattleitung darf Artikel zuordnen.')
        if not isinstance(payload, dict) or set(payload) != {'product_name','article_number','variant','reason'}:
            raise ValueError('Artikelname, Artikelnummer, Variante und Zuordnungsgrund angeben.')
        identity = {}
        for key in ('product_name','article_number','variant','reason'):
            value = payload[key]
            if not isinstance(value, str) or len(value)>300 or _note(value,300) != value.strip():
                raise ValueError('Artikelangaben eindeutig und mit höchstens 300 Zeichen angeben.')
            identity[key] = value.strip()
        if not identity['product_name'] or not identity['reason']:
            raise ValueError('Artikelname und Zuordnungsgrund sind erforderlich; unbekannte Nummer und Variante dürfen leer bleiben.')
        with self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            if self._accepted(db,draft) or draft['state'] in {'accepted','cancelled'}:
                raise ValueError('Bereits übergebenen oder abgebrochenen Vorgang nicht ändern.')
            fields = json.loads(draft['fields_json'])
            fields['manual_article'] = {'value':identity, 'proof':{'kind':'admin', 'actor':actor, 'at':self.clock()}}
            fields.pop('selected_article',None)
            fields.pop('possible_duplicate',None)
            fields.pop('duplicate_confirmation',None)
            db.execute("UPDATE einkauf_material_dialoge SET fields_json=?,review_json='{}',revision=revision+1 WHERE id=?",(_json(fields),draft_id))
            self._refresh(db,self._draft(db,draft_id))
            # Identity editing is entirely internal; do not queue a new chat reply.
            db.execute("UPDATE einkauf_material_rueckfragen SET state='superseded' WHERE draft_id=? AND state='queued'",(draft_id,))
            return self._view(db,draft_id)

    @staticmethod
    def _check_manual_identity(fields, details, product):
        manual = fields.get('manual_article',{}).get('value')
        if details and details.get('artikelkorrektur') is True and not manual:
            raise ValueError('Der Mitarbeiter hat die Artikelerkennung abgelehnt. Zuerst den Artikel am Original manuell zuordnen.')
        if manual and any(manual.get(key) and manual[key] != product.get(key)
                          for key in ('product_name','article_number','variant')):
            raise ValueError('Der Bestellartikel muss zur manuellen Artikelzuordnung passen. Zuerst die Zuordnung ändern.')

    @_originals_guard
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
            details = portal_request_details(self._source(db,draft))
            self._check_manual_identity(json.loads(draft['fields_json']), details, review)
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
        details = portal_request_details(source)
        if details and details['vorgang']=='anfrage':
            raise PermissionError('Diese Bildanforderung ist nur eine Teileanfrage, keine Bestellung.')
        if draft['state'] not in {'approved','accepted'}:
            raise PermissionError('Materialbedarf ist noch nicht vollständig bestätigt.')
        snapshot = json.loads(draft['snapshot_json'])
        if _fingerprint(snapshot) != draft['snapshot_hash'] or snapshot['request_key'] != 'material:' + str(draft_id):
            raise PermissionError('Bestellfreigabe wurde verändert.')
        if actor is not None and actor != snapshot['actor'] or request_key is not None and request_key != snapshot['request_key']:
            raise PermissionError('Bestellfreigabe gehört einem anderen Vorgang.')
        fields = json.loads(draft['fields_json'])
        review = json.loads(draft['review_json'])
        try:
            self._check_manual_identity(fields, details, review)
        except ValueError as exc:
            raise PermissionError(str(exc)) from None
        duplicate = fields.get('possible_duplicate',{}).get('value')
        if not duplicate and draft['state']=='approved':
            duplicate = self._duplicate_of(db,draft,source,self._duplicate_signature(fields,review,draft))
        if duplicate and fields.get('duplicate_confirmation',{}).get('value')!=duplicate:
            raise PermissionError('Mögliche Doppelbestellung muss ausdrücklich zusätzlich bestätigt werden.')
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
        for key in ('order_requested','quantity','unit','urgent') + (('selected_article',) if selected else ()) + (('duplicate_confirmation',) if duplicate else ()):
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

    @_originals_guard
    def recheck(self, draft_id, revision):
        """Refresh catalog matches from stored labels, never rerun vision or send."""
        with self.db() as db:
            draft = self._draft(db,draft_id,revision,lock=True)
            source = self._source(db,draft)
            if self._accepted(db,draft) or draft['state']=='cancelled':
                raise ValueError('Übergebenen oder abgebrochenen Vorgang nicht erneut freigeben.')
            if draft['error_code']=='antwort_unverstaendlich':
                raise ValueError('Die unklare Mitarbeiterantwort zuerst über denselben Vorgang klären.')
        analysis = None
        if source['mime']!='text/plain' and draft['analysis_state']=='done':
            analysis = self.p.assistant_material_photos.status(
                {'actor':'mitarbeiter:'+str(source['employee_id']),'lesen':True,'einkaufen':True},
                source['assistant_photo_id'])
            if analysis.get('status')!='pruefen' or analysis.get('artikelsuche_verfuegbar') is False:
                raise ValueError('Aktuelle Artikelsuche nicht verfügbar; gespeicherten Stand beibehalten.')
        with self.db() as db:
            current = self._draft(db,draft_id,revision,lock=True)
            unchanged = ('fields_json','review_json','analysis_json','analysis_state','error_code','state','dispatch_id')
            if self._accepted(db,current) or any(current[key]!=draft[key] for key in unchanged):
                raise ValueError('Materialvorgang wurde während der Prüfung geändert. Bitte neu laden.')
            if analysis is not None:
                db.execute('UPDATE einkauf_material_dialoge SET analysis_json=? WHERE id=?',(_json(analysis),draft_id))
            db.execute("UPDATE einkauf_material_dialoge SET revision=revision+1,error_code='' WHERE id=?",(draft_id,))
            self._refresh(db,self._draft(db,draft_id))
            return self._view(db,draft_id)

    @_originals_guard
    def order_attempt(self, draft_id, revision, result):
        if not isinstance(result,dict):
            raise ValueError('Bestellstatus fehlt.')
        with self.db() as db:
            db.execute('UPDATE einkauf_material_dialoge SET updated_at=updated_at WHERE id=?',(_row_id(draft_id),))
            current = self._draft(db,draft_id)
            if current['state'] in EXTERNAL_STATES:
                return self._view(db,draft_id)
            draft = self._draft(db,draft_id,revision)
            order_id = str(result.get('id') or '')
            if not order_id:
                durable = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE request_id=?',('material:'+str(draft_id),)).fetchone()
                order_id = draft['dispatch_id'] or (str(durable['id']) if durable else '')
            if draft['dispatch_id'] and draft['dispatch_id'] != order_id:
                raise ValueError('Bestellreferenz ist bereits fest gespeichert.')
            if not order_id and draft['state']=='approved':
                try:
                    source = self._source(db,draft,lock=True)
                    fields,review = json.loads(draft['fields_json']),json.loads(draft['review_json'])
                    duplicate = self._duplicate_of(db,draft,source,self._duplicate_signature(fields,review,draft))
                    if duplicate and fields.get('duplicate_confirmation',{}).get('value')!=duplicate:
                        self._refresh(db,draft)
                        return self._view(db,draft_id)
                except PermissionError:
                    pass
            state = 'accepted' if order_id else 'review'
            db.execute('UPDATE einkauf_material_dialoge SET state=?,dispatch_id=?,dispatch_state=?,error_code=?,updated_at=? WHERE id=?',
                (state,order_id,str(result.get('state','blocked'))[:50],'' if order_id else 'bestelluebergabe_offen',self.clock(),draft_id))

    @staticmethod
    def _reply_eligible(draft, question):
        missing = set(json.loads(draft['missing_json']))
        if question['field']=='internal_review':
            return (draft['state']=='review' and not draft['error_code']
                    and bool(missing & INTERNAL_FIELDS) and not missing & EMPLOYEE_FIELDS)
        return draft['state']=='open' and question['field'] in missing & EMPLOYEE_FIELDS

    def send_question(self):
        with self.db() as db:
            db.execute("UPDATE einkauf_material_rueckfragen SET state='uncertain',error_code='sendestatus_unklar_nicht_erneut_senden' WHERE state='sending' AND updated_at<?",(self.clock()-120,))
        if self.p.app.config.get('MATERIAL_WHATSAPP_REPLIES_ENABLED') is not True:
            return None
        with self.db() as db:
            # Portal questions stay attached to their personal form. Never
            # put a portal reference or identity into the Meta send queue.
            question = db.execute("""SELECT q.* FROM einkauf_material_rueckfragen q
                JOIN einkauf_material_dialoge d ON d.id=q.draft_id
                JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                WHERE q.state='queued' AND n.phone_number_id<>'portal:personal' ORDER BY q.id LIMIT 1""").fetchone()
            if not question:
                return None
            question = dict(question)
            try:
                draft = self._draft(db,question['draft_id'],question['revision'],lock=True)
                source = self._source(db,draft)
                if not self._reply_eligible(draft,question):
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
                current = self._draft(db,draft['id'],draft['revision'],lock=True)
                if not self._reply_eligible(current,question):
                    raise ValueError('Vorgang benötigt diese Nachricht nicht mehr.')
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
                    WHERE n.state='ready' AND n.phone_number_id<>'portal:personal' AND d.id IS NULL ORDER BY n.id LIMIT 1''').fetchone()
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
                draft = db.execute("""SELECT d.id FROM einkauf_material_dialoge d
                    JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                    WHERE n.phone_number_id<>'portal:personal'
                    AND d.state NOT IN ('accepted','cancelled','review','external_pending','external_sent')
                    AND (d.analysis_state='pending' OR (d.analysis_state='processing' AND d.analysis_until<?))
                    ORDER BY d.id LIMIT 1""",(self.clock(),)).fetchone()
            if not draft:
                break
            try:
                self.analyze(draft['id'])
                break
            except (PermissionError,ValueError):
                with self.db() as db:
                    db.execute("UPDATE einkauf_material_dialoge SET state='review',analysis_state='failed',error_code='fotoauslese_oder_berechtigung_klaeren' WHERE id=? AND state NOT IN ('external_pending','external_sent') AND (analysis_state='pending' OR analysis_until<?)",(draft['id'],self.clock()))
        text = self.process_text()
        with self.db() as db:
            approved = db.execute("""SELECT d.id,d.revision FROM einkauf_material_dialoge d
                JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                WHERE d.state='approved' AND n.phone_number_id<>'portal:personal'
                ORDER BY d.updated_at,d.id LIMIT 1""").fetchone()
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
