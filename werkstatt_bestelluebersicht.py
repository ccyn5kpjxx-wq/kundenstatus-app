"""Read-only projection of the existing order queue, actions and mail receipts.

No schema initialization, dispatch, outbox recovery, file reads or network calls.
Amounts come from saved order snapshots; a mail receipt is not a delivery receipt.
"""
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import re

from werkstatt_bestellplan import BERLIN


STATES = {
    'queued': 'Eingeplant', 'ready': 'Mail vorbereitet',
    'sending': 'Versand gestartet – Abschluss offen',
    'sent': 'Mailserver hat angenommen', 'copy_pending': 'Angenommen – Gesendet-Kopie fehlt',
    'partial': 'Teilweise angenommen – prüfen', 'uncertain': 'Versand unklar – prüfen',
    'not_sent': 'Nicht angenommen', 'blocked': 'Versand gesperrt',
    'draft': 'Vorschlag – noch nicht bestellt',
    'approved_pending': 'Bestätigt – Übergabe nicht belegt',
    'unknown': 'Stand prüfen',
}
AUDIT_LABELS = {
    'vorschlag': 'Vorschlag angelegt', 'nachbestellung_vorbereitet': 'Nachbestellung als neuen Vorschlag vorbereitet',
    'bestellung_bestaetigt': 'Konkrete Bestellung bestätigt',
    'bestellung_nicht_angenommen': 'Übergabe abgewiesen; erneute Prüfung erforderlich',
    'bestellung_blockiert': 'Bestellung blockiert',
}


def _text(value, limit=500):
    return str(value)[:limit] if isinstance(value, (str, int)) and not isinstance(value, bool) else ''


def _object(value):
    if not isinstance(value, str) or len(value) > 300000:
        return {}
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def _money(value):
    if type(value) is not int or not 0 <= value <= 10**12:
        return 'nicht belegt'
    return f'{value // 100:,}'.replace(',', '.') + f',{value % 100:02d} €'


def _stamp(value):
    try:
        if isinstance(value, (float, int)) and not isinstance(value, bool):
            stamp = datetime.fromtimestamp(value, timezone.utc)
        else:
            try:
                stamp = datetime.fromisoformat(value)
            except ValueError:
                # Portal now_str() stores German local time; newer sources may
                # use ISO timestamps. Both are evidence, never import time.
                stamp = datetime.strptime(value, '%d.%m.%Y %H:%M' if len(value) == 16 else '%d.%m.%Y %H:%M:%S')
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=BERLIN)
        return stamp.astimezone(BERLIN).strftime('%d.%m.%Y %H:%M')
    except (ValueError, TypeError, OverflowError, OSError):
        return 'Zeitpunkt nicht belegt'


def _number(value):
    try:
        return min(1000000, max(1, int(value)))
    except (ValueError, TypeError):
        return 1


def _like(value):
    return '%' + value.replace('!', '!!').replace('%', '!%').replace('_', '!_') + '%'


class OrderOverview:
    PAGE_SIZE = 25

    def __init__(self, get_db):
        self.get_db = get_db

    @staticmethod
    def _optional(db, table):
        # Names are constants owned by this module, never query parameters.
        try:
            db.execute('SELECT 1 FROM ' + table + ' WHERE 1=0').fetchall()
            return True
        except Exception as exc:
            if getattr(exc, 'sqlstate', None) == '42P01' or 'no such table' in str(exc).lower():
                db.rollback()
                return False
            raise

    @staticmethod
    def _person(actor, people):
        if actor == 'admin':
            return 'Werkstattleitung (Admin)'
        match = re.fullmatch(r'mitarbeiter:([1-9][0-9]*)', actor or '')
        if match:
            mid = match.group(1)
            return people.get(mid) or f'Mitarbeiter #{mid} (Name nicht mehr hinterlegt)'
        return 'Nicht zugeordneter Zugang'

    def _line(self, row, people, contacts, *, draft=False, now=None):
        row = dict(row)
        saved = _object(row.get('payload') if draft else row.get('snapshot_json'))
        intent = saved.get('versand' if draft else 'order')
        intent = intent if isinstance(intent, dict) else {}
        warnings = []
        intact = bool(intent)
        if not draft:
            fingerprint = hashlib.sha256(json.dumps(intent, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            intact = intact and fingerprint == row.get('request_fingerprint') and saved.get('actor_id') == row.get('actor_id')
            if not intact:
                warnings.append('Gespeicherter Bestellinhalt ist unvollständig oder stimmt nicht mit seinem Freigabenachweis überein. Beträge nicht als geprüft verwenden.')
        actor = row.get('actor', '') if draft else row.get('actor_id', '')
        state = row.get('view_state', 'unknown')
        if state not in STATES:
            state = 'unknown'
        verified = not draft and intact and intent.get('price_verified') is True and intent.get('price_basis') == 'gross' and intent.get('currency') == 'EUR'
        supplier_id = _text(intent.get('supplier_id'), 128)
        contact = contacts.get(supplier_id)
        if contact and (contact.get('recipient') != intent.get('recipient') or not contact.get('verified_at')):
            warnings.append('Der aktuelle Lieferantenkontakt weicht von der Freigabe ab oder ist nicht bestätigt. Der damalige Empfänger bleibt unverändert dargestellt.')
        total = intent.get('expected_total_cents') if verified else saved.get('gesamt_cent') if draft else None
        cap = intent.get('max_total_cents') if intact else None
        if type(cap) is int and cap > 25000:
            warnings.append('Gespeicherter Kostenrahmen überschreitet die freigegebene Betriebsgrenze von 250,00 €.')
        if row.get('batch_state') == 'blocked':
            reason = _object(row.get('result_json')).get('message')
            if reason:
                warnings.append(_text(reason))
        urgent = intent.get('urgent')
        overdue = not draft and state in {'queued', 'ready'} and isinstance(row.get('due_at'), (int, float)) and row['due_at'] < now.timestamp()
        if overdue:
            warnings.append('Versandtermin erreicht; ein Versandabschluss ist noch nicht belegt.')
        return {
            'id': row['id'], 'draft': draft, 'actor': actor, 'person': self._person(actor, people),
            'supplier_id': supplier_id, 'supplier': _text(saved.get('lieferant') if draft else saved.get('supplier_name')) or 'Lieferant nicht belegt',
            'product': _text(intent.get('product_name') or (saved.get('bezeichnung') if draft else '')) or 'Artikel nicht belegt',
            'sku': _text(intent.get('article_number'), 128) or _text(intent.get('product_id'), 128) or 'nicht belegt',
            'variant': _text(intent.get('variant')) or 'nicht belegt',
            'quantity': _text(intent.get('quantity'), 60) or 'nicht belegt', 'unit': _text(intent.get('unit'), 60),
            'recipient': _text(intent.get('recipient'), 254) or 'nicht belegt',
            'urgency': 'Dringend' if urgent is True else 'Sammelbestellung' if urgent is False else 'Nicht geklärt',
            'created': _stamp(row.get('erstellt_am') if draft else row.get('created_at')),
            'due': 'Noch nicht eingeplant' if draft else _stamp(row.get('due_at')),
            'state': state, 'state_label': STATES[state], 'warnings': warnings, 'overdue': overdue,
            'total': _money(total), 'cap': _money(cap), 'verified': verified,
            'unit_price': _money(intent.get('unit_price_cents') if verified or draft else None),
            'shipping': _money(intent.get('shipping_cents') if verified or draft else None),
            'extra': _money(intent.get('extra_costs_cents') if verified or draft else None),
            'price_source': _text(intent.get('price_source')) or 'nicht belegt',
            'request_id': _text(row.get('request_id'), 128), 'action_id': row.get('action_id') or (row['id'] if draft else None),
            'order_id': row.get('auftrag_id') or 0, 'batch_id': _text(row.get('batch_id'), 128),
            'attempts': row.get('attempts') or 0, 'mail_updated': _stamp(row.get('mail_updated')) if row.get('mail_updated') else '',
            'message_id': _text(row.get('message_id'), 500),
        }

    def page(self, query=None, *, now=None):
        query = query or {}
        now = now or datetime.now(timezone.utc)
        filters = {key: _text(query.get(key), 160).strip() for key in ('q', 'person', 'supplier', 'state', 'urgency', 'from', 'to', 'batch')}
        if filters['state'] not in STATES and filters['state'] != 'overdue':
            filters['state'] = ''
        if filters['urgency'] not in {'urgent', 'weekly'}:
            filters['urgency'] = ''
        errors, dates = [], {}
        for key in ('from', 'to'):
            if filters[key]:
                try:
                    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', filters[key]):
                        raise ValueError()
                    dates[key] = date.fromisoformat(filters[key])
                    if dates[key].year >= 9999:
                        raise ValueError()
                except ValueError:
                    errors.append('Zeitraum bitte als gültiges Datum angeben.')
                    filters[key] = ''
                    dates.pop(key, None)
        if dates.get('from') and dates.get('to') and dates['from'] > dates['to']:
            errors.append('Das Anfangsdatum liegt nach dem Enddatum.')
        db = self.get_db()
        try:
            available = {name: self._optional(db, name) for name in ('assistent_aktionen', 'mitarbeiter', 'assistent_audit', 'mailbox_outbox')}
            people = {str(row['id']): row['name'] for row in db.execute('SELECT id,name FROM mitarbeiter').fetchall()} if available['mitarbeiter'] else {}
            contacts = {row['id']: dict(row) for row in db.execute('SELECT id,name,recipient,verified_at FROM assistent_bestellkontakte').fetchall()}
            mail_join = 'LEFT JOIN mailbox_outbox m ON m.token=b.id' if available['mailbox_outbox'] else ''
            mail_fields = 'm.state AS mail_state,m.updated_at AS mail_updated,m.message_id' if available['mailbox_outbox'] else 'NULL AS mail_state,NULL AS mail_updated,NULL AS message_id'
            action_join = "LEFT JOIN assistent_aktionen a ON a.actor=o.actor_id AND a.art='bestellung' AND o.request_id=('avatar:' || a.id)" if available['assistent_aktionen'] else ''
            action_fields = 'a.id AS action_id,a.auftrag_id' if available['assistent_aktionen'] else 'NULL AS action_id,NULL AS auftrag_id'
            base = f'''SELECT o.*,b.state AS batch_state,b.attempts,b.result_json,
                       {mail_fields},{action_fields} FROM assistent_bestellanforderungen o
                       LEFT JOIN assistent_bestellpakete b ON b.id=o.batch_id {mail_join} {action_join}'''
            state_expr = """CASE WHEN batch_state='blocked' AND mail_state='not_sent' THEN 'blocked'
                WHEN mail_state IN ('sent','copy_pending','partial','uncertain','not_sent','sending') THEN mail_state
                WHEN batch_state IN ('ready','sending','sent','copy_pending','partial','uncertain','not_sent','blocked') THEN batch_state
                WHEN batch_id<>'' THEN 'uncertain' ELSE 'queued' END"""
            base = f'SELECT source_rows.*, {state_expr} AS view_state FROM ({base}) source_rows'
            conditions, params = self._conditions(filters, dates, draft=False)
            if filters['state'] == 'overdue':
                conditions += ["view_state IN ('queued','ready')", 'due_at<?'];params.append(now.timestamp())
            elif filters['state']:
                conditions.append('view_state=?');params.append(filters['state'])
            where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
            counts = {row['view_state']: row['n'] for row in db.execute(f'SELECT view_state,COUNT(*) AS n FROM ({base}) entries{where} GROUP BY view_state', params).fetchall()}
            count = sum(counts.values())
            page = min(_number(query.get('page')), max(1, (count + self.PAGE_SIZE - 1) // self.PAGE_SIZE))
            rows = db.execute(f'SELECT * FROM ({base}) entries{where} ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?', (*params, self.PAGE_SIZE, (page-1)*self.PAGE_SIZE)).fetchall()
            lines = [self._line(row, people, contacts, now=now) for row in rows]
            draft_base = """SELECT a.*,CASE WHEN SUBSTR(a.erstellt_am,3,1)='.' AND SUBSTR(a.erstellt_am,6,1)='.'
                THEN SUBSTR(a.erstellt_am,7,4)||'-'||SUBSTR(a.erstellt_am,4,2)||'-'||SUBSTR(a.erstellt_am,1,2)||' '||SUBSTR(a.erstellt_am,12)
                ELSE REPLACE(a.erstellt_am,'T',' ') END AS sort_created,
                CASE WHEN a.status='vorschlag' THEN 'draft' WHEN a.status='intern_freigegeben'
                THEN 'approved_pending' ELSE 'unknown' END AS view_state FROM assistent_aktionen a
                WHERE a.art='bestellung' AND NOT EXISTS (SELECT 1 FROM assistent_bestellanforderungen o
                WHERE o.actor_id=a.actor AND o.request_id=('avatar:' || a.id))"""
            draft_count, drafts, draft_page = 0, [], 1
            if available['assistent_aktionen']:
                conditions, params = self._conditions(filters, dates, draft=True)
                if filters['state']:
                    conditions.append('view_state=?');params.append(filters['state'])
                where_draft = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
                draft_count = db.execute(f'SELECT COUNT(*) AS n FROM ({draft_base}) entries{where_draft}', params).fetchone()['n']
                draft_page = min(_number(query.get('draft_page')), max(1, (draft_count+self.PAGE_SIZE-1)//self.PAGE_SIZE))
                draft_rows = db.execute(f'SELECT * FROM ({draft_base}) entries{where_draft} ORDER BY sort_created DESC,id DESC LIMIT ? OFFSET ?', (*params, self.PAGE_SIZE, (draft_page-1)*self.PAGE_SIZE)).fetchall()
                drafts = [self._line(row, people, contacts, draft=True, now=now) for row in draft_rows]
            actors = {row['actor_id'] for row in db.execute('SELECT DISTINCT actor_id FROM assistent_bestellanforderungen').fetchall()}
            if available['assistent_aktionen']:
                actors.update(row['actor'] for row in db.execute("SELECT DISTINCT actor FROM assistent_aktionen WHERE art='bestellung'").fetchall())
            selected = None
            requested = _text(query.get('bestellung'), 128)
            requested_draft = _text(query.get('vorschlag'), 128)
            if requested or requested_draft:
                if requested:
                    row = db.execute(f'SELECT * FROM ({base}) entries WHERE id=?', (requested,)).fetchone()
                else:
                    row = db.execute(f'SELECT * FROM ({draft_base}) entries WHERE id=?', (requested_draft,)).fetchone() if available['assistent_aktionen'] else None
                if row:
                    selected = self._line(row, people, contacts, draft=not requested, now=now)
                    selected.update(events=[], siblings=[], sibling_count=0, batch_cap='nicht belegt')
                    if selected['action_id'] and available['assistent_audit']:
                        audit = db.execute('SELECT aktion,zeit FROM assistent_audit WHERE actor=? AND details=? ORDER BY id', (selected['actor'], selected['action_id'])).fetchall()
                        selected['events'] = [{'label': AUDIT_LABELS[row['aktion']], 'time': _stamp(row['zeit'])} for row in audit if row['aktion'] in AUDIT_LABELS]
                    if selected['batch_id']:
                        selected['sibling_count'] = db.execute('SELECT COUNT(*) AS n FROM assistent_bestellanforderungen WHERE batch_id=?', (selected['batch_id'],)).fetchone()['n']
                        siblings = db.execute(f'SELECT * FROM ({base}) entries WHERE batch_id=? ORDER BY created_at,id LIMIT 100', (selected['batch_id'],)).fetchall()
                        selected['siblings'] = [self._line(row, people, contacts, now=now) for row in siblings]
                        batch = db.execute('SELECT payload_json,fingerprint,created_at FROM assistent_bestellpakete WHERE id=?', (selected['batch_id'],)).fetchone()
                        if batch:
                            payload = _object(batch['payload_json'])
                            fingerprint = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                            if payload and fingerprint == batch['fingerprint']:
                                selected['batch_cap'] = _money(payload.get('max_total_cents'))
                            else:
                                selected['warnings'].append('Das gespeicherte Mailpaket stimmt nicht mit seinem Inhaltsnachweis überein. Paketbetrag nicht belegt.')
                            selected['batch_created'] = _stamp(batch['created_at'])
                else:
                    errors.append('Die ausgewählte Bestellung ist nicht vorhanden oder wurde noch nicht an den Versand übergeben.')
            return {'filters': filters, 'errors': errors, 'items': lines, 'count': count, 'page': page,
                    'pages': max(1, (count+self.PAGE_SIZE-1)//self.PAGE_SIZE), 'counts': counts,
                    'drafts': drafts, 'draft_count': draft_count, 'draft_page': draft_page,
                    'draft_pages': max(1, (draft_count+self.PAGE_SIZE-1)//self.PAGE_SIZE), 'selected': selected,
                    'people': [{'id': actor, 'name': self._person(actor, people)} for actor in sorted(actors)],
                    'suppliers': sorted(contacts.values(), key=lambda row: row['name'].casefold()), 'states': STATES,
                    'stand': _stamp(now.isoformat())}
        finally:
            db.close()

    @staticmethod
    def _conditions(filters, dates, *, draft):
        conditions, params = [], []
        payload = 'payload' if draft else 'snapshot_json'
        if filters['batch']:
            if draft:
                conditions.append('1=0')
            else:
                conditions.append('batch_id=?');params.append(filters['batch'])
        if filters['person']:
            conditions.append(('actor' if draft else 'actor_id')+'=?');params.append(filters['person'])
        for key, value in (('supplier_id', filters['supplier']), ('urgent', {'urgent': True, 'weekly': False}.get(filters['urgency']))):
            if value is not None and value != '':
                encoded = json.dumps(value, ensure_ascii=False)
                conditions.append(f"({payload} LIKE ? ESCAPE '!' OR {payload} LIKE ? ESCAPE '!')")
                params.extend((_like(f'"{key}":{encoded}'), _like(f'"{key}": {encoded}')))
        if filters['q']:
            conditions.append(f"LOWER({payload}) LIKE ? ESCAPE '!'");params.append(_like(filters['q'].lower()))
        for key, operator in (('from', '>='), ('to', '<')):
            if key in dates:
                day = dates[key] + (timedelta(days=1) if key == 'to' else timedelta())
                if draft:
                    value = day.isoformat()
                else:
                    value = datetime.combine(day, time(), BERLIN).timestamp()
                conditions.append(('sort_created' if draft else 'created_at')+operator+'?');params.append(value)
        return conditions, params
