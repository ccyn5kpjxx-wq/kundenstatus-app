"""Internal evidence ledger for material needs and orders already sent elsewhere.

This module has no dispatcher, messaging client or order-queue integration.
Unknown quantities/prices remain NULL. Documents and reconciliation events are
immutable evidence; OCR text is only an untrusted, editable-in-the-source proposal.
"""
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import tempfile
import unicodedata

from flask import Blueprint, abort, jsonify, request, send_file, session
from PIL import Image, UnidentifiedImageError

from werkstatt_artikel_import import _price_evidence
from werkstatt_artikel_identity import parse_unit_price


MAX_FILE_BYTES = 8 * 1024 * 1024
TABLES = ('einkauf_eingang', 'einkauf_eingang_positionen', 'einkauf_eingang_dateien',
          'einkauf_eingang_lieferungen', 'einkauf_eingang_preise', 'einkauf_eingang_klaerungen')
FILE_KINDS = {'materialfoto', 'lieferschein', 'rechnung'}
FILE_METADATA = 'id,eingang_id,kind,original_name,mime,suffix,sha256,created_at,created_by,extraction_status,draft_text'
_SECRET = re.compile(r'\b(?:password|passwort|api[_ -]?key|access[_ -]?token|secret|authorization)\b', re.I)
_BANK_LABEL = re.compile(r'\b(?:iban|bic|swift|bankverbindung|kontoverbindung|kontoinhaber|kontonummer|kontoauszug|'
                         r'kontostand|bankkonto|bankleitzahl|blz|kreditkartennummer|mandatsreferenz|lastschrift|sepa)\b', re.I)
_IBAN_CANDIDATE = re.compile(r'(?=(\b[A-Z]{2}\s*\d{2}(?:[ \t]?[A-Z0-9]){11,34}\b))', re.I)
# Structural rejection remains conservative for known IBAN jurisdictions, even
# if OCR damaged the checksum. Other prefixes still undergo checksum detection.
# A product SKU such as DP7000 followed by words is not itself a bank number.
_IBAN_COUNTRIES = frozenset('AD AE AL AT AZ BA BE BG BH BI BR BY CH CR CY CZ DE DJ DK DO EE EG ES FI FK FO FR GB GE GI GL GR GT HR HU IE IL IQ IS IT JO KZ KW LC LB LI LT LU LV LY MC MD ME MK MN MR MT MU NI NL NO OM PK PL PS PT QA RO RS RU SA SC SD SE SI SK SM SO ST SV TL TN TR UA VA VG XK'.split())


def _has_bank_data(value):
    if _BANK_LABEL.search(value):
        return True
    for match in _IBAN_CANDIDATE.finditer(value):
        candidate = re.sub(r'\s+', '', match.group(1)).upper()
        if candidate[:2] in _IBAN_COUNTRIES:
            return True
        # Check each possible complete IBAN prefix, so trailing product text
        # cannot disguise a real identifier consumed by the broad scanner.
        for length in range(15, min(34, len(candidate)) + 1):
            part = candidate[:length]
            digits = ''.join(str(ord(char) - 55) if char.isalpha() else char for char in part[4:] + part[:4])
            if int(digits) % 97 == 1:
                return True
    return False


class IntakeConflict(ValueError):
    pass


def _text(value, field, limit=500, optional=False):
    if value is None and optional:
        return ''
    if not isinstance(value, str):
        raise ValueError(field + ' fehlt oder ist ungültig.')
    value = value.strip()
    if len(value) > limit or (not value and not optional) or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError(field + ' fehlt oder ist ungültig.')
    if _has_bank_data(value) or _SECRET.search(value):
        raise ValueError(field + ' darf keine Bank- oder Zugangsdaten enthalten.')
    return value


def _norm(value):
    return ' '.join(unicodedata.normalize('NFKC', value or '').casefold().split())


def _decimal(value, field, *, optional=False, zero=False, maximum='1000000', places=4):
    if value is None or value == '':
        if optional:
            return None
        raise ValueError(field + ' fehlt.')
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError(field + ' muss eine eindeutige Dezimalzahl sein.')
    raw = str(value).strip()
    if not re.fullmatch(r'\d+(?:[.,]\d{1,' + str(places) + r'})?', raw):
        raise ValueError(field + ' muss eine eindeutige Dezimalzahl sein.')
    try:
        number = Decimal(raw.replace(',', '.'))
    except InvalidOperation as exc:
        raise ValueError(field + ' ist ungültig.') from exc
    if not (Decimal('0') <= number <= Decimal(maximum)) or (not zero and number == 0):
        raise ValueError(field + ' liegt außerhalb des erlaubten Bereichs.')
    return format(number.normalize(), 'f')


def _id(value, field='Referenz'):
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and re.fullmatch(r'[1-9][0-9]{0,15}', value):
        return int(value)
    raise ValueError(field + ' ist ungültig.')


def _stamp(value):
    raw = _text(value, 'Quellenzeitpunkt', 50)
    try:
        stamp = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError()
        return stamp.astimezone(timezone.utc).isoformat()
    except ValueError as exc:
        raise ValueError('Quellenzeitpunkt mit Datum, Uhrzeit und Zeitzone angeben.') from exc


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _hash(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _keys(data, allowed):
    if not isinstance(data, dict) or set(data) - set(allowed):
        raise ValueError('Unbekannte oder ungültige Eingabefelder.')


def _image_or_pdf(raw):
    if raw.startswith(b'%PDF-'):
        try:
            import fitz
            with fitz.open(stream=raw, filetype='pdf') as document:
                if document.is_encrypted or not 1 <= document.page_count <= 100:
                    raise ValueError('PDF muss lesbar sein und höchstens 100 Seiten haben.')
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError('Kein lesbares PDF.') from exc
        return 'application/pdf', '.pdf'
    try:
        with Image.open(io.BytesIO(raw)) as image:
            if (image.format not in {'JPEG', 'PNG', 'WEBP'} or
                    image.width * image.height > 20_000_000 or getattr(image, 'n_frames', 1) != 1):
                raise ValueError('Einzelbild als JPEG, PNG oder WebP mit höchstens 20 Megapixeln erforderlich.')
            mime, suffix = {'JPEG': ('image/jpeg', '.jpg'), 'PNG': ('image/png', '.png'), 'WEBP': ('image/webp', '.webp')}[image.format]
            image.verify()
            return mime, suffix
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as exc:
        raise ValueError('Datei ist kein gültiges Bild oder PDF.') from exc


def _line_identity(line, supplier):
    return {key: _norm(value) for key, value in {
        'supplier': supplier, 'product': line.get('product'), 'sku': line.get('sku'),
        'variant': line.get('variant'), 'unit': line.get('unit'), 'pack': line.get('pack'),
    }.items()}


def _catalog_identity(row):
    evidence = row.get('package_evidence') or {}
    pack = ''
    if evidence.get('basis') == 'explicit_description':
        pack = ' '.join(str(evidence.get(key) or '') for key in ('value', 'unit', 'per_unit')).strip()
    return _line_identity({'product': row.get('produkt_name'), 'sku': row.get('artikelnummer'),
                           'variant': ' | '.join(str(row.get(key) or '').strip() for key in ('groesse', 'farbe', 'gebinde') if row.get(key)),
                           'unit': row.get('ve'), 'pack': pack}, row.get('lieferant'))


def _latest_price(records, role, identity):
    """Invoice dates outrank insertion order; conflicting evidence stays open."""
    candidates, warnings = [], []
    for row in records:
        if row['role'] != role:
            continue
        price = row['data']
        if price.get('identity') != identity:
            warnings.append('Preisquelle passt nicht mehr zur Artikelidentität und wird nicht verrechnet.')
            continue
        candidates.append(price)
    if not candidates:
        return None, warnings
    dated = [row for row in candidates if row.get('date')]
    unknown_date = len(dated) != len(candidates)
    if unknown_date:
        warnings.append('Eine Preisquelle hat kein Belegdatum. Welcher Preis insgesamt der neueste ist, bleibt ungeklärt.')
    if dated:
        newest = max(row['date'] for row in dated)
        selected = [row for row in dated if row['date'] == newest]
    else:
        selected = candidates
    # Human verification of this date outranks an unreviewed OCR observation
    # of the same date. It never outranks a later dated invoice automatically.
    verified = [row for row in selected if row.get('verified') is True]
    if verified:
        selected = verified
    fields = ('amount', 'identity', 'unit', 'pack', 'currency', 'tax_basis', 'tax_rate', 'discount_basis')
    if len({_json({key: row.get(key) for key in fields}) for row in selected}) != 1:
        warnings.append('Am ausgewählten Belegdatum stehen unterschiedliche Preise oder Preisbasen. Kein einzelner Planwert wird behauptet.')
        return None, warnings
    chosen = dict(selected[-1], selection='latest_dated' if dated else 'undated', selection_complete=not unknown_date)
    return chosen, warnings


class MaterialIntake:
    def __init__(self, portal):
        self.p = portal
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
                CREATE TABLE IF NOT EXISTS einkauf_eingang (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_key TEXT NOT NULL UNIQUE,
                    payload_hash TEXT NOT NULL, supplier TEXT NOT NULL, external_ref TEXT NOT NULL,
                    source_at TEXT NOT NULL, already_ordered INTEGER NOT NULL,
                    original_author TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE IF NOT EXISTS einkauf_eingang_positionen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, eingang_id INTEGER NOT NULL,
                    position INTEGER NOT NULL, product TEXT NOT NULL, sku TEXT NOT NULL DEFAULT '',
                    variant TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '', pack TEXT NOT NULL DEFAULT '',
                    quantity TEXT, urgent INTEGER, category TEXT NOT NULL,
                    original_author TEXT, UNIQUE(eingang_id,position));
                CREATE TABLE IF NOT EXISTS einkauf_eingang_dateien (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, eingang_id INTEGER NOT NULL, kind TEXT NOT NULL,
                    original_name TEXT NOT NULL, mime TEXT NOT NULL, suffix TEXT NOT NULL,
                    sha256 TEXT NOT NULL, original_base64 TEXT NOT NULL, created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL, extraction_status TEXT NOT NULL DEFAULT 'offen',
                    draft_text TEXT NOT NULL DEFAULT '', UNIQUE(eingang_id,sha256));
                CREATE TABLE IF NOT EXISTS einkauf_eingang_lieferungen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, eingang_id INTEGER NOT NULL,
                    line_id INTEGER NOT NULL, file_id INTEGER NOT NULL, position INTEGER NOT NULL,
                    quantity TEXT NOT NULL, unit TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL, created_by TEXT NOT NULL, UNIQUE(file_id,position));
                CREATE TABLE IF NOT EXISTS einkauf_eingang_preise (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, eingang_id INTEGER NOT NULL,
                    line_id INTEGER NOT NULL, role TEXT NOT NULL, source_key TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL, UNIQUE(line_id,role,source_key));
                CREATE TABLE IF NOT EXISTS einkauf_eingang_klaerungen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, eingang_id INTEGER NOT NULL,
                    line_id INTEGER NOT NULL, before_json TEXT NOT NULL, after_json TEXT NOT NULL,
                    reason TEXT NOT NULL, created_at TEXT NOT NULL, created_by TEXT NOT NULL);
            ''')

    @staticmethod
    def _actor(actor):
        if actor != 'admin':
            raise PermissionError('Diese Belegzuordnung ist der Werkstattleitung vorbehalten.')
        return actor

    @staticmethod
    def _group(db, group_id):
        row = db.execute('SELECT * FROM einkauf_eingang WHERE id=?', (_id(group_id),)).fetchone()
        if not row:
            raise ValueError('Einkaufseingang nicht gefunden.')
        return dict(row)

    @staticmethod
    def _line(db, group_id, line_id):
        row = db.execute('SELECT * FROM einkauf_eingang_positionen WHERE eingang_id=? AND id=?', (_id(group_id), _id(line_id))).fetchone()
        if not row:
            raise ValueError('Position nicht gefunden.')
        return dict(row)

    @staticmethod
    def _file(db, group_id, file_id):
        row = db.execute('SELECT * FROM einkauf_eingang_dateien WHERE eingang_id=? AND id=?', (_id(group_id), _id(file_id))).fetchone()
        if not row:
            raise ValueError('Originalbeleg nicht gefunden.')
        return dict(row)

    @staticmethod
    def _file_metadata(db, group_id, file_id):
        row = db.execute('SELECT ' + FILE_METADATA + ' FROM einkauf_eingang_dateien WHERE eingang_id=? AND id=?',
                         (_id(group_id), _id(file_id))).fetchone()
        if not row:
            raise ValueError('Originalbeleg nicht gefunden.')
        return dict(row)

    @staticmethod
    def _lock(db, group_id, revision=None):
        # Acquire the parent write lock on both SQLite and PostgreSQL.
        db.execute('UPDATE einkauf_eingang SET revision=revision WHERE id=?', (_id(group_id),))
        row = MaterialIntake._group(db, group_id)
        if revision is not None and row['revision'] != _id(revision, 'Bearbeitungsstand'):
            raise IntakeConflict('Der Eingang wurde geändert. Bitte neu laden und die Zuordnung erneut prüfen.')
        return row

    @staticmethod
    def _bump(db, group_id):
        db.execute('UPDATE einkauf_eingang SET revision=revision+1 WHERE id=?', (group_id,))

    def create(self, payload, actor='admin'):
        actor = self._actor(actor)
        _keys(payload, {'supplier', 'source_key', 'external_ref', 'source_at', 'already_ordered', 'original_author', 'lines'})
        if type(payload.get('already_ordered')) is not bool:
            raise ValueError('Bereits extern bestellt muss ausdrücklich ja oder nein sein.')
        lines = payload.get('lines')
        if not isinstance(lines, list) or not 1 <= len(lines) <= 100:
            raise ValueError('Ein bis 100 Materialpositionen angeben.')
        record = {'supplier': _text(payload.get('supplier'), 'Lieferant', 200),
                  'source_key': _text(payload.get('source_key'), 'Quellenschlüssel', 200),
                  'external_ref': _text(payload.get('external_ref'), 'Quellenverweis', 500),
                  'source_at': _stamp(payload.get('source_at')), 'already_ordered': payload['already_ordered'],
                  'original_author': _text(payload.get('original_author'), 'Ursprünglicher Mitarbeiter', 200, True) or None, 'lines': []}
        for line in lines:
            _keys(line, {'product', 'sku', 'variant', 'unit', 'pack', 'quantity', 'urgent', 'category', 'original_author'})
            urgent = line.get('urgent')
            if urgent is not None and type(urgent) is not bool:
                raise ValueError('Dringlichkeit muss ja, nein oder ungeklärt sein.')
            category = line.get('category', 'ungeklaert')
            if category not in {'farbe_lack', 'material', 'ungeklaert'}:
                raise ValueError('Unbekannte Materialkategorie.')
            item = {key: _text(line.get(key), key, 500 if key in {'product', 'variant'} else 100, key != 'product')
                    for key in ('product', 'sku', 'variant', 'unit', 'pack')}
            item.update(quantity=_decimal(line.get('quantity'), 'Bestellte Menge', optional=True), urgent=urgent, category=category,
                        original_author=_text(line.get('original_author'), 'Ursprünglicher Mitarbeiter', 200, True) or None)
            if item['quantity'] is not None and not item['unit']:
                raise ValueError('Eine belegte Menge benötigt ihre Bestelleinheit.')
            record['lines'].append(item)
        fingerprint = _hash(record)
        with self.db() as db:
            created = db.execute('''INSERT INTO einkauf_eingang
                (source_key,payload_hash,supplier,external_ref,source_at,already_ordered,original_author,created_by,created_at)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING RETURNING id''',
                (record['source_key'], fingerprint, record['supplier'], record['external_ref'], record['source_at'],
                 int(record['already_ordered']), record['original_author'], actor, _now())).fetchone()
            group = dict(db.execute('SELECT * FROM einkauf_eingang WHERE source_key=?', (record['source_key'],)).fetchone())
            if group['payload_hash'] != fingerprint:
                raise IntakeConflict('Diese Quelle ist bereits mit anderen Angaben erfasst. Keine zweite Bestellung angelegt.')
            if created:
                for number, line in enumerate(record['lines'], 1):
                    db.execute('''INSERT INTO einkauf_eingang_positionen
                        (eingang_id,position,product,sku,variant,unit,pack,quantity,urgent,category,original_author)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)''', (group['id'], number, *(line[k] for k in ('product','sku','variant','unit','pack','quantity')),
                        None if line['urgent'] is None else int(line['urgent']), line['category'], line['original_author']))
        return self.detail(group['id'])

    @staticmethod
    def _file_view(row):
        return {key: row[key] for key in ('id', 'eingang_id', 'kind', 'original_name', 'mime', 'sha256',
                'created_at', 'created_by', 'extraction_status', 'draft_text')}

    def list(self, limit=100):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('Ungültige Listengröße.')
        with self.db() as db:
            rows = db.execute('''SELECT e.*, (SELECT COUNT(*) FROM einkauf_eingang_positionen p WHERE p.eingang_id=e.id) AS line_count,
                (SELECT COUNT(*) FROM einkauf_eingang_dateien f WHERE f.eingang_id=e.id) AS file_count
                FROM einkauf_eingang e ORDER BY source_at DESC,id DESC LIMIT ?''', (limit,)).fetchall()
        return [dict(dict(row), already_ordered=bool(row['already_ordered']), dispatchable=False) for row in rows]

    def detail(self, group_id):
        with self.db() as db:
            group = self._group(db, group_id)
            lines = [dict(row) for row in db.execute('SELECT * FROM einkauf_eingang_positionen WHERE eingang_id=? ORDER BY position', (group['id'],)).fetchall()]
            files = [self._file_view(dict(row)) for row in db.execute('SELECT ' + FILE_METADATA + ' FROM einkauf_eingang_dateien WHERE eingang_id=? ORDER BY id', (group['id'],)).fetchall()]
            deliveries = [dict(row) for row in db.execute('SELECT * FROM einkauf_eingang_lieferungen WHERE eingang_id=? ORDER BY id', (group['id'],)).fetchall()]
            prices = [dict(dict(row), data=json.loads(row['payload_json'])) for row in db.execute('SELECT * FROM einkauf_eingang_preise WHERE eingang_id=? ORDER BY id', (group['id'],)).fetchall()]
            clarifications = [dict(row) for row in db.execute('SELECT * FROM einkauf_eingang_klaerungen WHERE eingang_id=? ORDER BY id', (group['id'],)).fetchall()]
        categories = {key: {'amount': '0.00', 'complete': True, 'covered_lines': 0} for key in ('farbe_lack', 'material', 'ungeklaert')}
        for line in lines:
            line['urgent'] = None if line['urgent'] is None else bool(line['urgent'])
            events = [row for row in deliveries if row['line_id'] == line['id']]
            received = sum((Decimal(row['quantity']) for row in events), Decimal(0))
            line['delivered_quantity'] = format(received.normalize(), 'f')
            line['delivery_events'] = events
            ordered = Decimal(line['quantity']) if line['quantity'] is not None else None
            line['delivery_state'] = ('offen' if not events else 'liefermenge_belegt_bestellmenge_offen' if ordered is None
                                      else 'teillieferung' if received < ordered else 'geliefert' if received == ordered else 'mehrlieferung_pruefen')
            records = [row for row in prices if row['line_id'] == line['id']]
            line['prices'] = records
            identity = _line_identity(line, group['supplier'])
            line['plan_price'], plan_warnings = _latest_price(records, 'plan', identity)
            line['invoice_price'], invoice_warnings = _latest_price(records, 'invoice', identity)
            line['price_warnings'] = list(dict.fromkeys(plan_warnings + invoice_warnings))
            line['price_comparison'] = self.compare_prices(line['plan_price'], line['invoice_price'])
            price = line['plan_price']
            line['planned_total'] = None
            category = categories[line['category']]
            # Totals are deliberately gross EUR only; never sum unknown bases.
            if ordered is not None and price and price.get('tax_basis') == 'gross' and price.get('currency') == 'EUR' and _norm(price.get('unit')) == _norm(line['unit']) and _norm(price.get('pack')) == _norm(line['pack']):
                amount = (ordered * Decimal(price['amount'])).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                line['planned_total'] = format(amount, '.2f')
                category['amount'] = format(Decimal(category['amount']) + amount, '.2f')
                category['covered_lines'] += 1
                if not price.get('selection_complete', True):
                    category['complete'] = False
            else:
                category['complete'] = False
        group.update(already_ordered=bool(group['already_ordered']), dispatchable=False, lines=lines, files=files,
                     clarifications=clarifications,
                     totals={'currency': 'EUR', 'tax_basis': 'gross', 'categories': categories,
                             'known_subtotal': format(sum(Decimal(row['amount']) for row in categories.values()), '.2f'),
                             'complete': all(row['complete'] for row in categories.values()),
                             'label': 'Planwerte aus historischen Quellen; keine heutige Preiszusage oder Zahlungsbestätigung.'})
        return group

    def attach(self, group_id, file, kind, actor='admin'):
        actor = self._actor(actor)
        if kind not in FILE_KINDS:
            raise ValueError('Datei als Materialfoto, Lieferschein oder Rechnung einordnen.')
        if not file or not getattr(file, 'filename', ''):
            raise ValueError('Originaldatei auswählen.')
        raw = file.read(MAX_FILE_BYTES + 1)
        if not raw or len(raw) > MAX_FILE_BYTES:
            raise ValueError('Originaldatei leer oder größer als 8 MB.')
        mime, suffix = _image_or_pdf(raw)
        filename = str(file.filename).replace('\\', '/').rsplit('/', 1)[-1]
        filename = _text(filename, 'Dateiname', 240)
        digest = hashlib.sha256(raw).hexdigest()
        with self.db() as db:
            group = self._lock(db, group_id)
            existing = db.execute('SELECT ' + FILE_METADATA + ' FROM einkauf_eingang_dateien WHERE eingang_id=? AND sha256=?', (group['id'], digest)).fetchone()
            if existing:
                if existing['kind'] != kind:
                    raise IntakeConflict('Dieses Original ist bereits in einer anderen Belegart zugeordnet.')
                return self._file_view(dict(existing))
            row = db.execute('''INSERT INTO einkauf_eingang_dateien
                (eingang_id,kind,original_name,mime,suffix,sha256,original_base64,created_at,created_by)
                VALUES(?,?,?,?,?,?,?,?,?) RETURNING id''',
                (group['id'], kind, filename, mime, suffix, digest, base64.b64encode(raw).decode('ascii'), _now(), actor)).fetchone()
            self._bump(db, group['id'])
            return self._file_view(self._file_metadata(db, group['id'], row['id']))

    def original(self, group_id, file_id):
        with self.db() as db:
            row = self._file(db, group_id, file_id)
        raw = base64.b64decode(row['original_base64'], validate=True)
        if not raw or len(raw) > MAX_FILE_BYTES or hashlib.sha256(raw).hexdigest() != row['sha256']:
            raise ValueError('Originaldatei konnte nicht verifiziert werden.')
        return raw, row['mime'], row['original_name']

    def analyze_file(self, group_id, file_id, reader=None, reuse_success=False):
        with self.db() as db:
            row = self._file_metadata(db, group_id, file_id)
        if reuse_success and row['extraction_status'] == 'pruefen' and row['draft_text'].strip():
            return self._file_view(row)
        raw, _, _ = self.original(group_id, file_id)
        reader = reader or getattr(self.p, 'extract_document_text_local', None)
        if not callable(reader):
            raise ValueError('Lokale Belegauslese ist nicht verfügbar.')
        text, state = '', 'pruefen'
        try:
            with tempfile.TemporaryDirectory(prefix='werkstatt-eingang-') as directory:
                path = Path(directory) / ('original' + row['suffix'])
                path.write_bytes(raw)
                extracted = reader(path, row['original_name'])
                if isinstance(extracted, str):
                    text = '\n'.join(line for line in extracted.splitlines() if not _has_bank_data(line) and not _SECRET.search(line))[:24000]
                    text = ''.join(c for c in text if (ord(c) >= 32 or c in '\n\t') and not 0xD800 <= ord(c) <= 0xDFFF)
                if not text.strip():
                    state = 'keine_auslese'
        except TimeoutError:
            state = 'zeitlimit'
        except Exception:
            state = 'fehler'
        with self.db() as db:
            db.execute('UPDATE einkauf_eingang_dateien SET draft_text=?,extraction_status=? WHERE id=? AND eingang_id=?',
                       (text, state, row['id'], row['eingang_id']))
            return self._file_view(self._file_metadata(db, group_id, file_id))

    def record_delivery(self, group_id, payload, actor='admin'):
        from werkstatt_liefereingang import document_position
        actor = self._actor(actor)
        _keys(payload, {'revision', 'line_id', 'file_id', 'position', 'quantity', 'unit'})
        _id(payload.get('revision'), 'Bearbeitungsstand')
        record = {'line_id': _id(payload.get('line_id')), 'file_id': _id(payload.get('file_id')),
                  'position': document_position(payload.get('position')),
                  'quantity': _decimal(payload.get('quantity'), 'Gelieferte Menge'),
                  'unit': _text(payload.get('unit'), 'Liefereinheit', 100)}
        digest = _hash(record)
        with self.db() as db:
            group = self._lock(db, group_id)
            line = self._line(db, group_id, record['line_id'])
            file = self._file(db, group_id, record['file_id'])
            if file['kind'] != 'lieferschein':
                raise ValueError('Liefermengen benötigen einen ausdrücklich zugeordneten Lieferschein.')
            if not line['unit'] or _norm(record['unit']) != _norm(line['unit']):
                raise ValueError('Liefereinheit stimmt nicht eindeutig mit der bestellten Einheit überein.')
            record['unit'] = line['unit']
            digest = _hash(record)
            old = db.execute('SELECT payload_hash FROM einkauf_eingang_lieferungen WHERE file_id=? AND position=?', (record['file_id'], record['position'])).fetchone()
            if old:
                if old['payload_hash'] != digest:
                    raise IntakeConflict('Diese Lieferscheinposition ist bereits anders zugeordnet. Keine doppelte Lieferbuchung.')
            else:
                self._lock(db, group_id, payload.get('revision'))
                db.execute('''INSERT INTO einkauf_eingang_lieferungen
                    (eingang_id,line_id,file_id,position,quantity,unit,payload_hash,created_at,created_by)
                    VALUES(?,?,?,?,?,?,?,?,?)''', (group['id'], record['line_id'], record['file_id'], record['position'],
                    record['quantity'], record['unit'], digest, _now(), actor))
                self._bump(db, group_id)
        return self.detail(group_id)

    def catalog_candidates(self, group_id, line_id):
        with self.db() as db:
            group = self._group(db, group_id)
            line = self._line(db, group_id, line_id)
        identity = _line_identity(line, group['supplier'])
        catalog = getattr(getattr(self.p, 'cockpit_data', None), 'catalog', None)
        if not catalog:
            raise ValueError('Rechnungsartikelkatalog ist nicht verfügbar.')
        snapshot = catalog.knowledge_rows(limit=5000)
        matches = []
        for row in snapshot.get('items', []):
            if not identity['sku'] or not identity['unit'] or _catalog_identity(row) != identity:
                continue
            price = parse_unit_price(row.get('historischer_preishinweis'))
            if price is not None and price > Decimal('10000000'):
                price = None
            source = row.get('quelle') or {}
            raw_date = source.get('datum')
            try:
                date = datetime.strptime(raw_date, '%Y-%m-%d').date().isoformat() if isinstance(raw_date, str) else None
            except ValueError:
                date = None
            evidence = _price_evidence(row.get('price_evidence'))
            matches.append({'proposal_id': _id(row.get('vorschlag_id')), 'identity': identity,
                            'amount': format(price, 'f') if price is not None else None, 'date': date, 'source': source,
                            'evidence': evidence, 'currency': evidence.get('currency') or 'unknown', 'unit': line['unit'], 'pack': line['pack'],
                            'tax_basis': evidence.get('tax_basis') or ('net' if evidence['basis'] == 'gebindepreis_netto_abgeleitet' else 'unknown'),
                            'tax_rate': evidence.get('tax_rate'), 'verified': False, 'label': 'Historischer Preishinweis – Auslese ungeprüft.'})
        matches.sort(key=lambda row: (row['date'] or '', row['proposal_id']), reverse=True)
        warnings = ['Historische Auslese ist keine heutige Lieferantenpreisfreigabe.']
        suggested = matches[0]['proposal_id'] if matches and matches[0]['date'] else None
        if any(not row['date'] for row in matches):
            warnings.append('Mindestens eine Quelle hat kein belegtes Rechnungsdatum; neuester Preis nicht vollständig bestimmbar.')
            suggested = None
        if suggested:
            latest = [row for row in matches if row['date'] == matches[0]['date']]
            if any(row['amount'] is None for row in latest):
                suggested = None
                warnings.append('Am jüngsten Rechnungsdatum ist ein Preis ungeklärt; kein älterer Preis wird vorgeschlagen.')
            if len({_json({key: row[key] for key in ('amount', 'tax_basis', 'evidence')}) for row in latest}) > 1:
                suggested = None
                warnings.append('Am jüngsten Belegdatum stehen unterschiedliche Preisangaben; Quelle prüfen.')
        if snapshot.get('truncated'):
            warnings.append('Katalogabfrage begrenzt; es können neuere Quellen fehlen.')
            suggested = None
        return {'matches': matches[:100], 'suggested_id': suggested, 'warnings': warnings, 'identity': identity}

    def _save_price(self, db, group, line, record, source_key, revision, actor):
        digest = _hash(record)
        if record['role'] == 'invoice':
            assigned = db.execute("SELECT line_id FROM einkauf_eingang_preise WHERE eingang_id=? AND role='invoice' AND source_key=?", (group['id'], source_key)).fetchone()
            if assigned and assigned['line_id'] != line['id']:
                raise IntakeConflict('Diese Rechnungsposition ist bereits einer anderen Artikelposition zugeordnet.')
        old = db.execute('SELECT payload_hash FROM einkauf_eingang_preise WHERE line_id=? AND role=? AND source_key=?',
                         (line['id'], record['role'], source_key)).fetchone()
        if old:
            if old['payload_hash'] != digest:
                raise IntakeConflict('Diese Preisquelle ist bereits mit anderen Angaben zugeordnet.')
            return
        self._lock(db, group['id'], revision)
        db.execute('''INSERT INTO einkauf_eingang_preise
            (eingang_id,line_id,role,source_key,payload_hash,payload_json,created_at,created_by)
            VALUES(?,?,?,?,?,?,?,?)''', (group['id'], line['id'], record['role'], source_key, digest, _json(record), _now(), actor))
        self._bump(db, group['id'])

    def set_catalog_price(self, group_id, line_id, payload, actor='admin'):
        actor = self._actor(actor)
        _keys(payload, {'revision', 'proposal_id'})
        _id(payload.get('revision'), 'Bearbeitungsstand')
        proposed_id = _id(payload.get('proposal_id'))
        candidates = self.catalog_candidates(group_id, line_id)
        selected = next((row for row in candidates['matches'] if row['proposal_id'] == proposed_id), None)
        if not selected:
            raise ValueError('Diese aktuelle Rechnungsquelle passt nicht exakt zu Lieferant, Artikel, Variante und Einheit.')
        if selected['amount'] is None:
            raise ValueError('Diese historische Preisquelle ist ungeklärt. Zuerst am Original prüfen.')
        record = dict(selected, role='plan', source_type='catalog',
                      warnings=candidates['warnings'], latest_by_date=candidates['suggested_id'] == proposed_id,
                      discount_basis=selected['evidence'].get('calculation') or 'unknown')
        with self.db() as db:
            group = self._lock(db, group_id)
            line = self._line(db, group_id, line_id)
            if _line_identity(line, group['supplier']) != record['identity']:
                raise IntakeConflict('Artikelzuordnung wurde geändert.')
            self._save_price(db, group, line, record, 'catalog:' + str(proposed_id), payload.get('revision'), actor)
        return self.detail(group_id)

    def record_price(self, group_id, payload, actor='admin'):
        actor = self._actor(actor)
        _keys(payload, {'revision', 'line_id', 'file_id', 'position', 'page', 'role', 'amount', 'unit', 'pack',
                        'currency', 'tax_basis', 'tax_rate', 'discount_basis', 'source_date', 'reviewed', 'identity'})
        _id(payload.get('revision'), 'Bearbeitungsstand')
        if payload.get('reviewed') is not True:
            raise ValueError('Preis, Artikelidentität und Preisbasis am Original ausdrücklich prüfen.')
        if payload.get('role') not in {'plan', 'invoice'}:
            raise ValueError('Preis als historische Planung oder neue Rechnung einordnen.')
        if payload.get('currency') != 'EUR' or payload.get('tax_basis') not in {'net', 'gross'}:
            raise ValueError('Belegten EUR-Preis und Netto-/Bruttobasis angeben.')
        try:
            source_date = datetime.strptime(payload.get('source_date', ''), '%Y-%m-%d').date().isoformat()
        except (TypeError, ValueError) as exc:
            raise ValueError('Belegdatum fehlt oder ist ungültig.') from exc
        record = {'role': payload['role'], 'source_type': 'original', 'file_id': _id(payload.get('file_id')),
                  'position': _id(payload.get('position')), 'page': _id(payload.get('page')),
                  'amount': _decimal(payload.get('amount'), 'Einheitspreis', zero=True, maximum='10000000'),
                  'unit': _text(payload.get('unit'), 'Preiseinheit', 100), 'pack': _text(payload.get('pack'), 'Packinhalt', 100, True),
                  'currency': 'EUR', 'tax_basis': payload['tax_basis'],
                  'tax_rate': _decimal(payload.get('tax_rate'), 'Steuersatz', zero=True, maximum='100', optional=True),
                  'discount_basis': _text(payload.get('discount_basis'), 'Rabattbasis (z.B. nach Rabatt)', 150),
                  'date': source_date, 'verified': True,
                  'label': 'Am Original geprüfter historischer Vergleichspreis.' if payload['role'] == 'plan' else 'Am Rechnungsoriginal geprüfter Preis.'}
        with self.db() as db:
            group = self._lock(db, group_id)
            line = self._line(db, group_id, payload.get('line_id'))
            file = self._file(db, group_id, record['file_id'])
            if file['kind'] != 'rechnung':
                raise ValueError('Ein belegter Preis benötigt ein Rechnungsoriginal. Lieferscheine allein belegen keine Preise.')
            identity = _line_identity(line, group['supplier'])
            incoming = payload.get('identity')
            if not isinstance(incoming, dict) or set(incoming) != set(identity) or any(not isinstance(v, str) for v in incoming.values()):
                raise ValueError('Alle Artikelmerkmale am Preisbeleg bestätigen.')
            if {key: _norm(value) for key, value in incoming.items()} != identity or not identity['sku'] or not identity['unit']:
                raise ValueError('Preisbeleg und Bestellartikel sind nicht eindeutig identisch.')
            if _norm(record['unit']) != identity['unit'] or _norm(record['pack']) != identity['pack']:
                raise ValueError('Preis pro Stück/Packung passt nicht zur Bestelleinheit.')
            record.update(identity=identity, unit=line['unit'], pack=line['pack'], supplier=group['supplier'], source_sha256=file['sha256'],
                          source_reference=file['original_name'])
            self._save_price(db, group, line, record, 'original:' + str(file['id']) + ':' + str(record['page']) + ':' + str(record['position']), payload.get('revision'), actor)
        return self.detail(group_id)

    def update_line(self, group_id, line_id, payload, actor='admin'):
        """Explicit clarification, with immutable before/after audit evidence."""
        actor = self._actor(actor)
        fields = {'product', 'sku', 'variant', 'unit', 'pack', 'quantity', 'urgent', 'category', 'original_author'}
        _keys(payload, fields | {'revision', 'reviewed', 'reason'})
        if payload.get('reviewed') is not True:
            raise ValueError('Geänderte Artikelangaben ausdrücklich prüfen.')
        revision = _id(payload.get('revision'), 'Bearbeitungsstand')
        reason = _text(payload.get('reason'), 'Quelle/Grund der Klärung', 500)
        with self.db() as db:
            group = self._lock(db, group_id, revision)
            line = self._line(db, group_id, line_id)
            before = {key: line[key] for key in fields}
            after = dict(before)
            for key in fields & set(payload):
                value = payload[key]
                if key in {'product', 'sku', 'variant', 'unit', 'pack', 'original_author'}:
                    after[key] = _text(value, key, 500 if key in {'product', 'variant'} else 200 if key == 'original_author' else 100, key != 'product')
                    if key == 'original_author' and not after[key]:
                        after[key] = None
                elif key == 'quantity':
                    after[key] = _decimal(value, 'Bestellte Menge', optional=True)
                elif key == 'urgent':
                    if value is not None and type(value) is not bool:
                        raise ValueError('Dringlichkeit muss ja, nein oder ungeklärt sein.')
                    after[key] = None if value is None else int(value)
                elif key == 'category':
                    if value not in {'farbe_lack', 'material', 'ungeklaert'}:
                        raise ValueError('Unbekannte Materialkategorie.')
                    after[key] = value
            if after['quantity'] is not None and not after['unit']:
                raise ValueError('Eine belegte Menge benötigt ihre Bestelleinheit.')
            if _line_identity(before, group['supplier']) != _line_identity(after, group['supplier']):
                if db.execute('SELECT 1 FROM einkauf_eingang_preise WHERE line_id=? LIMIT 1', (line['id'],)).fetchone() or db.execute('SELECT 1 FROM einkauf_eingang_lieferungen WHERE line_id=? LIMIT 1', (line['id'],)).fetchone():
                    raise IntakeConflict('Die Artikelidentität hat bereits Belegzuordnungen und kann nicht still geändert werden.')
            if before != after:
                ordered_fields = sorted(fields)
                db.execute('UPDATE einkauf_eingang_positionen SET '+','.join(key+'=?' for key in ordered_fields)+' WHERE id=? AND eingang_id=?',
                           (*(after[key] for key in ordered_fields), line['id'], group['id']))
                db.execute('''INSERT INTO einkauf_eingang_klaerungen
                    (eingang_id,line_id,before_json,after_json,reason,created_at,created_by) VALUES(?,?,?,?,?,?,?)''',
                    (group['id'], line['id'], _json(before), _json(after), reason, _now(), actor))
                self._bump(db, group_id)
        return self.detail(group_id)

    @staticmethod
    def compare_prices(plan, invoice):
        if not plan or not invoice:
            return {'comparable': False, 'reason': 'Plan- oder Rechnungspreis fehlt.'}
        fields = ('identity', 'unit', 'pack', 'currency', 'tax_basis', 'tax_rate', 'discount_basis')
        if (plan.get('tax_basis') not in {'net', 'gross'} or plan.get('tax_rate') is None or
                plan.get('discount_basis') in {None, '', 'unknown'} or any(plan.get(k) != invoice.get(k) for k in fields)):
            return {'comparable': False, 'reason': 'Identität, Einheit, Packung, Steuer- oder Rabattbasis nicht gleich belegt.'}
        first, actual = Decimal(plan['amount']), Decimal(invoice['amount'])
        difference = actual - first
        return {'comparable': True, 'difference': format(difference.normalize(), 'f'),
                'percent': format((difference / first * 100).quantize(Decimal('.01')), '.2f') if first else None,
                'currency': plan['currency'], 'tax_basis': plan['tax_basis'], 'cause': None}


HistoricalIntake = MaterialIntake


def register_intake(p):
    if 'werkstatt_intake' in p.app.extensions:
        return p.app.extensions['werkstatt_intake']
    service = MaterialIntake(p)
    bp = Blueprint('werkstatt_intake', __name__, url_prefix='/admin/assistent-bestellungen/eingang')

    @bp.before_request
    def guard():
        if not session.get('admin'):
            abort(403)
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            expected = session.get('csrf_token')
            supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token')
            if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
                abort(400, description='Die Seite ist veraltet. Bitte erneut öffnen.')

    @bp.after_request
    def private(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @bp.errorhandler(IntakeConflict)
    def conflict(exc):
        return jsonify(error=str(exc)), 409

    @bp.errorhandler(ValueError)
    def invalid(exc):
        return jsonify(error=str(exc)), 400

    @bp.route('', methods=['GET', 'POST'])
    def index():
        return jsonify(service.create(request.get_json(silent=True))) if request.method == 'POST' else jsonify(items=service.list())

    @bp.get('/<int:group_id>')
    def detail(group_id):
        return jsonify(service.detail(group_id))

    @bp.post('/<int:group_id>/dateien')
    def attach(group_id):
        return jsonify(service.attach(group_id, request.files.get('file'), request.form.get('kind')))

    @bp.get('/<int:group_id>/dateien/<int:file_id>/original')
    def original(group_id, file_id):
        raw, mime, filename = service.original(group_id, file_id)
        return send_file(io.BytesIO(raw), mimetype=mime, as_attachment=True, download_name=filename, max_age=0)

    @bp.post('/<int:group_id>/dateien/<int:file_id>/analyse')
    def analyze(group_id, file_id):
        return jsonify(service.analyze_file(group_id, file_id))

    @bp.post('/<int:group_id>/lieferungen')
    def delivery(group_id):
        return jsonify(service.record_delivery(group_id, request.get_json(silent=True)))

    @bp.get('/<int:group_id>/positionen/<int:line_id>/katalog')
    def candidates(group_id, line_id):
        return jsonify(service.catalog_candidates(group_id, line_id))

    @bp.post('/<int:group_id>/positionen/<int:line_id>/katalogpreis')
    def catalog_price(group_id, line_id):
        return jsonify(service.set_catalog_price(group_id, line_id, request.get_json(silent=True)))

    @bp.post('/<int:group_id>/preise')
    def price(group_id):
        return jsonify(service.record_price(group_id, request.get_json(silent=True)))

    @bp.post('/<int:group_id>/positionen/<int:line_id>/klaeren')
    def clarify(group_id, line_id):
        return jsonify(service.update_line(group_id, line_id, request.get_json(silent=True)))

    p.app.register_blueprint(bp)
    p.app.extensions['werkstatt_intake'] = service
    p.workshop_intake = service
    p.workshop_intake_init_schema = service.init_schema
    return service
