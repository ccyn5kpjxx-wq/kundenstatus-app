"""Owned upload, order-data and quotation proposals for the avatar.

Models can prepare immutable proposals only. Human confirmation is performed by
the existing authenticated, CSRF-protected action routes in werkstatt_assistent.
"""
import hashlib
import io
import json
import secrets

from flask import abort, jsonify, request, send_file
from werkstatt_assistent_auftrag import WorkshopOrderActions, OrderActionError, CREATE_FIELDS
from werkstatt_assistent_uploads import AssistantUploads
from werkstatt_assistent_angebote import OfferService


DOCUMENT_KINDS = frozenset({'farbe', 'kontakt', 'auftrag_neu', 'datei'})
MAIL_KINDS = frozenset({'lieferantenanfrage', 'kundenangebot'})
KINDS = DOCUMENT_KINDS | MAIL_KINDS
PROPOSAL_TOOLS = {'farbton_vorschlagen': 'farbe', 'kontakt_vorschlagen': 'kontakt',
                  'auftrag_vorschlagen': 'auftrag_neu', 'datei_zuordnung_vorschlagen': 'datei',
                  'angebotsanfrage_vorschlagen': 'lieferantenanfrage',
                  'kundenangebot_vorschlagen': 'kundenangebot'}
READ_TOOLS = frozenset({'unterlagen_lesen', 'angebote_lesen'})
TOOLS = []


def _tool(name, description, properties, required):
    # These are partial edits: omitted fields must stay omitted. The server
    # validates the allowlist and actual required values before every preview.
    TOOLS.append({'type': 'function', 'name': name, 'description': description, 'strict': False,
                  'parameters': {'type': 'object', 'properties': properties,
                                 'required': required, 'additionalProperties': False}})


ORDER = {'auftrag_id': {'type': 'integer', 'minimum': 1}}
for name, fields, description in [
    ('farbton_vorschlagen', ('farbcode', 'farbton', 'farbton_2'), 'Diktierten Farbcode oder Farbton im konkreten Auftrag ergänzen. Nur ausdrücklich genannte Felder angeben, nicht raten. App zeigt vorher/nachher zur Bestätigung.'),
    ('kontakt_vorschlagen', ('kunde_name', 'kunde_email', 'kontakt_telefon'), 'Explizit genannte Kundenkontaktdaten zur separaten Prüfung vorbereiten. Fehlende E-Mail für späteres Kundenangebot ergänzen, keine Nachricht senden.'),
    ('auftrag_vorschlagen', tuple(CREATE_FIELDS), 'Neuen Auftrag nach Prüfung vorbereiten. Angaben aus eigener Unterlage per unterlagen_lesen lesen. Kunde und Halter können verschieden sein. Keine Auftragsnummer erfinden; Auftrag erhält sie beim Speichern. Kein Angebot/Versand automatisch.')]:
    properties = {'felder': {'type': 'object', 'properties': {k: {'type': 'string'} for k in fields}, 'additionalProperties': False}}
    properties.update({'upload_id': {'type': 'string'}} if name == 'auftrag_vorschlagen' else ORDER)
    _tool(name, description, properties, ['felder'] if name == 'auftrag_vorschlagen' else ['auftrag_id', 'felder'])
_tool('bild_anfordern', 'Zeigt den Knopf zum Bild/PDF senden. Datei muss der Mitarbeiter selbst auswählen. Auftrag 0 für Neuanlage. Es wird noch nichts einem Auftrag zugeordnet.', {'auftrag_id': {'type': 'integer', 'minimum': 0}}, ['auftrag_id'])
_tool('unterlagen_lesen', 'Eigene hochgeladene Dateien, geprüfte Feldervorschläge und Quellen lesen. Keine fremden Uploads. Leere upload_id listet letzte eigene Uploads; unsichere Auslese nicht als bestätigt darstellen.', {'upload_id': {'type': 'string'}}, [])
_tool('datei_zuordnung_vorschlagen', 'Eigene hochgeladene Datei einem eindeutig genannten Auftrag zuordnen, nach separater Bestätigung. Fahrzeugdaten werden dabei nicht automatisch geändert.', {**ORDER, 'upload_id': {'type': 'string'}}, ['auftrag_id', 'upload_id'])
_tool('angebote_lesen', 'Nur diesem Auftrag bereits zugeordnete Angebote mit Quelle lesen; Einkaufspreise nicht automatisch zu Kundenverkaufspreisen machen.', ORDER, ['auftrag_id'])
for name, kind in [('angebotsanfrage_vorschlagen', 'lieferantenanfrage'), ('kundenangebot_vorschlagen', 'kundenangebot')]:
    props = {**ORDER, 'text': {'type': 'string'}, 'attachment_ids': {'type': 'array', 'items': {'type': 'integer'}}}
    props.update({'supplier_id': {'type': 'string'}} if kind == 'lieferantenanfrage' else {'gesamt_brutto': {'type': 'string'}})
    _tool(name, ('Unverbindliche Anfrage an verifizierten Lieferantenkontakt vorbereiten; Lieferant mit lieferanten_lesen eindeutig wählen. Keine Bestellung.' if kind == 'lieferantenanfrage' else 'Kundenangebot an gespeicherte Kunden-E-Mail vorbereiten. Verkaufspreis gesamt_brutto ausdrücklich erfragen, nicht aus Lieferantenangebot erfinden. Keine Bestellung und keine Kundenannahme.') + ' Vollständige Mail mit Empfänger, Text und ausdrücklich ausgewählten erlaubten Anhängen wird separat bestätigt. Fahrzeugschein nie automatisch weiterleiten.', props, ['auftrag_id', 'text'])


RULES = (
    'Interne Auftragsnummer ist id (z.B. Auftrag 402); auftragsnummer ist nur eine fremde Referenz. '
    'Nach ausdrücklich diktiertem Farbcode farbton_vorschlagen verwenden; fehlenden Farbcode klar als fehlend nennen. '
    'Bei Bild senden oder Fahrzeugschein bild_anfordern, danach eigene unterlagen_lesen. '
    'Für bestehenden Auftrag datei_zuordnung_vorschlagen, für Neuanlage Auftragdaten und Upload gemeinsam mit auftrag_vorschlagen vorbereiten. '
    'Auslese ist unsicher: Kunde/Halter, Kennzeichen/FIN und Fahrzeug vor Bestätigung prüfen; keine Arbeiten aus Fahrzeugschein ableiten. '
    'Kundenkontakt auf ausdrückliche Angabe mit kontakt_vorschlagen ergänzen. '
    'Unverbindliche Lieferantenanfrage mit angebotsanfrage_vorschlagen, ausdrücklich bepreistes Kundenangebot mit kundenangebot_vorschlagen vorbereiten. '
    'Lieferantenangebot ist keine Kundenkalkulation: Verkaufspreis und Umfang separat klären. '
    'Keine fehlenden Empfänger oder Preise erfinden. Vorschläge werden von der App vollständig angezeigt; erst menschliche Bestätigung speichert/sendet. '
)


class Workflow:
    def __init__(self, p, bp, protected, capabilities, order_context, db_scope, audit):
        self.p, self.caps, self.order, self.db, self.audit = p, capabilities, order_context, db_scope, audit
        p.workshop_order_actions = WorkshopOrderActions(p)
        p.assistant_uploads = AssistantUploads(p)
        p.assistant_offers = OfferService(p, attachment_resolver=p.assistant_uploads.attachment_resolver)

        @bp.route('/unterlagen', methods=['GET', 'POST'])
        @protected
        def workflow_uploads(who):
            self.require(who, 'auftrag')
            if request.method == 'GET':
                return jsonify(p.assistant_uploads.list(who['actor'], limit=20))
            return jsonify(p.assistant_uploads.stage(who['actor'], request.files.get('file'), request.form.get('request_id'), request.form.get('purpose', 'sonstiges')))

        @bp.post('/unterlagen/<upload_id>/analyse')
        @protected
        def workflow_analysis(who, upload_id):
            self.require(who, 'auftrag')
            return jsonify(p.assistant_uploads.analyze(who['actor'], upload_id))

        @bp.get('/unterlagen/<upload_id>/original')
        @protected
        def workflow_original(who, upload_id):
            self.require(who, 'auftrag')
            content, mime, name = p.assistant_uploads.read_content(who['actor'], upload_id)
            response = send_file(io.BytesIO(content), mimetype=mime, download_name=name, as_attachment=not mime.startswith('image/'), max_age=0)
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['Cache-Control'] = 'private, no-store'
            return response

        @bp.get('/lieferanten')
        @protected
        def workflow_suppliers(who):
            self.require(who, 'angebote')
            return jsonify([item for row in p.workshop_orders.contacts()
                            if (item := p.workshop_orders.resolve_supplier(row['id'])) and item['verified']])

        @bp.get('/angebote/<int:order_id>')
        @protected
        def workflow_offers(who, order_id):
            self.require(who, 'angebote')
            self.order(order_id)
            return jsonify(p.assistant_offers.source_offers(who['actor'], order_id))

    def require(self, who, capability):
        if not self.caps(who).get(capability):
            raise ValueError('Diese Funktion ist für deinen Zugang nicht freigeschaltet.')

    def available_names(self, who):
        names = set()
        if self.caps(who).get('auftrag'):
            names |= {'farbton_vorschlagen', 'kontakt_vorschlagen', 'auftrag_vorschlagen', 'datei_zuordnung_vorschlagen', 'bild_anfordern', 'unterlagen_lesen'}
        if self.caps(who).get('angebote'):
            names |= {'angebotsanfrage_vorschlagen', 'kundenangebot_vorschlagen', 'angebote_lesen'}
        return names

    def read(self, who, name, args):
        if name == 'unterlagen_lesen':
            self.require(who, 'auftrag')
            return self.p.assistant_uploads.get(who['actor'], args['upload_id']) if args.get('upload_id') else self.p.assistant_uploads.list(who['actor'])
        self.require(who, 'angebote')
        self.order(args.get('auftrag_id'))
        return self.p.assistant_offers.source_offers(who['actor'], args['auftrag_id'])

    def proposal(self, who, args):
        kind = args.get('art')
        self.require(who, 'auftrag' if kind in DOCUMENT_KINDS else 'angebote')
        order_id = 0 if kind == 'auftrag_neu' else args.get('auftrag_id')
        if kind != 'auftrag_neu':
            order = self.order(order_id)
        try:
            if kind in {'farbe', 'kontakt', 'auftrag_neu'}:
                fields = args.get('felder')
                if not isinstance(fields, dict):
                    raise ValueError('Felder als Objekt angeben.')
                source = None
                if kind == 'auftrag_neu':
                    if args.get('upload_id'):
                        source = self.p.assistant_uploads.get(who['actor'], args['upload_id'])
                        if source.get('auftrag_id'):
                            raise ValueError('Diese Unterlage gehört bereits zu einem Auftrag. Den vorhandenen Auftrag verwenden.')
                        if source.get('status') != 'pruefen':
                            raise ValueError('Bitte die Dateiauswertung zuerst abschließen und prüfen.')
                    preview = self.p.workshop_order_actions.preview_create(fields, who, source_id=source['id'] if source else None)
                else:
                    preview = getattr(self.p.workshop_order_actions, 'preview_color' if kind == 'farbe' else 'preview_contact')(order_id, fields, who)
                payload = {'auftrag': preview, 'text': preview['zusammenfassung']}
                if source:
                    payload['text'] += ' Zugeordnete Unterlage: ' + source['original_name'] + '.'
            elif kind == 'datei':
                source = self.p.assistant_uploads.get(who['actor'], args.get('upload_id'))
                if source.get('auftrag_id') and source['auftrag_id'] != order_id:
                    raise ValueError('Diese Datei ist bereits einem anderen Auftrag zugeordnet.')
                if source.get('status') not in {'pruefen', 'zugeordnet'}:
                    raise ValueError('Bitte die Dateiauswertung zuerst abschließen und prüfen.')
                payload = {'upload_id': source['id'], 'original_name': source['original_name'],
                           'text': f"Datei {source['original_name']} intern zu Auftrag {order_id}, {order.get('fahrzeug')}, {order.get('kennzeichen') or 'ohne Kennzeichen'} hinzufügen. Fahrzeugdaten bleiben unverändert. Keine externe Freigabe."}
            elif kind in MAIL_KINDS:
                from werkstatt_assistent import cents
                total = cents(args['gesamt_brutto']) if args.get('gesamt_brutto') is not None else None
                payload = self.p.assistant_offers.prepare(who['actor'], kind, order_id, args.get('text'),
                    supplier_id=args.get('supplier_id'), gross_total_cents=total, attachment_ids=args.get('attachment_ids', []))
            else:
                raise ValueError('Unbekannte Aktion.')
        except OrderActionError as exc:
            raise ValueError(str(exc)) from None
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        fingerprint = hashlib.sha256(f"{who['actor']}:{order_id}:{kind}:{serialized}".encode()).hexdigest()
        with self.db() as db:
            if kind == 'auftrag_neu':
                # A later visit by a returned/archived vehicle is a new intake.
                # Stable chaining still deduplicates simultaneous proposals and
                # HTTP retries without resetting any previous confirmation.
                while True:
                    prior = db.execute('SELECT a.id,a.status,o.archiviert,o.status AS order_status FROM assistent_aktionen a LEFT JOIN auftraege o ON o.id=a.auftrag_id WHERE a.fingerprint=?', (fingerprint,)).fetchone()
                    if not prior or prior['status'] != 'dokumentiert' or (not prior['archiviert'] and prior['order_status'] != 5):
                        break
                    fingerprint = hashlib.sha256((fingerprint + ':' + prior['id'] + ':new-intake').encode()).hexdigest()
            db.execute('INSERT INTO assistent_aktionen(id,actor,auftrag_id,art,payload,fingerprint,erstellt_am) VALUES(?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING',
                       (secrets.token_hex(16), who['actor'], order_id, kind, serialized, fingerprint, self.p.now_str()))
            row = db.execute('SELECT * FROM assistent_aktionen WHERE fingerprint=?', (fingerprint,)).fetchone()
            self.audit(db, who, order_id or None, 'vorschlag', row['id'])
        return row

    def confirm(self, who, row):
        kind, payload = row['art'], json.loads(row['payload'])
        self.require(who, 'auftrag' if kind in DOCUMENT_KINDS else 'angebote')
        if kind in MAIL_KINDS:
            if payload.get('missing_fields'):
                raise ValueError('Mail noch unvollständig. Fehlende Angaben ergänzen und eine neue Vorschau vorbereiten.')
            with self.db() as db:
                changed = db.execute("UPDATE assistent_aktionen SET status='intern_freigegeben' WHERE id=? AND status='vorschlag'", (row['id'],)).rowcount
                if changed:
                    self.audit(db, who, row['auftrag_id'], 'angebotsmail_bestaetigt', row['id'])
            result = self.p.assistant_offers.submit_approved_action(who['actor'], row['id'])
            if result.get('state') == 'blocked':
                # Rejected before durable outbox acceptance: a later attempt
                # needs a fresh human approval of this exact stored preview.
                with self.db() as db:
                    db.execute('UPDATE assistent_aktionen SET status=status WHERE id=?', (row['id'],))
                    accepted = db.execute('SELECT token FROM mailbox_outbox WHERE token=?',
                        (self.p.assistant_offers._token(who['actor'], row['id']),)).fetchone()
                    if not accepted:
                        db.execute("UPDATE assistent_aktionen SET status='vorschlag' WHERE id=? AND status='intern_freigegeben'", (row['id'],))
            return {'ok': True, 'status': result.get('state'), 'hinweis': result.get('message'), 'versandstatus': result}
        try:
            if kind == 'datei':
                self.order(row['auftrag_id'])
                result = self.p.assistant_uploads.attach(who['actor'], payload['upload_id'], row['auftrag_id'], confirmed=True)
            else:
                preview = payload['auftrag']
                source_id = preview.get('source_id')
                def attach_source(db, order_id):
                    return self.p.assistant_uploads.attach(who['actor'], source_id, order_id, confirmed=True, db=db)
                result = self.p.workshop_order_actions.confirm(preview, who, 'assistant:' + row['id'],
                    attach_source=attach_source if source_id else None)
        except OrderActionError as exc:
            raise ValueError(str(exc)) from None
        order_id = result['auftrag_id']
        with self.db() as db:
            changed = db.execute("UPDATE assistent_aktionen SET status='dokumentiert',auftrag_id=? WHERE id=? AND status='vorschlag'", (order_id, row['id'])).rowcount
            if changed:
                self.audit(db, who, order_id, kind + '_gespeichert', row['id'])
        return {'ok': True, 'status': 'dokumentiert', 'hinweis': f'Auftrag {order_id}: gespeichert. Keine E-Mail versendet.',
                'auftrag': self.order(order_id), 'neuer_auftrag': kind == 'auftrag_neu', 'wiederholt': result.get('wiederholt', result.get('duplicate', False))}
