"""Customer signs the booked terms before Checkout; verified payment freezes the PDF."""

from base64 import b64decode, b64encode
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from PIL import Image as PillowImage, UnidentifiedImageError
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Image, KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


LESSOR_NAME = 'Gärtner GmbH Karosserie + Lack'
LESSOR_ADDRESS = 'Binauer Höhe 4, 74821 Mosbach, Deutschland'
OFFER_NAME = 'Autovermietung MOS'


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_contracts (
        hold_id TEXT PRIMARY KEY, contract_json TEXT NOT NULL, contract_sha256 TEXT NOT NULL,
        signer_name TEXT NOT NULL, signed_at TEXT NOT NULL, signature_png_base64 TEXT NOT NULL,
        pdf_base64 TEXT NOT NULL, pdf_sha256 TEXT NOT NULL)''')


def snapshot(payload):
    """The signed terms contain only details known and displayed before payment."""
    q, customer = payload['quote'], payload['customer']
    if q.get('accepted_terms') is not True:
        raise ValueError('Die Mietbedingungen wurden bei der Buchung nicht bestätigt.')
    if q.get('lessor_name', LESSOR_NAME) != LESSOR_NAME or q.get('lessor_address', LESSOR_ADDRESS) != LESSOR_ADDRESS:
        raise ValueError('Die Vertragspartei der Buchung muss geprüft werden.')
    if q.get('deposit_authorized_cents', 0) and (
        q.get('deposit_method') != 'card_authorization_at_booking'
        or q['deposit_authorized_cents'] != q['deposit_cents']
        or q['deposit_charged_cents'] != 0
        or q['amount_cents'] != q['rental_cents']
    ):
        raise ValueError('Zahlbetrag und Kreditkartenreservierung stimmen nicht überein.')
    contract = {
        'lessor_name': LESSOR_NAME, 'lessor_address': LESSOR_ADDRESS,
        'offer_name': OFFER_NAME, 'customer_name': customer['name'],
        'customer_email': customer['email'], 'vehicle_name': q['vehicle_name'],
        'vehicle_id': q['vehicle_id'], 'vehicle_plate': q['vehicle_plate'],
        'vehicle_vin': q['vehicle_vin'],
        'start_slot': q['start_slot'], 'end_slot': q['end_slot'], 'days': q['days'],
        'daily_cents': q['daily_cents'], 'rental_cents': q['rental_cents'],
        'deposit_cents': q['deposit_cents'],
        'deposit_charged_cents': q['deposit_charged_cents'],
        'amount_cents': q['amount_cents'], 'deductible_cents': q['deductible_cents'],
        'included_km': q['included_km'], 'extra_km_cents': q['extra_km_cents'],
        'terms_version': q['rules_version'], 'terms_text': q['terms_text'],
        'test_only': q['test_only'],
    }
    # Preserve hashes of already signed contracts. New deposit/cancellation
    # fields are signed only when they were present in the displayed quote.
    for key in ('deposit_authorized_cents', 'deposit_method', 'cancellation_policy'):
        if key in q:
            contract[key] = q[key]
    return contract


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def snapshot_hash(value):
    return sha256(canonical(value).encode('utf-8')).hexdigest()


def read(db, hold_id):
    row = db.execute('SELECT * FROM miet_checkout_contracts WHERE hold_id=?', (hold_id,)).fetchone()
    return dict(row) if row else None


def signature_png(data_url):
    prefix = 'data:image/png;base64,'
    if not isinstance(data_url, str) or not data_url.startswith(prefix) or len(data_url) > 56000:
        raise ValueError('Bitte eine gültige Unterschrift zeichnen.')
    try:
        raw = b64decode(data_url[len(prefix):], validate=True)
        if len(raw) > 40000 or not raw.startswith(b'\x89PNG\r\n\x1a\n'):
            raise ValueError()
        with PillowImage.open(BytesIO(raw)) as image:
            if image.format != 'PNG' or not (200 <= image.width <= 1200 and 60 <= image.height <= 400):
                raise ValueError()
            image.load()
            # A blank canvas, a solid block, and a single accidental tap are not signatures.
            rgba = image.convert('RGBA')
            marks = []
            for y in range(0, rgba.height, 2):
                for x in range(0, rgba.width, 2):
                    r, g, b, a = rgba.getpixel((x, y))
                    if a > 120 and max(r, g, b) < 170:
                        marks.append((x, y))
            if (len(marks) < 40 or len(marks) > (rgba.width * rgba.height // 4) // 5
                or max(x for x, _ in marks) - min(x for x, _ in marks) < 35
                or max(y for _, y in marks) - min(y for _, y in marks) < 8):
                raise ValueError()
        return raw
    except (ValueError, OSError, UnidentifiedImageError) as exc:
        raise ValueError('Bitte eine lesbare Unterschrift mit Maus oder Finger zeichnen.') from exc


def _eur(cents):
    return f'{cents / 100:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.') + ' EUR'


def _slot(value):
    return datetime.fromisoformat(value).strftime('%d.%m.%Y, %H:%M Uhr')


def render_pdf(contract, signed_at, signature, document_hash, signature_record_hash, reference):
    """Render once after verified payment; keep the exact PDF bytes in the DB."""
    from reportlab.lib.pagesizes import A4

    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=A4, rightMargin=46, leftMargin=46,
                            topMargin=50, bottomMargin=52, title='MOS Mietvertrag')
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='MOSBrand', parent=styles['Normal'], fontName='Helvetica-Bold',
                              fontSize=9, leading=12, textColor=colors.HexColor('#8b3500')))
    styles.add(ParagraphStyle(name='MOSBody', parent=styles['Normal'], fontSize=9.4, leading=14, spaceAfter=7))
    styles.add(ParagraphStyle(name='MOSSmall', parent=styles['Normal'], fontSize=8, leading=11, spaceAfter=5))
    styles.add(ParagraphStyle(name='MOSTitle', parent=styles['Heading1'], fontSize=20, leading=23, spaceAfter=12))
    styles.add(ParagraphStyle(name='MOSSection', parent=styles['Heading2'], fontSize=12, leading=15,
                              spaceBefore=12, spaceAfter=7))
    styles.add(ParagraphStyle(name='MOSTest', parent=styles['Normal'], alignment=TA_CENTER,
                              fontSize=10, leading=13, textColor=colors.HexColor('#a4311b'), spaceAfter=12))

    def para(value, style='MOSBody'):
        return Paragraph(escape(str(value)).replace('\n', '<br/>'), styles[style])

    story = [para(contract['offer_name'].upper(), 'MOSBrand'), para('Mietvertrag', 'MOSTitle')]
    if contract['test_only']:
        story.append(para('TESTDOKUMENT - KEIN ECHTER MIETVERTRAG', 'MOSTest'))
    story += [
        para('Vermieter und Vertragspartner ist ' + contract['lessor_name'] + ', ' + contract['lessor_address'] +
             '. "Autovermietung MOS" ist nur die Bezeichnung des Mietangebots und keine eigene Vertragspartei.'),
        para('Buchungsreferenz: ' + reference, 'MOSSmall'),
        para('Vertragsparteien', 'MOSSection'),
        para('Vermieter: ' + contract['lessor_name'] + ', ' + contract['lessor_address']),
        para('Mieter: ' + contract['customer_name'] + ' · ' + contract['customer_email']),
        para('Fahrzeug und Zahlung', 'MOSSection'),
    ]
    rows = [
        ('Fahrzeug', contract['vehicle_name']),
        ('Kennzeichen', contract['vehicle_plate'] or 'Wird vor Übergabe mitgeteilt'),
        ('FIN', contract['vehicle_vin'] or 'Wird vor Übergabe mitgeteilt'),
        ('Abholung', _slot(contract['start_slot'])),
        ('Rückgabe', _slot(contract['end_slot'])),
        ('Mietdauer / Tagessatz', f"{contract['days']} Miettag(e) / {_eur(contract['daily_cents'])}"),
        ('Mietpreis inkl. MwSt.', _eur(contract['rental_cents'])),
    ]
    if contract.get('deposit_authorized_cents', 0):
        rows += [
            ('Kaution auf Kreditkarte reserviert', _eur(contract['deposit_authorized_cents']) + ' - keine Abbuchung'),
            ('Zahlbetrag (nur Miete)', _eur(contract['amount_cents'])),
        ]
    else:
        rows += [
            ('Rückzahlbare Kaution', _eur(contract['deposit_cents'])),
            ('Kaution im Zahlbetrag', _eur(contract['deposit_charged_cents'])),
            ('Vorgesehener Zahlbetrag', _eur(contract['amount_cents'])),
        ]
    rows += [
        ('Vertragliche Selbstbeteiligung', _eur(contract['deductible_cents'])),
        ('Freikilometer', str(contract['included_km']) + ' km'),
        ('Mehrkilometer', _eur(contract['extra_km_cents']) + ' pro km'),
    ]
    table = Table([[para(k, 'MOSSmall'), para(v, 'MOSSmall')] for k, v in rows], colWidths=[185, 300], hAlign='LEFT')
    table.setStyle(TableStyle([('VALIGN', (0, 0), (-1, -1), 'TOP'), ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#f5f3ee')]),
                               ('LEFTPADDING', (0, 0), (-1, -1), 7), ('RIGHTPADDING', (0, 0), (-1, -1), 7),
                               ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
    story += [table, para('Vereinbarte Mietbedingungen', 'MOSSection'),
        para('Version ' + contract['terms_version'] + ' - diese Fassung wurde vor der Zahlung angezeigt und unterschrieben.', 'MOSSmall')]
    for block in contract['terms_text'].split('\n'):
        if block.strip():
            story.append(para(block))
        else:
            story.append(Spacer(1, 5))
    local_time = datetime.fromisoformat(signed_at).astimezone(ZoneInfo('Europe/Berlin')).strftime('%d.%m.%Y, %H:%M:%S %Z')
    sig_width, sig_height = PillowImage.open(BytesIO(signature)).size
    rendered_width = min(185, sig_width * 0.6)
    rendered_height = rendered_width * sig_height / sig_width
    story.append(KeepTogether([
        para('Digitale Unterschrift des Mieters', 'MOSSection'),
        para('Der Mieter hat die angezeigten Vertragsbedingungen vor der Zahlung mit Maus oder Finger unterschrieben. '
             'Die Buchungsbestätigung dokumentiert die anschließende Zahlung. '
             'Diese einfache elektronische Unterschrift ist keine qualifizierte elektronische Signatur.', 'MOSSmall'),
        Image(BytesIO(signature), width=rendered_width, height=rendered_height),
        para(contract['customer_name'] + ' · ' + local_time, 'MOSSmall'),
        para('Vertragsdaten SHA-256: ' + document_hash, 'MOSSmall'),
        para('Unterschriftsnachweis SHA-256: ' + signature_record_hash, 'MOSSmall'),
    ]))

    def footer(canvas, document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor('#dedbd5'))
        canvas.line(46, 40, A4[0] - 46, 40)
        canvas.setFont('Helvetica', 7.5)
        canvas.drawString(46, 28, contract['lessor_name'] + ' | ' + reference)
        canvas.drawRightString(A4[0] - 46, 28, f'Seite {document.page}')
        canvas.restoreState()

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()


def presign_quote(quote, customer, data_url):
    """Validate the drawn signature before reserving inventory or opening Stripe."""
    signature = signature_png(data_url)
    prepared = {**quote, 'accepted_terms': True}
    contract = snapshot({'quote': prepared, 'customer': customer})
    prepared['signature_png_base64'] = b64encode(signature).decode('ascii')
    prepared['signed_at'] = datetime.now(timezone.utc).isoformat()
    prepared['signed_contract_hash'] = snapshot_hash(contract)
    prepared['signature_record_hash'] = signature_record_hash(
        prepared['signed_contract_hash'], prepared['signed_at'], signature)
    return prepared


def signature_record_hash(contract_hash, signed_at, signature):
    """Bind the signed version, signature bytes and exact signing instant together."""
    return snapshot_hash({'contract_sha256': contract_hash, 'signed_at': signed_at,
                          'signature_sha256': sha256(signature).hexdigest()})


def signed_payload(payload):
    """Check that a hold still contains the exact signed pre-payment snapshot."""
    quote = payload['quote']
    if not quote.get('signature_png_base64') or not quote.get('signed_at'):
        raise ValueError('Bitte den Mietvertrag vor der Zahlung unterschreiben.')
    signature = signature_png('data:image/png;base64,' + quote['signature_png_base64'])
    signed_at = datetime.fromisoformat(quote['signed_at'])
    if signed_at.tzinfo is None:
        raise ValueError('Zeitpunkt der Unterschrift fehlt.')
    contract = snapshot(payload)
    digest = snapshot_hash(contract)
    if quote.get('signed_contract_hash') != digest:
        raise ValueError('Der unterschriebene Vertrag stimmt nicht mit der Buchung überein.')
    if quote.get('signature_record_hash') != signature_record_hash(digest, quote['signed_at'], signature):
        raise ValueError('Der Unterschriftsnachweis stimmt nicht mit der Buchung überein.')
    return contract, digest, signature


def finalize(portal, hold_id):
    """A paid, confirmed hold gets exactly one immutable copy of its pre-signed terms."""
    db = portal.get_db()
    try:
        if not portal.USE_POSTGRES:
            db.execute('BEGIN IMMEDIATE')
        suffix = ' FOR UPDATE' if portal.USE_POSTGRES else ''
        row = db.execute('SELECT * FROM miet_checkout_holds WHERE id=?' + suffix, (hold_id,)).fetchone()
        if not row:
            raise ValueError('Buchung nicht gefunden.')
        hold = dict(row)
        existing = read(db, hold_id)
        if existing:
            return existing
        if hold['status'] != 'confirmed' or not hold['mietvorgang_id'] or not hold['payment_intent']:
            return None
        payload = json.loads(hold['payload'])
        if not payload['quote'].get('signature_png_base64'):
            return None  # Historic in-flight holds are not reinterpreted as pre-signed.
        contract, digest, signature = signed_payload(payload)
        signed_at = payload['quote']['signed_at']
        pdf = render_pdf(contract, signed_at, signature, digest,
                         payload['quote']['signature_record_hash'], hold_id)
        db.execute('''INSERT INTO miet_checkout_contracts
            (hold_id,contract_json,contract_sha256,signer_name,signed_at,signature_png_base64,pdf_base64,pdf_sha256)
            VALUES (?,?,?,?,?,?,?,?)''',
            (hold_id, canonical(contract), digest, contract['customer_name'], signed_at,
             payload['quote']['signature_png_base64'], b64encode(pdf).decode('ascii'), sha256(pdf).hexdigest()))
        db.commit()
        return read(db, hold_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
