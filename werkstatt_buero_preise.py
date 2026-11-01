"""Read-only internal price reference and authenticated original PDF."""
import hashlib
import io
from pathlib import Path

from flask import Blueprint, abort, render_template, request, send_file, session


PRICE_DATE = '30.10.2026'
PDF_FILENAME = 'Interne_Preisliste_Netto_Arbeitsstand.pdf'
PDF_PATH = Path(__file__).resolve().parent / 'resources' / 'buero' / PDF_FILENAME
PDF_SHA256 = 'b316f8804f4b54fc6f3d3d3a762455e519aefbaddc95f381df6f31b258e66da1'

# Explicit source values in cents, never computed from a new markup rule.
PAINT_PRICES = (
    ('Kotflügel', 20000, 22000, 18000, 19800, False),
    ('Tür', 22500, 24750, 20250, 22275, False),
    ('Seitenwand', 25000, 27500, 22500, 24750, False),
    ('Kofferraumdeckel', 25000, 27500, 22500, 24750, False),
    ('Motorhaube', 30000, 33000, 27000, 29700, False),
    ('Stoßstange vorne oder hinten', 30000, 33000, 27000, 29700, False),
    ('Dach', 40000, 44000, 36000, 39600, True),
)
EXTRAS = (
    ('Demontage und Montage zusammen', 'Je Bauteil · nur bei tatsächlicher Ausführung', 10000, 13000, False),
    ('Beilackieren / Farbangleich', 'Je eigenständigem angrenzendem Bauteil', 12000, 15000, False),
    ('Dreischichtlack', 'Zusätzlich je entsprechend lackiertem Bauteil', 5000, 5000, True),
)

TYRE_PRICE_DATE = '28.10.2026'
TYRE_CHECKED_DATE = '30.10.2026'
TYRE_PDF_FILENAME = 'Preisliste_Reifenservice_Achsvermessung.pdf'
TYRE_PDF_PATH = Path(__file__).resolve().parent / 'resources' / 'buero' / TYRE_PDF_FILENAME
TYRE_PDF_SHA256 = '3d34d0f736b02ed077a42c029d49db3810b7926f283dd1dc34b1b542a54a458f'

# Sanitized snapshot of the twenty verified services; no private provider IDs.
TYRE_TARIFFS = (('radwechsel_bis17',
  'RS-RW-B17',
  'Radwechsel bis 17 Zoll',
  'Satz (4 Räder)',
  2941,
  3500,
  'GROSS',
  'Vier Kompletträder, ohne Wuchten. Sichtkontrolle und Luftdruckprüfung enthalten.'),
 ('radwechsel_18_20',
  'RS-RW-18-20',
  'Radwechsel 18–20 Zoll',
  'Satz (4 Räder)',
  3361,
  4000,
  'GROSS',
  'Vier Kompletträder, ohne Wuchten. Sichtkontrolle und Luftdruckprüfung enthalten.'),
 ('radwechsel_wuchten_bis17',
  'RS-RWW-B17',
  'Radwechsel mit Wuchten bis 17 Zoll',
  'Satz (4 Räder)',
  5042,
  6000,
  'GROSS',
  'Vier Räder, einschließlich Standardgewichten. Sichtkontrolle und Luftdruckprüfung enthalten.'),
 ('radwechsel_wuchten_18_20',
  'RS-RWW-18-20',
  'Radwechsel mit Wuchten 18–20 Zoll',
  'Satz (4 Räder)',
  5882,
  7000,
  'GROSS',
  'Vier Räder, einschließlich Standardgewichten. Sichtkontrolle und Luftdruckprüfung enthalten.'),
 ('einlagerung',
  'RS-LAG-6M',
  'Einlagerung bis 20 Zoll',
  'Satz (4 Räder)',
  3782,
  4500,
  'GROSS',
  'Vier Kompletträder, sechs Monate ab Annahme, Kennzeichnung und Sichtcheck; ohne Wäsche.'),
 ('montage_13_16',
  'RS-MON-13-16',
  'Reifenmontage 13–16 Zoll',
  'Reifen',
  1891,
  2250,
  'GROSS',
  'Radaus-/einbau, Ab-/Aufziehen, Wuchten, Standardgewichte und Standard-Gummiventil ohne RDKS. Reifen selbst und '
  'Entsorgung separat. Radwechsel/Wuchten nicht zusätzlich berechnen. Bei Mischbereifung gilt Zollklasse je Rad.'),
 ('montage_17_18',
  'RS-MON-17-18',
  'Reifenmontage 17–18 Zoll',
  'Reifen',
  2101,
  2500,
  'GROSS',
  'Radaus-/einbau, Ab-/Aufziehen, Wuchten, Standardgewichte und Standard-Gummiventil ohne RDKS. Reifen selbst und '
  'Entsorgung separat. Radwechsel/Wuchten nicht zusätzlich berechnen. Bei Mischbereifung gilt Zollklasse je Rad.'),
 ('montage_19_20',
  'RS-MON-19-20',
  'Reifenmontage 19–20 Zoll',
  'Reifen',
  2521,
  3000,
  'GROSS',
  'Radaus-/einbau, Ab-/Aufziehen, Wuchten, Standardgewichte und Standard-Gummiventil ohne RDKS. Reifen selbst und '
  'Entsorgung separat. Radwechsel/Wuchten nicht zusätzlich berechnen. Bei Mischbereifung gilt Zollklasse je Rad.'),
 ('saison_radwechsel_bis17',
  'RS-SAI-RW-B17',
  'Einlagerung und Radwechsel bis 17 Zoll',
  'Satz (4 Räder)',
  6723,
  8000,
  'GROSS',
  'Vier Räder, eine Saison / maximal sechs Monate, ohne Wäsche. Genau ein Wechsel enthalten; keine Jahrespreise. '
  'Lagerung und Wechsel desselben Pakets nicht nochmals berechnen.'),
 ('saison_radwechsel_18_20',
  'RS-SAI-RW-18-20',
  'Einlagerung und Radwechsel 18–20 Zoll',
  'Satz (4 Räder)',
  7143,
  8500,
  'GROSS',
  'Vier Räder, eine Saison / maximal sechs Monate, ohne Wäsche. Genau ein Wechsel enthalten; keine Jahrespreise. '
  'Lagerung und Wechsel desselben Pakets nicht nochmals berechnen.'),
 ('saison_wuchten_bis17',
  'RS-SAI-RWW-B17',
  'Einlagerung und Radwechsel mit Wuchten bis 17 Zoll',
  'Satz (4 Räder)',
  8824,
  10500,
  'GROSS',
  'Vier Räder, eine Saison / maximal sechs Monate, ohne Wäsche. Genau ein Wechsel enthalten; keine Jahrespreise. '
  'Lagerung und Wechsel desselben Pakets nicht nochmals berechnen.'),
 ('saison_wuchten_18_20',
  'RS-SAI-RWW-18-20',
  'Einlagerung und Radwechsel mit Wuchten 18–20 Zoll',
  'Satz (4 Räder)',
  9664,
  11500,
  'GROSS',
  'Vier Räder, eine Saison / maximal sechs Monate, ohne Wäsche. Genau ein Wechsel enthalten; keine Jahrespreise. '
  'Lagerung und Wechsel desselben Pakets nicht nochmals berechnen.'),
 ('raederwaesche',
  'RS-WAE-SATZ',
  'Räderwäsche',
  'Satz (4 Räder)',
  1261,
  1500,
  'GROSS',
  'Vier Räder, normale Verschmutzung.'),
 ('wuchten_los_bis17',
  'RS-WU-B17',
  'Wuchten loser Räder bis 17 Zoll',
  'Rad',
  525,
  625,
  'GROSS',
  'Einschließlich Gewichten.'),
 ('wuchten_los_18_20',
  'RS-WU-18-20',
  'Wuchten loser Räder 18–20 Zoll',
  'Rad',
  630,
  750,
  'GROSS',
  'Einschließlich Gewichten.'),
 ('runflat',
  'RS-RUNFLAT',
  'Runflat / Niederquerschnitt bei Reifenmontage',
  'Reifen',
  420,
  500,
  'GROSS',
  'Einmal je Reifen; nicht beim reinen Komplettradwechsel.'),
 ('entsorgung', 'RS-ENTS', 'Altreifenentsorgung ohne Felge', 'Reifen', 336, 400, 'GROSS', ''),
 ('metallventil',
  'RS-MET-VENT',
  'Metallventil statt Standard-Gummiventil',
  'Ventil',
  378,
  450,
  'GROSS',
  'Ohne RDKS. Ersetzt das enthaltene Gummiventil, kein RDKS-Servicekit.'),
 ('av-klein',
  'AV-KLEIN',
  'Kleine Achsvermessung',
  'Fahrzeug',
  20500,
  24395,
  'NET',
  'Kleine Variante; Detailumfang in den Quellen nicht spezifiziert. Passende Variante, Termin und eventuelle '
  'Zusatzarbeiten/Kosten vorab abstimmen.'),
 ('av-gross',
  'AV-GROSS',
  'Große Achsvermessung - alle vier Räder',
  'Fahrzeug',
  25000,
  29750,
  'NET',
  'Große Variante für alle vier Räder. Einstellarbeiten, Messprotokoll oder Kalibrierung nicht ausdrücklich als '
  'inklusive bezeichnet. Eventuelle Zusatzarbeiten/Kosten vorab abstimmen.'))
TYRE_GROUPS = (
    ('Radwechsel & Einlagerung', TYRE_TARIFFS[:5]),
    ('Reifenmontage', TYRE_TARIFFS[5:8]),
    ('Saisonpakete', TYRE_TARIFFS[8:12]),
    ('Zusatzleistungen', TYRE_TARIFFS[12:18]),
)
ALIGNMENT_TARIFFS = TYRE_TARIFFS[18:]
# These source totals describe quantity four, never additional service articles.
TYRE_SET_EXAMPLES = (('13–16 Zoll', 7563, 9000), ('17–18 Zoll', 8403, 10000), ('19–20 Zoll', 10084, 12000))


def euro(cents):
    sign, value = ('-' if cents < 0 else ''), abs(cents)
    return sign + f'{value // 100:,}'.replace(',', '.') + f',{value % 100:02d} €'


class OfficePrices:
    def __init__(self, p):
        self.p = p

    def require(self):
        if session.get('admin'):
            return
        who = self.p.office_access.identity()
        if not who or 'preise' not in who.get('office_categories', ()):
            abort(403, description='Die interne Preisliste ist für diesen Bürozugang nicht freigegeben.')

    @staticmethod
    def _original(path, expected_sha):
        try:
            if path.is_symlink() or path.stat().st_size > 1_000_000:
                raise OSError()
            raw = path.read_bytes()
            if not raw.startswith(b'%PDF-') or hashlib.sha256(raw).hexdigest() != expected_sha:
                raise OSError()
            return raw
        except OSError:
            abort(503, description='Die hinterlegte Originalpreisliste ist gerade nicht verfügbar.')

    def original(self):
        return self._original(PDF_PATH, PDF_SHA256)

    def tyre_original(self):
        return self._original(TYRE_PDF_PATH, TYRE_PDF_SHA256)


def register_office_prices(p):
    service = OfficePrices(p)
    p.office_prices = service
    bp = Blueprint('office_prices', __name__)

    @bp.before_request
    def authorize():
        service.require()

    @bp.get('/werkstatt/buero/preise')
    def index():
        service.original()  # The displayed matrix must match its bundled source.
        service.tyre_original()
        return render_template('buero_preise.html', price_date=PRICE_DATE,
                               paint_prices=PAINT_PRICES, extra_prices=EXTRAS, price_euro=euro,
                               tyre_groups=TYRE_GROUPS, alignment_prices=ALIGNMENT_TARIFFS,
                               tyre_set_examples=TYRE_SET_EXAMPLES,
                               tyre_price_date=TYRE_PRICE_DATE, tyre_checked_date=TYRE_CHECKED_DATE)

    @bp.get('/werkstatt/buero/preise/pdf')
    def pdf():
        return send_file(io.BytesIO(service.original()), mimetype='application/pdf',
                         as_attachment=request.args.get('download') == '1', download_name=PDF_FILENAME,
                         etag=False, conditional=False, max_age=0)

    @bp.get('/werkstatt/buero/preise/reifen-pdf')
    def tyre_pdf():
        return send_file(io.BytesIO(service.tyre_original()), mimetype='application/pdf',
                         as_attachment=request.args.get('download') == '1', download_name=TYRE_PDF_FILENAME,
                         etag=False, conditional=False, max_age=0)

    @bp.after_request
    def private(response):
        response.headers.update({'Cache-Control': 'private, no-store, max-age=0',
            'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff',
            'X-Robots-Tag': 'noindex, nofollow, noarchive'})
        return response

    p.app.register_blueprint(bp)
    return service
