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


def euro(cents):
    return f'{cents // 100:,}'.replace(',', '.') + f',{cents % 100:02d} €'


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
    def original():
        try:
            if PDF_PATH.is_symlink() or PDF_PATH.stat().st_size > 1_000_000:
                raise OSError()
            raw = PDF_PATH.read_bytes()
            if not raw.startswith(b'%PDF-') or hashlib.sha256(raw).hexdigest() != PDF_SHA256:
                raise OSError()
            return raw
        except OSError:
            abort(503, description='Die hinterlegte Originalpreisliste ist gerade nicht verfügbar.')


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
        return render_template('buero_preise.html', price_date=PRICE_DATE,
                               paint_prices=PAINT_PRICES, extra_prices=EXTRAS, price_euro=euro)

    @bp.get('/werkstatt/buero/preise/pdf')
    def pdf():
        return send_file(io.BytesIO(service.original()), mimetype='application/pdf',
                         as_attachment=request.args.get('download') == '1', download_name=PDF_FILENAME,
                         etag=False, conditional=False, max_age=0)

    @bp.after_request
    def private(response):
        response.headers.update({'Cache-Control': 'private, no-store, max-age=0',
            'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff',
            'X-Robots-Tag': 'noindex, nofollow, noarchive'})
        return response

    p.app.register_blueprint(bp)
    return service
