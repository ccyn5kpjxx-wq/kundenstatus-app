"""Restricted office planning, sourced from orders and shared calendar notes.

Registration does not open a database. Office notes use the existing calendar
table with an explicit category; unrelated calendar notes never enter this view.
No repair state, transport release, notifications or financial data are changed.
"""
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timedelta
import hashlib
import hmac
import json
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, flash, redirect, render_template, request, session

from werkstatt_tagesbriefing import build_briefing


BERLIN = ZoneInfo('Europe/Berlin')
NOTE_FIELDS = ('id', 'datum', 'titel', 'notiz', 'kategorie', 'wiederholung', 'erstellt_am')
# Do not hydrate full orders: descriptions, analyses, money, staff records and
# original documents are intentionally absent from this query and view model.
ORDER_FIELDS = ('id', 'fahrzeug', 'kennzeichen', 'status', 'archiviert', 'transport_art',
                'annahme_datum', 'annahme_uhrzeit', 'abholtermin', 'abhol_uhrzeit',
                'fertig_datum', 'fertig_uhrzeit', 'abhol_adresse', 'fahrzeug_abholbereit')
GROUPS = (
    ('abholung_durch_werkstatt_heute', 'Bei Kunde oder Partner holen'),
    ('rueckbringung_heute', 'Zum Kunden oder Partner zurückbringen'),
    ('anlieferung_heute', 'Kunde bringt das Fahrzeug'),
    ('kundenabholung_heute', 'Kunde holt das Fahrzeug'),
    ('heute_faellig', 'Fertigstellung heute geplant'),
    ('ueberfaellig', 'Fertigtermin überschritten'),
    ('transport_ungeklaert', 'Transportart klären'),
)
NOTE_CLOCK = re.compile(r'^((?:[01]\d|2[0-3]):[0-5]\d) · (.+)$', re.S)


class PlanningConflict(ValueError):
    pass


def _day(value=None):
    if value is None:
        return datetime.now(BERLIN).date()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError('Das Datum braucht eine Zeitzone.')
        return value.astimezone(BERLIN).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for pattern in ('%Y-%m-%d', '%d.%m.%Y'):
            try:
                return datetime.strptime(value, pattern).date()
            except ValueError:
                pass
    raise ValueError('Bitte ein gültiges Datum auswählen.')


def _text(value, maximum=300):
    return ' '.join(str(value or '').split())[:maximum]


def _note_input(payload):
    if set(payload) - {'datum', 'uhrzeit', 'titel', 'notiz', 'version', 'csrf_token'}:
        raise ValueError('Nur Datum, Titel und Hinweis des Bürotermins übergeben.')
    values = {}
    for key, maximum in (('titel', 160), ('notiz', 2000)):
        raw = payload.get(key, '')
        if not isinstance(raw, str) or len(raw) > maximum or '\x00' in raw:
            raise ValueError('Titel oder Hinweis ist zu lang oder ungültig.')
        values[key] = raw.strip()
    if not values['titel']:
        raise ValueError('Bitte einen Titel für den Termin eintragen.')
    clock = payload.get('uhrzeit', '')
    if not isinstance(clock, str) or (clock and not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', clock)):
        raise ValueError('Bitte eine gültige Uhrzeit eintragen oder das Feld leer lassen.')
    # The established shared calendar has no time column. Keep optional office
    # times visible there as well, without introducing a second calendar table.
    if clock:
        values['titel'] = clock + ' · ' + values['titel']
    values['datum'] = _day(payload.get('datum', '')).strftime('%d.%m.%Y')
    return values


class OfficePlanning:
    def __init__(self, portal):
        self.p = portal

    def identity(self):
        if session.get('admin'):
            return {'mitarbeiter_id': None, 'name': 'Werkstattleitung', 'actor': 'admin'}
        who = self.p.office_access.identity()
        if not who:
            raise PermissionError('Bitte mit dem persönlichen Bürozugang anmelden.')
        return {'mitarbeiter_id': who.get('mitarbeiter_id'),
                'name': _text(who.get('name') or who.get('mitarbeiter_name'), 100),
                'actor': who['actor']}

    @contextmanager
    def db(self):
        connection = self.p.get_db()
        try:
            yield connection
        finally:
            connection.close()

    def _rows(self):
        with self.db() as db:
            orders = [dict(row) for row in db.execute(
                'SELECT ' + ','.join('a.' + key for key in ORDER_FIELDS)
                + ',h.name AS autohaus_name FROM auftraege a '
                  'LEFT JOIN autohaeuser h ON h.id=a.autohaus_id '
                  'WHERE COALESCE(a.archiviert,0)=0 AND a.status IN (1,2,3,4)'
            ).fetchall()]
            notes = [dict(row) for row in db.execute(
                'SELECT ' + ','.join(NOTE_FIELDS) + " FROM kalender_notizen "
                "WHERE kategorie='buero' AND COALESCE(wiederholung,'einmalig')='einmalig'"
            ).fetchall()]
        return orders, notes

    def _version(self, row):
        raw = json.dumps([row.get(key) for key in NOTE_FIELDS], ensure_ascii=False,
                         separators=(',', ':')).encode('utf-8')
        secret = self.p.app.secret_key
        if not secret:
            raise RuntimeError('Ein Anwendungsschlüssel ist für die Terminbearbeitung erforderlich.')
        if isinstance(secret, str):
            secret = secret.encode('utf-8')
        return hmac.new(secret, b'office-note-v1:' + raw, hashlib.sha256).hexdigest()

    def _note_view(self, row):
        day = _day(row['datum'])
        clock = NOTE_CLOCK.fullmatch(row['titel'])
        return {'id': row['id'], 'datum': day.isoformat(), 'datum_text': day.strftime('%d.%m.%Y'),
                'uhrzeit': clock[1] if clock else '', 'titel': clock[2] if clock else row['titel'],
                'notiz': row.get('notiz') or '', 'version': self._version(row)}

    def _view(self, orders, notes, day, who):
        report = build_briefing(orders, day)
        by_id = {row['id']: row for row in orders}
        groups = []
        for key, title in GROUPS:
            items = []
            for event in report['kategorien'][key]:
                row = by_id[event['auftrag_id']]
                item = {field: event[field] for field in
                        ('auftrag_id', 'fahrzeug', 'kennzeichen', 'datum', 'uhrzeit')}
                item['datum_text'] = _day(event['datum']).strftime('%d.%m.%Y')
                item['partner'] = _text(row.get('autohaus_name'), 120)
                item['adresse'] = _text(row.get('abhol_adresse'), 300) if key == 'abholung_durch_werkstatt_heute' else ''
                item['hinweise'] = []
                if not item['uhrzeit']:
                    item['hinweise'].append('Uhrzeit fehlt – bitte abstimmen.')
                if key == 'abholung_durch_werkstatt_heute':
                    if not item['adresse']:
                        item['hinweise'].append('Abholadresse fehlt – bitte abstimmen.')
                    item['hinweise'].append('Partner hat das Fahrzeug als abholbereit gemeldet.'
                                            if row.get('fahrzeug_abholbereit')
                                            else 'Abholbereitschaft ist noch nicht gemeldet.')
                elif key == 'rueckbringung_heute':
                    item['hinweise'].append('Rückbringadresse bitte mit der Werkstattleitung abstimmen.')
                items.append(item)
            groups.append({'key': key, 'titel': title, 'items': items})
        warnings = []
        for row in orders:
            if row.get('transport_art') != 'hol_und_bring':
                continue
            for field, label, relevant in (
                ('annahme_datum', 'Abholtermin', int(row['status']) < 3),
                ('abholtermin', 'Rückbringtermin', True),
            ):
                if not relevant:
                    continue
                try:
                    _day(row.get(field, ''))
                except ValueError:
                    warnings.append({'auftrag_id': row['id'], 'kennzeichen': _text(row.get('kennzeichen'), 30),
                                     'hinweis': label + ' fehlt oder ist ungültig.'})
        for warning in report['datenhinweise']:
            if not any(item['auftrag_id'] == warning['auftrag_id'] and
                       warning['feld'] in ('annahme_datum', 'abholtermin') for item in warnings):
                warnings.append({'auftrag_id': warning['auftrag_id'], 'kennzeichen': '', 'hinweis': warning['hinweis']})
        office_notes = []
        for row in notes:
            try:
                if _day(row['datum']) == day:
                    office_notes.append(self._note_view(row))
            except ValueError:
                continue
        office_notes.sort(key=lambda note: (note['uhrzeit'] or '99:99', note['titel'].casefold(), note['id']))
        return {'datum': day.isoformat(), 'datum_text': day.strftime('%d.%m.%Y'),
                'employee': who, 'gruppen': groups, 'offene_hinweise': warnings,
                'termine': office_notes, 'anzahl_fahrten': sum(len(group['items']) for group in groups[:2]),
                'anzahl_termine': len(office_notes) + report['anzahl_ereignisse']}

    def today(self, day=None):
        """Authorized compact home data; see returned keys in _view()."""
        who = self.identity()
        orders, notes = self._rows()
        return self._view(orders, notes, _day(day), who)

    def calendar(self, day=None):
        who = self.identity()
        target = _day(day)
        orders, notes = self._rows()
        return {'employee': who, 'datum': target.isoformat(),
                'tage': [self._view(orders, notes, target + timedelta(days=offset), who) for offset in range(7)]}

    def save_note(self, payload, note_id=None):
        self.identity()
        data = _note_input(payload)
        lock = getattr(self.p, 'portal_originals_operation_lock', None)
        with lock() if lock else nullcontext():
            with self.db() as db:
                try:
                    # Recheck revocation immediately before the write.
                    self.identity()
                    if note_id is None:
                        row = db.execute('''INSERT INTO kalender_notizen
                            (datum,titel,notiz,kategorie,wiederholung,erstellt_am)
                            VALUES(?,?,?,'buero','einmalig',?) RETURNING id''',
                            (data['datum'], data['titel'], data['notiz'], self.p.now_str())).fetchone()
                    else:
                        prior = db.execute('SELECT ' + ','.join(NOTE_FIELDS) +
                            " FROM kalender_notizen WHERE id=? AND kategorie='buero' "
                            "AND COALESCE(wiederholung,'einmalig')='einmalig'", (note_id,)).fetchone()
                        if prior is None:
                            raise LookupError('Bürotermin nicht gefunden.')
                        prior = dict(prior)
                        supplied = payload.get('version')
                        if not isinstance(supplied, str) or not hmac.compare_digest(supplied, self._version(prior)):
                            raise PlanningConflict('Der Termin wurde zwischenzeitlich geändert. Bitte neu laden.')
                        row = db.execute('''UPDATE kalender_notizen SET datum=?,titel=?,notiz=?
                            WHERE id=? AND kategorie='buero'
                            AND COALESCE(wiederholung,'einmalig')='einmalig'
                            AND datum=? AND titel=? AND COALESCE(notiz,'')=?
                            AND COALESCE(erstellt_am,'')=? RETURNING id''',
                            (data['datum'], data['titel'], data['notiz'], note_id, prior['datum'], prior['titel'],
                             prior.get('notiz') or '', prior.get('erstellt_am') or '')).fetchone()
                        if row is None:
                            raise PlanningConflict('Der Termin wurde zwischenzeitlich geändert. Bitte neu laden.')
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
        backup = getattr(self.p, 'schedule_change_backup', None)
        if backup:
            backup('buero-kalender')
        return row['id']


def register_office_planning(p):
    service = OfficePlanning(p)
    bp = Blueprint('office_planning', __name__, url_prefix='/werkstatt/buero')
    if isinstance(getattr(p, 'KALENDER_KATEGORIEN', None), dict):
        p.KALENDER_KATEGORIEN.setdefault('buero', {'label': 'Bürotermin', 'farbe': 'info'})

    def csrf_token():
        if not session.get('csrf_token'):
            session['csrf_token'] = secrets.token_urlsafe(32)
        return session['csrf_token']

    @bp.before_request
    def authorize():
        try:
            service.identity()
        except PermissionError:
            abort(403)
        if request.method == 'POST':
            expected, supplied = session.get('csrf_token'), request.form.get('csrf_token')
            if not expected or not supplied or not hmac.compare_digest(str(expected), str(supplied)):
                abort(400)

    @bp.after_request
    def private(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @bp.get('/planung')
    def planning():
        try:
            data = service.today(request.args.get('datum') or None)
        except ValueError as exc:
            return str(exc), 400
        return render_template('buero_planung.html', **data, kalender=False, csrf_token=csrf_token())

    @bp.get('/kalender')
    def calendar():
        try:
            data = service.calendar(request.args.get('datum') or None)
        except ValueError as exc:
            return str(exc), 400
        return render_template('buero_planung.html', **data, kalender=True, csrf_token=csrf_token())

    @bp.post('/kalender/termine')
    @bp.post('/kalender/termine/<int:note_id>')
    def save_note(note_id=None):
        try:
            service.save_note(request.form.to_dict(), note_id)
        except PlanningConflict as exc:
            return str(exc), 409
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except ValueError as exc:
            return str(exc), 400
        flash('Bürotermin gespeichert. Er ist auch im Werkstattkalender sichtbar.', 'success')
        return redirect('/werkstatt/buero/kalender?datum=' + _day(request.form['datum']).isoformat(), code=303)

    p.app.register_blueprint(bp)
    return service
