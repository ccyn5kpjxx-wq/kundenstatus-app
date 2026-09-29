"""Personal, explicitly confirmed clock events; no payroll or automatic breaks."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import re
import uuid
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, render_template, request

BERLIN = ZoneInfo('Europe/Berlin')
LABELS = {'kommen': 'Arbeitsbeginn', 'gehen': 'Arbeitsende', 'pause': 'Pause beginnen', 'weiter': 'Pause beenden'}
TRANSITIONS = {'abwesend': {'kommen': 'arbeitet'}, 'arbeitet': {'pause': 'pause', 'gehen': 'abwesend'},
               'pause': {'weiter': 'arbeitet', 'gehen': 'abwesend'}}


def personal_id(who):
    mid = who.get('mitarbeiter_id') if isinstance(who, dict) else None
    if type(mid) is not int or mid < 1 or who.get('actor') != f'mitarbeiter:{mid}' or not who.get('lesen'):
        raise ValueError('Dafür bitte mit deinem persönlichen Mitarbeiterzugang anmelden.')
    return mid


class TimeTracking:
    def __init__(self, portal, now=None):
        self.p = portal
        self.now = now or (lambda: datetime.now(timezone.utc))
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
            db.executescript('''CREATE TABLE IF NOT EXISTS mitarbeiter_zeitstatus (
                mitarbeiter_id INTEGER PRIMARY KEY, zustand TEXT NOT NULL DEFAULT 'abwesend', revision INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS mitarbeiter_zeitstempel (
                id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL, aktion TEXT NOT NULL,
                zeit TEXT NOT NULL, request_id TEXT NOT NULL, revision INTEGER NOT NULL,
                UNIQUE(mitarbeiter_id, request_id));
                CREATE INDEX IF NOT EXISTS idx_mitarbeiter_zeit_zeit ON mitarbeiter_zeitstempel(mitarbeiter_id,zeit);''')

    def state(self, who):
        mid = personal_id(who)
        with self.db() as db:
            employee = db.execute('SELECT name FROM mitarbeiter WHERE id=? AND aktiv=1', (mid,)).fetchone()
            if not employee:
                raise ValueError('Der Mitarbeiterzugang ist nicht mehr aktiv.')
            row = db.execute('SELECT zustand,revision FROM mitarbeiter_zeitstatus WHERE mitarbeiter_id=?', (mid,)).fetchone()
        return {'mitarbeiter_id': mid, 'name': employee['name'], 'zustand': row['zustand'] if row else 'abwesend',
                'revision': row['revision'] if row else 0}

    def preview(self, who, action):
        state = self.state(who)
        if not isinstance(action,str) or action not in TRANSITIONS.get(state['zustand'], {}):
            raise ValueError('Dieser Stempel passt nicht zum aktuellen Stand. Erst den eigenen Zeitstatus prüfen.')
        return {'aktion': action, 'revision': state['revision'],
                'text': f"{LABELS[action]} für {state['name']} mit der Serverzeit beim Bestätigen erfassen."}

    def stamp(self, who, action, request_id, revision):
        mid = personal_id(who)
        if not isinstance(action,str) or action not in LABELS or not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9:_-]{8,120}', request_id):
            raise ValueError('Zeitstempel ist ungültig.')
        if type(revision) is not int or revision < 0:
            raise ValueError('Aktuellen Zeitstatus zuerst prüfen.')
        now = self.now().astimezone(timezone.utc)
        with self.db() as db:
            # Lock the existing employee before reading the idempotency key.
            # Concurrent repeated confirmations return the original stamp.
            if db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=? AND aktiv=1', (mid,)).rowcount != 1:
                raise ValueError('Mitarbeiterzugang ist nicht mehr aktiv.')
            prior = db.execute('SELECT aktion,zeit FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND request_id=?', (mid, request_id)).fetchone()
            if prior:
                if prior['aktion'] != action:
                    raise ValueError('Diese Bestätigung gehört zu einem anderen Zeitstempel.')
                return self._result(action, prior['zeit'], True)
            db.execute("INSERT INTO mitarbeiter_zeitstatus(mitarbeiter_id) VALUES(?) ON CONFLICT(mitarbeiter_id) DO NOTHING RETURNING mitarbeiter_id", (mid,)).fetchall()
            state = db.execute('SELECT zustand,revision FROM mitarbeiter_zeitstatus WHERE mitarbeiter_id=?', (mid,)).fetchone()
            next_state = TRANSITIONS.get(state['zustand'], {}).get(action)
            if not next_state or state['revision'] != revision:
                raise ValueError('Der Zeitstatus wurde inzwischen geändert. Bitte neu prüfen und bestätigen.')
            changed = db.execute('UPDATE mitarbeiter_zeitstatus SET zustand=?,revision=revision+1 WHERE mitarbeiter_id=? AND revision=?',
                                 (next_state, mid, revision)).rowcount
            if changed != 1:
                raise ValueError('Der Zeitstatus wurde inzwischen geändert. Bitte neu prüfen.')
            db.execute('INSERT INTO mitarbeiter_zeitstempel(id,mitarbeiter_id,aktion,zeit,request_id,revision) VALUES(?,?,?,?,?,?)',
                       (uuid.uuid4().hex, mid, action, now.isoformat(), request_id, revision+1))
        return self._result(action, now.isoformat(), False)

    @staticmethod
    def _result(action, when, repeated):
        local = datetime.fromisoformat(when).astimezone(BERLIN)
        return {'ok': True, 'wiederholt': repeated, 'aktion': action, 'zeit': when,
                'hinweis': f"{LABELS[action]} um {local.strftime('%H:%M')} Uhr erfasst."}

    def report(self, mid, month):
        if type(mid) is not int or mid < 1 or not isinstance(month, str) or not re.fullmatch(r'20\d{2}-(?:0[1-9]|1[0-2])', month):
            raise ValueError('Mitarbeiter und Monat gültig auswählen.')
        start = datetime.strptime(month, '%Y-%m').replace(tzinfo=BERLIN)
        end = (start.replace(day=28)+timedelta(days=4)).replace(day=1)
        start_utc, end_utc = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        now = self.now().astimezone(timezone.utc)
        with self.db() as db:
            employee = db.execute('SELECT id,name FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
            if not employee:
                raise ValueError('Mitarbeiter nicht gefunden.')
            previous = db.execute("SELECT zeit FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND aktion='kommen' AND zeit<? ORDER BY zeit DESC LIMIT 1", (mid,start_utc.isoformat())).fetchone()
            since = previous['zeit'] if previous else start_utc.isoformat()
            closing = db.execute("SELECT zeit FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND aktion='gehen' AND zeit>=? ORDER BY zeit LIMIT 1", (mid,end_utc.isoformat())).fetchone()
            until = closing['zeit'] if closing else max(now,end_utc).isoformat()
            events = [dict(row) for row in db.execute('SELECT aktion,zeit,revision FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND zeit>=? AND zeit<=? ORDER BY revision LIMIT 10001', (mid,since,until)).fetchall()]
        if len(events) > 10000:
            raise ValueError('Zu viele Zeitstempel in diesem Zeitraum. Bitte Werkstattleitung prüfen lassen.')
        shifts, shift, paused, cursor = [], None, False, None
        def seconds(a,b):
            return max(0, int((min(b,end_utc)-max(a,start_utc)).total_seconds()))
        def finish(when, ongoing):
            if not shift:
                return
            if cursor:
                shift['pause_sekunden' if paused else 'arbeit_sekunden'] += seconds(cursor,when)
            if when > start_utc and shift['start'] < end_utc:
                shift.update(ende=None if ongoing else when, offen=ongoing,
                             pruefen=(when-shift['start']).total_seconds()>24*3600)
                shifts.append(shift.copy())
        for event in events:
            when=datetime.fromisoformat(event['zeit']).astimezone(timezone.utc)
            action=event['aktion']
            if action=='kommen':
                if shift:
                    raise ValueError('Zeitstempel sind nicht lückenlos. Werkstattleitung muss prüfen.')
                shift={'start':when,'arbeit_sekunden':0,'pause_sekunden':0};cursor=when;paused=False
            elif not shift:
                raise ValueError('Arbeitsbeginn fehlt zu vorhandenen Zeitstempeln. Bitte prüfen lassen.')
            elif action=='gehen':
                finish(when,False);shift=None;cursor=None;paused=False
            elif action in ('pause','weiter'):
                if paused == (action=='pause'):
                    raise ValueError('Pausenfolge ist widersprüchlich. Bitte prüfen lassen.')
                shift['pause_sekunden' if paused else 'arbeit_sekunden']+=seconds(cursor,when)
                paused=action=='pause';cursor=when
        if shift:
            finish(now,True)
        for item in shifts:
            item['beginn']=item.pop('start').astimezone(BERLIN).strftime('%d.%m.%Y %H:%M')
            item['ende']=item['ende'].astimezone(BERLIN).strftime('%d.%m.%Y %H:%M') if item['ende'] else None
            item['arbeitszeit']=self.duration(item['arbeit_sekunden'])
            item['pause']=self.duration(item['pause_sekunden'])
        valid=[item for item in shifts if not item['offen'] and not item['pruefen']]
        return {'mitarbeiter':dict(employee),'monat':month,'schichten':shifts,
                'abgeschlossene_arbeitszeit':self.duration(sum(item['arbeit_sekunden'] for item in valid)),
                'pruefen':any(item['pruefen'] for item in shifts),
                'hinweis':'Erfasste Zeiten, keine Lohnabrechnung. Pausen werden nur nach eigenem Stempel abgezogen. Offene oder auffällige Schichten sind nicht in der Summe abgeschlossener Zeiten.'}

    @staticmethod
    def duration(seconds):
        minutes=seconds//60
        return f'{minutes//60}:{minutes%60:02d} Stunden'

    def summary(self, who, month=None):
        state=self.state(who)
        report=self.report(state['mitarbeiter_id'],month or self.now().astimezone(BERLIN).strftime('%Y-%m'))
        return {**report,'status':state}


def register_time_views(p, bp, protected, service):
    @bp.get('/arbeitszeit')
    @protected
    def personal_time(who):
        report=service.summary(who,request.args.get('monat'))
        return render_template('assistent_arbeitszeit.html',report=report,admin=False,employees=[])

    admin=Blueprint('arbeitszeit_admin',__name__)
    @admin.get('/admin/arbeitszeit')
    @p.admin_required
    def index():
        with service.db() as db:
            employees=[dict(row) for row in db.execute('SELECT id,name,aktiv FROM mitarbeiter ORDER BY aktiv DESC,name,id').fetchall()]
        report=None;error=''
        try:
            selected=int(request.args.get('mitarbeiter_id') or 0)
            if selected:
                report=service.report(selected,request.args.get('monat') or service.now().astimezone(BERLIN).strftime('%Y-%m'))
        except (ValueError,TypeError) as exc:
            error=str(exc)
        return render_template('assistent_arbeitszeit.html',report=report,admin=True,employees=employees,error=error,
                               month=request.args.get('monat') or service.now().astimezone(BERLIN).strftime('%Y-%m'))
    p.app.register_blueprint(admin)
