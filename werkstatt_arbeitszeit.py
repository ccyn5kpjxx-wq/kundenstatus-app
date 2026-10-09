"""Confirmed raw clock events and a separate daily break calculation.

Raw work/break seconds remain unchanged. The derived view applies a minimum
45-minute break once per original Berlin work-start date and rounds segment
boundaries to the nearest five minutes using the visible full minute.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import re
import uuid
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request

BERLIN = ZoneInfo('Europe/Berlin')
LABELS = {'kommen': 'Arbeitsbeginn', 'gehen': 'Arbeitsende', 'pause': 'Pause beginnen', 'weiter': 'Pause beenden'}
TRANSITIONS = {'abwesend': {'kommen': 'arbeitet'}, 'arbeitet': {'gehen': 'abwesend'},
               'pause': {'gehen': 'abwesend'}}
DAILY_BREAK_SECONDS = 45 * 60


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
            if action in ('pause', 'weiter'):
                raise ValueError('Pausen werden rechnerisch berücksichtigt. Nur Arbeitsbeginn oder Arbeitsende stempeln.')
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
            since = start_utc.isoformat()
            if previous:
                # The last crossing shift may share its start day with earlier
                # shifts. Their real pauses and daily deduction must count even
                # when those earlier shifts lie entirely in the preceding month.
                previous_day = datetime.fromisoformat(previous['zeit']).astimezone(BERLIN).replace(hour=0, minute=0, second=0, microsecond=0)
                first = db.execute("SELECT zeit FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND aktion='kommen' AND zeit>=? AND zeit<=? ORDER BY zeit LIMIT 1",
                                   (mid, previous_day.astimezone(timezone.utc).isoformat(), previous['zeit'])).fetchone()
                since = first['zeit']
            closing = db.execute("SELECT zeit FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND aktion='gehen' AND zeit>=? ORDER BY zeit LIMIT 1", (mid,end_utc.isoformat())).fetchone()
            until = closing['zeit'] if closing else max(now,end_utc).isoformat()
            events = [dict(row) for row in db.execute('SELECT aktion,zeit,revision FROM mitarbeiter_zeitstempel WHERE mitarbeiter_id=? AND zeit>=? AND zeit<=? ORDER BY revision LIMIT 10001', (mid,since,until)).fetchall()]
        if len(events) > 10000:
            raise ValueError('Zu viele Zeitstempel in diesem Zeitraum. Bitte Werkstattleitung prüfen lassen.')
        all_shifts, shift, paused, cursor, previous_event = [], None, False, None, None
        def seconds(a,b):
            return max(0, int((min(b,end_utc)-max(a,start_utc)).total_seconds()))
        def finish(when, ongoing):
            if not shift:
                return
            if cursor:
                shift['pause_sekunden' if paused else 'arbeit_sekunden'] += seconds(cursor,when)
                shift['_segments'].append(('pause' if paused else 'arbeit', cursor, when))
                shift['_invalid'] |= when < cursor
            shift.update(ende=None if ongoing else when, offen=ongoing,
                         pruefen=(when-shift['start']).total_seconds()>24*3600,
                         _in_month=when > start_utc and shift['start'] < end_utc)
            all_shifts.append(shift.copy())
        for event in events:
            when=datetime.fromisoformat(event['zeit']).astimezone(timezone.utc)
            action=event['aktion']
            backwards = previous_event is not None and when < previous_event
            previous_event = when
            if backwards:
                if shift:
                    shift['_invalid'] = True
                elif all_shifts:
                    all_shifts[-1]['_invalid'] = True
            if action=='kommen':
                if shift:
                    raise ValueError('Zeitstempel sind nicht lückenlos. Werkstattleitung muss prüfen.')
                shift={'start':when,'arbeit_sekunden':0,'pause_sekunden':0,'_segments':[],'_invalid':backwards};cursor=when;paused=False
            elif not shift:
                raise ValueError('Arbeitsbeginn fehlt zu vorhandenen Zeitstempeln. Bitte prüfen lassen.')
            elif action=='gehen':
                finish(when,False);shift=None;cursor=None;paused=False
            elif action in ('pause','weiter'):
                if paused == (action=='pause'):
                    raise ValueError('Pausenfolge ist widersprüchlich. Bitte prüfen lassen.')
                shift['pause_sekunden' if paused else 'arbeit_sekunden']+=seconds(cursor,when)
                shift['_segments'].append(('pause' if paused else 'arbeit', cursor, when))
                shift['_invalid'] |= when < cursor
                paused=action=='pause';cursor=when
            else:
                # Preserve the old raw view, but never calculate a finalized
                # total from an unknown historical action.
                shift['_invalid'] = True
        if shift:
            finish(now,True)
        days = self._calculated_days(all_shifts, start_utc, end_utc)
        shifts = [item for item in all_shifts if item['_in_month']]
        for item in shifts:
            item['original_beginn_iso'] = item['start'].isoformat()
            item['original_ende_iso'] = item['ende'].isoformat() if item['ende'] else None
            item['original_beginn'] = item['start'].astimezone(BERLIN).strftime('%d.%m.%Y %H:%M:%S')
            item['original_ende'] = item['ende'].astimezone(BERLIN).strftime('%d.%m.%Y %H:%M:%S') if item['ende'] else None
            item['original_beginn_zeitzone'] = self.offset_label(item['start'].astimezone(BERLIN))
            item['original_ende_zeitzone'] = self.offset_label(item['ende'].astimezone(BERLIN)) if item['ende'] else None
            item['beginn']=item.pop('start').astimezone(BERLIN).strftime('%d.%m.%Y %H:%M')
            item['ende']=item['ende'].astimezone(BERLIN).strftime('%d.%m.%Y %H:%M') if item['ende'] else None
            item['arbeitszeit']=self.duration(item['arbeit_sekunden'])
            item['pause']=self.duration(item['pause_sekunden'])
            for private in ('_segments', '_invalid', '_in_month'):
                item.pop(private)
        valid=[item for item in shifts if not item['offen'] and not item['pruefen']]
        result = {'mitarbeiter':dict(employee),'monat':month,'schichten':shifts,
                'abgeschlossene_arbeitszeit':self.duration(sum(item['arbeit_sekunden'] for item in valid)),
                'abgeschlossene_arbeit_sekunden':sum(item['arbeit_sekunden'] for item in valid),
                'berechnete_abgeschlossene_arbeitszeit':self.exact_duration(sum(day['berechnete_arbeit_sekunden'] or 0 for day in days)),
                'berechnete_abgeschlossene_arbeit_sekunden':sum(day['berechnete_arbeit_sekunden'] or 0 for day in days),
                'pausenabzug':self.exact_duration(sum(day['pausenabzug_sekunden'] or 0 for day in days)),
                'pausenabzug_sekunden':sum(day['pausenabzug_sekunden'] or 0 for day in days),
                'arbeitstage':days,
                'berechnung_pruefen':any(day['berechnung_pruefen'] for day in days),
                'pruefen':any(item['pruefen'] for item in shifts),
                'hinweis':'Beginn und Ende werden nur rechnerisch auf die nächstgelegenen 5 Minuten gerundet: volle Minute 0–2 abwärts, 3–4 aufwärts; Originalsekunden bleiben erhalten. Historische Pausengrenzen folgen derselben Rechenregel. Mindestens 45 Minuten Pause einmal je Arbeitstag (ursprüngliches Berliner Datum des Arbeitsbeginns); längere berechnete Altpausen bleiben erhalten. Der zusätzliche Pausenabzug ergänzt nur die fehlenden Minuten. Keine Änderung der Rohstempel, gemessenen Zeiten oder Lohnabrechnung. Offene oder auffällige Arbeitstage sind nicht in der berechneten Summe abgeschlossener Zeiten.'}
        # The optional personal profile supplies a separately labelled Sollplan.
        # It never changes events, measured work/break duration or monthly sums.
        work_plan = getattr(getattr(self.p, 'employee_portal', None), 'work_plan', None)
        if callable(work_plan):
            result['arbeitsplan'] = work_plan(mid)
        return result

    @classmethod
    def _calculated_days(cls, shifts, month_start, month_end):
        """Allocate the extra daily deduction before clipping to a month.

        Round every historical segment boundary on the real UTC timeline,
        keeping spring gaps and both autumn folds valid. Attribution of the
        extra break to the earliest working seconds is accounting only; it
        does not claim when a real break happened and creates no clock events.
        A workday containing an open/suspect shift has no finalized calculation.
        """
        groups = {}
        for item in shifts:
            day = item['start'].astimezone(BERLIN).date().isoformat()
            item['arbeitstag'] = day
            rounded_start = cls.round_clock(item['start']).astimezone(BERLIN)
            rounded_end = cls.round_clock(item['ende']).astimezone(BERLIN) if item['ende'] else None
            offsets = {item['start'].astimezone(BERLIN).utcoffset(), rounded_start.utcoffset()}
            if rounded_end:
                offsets.update((item['ende'].astimezone(BERLIN).utcoffset(), rounded_end.utcoffset()))
            item.update(berechneter_beginn=rounded_start.strftime('%d.%m.%Y %H:%M'),
                        berechnetes_ende=rounded_end.strftime('%d.%m.%Y %H:%M') if rounded_end else None,
                        berechneter_beginn_iso=rounded_start.isoformat(),
                        berechnetes_ende_iso=rounded_end.isoformat() if rounded_end else None,
                        berechneter_beginn_zeitzone=cls.offset_label(rounded_start),
                        berechnetes_ende_zeitzone=cls.offset_label(rounded_end) if rounded_end else None,
                        zeitumstellung=len(offsets) > 1)
            groups.setdefault(day, []).append(item)
        days = []
        for day, items in sorted(groups.items()):
            visible = [item for item in items if item['_in_month']]
            if not visible:
                continue
            incomplete = any(item['offen'] or item['pruefen'] or item['_invalid'] for item in items)
            segments = {id(item): [(kind, cls.round_clock(start), cls.round_clock(end))
                                   for kind, start, end in item['_segments']] for item in items}
            work = sum(max(0, int((end-start).total_seconds())) for item in items
                       for kind, start, end in segments[id(item)] if kind == 'arbeit')
            rounded_pause = sum(max(0, int((end-start).total_seconds())) for item in items
                                for kind, start, end in segments[id(item)] if kind == 'pause')
            remaining = min(work, max(0, DAILY_BREAK_SECONDS - rounded_pause))
            for item in items:
                extra = calculated_work = calculated_pause = 0
                if not incomplete:
                    for kind, start, end in segments[id(item)]:
                        included = max(0, int((min(end,month_end)-max(start,month_start)).total_seconds()))
                        if kind != 'arbeit':
                            calculated_pause += included
                            continue
                        calculated_work += included
                        used = min(remaining, max(0, int((end-start).total_seconds())))
                        stop = start + timedelta(seconds=used)
                        extra += max(0, int((min(stop,month_end)-max(start,month_start)).total_seconds()))
                        remaining -= used
                item['berechnung_pruefen'] = incomplete
                item['pausenabzug_sekunden'] = None if incomplete else extra
                item['pausenabzug'] = None if incomplete else cls.exact_duration(extra)
                item['berechnete_pause_sekunden'] = None if incomplete else calculated_pause
                item['berechnete_pause'] = None if incomplete else cls.exact_duration(calculated_pause)
                item['berechnete_arbeit_sekunden'] = None if incomplete else max(0, calculated_work - extra)
                item['berechnete_arbeitszeit'] = None if incomplete else cls.exact_duration(item['berechnete_arbeit_sekunden'])
            computed = None if incomplete else sum(item['berechnete_arbeit_sekunden'] for item in visible)
            extra = None if incomplete else sum(item['pausenabzug_sekunden'] for item in visible)
            days.append(dict(datum=day, berechnung_pruefen=incomplete,
                             berechnete_arbeit_sekunden=computed,
                             berechnete_arbeitszeit=None if incomplete else cls.exact_duration(computed),
                             pausenabzug_sekunden=extra,
                             pausenabzug=None if incomplete else cls.exact_duration(extra)))
        return days

    @staticmethod
    def round_clock(when):
        """Visible minute rule, independent of seconds and preserving DST fold."""
        instant = when.astimezone(timezone.utc)
        minute = instant.astimezone(BERLIN).minute % 5
        delta = -minute if minute <= 2 else 5 - minute
        return instant.replace(second=0, microsecond=0) + timedelta(minutes=delta)

    @staticmethod
    def offset_label(when):
        offset = when.strftime('%z')
        return 'UTC' + offset[:3] + ':' + offset[3:]

    @staticmethod
    def duration(seconds):
        minutes=seconds//60
        return f'{minutes//60}:{minutes%60:02d} Stunden'

    @staticmethod
    def exact_duration(seconds):
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        suffix = f':{seconds:02d}' if seconds else ''
        return f'{hours}:{minutes:02d}{suffix} Stunden'

    def summary(self, who, month=None):
        state=self.state(who)
        report=self.report(state['mitarbeiter_id'],month or self.now().astimezone(BERLIN).strftime('%Y-%m'))
        return {**report,'status':state}

    def admin_employees(self):
        """Current clock state for the team, independent of the report month."""
        now = self.now().astimezone(BERLIN)
        with self.db() as db:
            employees = [dict(row) for row in db.execute('''
                SELECT m.id,m.name,m.aktiv,s.zustand,z.aktion,z.zeit
                FROM mitarbeiter m
                LEFT JOIN mitarbeiter_zeitstatus s ON s.mitarbeiter_id=m.id
                LEFT JOIN mitarbeiter_zeitstempel z ON z.id=(
                    SELECT id FROM mitarbeiter_zeitstempel
                    WHERE mitarbeiter_id=m.id ORDER BY revision DESC LIMIT 1)
                ORDER BY m.aktiv DESC,m.name,m.id
            ''').fetchall()]
        for employee in employees:
            state = employee.pop('zustand')
            action, raw_time = employee.pop('aktion'), employee.pop('zeit')
            when = None
            if raw_time:
                try:
                    when = datetime.fromisoformat(raw_time).astimezone(BERLIN)
                except (ValueError, TypeError):
                    pass
            key, label, detail = 'pruefen', 'Zeitstatus prüfen', ''
            if not employee['aktiv']:
                key, label = 'inaktiv', 'Inaktiv'
            elif state in ('arbeitet', 'pause'):
                key = state
                label = 'Angestempelt' if state == 'arbeitet' else 'In Pause'
                if when:
                    detail = 'Seit ' + when.strftime('%H:%M Uhr' if when.date() == now.date() else '%d.%m.%Y, %H:%M Uhr')
            elif state in (None, 'abwesend'):
                if action == 'gehen' and when and when.date() == now.date():
                    key, label, detail = 'beendet', 'Beendet', when.strftime('Heute um %H:%M Uhr')
                elif not action or (when and when.date() < now.date() and action == 'gehen'):
                    key, label, detail = 'nicht_angestempelt', 'Noch nicht angestempelt', 'Heute noch kein Arbeitsbeginn'
            computed_detail, original_detail = detail, ''
            if when and key in ('arbeitet', 'pause', 'beendet'):
                rounded = self.round_clock(when).astimezone(BERLIN)
                same_day = rounded.date() == when.date() == now.date()
                rounded_label = rounded.strftime('%H:%M Uhr' if same_day else '%d.%m.%Y, %H:%M Uhr')
                original_label = when.strftime('%H:%M:%S Uhr' if when.date() == now.date() else '%d.%m.%Y, %H:%M:%S Uhr')
                if key == 'beendet':
                    computed_detail = ('Heute um ' if same_day else 'Arbeitsende ') + rounded_label
                    original_detail = 'Heute um ' + original_label
                else:
                    computed_detail = 'Seit ' + rounded_label
                    original_detail = 'Seit ' + original_label
                computed_detail += ' (' + self.offset_label(rounded) + ') · berechnet'
                original_detail += ' (' + self.offset_label(when) + ') · Originalstempel'
            employee['zeitstatus'] = {'key': key, 'label': label, 'detail': detail,
                                     'detail_berechnet': computed_detail, 'detail_original': original_detail}
        return employees


def register_time_views(p, bp, protected, service):
    @bp.get('/arbeitszeit')
    @protected
    def personal_time(who):
        # An existing personal session wins even if this shared browser still
        # carries an admin flag. Resolve it freshly; no client employee ID or
        # broad admin identity may become the subject of a personal time form.
        employee_portal = getattr(p, 'employee_portal', None)
        personal = employee_portal.identity() if employee_portal is not None else None
        if personal is not None:
            who = personal
        elif who.get('actor') == 'admin':
            return redirect('/admin/arbeitszeit')
        report=service.summary(who,request.args.get('monat'))
        return render_template('assistent_arbeitszeit.html',report=report,admin=False,employees=[],
                               request_id=p.employee_portal.new_time_form(who, report['status']['revision']))

    admin=Blueprint('arbeitszeit_admin',__name__)
    @admin.get('/admin/arbeitszeit')
    @p.admin_required
    def index():
        employees=service.admin_employees()
        report=None;error='';selected=0
        try:
            selected=int(request.args.get('mitarbeiter_id') or 0)
            if selected:
                report=service.report(selected,request.args.get('monat') or service.now().astimezone(BERLIN).strftime('%Y-%m'))
        except (ValueError,TypeError) as exc:
            error=str(exc)
        return render_template('assistent_arbeitszeit.html',report=report,admin=True,employees=employees,error=error,selected_employee_id=selected,
                               status_as_of=service.now().astimezone(BERLIN).strftime('%d.%m.%Y, %H:%M Uhr'),
                               month=request.args.get('monat') or service.now().astimezone(BERLIN).strftime('%Y-%m'))
    p.app.register_blueprint(admin)
