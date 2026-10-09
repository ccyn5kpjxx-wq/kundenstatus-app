"""Synthetic rendering contracts for the personal time/leave portal. No app import."""
from html.parser import HTMLParser
from pathlib import Path
import unittest
from urllib.parse import urlencode

from jinja2 import Environment, FileSystemLoader, meta, select_autoescape
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
ENV = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=select_autoescape(['html']))


def url_for(endpoint, **values):
    if endpoint == 'static':
        return '/static/' + values['filename']
    if endpoint == 'arbeitszeit_admin.index':
        return '/admin/arbeitszeit' + ('?' + urlencode(values) if values else '')
    return {'assistent.page': '/werkstatt/assistent', 'admin_mitarbeiter': '/admin/mitarbeiter',
            'betriebs_cockpit': '/admin/cockpit',
            'assistent.vacation_admin': '/werkstatt/assistent/urlaub/verwaltung',
            'assistent.vacation_apply': '/werkstatt/assistent/urlaub/antrag',
            'assistent.vacation_withdraw': '/werkstatt/assistent/urlaub/antrag/' + str(values.get('request_id', '')) + '/zurueckziehen'}[endpoint]


def render(template, **values):
    return ENV.get_template(template).render(
        url_for=url_for, csrf_field=lambda: '<input type="hidden" name="csrf_token" value="synthetic-csrf">',
        get_flashed_messages=lambda **kwargs: values.pop('flashes', []), **values)


class Forms(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.forms, self.active, self.links, self.ids = [], None, [], []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        data = dict(attrs)
        if data.get('id'):
            self.ids.append(data['id'])
        if tag == 'a':
            self.links.append(data.get('href', ''))
        if tag == 'form':
            if self.active is not None:
                raise AssertionError('Nested form')
            self.active = dict(data, inputs=[], buttons=[])
            self.forms.append(self.active)
        if self.active is not None and tag in ('input', 'button'):
            self.active['inputs' if tag == 'input' else 'buttons'].append(data)

    def handle_endtag(self, tag):
        if tag == 'form':
            self.active = None


class TextParts(HTMLParser):
    """Distinguish initially visible figures from deliberately opened originals."""
    def __init__(self, html):
        super().__init__()
        self.depth, self.main, self.details = 0, [], []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        if tag == 'details':
            self.depth += 1

    def handle_endtag(self, tag):
        if tag == 'details':
            self.depth -= 1

    def handle_data(self, text):
        (self.details if self.depth else self.main).append(text)


def plan(known=True):
    return dict(bekannt=known, tage=[0, 1, 2, 3, 4] if known else [],
                tage_label='Montag–Freitag' if known else '',
                wochenstunden='40' if known else '', tagesstunden='8' if known else '',
                pausenminuten=60 if known else None, beginn='08:00' if known else '',
                ende='17:00' if known else '', hinweis=('Sollplan. Keine Stempel oder automatischen Abzüge.' if known else
                'Noch kein persönlicher Arbeitsplan hinterlegt. Keine Stempel oder automatischen Abzüge.'))


def report(state='abwesend', known=False):
    return dict(mitarbeiter=dict(id=101, name='Synthetic Own'), monat='2026-10',
                status=dict(zustand=state, revision=7), schichten=[],
                abgeschlossene_arbeitszeit='7:35 Stunden', hinweis='Erfasste Zeiten, keine Lohnabrechnung.',
                berechnete_abgeschlossene_arbeitszeit='6:50 Stunden', pausenabzug='0:45 Stunden',
                berechnung_pruefen=False,
                arbeitsplan=plan(known))


def personal_context(**values):
    return dict(employee=dict(id=101, name='Synthetic Own'), profile={}, payrolls=[],
                betriebsurlaub=[], arbeitszeit=None, urlaub=dict(bekannt=False), **values)


class PersonalTimeUITests(unittest.TestCase):
    def test_admin_employee_cards_show_each_current_clock_status(self):
        statuses = [('arbeitet','Angestempelt'),('pause','In Pause'),('beendet','Beendet'),
                    ('nicht_angestempelt','Noch nicht angestempelt'),('inaktiv','Inaktiv')]
        employees = [dict(id=index,name='Synthetic '+key,aktiv=key!='inaktiv',
                          zeitstatus=dict(key=key,label=label,detail='Seit 09:00 Uhr'))
                     for index,(key,label) in enumerate(statuses,1)]
        html = render('assistent_arbeitszeit.html', admin=True, report=None, month='2026-01', error='',
                      employees=employees, status_as_of='08.10.2026, 10:00 Uhr')
        for key,label in statuses:
            self.assertIn('state-'+key, html)
            self.assertIn(label, html)
        self.assertEqual(html.count('class="status-dot"'), 5)
        self.assertIn('Aktueller Stempelstatus · Stand 08.10.2026, 10:00 Uhr', html)
        self.assertEqual(html.count('<small>Monatszeiten ansehen</small>'), 4)
        self.assertFalse(any(form.get('method') == 'post' for form in Forms(html).forms))

    def test_admin_selected_employee_heading_shows_current_status(self):
        data = report()
        del data['status']
        employee = dict(id=101,name='Synthetic Own',aktiv=True,
                        zeitstatus=dict(key='pause',label='In Pause',detail='Seit 12:00 Uhr'))
        html = render('assistent_arbeitszeit.html', admin=True, report=data, month='2026-01', error='',
                      employees=[employee])
        self.assertIn('report-clock-status', html)
        self.assertIn('In Pause', html)
        self.assertIn('Seit 12:00 Uhr', html)
        self.assertNotIn('/werkstatt/mein-konto/zeit', html)

    def test_current_clock_detail_uses_calculated_time_with_raw_seconds_collapsed(self):
        employee = dict(id=101, name='Synthetic Own', aktiv=True, zeitstatus=dict(key='beendet',
            label='Beendet', detail='Heute um 16:27 Uhr', detail_berechnet='Heute um 16:25 Uhr · berechnet',
            detail_original='Heute um 16:27:49 Uhr · Originalstempel'))
        for selected in (False, True):
            with self.subTest(selected=selected):
                html = render('assistent_arbeitszeit.html', admin=True, report=report() if selected else None,
                    month='2026-10', employees=[employee], error='')
                parts = TextParts(html)
                main, details = ' '.join(parts.main), ' '.join(parts.details)
                self.assertIn('Heute um 16:25 Uhr · berechnet', main)
                self.assertNotIn('Heute um 16:27 Uhr', main)
                self.assertNotIn('16:27:49', main)
                if selected:
                    self.assertIn('Heute um 16:27:49 Uhr · Originalstempel', details)
                else:
                    self.assertNotIn('Heute um 16:27:49 Uhr · Originalstempel', html)

    def test_legacy_current_clock_detail_is_explicitly_an_original_stamp(self):
        html = render('assistent_arbeitszeit.html', admin=True, report=None, month='2026-10', error='',
            employees=[dict(id=101, name='Synthetic Own', aktiv=True,
                zeitstatus=dict(key='arbeitet', label='Angestempelt', detail='Seit 08:32 Uhr'))])
        self.assertIn('Originalstempel: Seit 08:32 Uhr', ' '.join(TextParts(html).main))

    def test_personal_status_transitions_keep_all_bound_fields(self):
        for state, allowed in [('abwesend', {'kommen'}), ('arbeitet', {'gehen'}),
                               ('pause', {'gehen'}), ('unknown', set())]:
            with self.subTest(state=state):
                html = render('assistent_arbeitszeit.html', admin=False, report=report(state),
                              request_id='synthetic-personal-nonce', employees=[dict(id=2, name='Other Secret')])
                parsed = Forms(html)
                forms = [form for form in parsed.forms if form.get('method') == 'post']
                self.assertEqual(len(forms), 2)
                for form in forms:
                    self.assertEqual(form['action'], '/werkstatt/mein-konto/zeit')
                    fields = {item['name']: item.get('value') for item in form['inputs']}
                    self.assertEqual(set(fields), {'csrf_token', 'aktion', 'revision', 'request_id', 'confirmed'})
                    self.assertEqual((fields['csrf_token'], fields['revision'], fields['request_id'], fields['confirmed']),
                                     ('synthetic-csrf', '7', 'synthetic-personal-nonce', 'ja'))
                    self.assertEqual('disabled' not in form['buttons'][0], fields['aktion'] in allowed)
                self.assertEqual({next(item['value'] for item in form['inputs'] if item['name'] == 'aktion') for form in forms}, {'kommen','gehen'})
                self.assertNotIn('Pause beginnen', html)
                self.assertNotIn('Pause beenden', html)
                self.assertNotIn('Other Secret', html)
                self.assertNotIn('name="mitarbeiter_id"', html)
                self.assertFalse(any(link.startswith('/admin') for link in parsed.links))
                self.assertIn('class="access-header"', html)
                self.assertNotIn('/static/assistent.css', html)

    def test_admin_filters_remain_get_only_and_separate(self):
        html = render('assistent_arbeitszeit.html', admin=True, report=None, month='2026-10', error='',
                      employees=[dict(id=2, name='Synthetic Admin Employee', aktiv=False)])
        parsed = Forms(html)
        self.assertEqual(len(parsed.forms), 1)
        self.assertEqual(parsed.forms[0]['method'], 'get')
        self.assertIn('name="mitarbeiter_id"', html)
        self.assertIn('Synthetic Admin Employee · inaktiv', html)
        self.assertNotIn('employee-portal-menu', html)
        self.assertNotIn('access-header', html)
        self.assertIn('/static/arbeitszeit_admin.css', html)
        self.assertNotIn('/static/mitarbeiter_zeiten.css', html)
        self.assertIn('/admin/cockpit', parsed.links)
        self.assertIn('/admin/arbeitszeit?mitarbeiter_id=2&monat=2026-10', parsed.links)

    def test_calculation_is_primary_and_original_seconds_are_not_rewritten(self):
        data = report()
        data.update(abgeschlossene_arbeitszeit='7:35:07 Stunden',
                    berechnete_abgeschlossene_arbeitszeit='6:50 Stunden')
        data['schichten'] = [dict(beginn='08.10.2026 08:00', ende='08.10.2026 15:35',
            original_beginn='08.10.2026 08:00:02', original_ende='08.10.2026 15:35:09',
            berechneter_beginn='08.10.2026 08:00', berechnetes_ende='08.10.2026 15:35',
            arbeitszeit='7:35:07 Stunden', pause='0:00 Stunden',
            berechnete_arbeitszeit='6:50 Stunden', pausenabzug='0:45 Stunden',
            offen=False, pruefen=False, berechnung_pruefen=False)]
        for admin in (False, True):
            with self.subTest(admin=admin):
                html = render('assistent_arbeitszeit.html', admin=admin, report=data,
                    request_id='synthetic-nonce', month='2026-10',
                    employees=[dict(id=101,name='Synthetic Own',aktiv=1)])
                parts = TextParts(html)
                main, details = ' '.join(parts.main), ' '.join(parts.details)
                self.assertIn('6:50 Stunden', main)
                self.assertNotIn('6:50:07 Stunden', main)
                self.assertIn('08.10.2026 08:00', main)
                self.assertIn('08.10.2026 15:35', main)
                self.assertNotIn('08.10.2026 08:00:02', main)
                self.assertNotIn('08.10.2026 15:35:09', main)
                self.assertIn('08.10.2026 08:00:02', details)
                self.assertIn('08.10.2026 15:35:09', details)
                self.assertIn('Auf 5 Minuten gerundet', main)
                self.assertIn('0:45 Stunden', main)
                self.assertNotIn('7:35:07 Stunden', main)
                self.assertIn('7:35:07 Stunden', details)
                self.assertIn('Erfasste Pause', details)
                self.assertNotIn('Erfasste Pause', main)
                self.assertIn('Früher gestempelte Pausen zählen mit', main)

    def test_rounded_start_and_end_are_supplied_without_template_rounding(self):
        data = report()
        data['schichten'] = [dict(beginn='08.10.2026 14:32', ende='08.10.2026 15:33',
            original_beginn='08.10.2026 14:32:59', original_ende='08.10.2026 15:33:01',
            berechneter_beginn='08.10.2026 14:30', berechnetes_ende='08.10.2026 15:35',
            arbeitszeit='1:00 Stunden', pause='0:00 Stunden', berechnete_arbeitszeit='0:20 Stunden',
            pausenabzug='0:45 Stunden', offen=False, pruefen=False, berechnung_pruefen=False)]
        for admin in (False, True):
            with self.subTest(admin=admin):
                html = render('assistent_arbeitszeit.html', admin=admin, report=data,
                    request_id='synthetic-nonce', month='2026-10', employees=[])
                parts = TextParts(html)
                main, details = ' '.join(parts.main), ' '.join(parts.details)
                self.assertIn('08.10.2026 14:30', main)
                self.assertIn('08.10.2026 15:35', main)
                self.assertIn('Beginn · gerundet', main)
                self.assertIn('Ende · gerundet', main)
                self.assertNotIn('08.10.2026 14:32:59', main)
                self.assertNotIn('08.10.2026 15:33:01', main)
                self.assertIn('08.10.2026 14:32:59', details)
                self.assertIn('08.10.2026 15:33:01', details)
                self.assertIn('14:32 wird 14:30, 14:33 wird 14:35', main)

    def test_missing_rounded_timestamps_never_look_like_calculated_raw_stamps(self):
        data = report()
        data['schichten'] = [dict(beginn='08.10.2026 14:32', ende='08.10.2026 15:33',
            berechneter_beginn=None, berechnetes_ende=None, arbeitszeit='1:01 Stunden',
            pause='0:00 Stunden', berechnete_arbeitszeit=None, pausenabzug=None,
            offen=False, pruefen=False, berechnung_pruefen=True)]
        for admin in (False, True):
            with self.subTest(admin=admin):
                html = render('assistent_arbeitszeit.html', admin=admin, report=data,
                    request_id='synthetic-nonce', month='2026-10', employees=[])
                parts = TextParts(html)
                main, details = ' '.join(parts.main), ' '.join(parts.details)
                self.assertIn('Berechnung ausstehend', main)
                self.assertNotIn('08.10.2026 14:32', main)
                self.assertNotIn('08.10.2026 15:33', main)
                self.assertIn('08.10.2026 14:32', details)
                self.assertIn('08.10.2026 15:33', details)

    def test_dst_offsets_distinguish_equal_rounded_clock_labels(self):
        data = report()
        data['schichten'] = [dict(beginn='25.10.2026 02:30', ende='25.10.2026 02:30',
            berechneter_beginn='25.10.2026 02:30', berechnetes_ende='25.10.2026 02:30',
            zeitumstellung=True, berechneter_beginn_zeitzone='UTC+02:00', berechnetes_ende_zeitzone='UTC+01:00',
            original_beginn='25.10.2026 02:30:21', original_ende='25.10.2026 02:30:45',
            original_beginn_zeitzone='UTC+02:00', original_ende_zeitzone='UTC+01:00',
            arbeitszeit='1:00 Stunden', pause='0:00 Stunden', berechnete_arbeitszeit='0:15 Stunden',
            pausenabzug='0:45 Stunden', offen=False, pruefen=False, berechnung_pruefen=False)]
        for admin in (False, True):
            with self.subTest(admin=admin):
                html = render('assistent_arbeitszeit.html', admin=admin, report=data,
                    request_id='synthetic-nonce', month='2026-10', employees=[])
                parts = TextParts(html)
                main, details = ' '.join(parts.main), ' '.join(parts.details)
                self.assertIn('25.10.2026 02:30:21', details)
                self.assertIn('25.10.2026 02:30:45', details)
                self.assertIn('UTC+02:00', details)
                self.assertIn('UTC+01:00', details)
                self.assertIn('UTC+02:00', main)
                self.assertIn('UTC+01:00', main)
                self.assertIn('0:15 Stunden', main)

    def test_closed_shift_with_incomplete_day_is_not_a_finished_calculated_value(self):
        data = report('arbeitet')
        data.update(berechnung_pruefen=True, berechnete_abgeschlossene_arbeitszeit='0:00 Stunden',
                    pausenabzug='0:00 Stunden')
        data['schichten'] = [dict(beginn='08.10.2026 08:00', ende='08.10.2026 12:00',
            arbeitszeit='4:00 Stunden', pause='0:00 Stunden', berechnete_arbeitszeit=None,
            pausenabzug=None, offen=False, pruefen=False, berechnung_pruefen=True)]
        for admin in (False, True):
            with self.subTest(admin=admin):
                html = render('assistent_arbeitszeit.html', admin=admin, report=data,
                    request_id='synthetic-nonce', month='2026-10',
                    employees=[dict(id=101,name='Synthetic Own',aktiv=1)])
                main = ' '.join(TextParts(html).main)
                self.assertIn('Tagesberechnung ausstehend', main)
                self.assertIn('Diese Schicht ist nicht in der berechneten Monatszeit enthalten', main)
                self.assertIn('Ausstehend', main)
                self.assertNotIn('4:00 Stunden', main)

    def test_missing_calculation_never_falls_back_to_raw_sum(self):
        data = report()
        del data['berechnete_abgeschlossene_arbeitszeit']
        del data['pausenabzug']
        html = render('assistent_arbeitszeit.html', admin=False, report=data, request_id='synthetic-nonce')
        parts = TextParts(html)
        self.assertIn('Nicht verfügbar', ' '.join(parts.main))
        self.assertNotIn('7:35 Stunden', ' '.join(parts.main))
        self.assertIn('7:35 Stunden', ' '.join(parts.details))

    def test_profile_card_uses_calculated_month_without_a_raw_today_claim(self):
        data = personal_context()
        data['arbeitszeit'] = dict(status_label='Bei der Arbeit', monat_stunden='17:30 Stunden',
            heute_stunden='9:00', berechnete_monat_stunden='16:30 Stunden', pausenabzug='1:00 Stunden')
        html = render('mitarbeiter_portal.html', **data)
        self.assertIn('16:30 Stunden berechnet im laufenden Monat', html)
        self.assertIn('45 Minuten Pause pro Arbeitstag', html)
        self.assertNotIn('17:30 Stunden', html)
        self.assertNotIn('9:00 Stunden heute', html)
        self.assertNotIn('Start, Pause und Feierabend stempeln', html)

    def test_admin_report_needs_no_personal_status_or_stamp_and_keeps_warning_totals(self):
        data = report()
        del data['status']
        data['mitarbeiter']['name'] = 'Synthetic Very Long Employee Name <script>unsafe</script>'
        data['schichten'] = [dict(beginn='08.10.2026 08:00', ende=None, arbeitszeit='4:10 Stunden',
                                 pause='0:20 Stunden', offen=True, pruefen=False),
                             dict(beginn='07.10.2026 08:00', ende='08.10.2026 12:30', arbeitszeit='26:00 Stunden',
                                  pause='2:30 Stunden', offen=False, pruefen=True)]
        html = render('assistent_arbeitszeit.html', admin=True, report=data, month='2026-10', error='',
                      employees=[dict(id=101, name=data['mitarbeiter']['name'], aktiv=True)])
        parsed = Forms(html)
        self.assertEqual(len(parsed.forms), 1)
        self.assertEqual(parsed.forms[0]['method'], 'get')
        self.assertIn('/admin/cockpit', parsed.links)
        self.assertIn('name="mitarbeiter_id"', html)
        self.assertIn('name="monat"', html)
        self.assertIn('7:35 Stunden', html)
        self.assertIn('4:10 Stunden', html)
        self.assertIn('26:00 Stunden', html)
        self.assertIn('Noch offen', html)
        self.assertIn('24 Stunden', html)
        self.assertIn('&lt;script&gt;unsafe&lt;/script&gt;', html)
        self.assertNotIn('<script>', html)
        self.assertNotIn('/werkstatt/mein-konto/zeit', html)
        self.assertIn('/admin/arbeitszeit?monat=2026-10', parsed.links)

    def test_admin_validation_error_keeps_selected_employee_and_escaped_message(self):
        html = render('assistent_arbeitszeit.html', admin=True, report=None, month='invalid-month',
                      selected_employee_id=101, error='Synthetic <unsafe> month',
                      employees=[dict(id=101, name='Synthetic Selected', aktiv=True)])
        self.assertIn('<option value="101" selected>', html)
        self.assertIn('Synthetic &lt;unsafe&gt; month', html)
        self.assertIn('role="alert"', html)
        self.assertNotIn('<unsafe>', html)
        self.assertFalse(any(form.get('method') == 'post' for form in Forms(html).forms))

    def test_month_totals_open_shift_and_review_are_preserved(self):
        data = report('arbeitet')
        data['schichten'] = [dict(beginn='08.10.2026 08:00', ende=None, arbeitszeit='4:10 Stunden',
                                 pause='0:20 Stunden', offen=True, pruefen=False),
                             dict(beginn='07.10.2026 08:00', ende='08.10.2026 12:30', arbeitszeit='26:00 Stunden',
                                  pause='2:30 Stunden', offen=False, pruefen=True)]
        html = render('assistent_arbeitszeit.html', admin=False, report=data, request_id='synthetic-nonce')
        self.assertIn('7:35 Stunden', html)
        self.assertIn('Zwischenstand.', html)
        self.assertIn('Länger als 24 Stunden', html)
        self.assertIn('4:10 Stunden', html)
        self.assertIn('26:00 Stunden', html)
        parsed = Forms(html)
        month = [form for form in parsed.forms if form.get('method') == 'get'][0]
        self.assertEqual([(field['name'], field['value']) for field in month['inputs']], [('monat', '2026-10')])

    def test_known_plan_is_shared_with_personal_profile_without_changing_actual_hours(self):
        time = render('assistent_arbeitszeit.html', admin=False, report=report(known=True), request_id='synthetic-nonce')
        profile = render('mitarbeiter_portal.html', **personal_context(arbeitsplan=plan()))
        for html in (time, profile):
            for value in ('Montag–Freitag', '40 Stunden', '8 Stunden', '60 Minuten', '08:00 Uhr', '17:00 Uhr',
                          'Keine Stempel oder automatischen Abzüge.'):
                self.assertIn(value, html)
            self.assertEqual(Forms(html).ids.count('work-plan-heading'), 1)
        self.assertIn('7:35 Stunden', time)

    def test_unknown_or_missing_plan_is_never_a_fictitious_default(self):
        for values in ({'arbeitsplan': plan(False)}, {}):
            html = render('mitarbeiter_portal.html', **personal_context(**values))
            self.assertIn('Noch kein persönlicher Arbeitsplan hinterlegt', html)
            for invented in ('40 Stunden', '8 Stunden', '60 Minuten', '08:00 Uhr', '17:00 Uhr'):
                self.assertNotIn(invented, html)

    def test_personal_error_is_escaped_and_has_no_stamps(self):
        html = render('assistent_arbeitszeit.html', admin=False, report=None, error='<script>unsafe</script>')
        self.assertIn('&lt;script&gt;unsafe&lt;/script&gt;', html)
        self.assertNotIn('<script>', html)
        self.assertFalse(any(form.get('method') == 'post' for form in Forms(html).forms))

    def test_invalid_plan_review_hint_is_kept_in_personal_views(self):
        data = report()
        data['arbeitsplan']['hinweis'] = 'Der hinterlegte Arbeitsplan muss intern geprüft werden.'
        html = render('assistent_arbeitszeit.html', admin=False, report=data, request_id='synthetic-nonce')
        self.assertIn('Der hinterlegte Arbeitsplan muss intern geprüft werden.', html)
        self.assertNotIn('40 Stunden', html)
        admin = render('mitarbeiter_portal_admin.html', employee=dict(id=101, name='Synthetic Own'),
                       profile={}, payrolls=[], arbeitsplan=data['arbeitsplan'])
        self.assertIn('Der hinterlegte Arbeitsplan muss intern geprüft werden.', admin)
        self.assertNotIn('value="40"', admin)

    def vacation(self, **values):
        leave = dict(jahr=2026, bekannt=False, resttage=None, bedarf='Geprüftes Konto fehlt.',
                     hinweis='Offene Anträge sind noch nicht genehmigt.', antraege=[], kalendereintraege=[])
        leave.update(values)
        return render('assistent_urlaub.html', urlaub=leave, csrf='synthetic-vacation-csrf',
                      request_id='synthetic-vacation-nonce', today='2026-10-08', betriebsurlaub=[])

    def test_vacation_submission_keeps_csrf_identity_and_required_confirmation(self):
        html = self.vacation()
        parsed = Forms(html)
        form = next(form for form in parsed.forms if form.get('method') == 'post')
        self.assertEqual(form['action'], '/werkstatt/assistent/urlaub/antrag')
        inputs = {item['name']: item for item in form['inputs']}
        self.assertEqual(set(inputs), {'csrf_token', 'request_id', 'von', 'bis', 'confirmed'})
        self.assertEqual(inputs['csrf_token']['value'], 'synthetic-vacation-csrf')
        self.assertEqual(inputs['request_id']['value'], 'synthetic-vacation-nonce')
        for name in ('von', 'bis'):
            self.assertEqual(inputs[name]['min'], '2026-10-08')
            self.assertIn('required', inputs[name])
        self.assertEqual(inputs['confirmed']['value'], 'ja')
        self.assertIn('required', inputs['confirmed'])
        self.assertIn('Resturlaub noch nicht verlässlich bekannt', html)
        self.assertIn('class="access-header"', html)
        self.assertNotIn('/static/assistent.css', html)
        self.assertFalse(any(link.startswith('/admin') for link in parsed.links))

    def test_only_pending_leave_has_own_version_bound_withdrawal_and_zero_is_known(self):
        requests = [dict(id=mode, status=mode, status_label=mode, version=4, start_datum='2026-10-12',
                         end_datum='2026-10-13', tage=None) for mode in ('beantragt', 'genehmigt', 'abgelehnt')]
        html = self.vacation(bekannt=True, resttage='0', stichtag='2026-10-08', antraege=requests)
        posts = [form for form in Forms(html).forms if form.get('method') == 'post']
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[1]['action'], '/werkstatt/assistent/urlaub/antrag/beantragt/zurueckziehen')
        self.assertEqual({item['name']: item['value'] for item in posts[1]['inputs']},
                         {'csrf_token': 'synthetic-vacation-csrf', 'version': '4'})
        self.assertIn('<strong>0</strong><span>Tage verfügbar</span>', html)
        self.assertIn('Diese Termine verändern deinen Resturlaub nicht automatisch.', html)

    def test_admin_plan_form_is_separate_and_empty_until_proven(self):
        for known in (False, True):
            with self.subTest(known=known):
                html = render('mitarbeiter_portal_admin.html', employee=dict(id=101, name='Synthetic Own'),
                              profile={}, payrolls=[], contracts=[], arbeitsplan=plan(known))
                posts = [form for form in Forms(html).forms if form.get('method') == 'post']
                self.assertEqual([form['action'] for form in posts], ['/admin/mitarbeiter/101/portal',
                    '/admin/mitarbeiter/101/portal/arbeitsplan',
                    '/admin/mitarbeiter/101/portal/arbeitsvertrag', '/admin/mitarbeiter/101/portal/lohnzettel'])
                fields = posts[1]['inputs']
                self.assertEqual({item['name'] for item in fields}, {'csrf_token','wochenstunden','tagesstunden',
                    'pausenminuten','beginn','arbeitstage'})
                checked = [item['value'] for item in fields if item['name'] == 'arbeitstage' and 'checked' in item]
                self.assertEqual(checked, ['0','1','2','3','4'] if known else [])
                for item in fields:
                    if item['name'] not in ('csrf_token', 'arbeitstage'):
                        self.assertIn('required', item)
                        if not known:
                            self.assertEqual(item['value'], '')
                self.assertTrue(all(item['name'] not in ('wochenstunden', 'arbeitstage') for item in posts[0]['inputs']))

    def test_personal_dynamic_data_and_leave_notes_remain_escaped(self):
        data = report()
        data['mitarbeiter']['name'] = '<img src=x onerror=alert(1)>'
        data['arbeitsplan'] = plan()
        data['arbeitsplan']['tage_label'] = '<script>plan</script>'
        html = render('assistent_arbeitszeit.html', admin=False, report=data, request_id='synthetic-nonce')
        self.assertNotIn('<script>', html)
        self.assertNotIn('<img', html)
        self.assertIn('&lt;script&gt;plan&lt;/script&gt;', html)
        self.assertIn('&lt;img src=x onerror=alert(1)&gt;', html)
        vacation = self.vacation(bedarf='<script>leave</script>')
        self.assertNotIn('<script>', vacation)
        self.assertIn('&lt;script&gt;leave&lt;/script&gt;', vacation)


class InternalThemeScopeTests(unittest.TestCase):
    def test_admin_theme_does_not_follow_admin_session_into_public_or_partner_pages(self):
        template = ENV.get_template('base.html')
        source = (ROOT / 'templates' / 'base.html').read_text(encoding='utf-8')
        counts = {name: lambda: 0 for name in meta.find_undeclared_variables(ENV.parse(source))
                  if name.endswith('_count')}

        def synthetic_url(endpoint, **values):
            return '/static/' + values['filename'] if endpoint == 'static' else '/synthetic/' + endpoint

        for admin in (False, True):
            for path in ('/admin', '/admin/cockpit', '/admin/arbeitszeit', '/partner',
                         '/partner/kaesmann/dashboard', '/mietwagen', '/login', '/', '/admin-other'):
                with self.subTest(admin=admin, path=path):
                    html = template.render(url_for=synthetic_url, session={'admin': admin},
                        request=SimpleNamespace(path=path, endpoint='synthetic'), config={},
                        get_flashed_messages=lambda **kwargs: [], csrf_token=lambda: 'synthetic-csrf',
                        analysis_loading_news=lambda: [], **counts)
                    expected = admin and (path == '/admin' or path.startswith('/admin/'))
                    self.assertEqual('/static/werkstatt_theme.css' in html, expected)
                    self.assertEqual('class="ws-cockpit"' in html, expected)

    def test_admin_hr_pages_keep_explicit_cockpit_return(self):
        contexts = {
            'mitarbeiter_portal_admin.html': dict(employee=dict(id=101, name='Synthetic Own'),
                profile={}, payrolls=[], arbeitsplan=plan(False)),
            'mitarbeiter_einrichtung_admin.html': dict(employees=[], created_invitations=[],
                created_invitation=None),
            'mitarbeiter_betriebsurlaub.html': dict(items=[]),
            'assistent_urlaub_admin.html': dict(data=dict(jahr=2026, mitarbeiter=[]), weekdays=[],
                csrf='synthetic-csrf', today='2026-10-08'),
        }
        for name, values in contexts.items():
            with self.subTest(template=name):
                html = render(name, **values)
                self.assertIn('/admin/cockpit', Forms(html).links)
                self.assertIn('/static/werkstatt_theme.css', html)


if __name__ == '__main__':
    unittest.main()
