"""Render planning templates with synthetic data only; never import the Flask app."""

import json
from pathlib import Path
import re
import shutil
import subprocess
from types import SimpleNamespace
import unittest

from jinja2 import ChainableUndefined, ChoiceLoader, DictLoader, Environment, FileSystemLoader


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates"


def template_environment():
    env = Environment(
        loader=ChoiceLoader([
            DictLoader({
                "base.html": "<!doctype html><html lang='de'><head>{% block extra_css %}{% endblock %}</head><body>{% block content %}{% endblock %}{% block extra_js %}{% endblock %}</body></html>",
                "_mini_month_calendar.html": '<div data-calendar-test="true">Kalender mit Beispieldaten</div>',
                "_werkstatt_team_showcase.html": "",
            }),
            FileSystemLoader(str(TEMPLATES)),
        ]),
        autoescape=True,
        undefined=ChainableUndefined,
    )
    env.globals.update(
        url_for=lambda endpoint, **values: "/" + endpoint + ("?" + "&".join(f"{key}={value}" for key, value in values.items()) if values else ""),
        csrf_field=lambda: '<input name="csrf_token" value="synthetic-token">',
        csrf_token=lambda: "synthetic-token",
        get_flashed_messages=lambda **kwargs: [],
        request=SimpleNamespace(args={}),
        mahnungen_faellig_count=lambda: 0,
        admin_leads_count=lambda: 0,
        fahrzeugsuche_auktion_alert_count=lambda: 0,
        fahrzeugsuche_count=lambda: 0,
        fahrzeugverkauf_count=lambda: 0,
        admin_versicherung_count=lambda: 0,
        aufgaben_offen_heute_count=lambda: 0,
    )
    return env


def order(**changes):
    result = {
        "id": 71, "kennzeichen": "TEST 71", "fahrzeug": "Testfahrzeug",
        "status": 3, "status_label": "In Arbeit", "planung": {"badge": "Fertig", "badge_farbe": "danger", "hinweis": "Fertig überfällig seit 14.09.2026"},
        "produktion_schritt_label": "Vorbereitung", "produktion_schritte": [],
        "tagesaufgabe_vorschlag": 'Tür "links" vorbereiten',
        "fertig_datum": "15.09.2026", "abholtermin": "", "abhol_uhrzeit": "",
        "abholung_label": "Rückbringen", "werkstatt_angebot_text_display": "Tür lackieren",
    }
    result.update(changes)
    return result


def admin_context(**changes):
    result = {
        "datum_label": "15.09.2026", "wochentag": "Dienstag", "ist_heute": True,
        "gestern_label": "14.09.2026", "morgen_label": "16.09.2026",
        "plaene": [{"mitarbeiter": {"id": 4, "name": "Testperson"}, "aufgaben": [], "summe_minuten": 0, "summe_label": ""}],
        "auftraege": [order()], "zuweisbare_auftraege": [order()],
        "offene_zuweisung_count": 1,
    }
    result.update(changes)
    return result


class PlanningTemplatesTest(unittest.TestCase):
    def setUp(self):
        self.env = template_environment()

    def render(self, template, **context):
        return self.env.get_template(template).render(**context)

    def test_actions_precede_secondary_calendar_and_active_groups_are_open(self):
        item = {"name": "Testperson", "vehicle": "Testfahrzeug", "detail": "3 Dateien in diesem Auftrag", "title": "Unterlagen prüfen", "url": "/admin/auftrag/71"}
        actions = {"groups": {"angebote": [], "antworten": [], "dokumente": [item], "termine": []}, "total": 1, "waiting": 0}
        html = self.render("cockpit.html", aktionsuebersicht=actions, cockpit={}, start_inbox={"items": []}, mietwagen_heute={}, erinnerungen=[])
        self.assertLess(html.index('id="aktionen-titel"'), html.index('data-cockpit-kalender'))
        calendar_tag = re.search(r"<details[^>]*data-cockpit-kalender[^>]*>", html).group(0)
        self.assertNotIn("open", calendar_tag)
        self.assertRegex(html, r'<details class="aktion-gruppe" open>\s*<summary><span>Unterlagen prüfen</span>')
        self.assertIn('aria-label="1 Vorgang"', html)
        self.assertIn("3 Dateien in diesem Auftrag", html)
        self.assertIn("1 offene Aktion", html)

    def test_calendar_navigation_keeps_calendar_open(self):
        self.env.globals["request"] = SimpleNamespace(args={"monat": "2026-10"})
        html = self.render("cockpit.html", aktionsuebersicht={"groups": {}, "total": 0, "waiting": 0}, cockpit={}, start_inbox={"items": []}, mietwagen_heute={})
        self.assertRegex(html, r"<details[^>]*data-cockpit-kalender[^>]* open>")

    def test_assignment_uses_existing_form_and_escapes_order_text(self):
        selected = order(fahrzeug='<script>alert("x")</script>')
        html = self.render("aufgaben_admin.html", **admin_context(auftraege=[selected], zuweisbare_auftraege=[selected]))
        self.assertIn("Heute noch zuzuweisen", html)
        self.assertIn('action="/admin_aufgabe_neu"', html)
        self.assertEqual(html.count('name="auftrag_id"'), 1)
        self.assertIn('type="button" class="btn btn-soft btn-sm" data-auftrag-waehlen="71"', html)
        self.assertIn('data-aufgabe-vorschlag="Tür &#34;links&#34; vorbereiten"', html)
        self.assertNotIn('<script>alert("x")</script>', html)
        self.assertIn("&lt;script&gt;", html)
        for field in ("auftrag_id", "mitarbeiter_id", "beschreibung", "richtwert", "csrf_token", "datum"):
            self.assertIn('name="' + field + '"', html)

    def test_assignment_empty_and_no_staff_states(self):
        html = self.render("aufgaben_admin.html", **admin_context(zuweisbare_auftraege=[], ist_heute=False))
        self.assertIn("Für diesen Tag noch zuzuweisen", html)
        self.assertIn("keine weiteren Aufträge zur Zuteilung", html)
        no_staff = self.render("aufgaben_admin.html", **admin_context(plaene=[]))
        self.assertRegex(no_staff, r'<button type="submit"[^>]*disabled>Zuteilen</button>')
        self.assertIn("Noch keine aktiven Mitarbeiter angelegt", no_staff)

    def test_empty_employee_plan_is_not_reported_as_completed(self):
        html = self.render("werkstatt_aufgaben.html", gewaehlt={"id": 4, "name": "Testperson"}, mitarbeiter=[], aufgaben=[], summe_offen_label="", offene_zuweisung_count=10)
        self.assertIn("10 Aufträge noch ohne Tageszuteilung", html)
        self.assertIn("Zuteilung offen", html)
        self.assertNotIn("Alles erledigt", html)
        self.assertNotIn("Aufgaben sind erledigt", html)

    def test_open_task_without_duration_is_still_open(self):
        task = {"id": 9, "beschreibung": "Sichtkontrolle", "status": "offen", "richtwert_label": "", "auftrag_label": "TEST 71"}
        html = self.render("werkstatt_aufgaben.html", gewaehlt={"id": 4, "name": "Testperson"}, mitarbeiter=[], aufgaben=[task], summe_offen_label="")
        self.assertIn("1 offen", html)
        self.assertNotIn("Aufgaben sind erledigt", html)
        done = self.render("werkstatt_aufgaben.html", gewaehlt={"id": 4, "name": "Testperson"}, mitarbeiter=[], aufgaben=[{**task, "status": "erledigt"}], summe_offen_label="")
        self.assertIn("Deine zugeteilten Aufgaben sind erledigt", done)

    def test_board_distinguishes_finish_and_missing_return(self):
        column = {"key": "in_arbeit", "ziel_status": 3, "titel": "In Arbeit", "auftraege": [order()]}
        html = self.render("werkstatt_tafel.html", spalten=[column], countdown_ziel_iso="2026-08-19T09:35:00+02:00")
        self.assertIn("Fertig bis: <strong>15.09.2026", html)
        self.assertIn("Rückbringen noch nicht vereinbart", html)
        self.assertNotIn("Guten Flug", html)
        self.assertNotIn("bis Abflug", html)
        self.assertNotIn("data-countdown-ziel", html)
        self.assertIn("Fertig überfällig seit 14.09.2026", html)

    def test_board_keeps_confirmed_return_time(self):
        column = {"key": "in_arbeit", "ziel_status": 3, "titel": "In Arbeit", "auftraege": [order(fertig_datum="", abholtermin="16.09.2026", abhol_uhrzeit="15:30")]}
        html = self.render("werkstatt_tafel.html", spalten=[column])
        self.assertIn("Fertigtermin fehlt", html)
        self.assertIn("16.09.2026 · 15:30 Uhr", html)
        self.assertNotIn("noch nicht vereinbart", html)

    @unittest.skipUnless(shutil.which("node"), "Node.js is needed for the form interaction check")
    def test_selecting_order_prefills_without_saving_or_overwriting_manual_text(self):
        source = (TEMPLATES / "aufgaben_admin.html").read_text(encoding="utf-8")
        script = re.search(r"<script>(.*?)</script>", source, re.S).group(1)
        runner = r'''
const assert = require('node:assert/strict');
const vm = require('node:vm');
const events = [];
let submitted = 0;
const field = (value = '') => ({value, focus() { events.push('focus'); }});
const controls = {auftrag_id: field(), beschreibung: field(), mitarbeiter_id: field(), richtwert: field('2,5')};
controls.auftrag_id.options = [{value:'71'}, {value:'72'}];
const buttons = [
 {dataset:{auftragWaehlen:'71',aufgabeVorschlag:'Tür vorbereiten'}},
 {dataset:{auftragWaehlen:'72',aufgabeVorschlag:'Montage prüfen'}},
 {dataset:{auftragWaehlen:'999',aufgabeVorschlag:'Ungültig'}}
];
buttons.forEach(b => { b.addEventListener = (name, fn) => { b.click = fn; }; });
const hint = {textContent:''};
const form = {elements:{namedItem:name => controls[name]}, submit(){submitted++;}};
const doc = {
 querySelector:() => form,
 querySelectorAll:() => buttons,
 getElementById:id => id === 'zuteilung-hinweis' ? hint : {scrollIntoView(){events.push('scroll');}}
};
vm.runInNewContext(SCRIPT, {document:doc});
buttons[0].click();
assert.equal(controls.auftrag_id.value,'71');
assert.equal(controls.beschreibung.value,'Tür vorbereiten');
buttons[1].click();
assert.equal(controls.beschreibung.value,'Montage prüfen');
controls.beschreibung.value='Manuell vereinbarte Zusatzarbeit';
buttons[0].click();
assert.equal(controls.beschreibung.value,'Manuell vereinbarte Zusatzarbeit');
assert.match(hint.textContent,/bleibt erhalten/);
buttons[2].click();
assert.equal(controls.auftrag_id.value,'71');
assert.equal(controls.richtwert.value,'2,5');
assert.equal(submitted,0);
assert.ok(events.includes('focus'));
'''
        result = subprocess.run([shutil.which("node"), "-"], input="const SCRIPT = " + json.dumps(script) + ";\n" + runner, text=True, encoding="utf-8", capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
