"""Archive navigation regressions using the fully isolated smoke/audit harness.

Run with python scripts/test_navigation_archive.py. Imports the sibling
test_navigation_readmodels module for its audit guard and disposable portal.
"""
from contextlib import closing, contextmanager
from datetime import date, timedelta
import re
import unittest
from unittest.mock import patch

import test_navigation_readmodels as support
from flask import template_rendered

p=support.p

@contextmanager
def dashboard_context():
    captured=[]
    def record(sender,template,context,**extra):
        if template.name=='dashboard.html':
            captured.append(context)
    template_rendered.connect(record,p.app,weak=False)
    try:
        yield captured
    finally:
        template_rendered.disconnect(record,p.app)


class NavigationArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        finished=(date.today()-timedelta(days=1)).strftime(p.DATE_FMT)
        fixtures=[
            (65000,'NAVARCH-ACTIVE',dict(archiviert=0)),
            (65001,'NAVARCH-ACTIVE-NULL',dict(archiviert=None)),
            (65002,'NAVARCH-ARCHIVE-PAINT',dict(archiviert=1,status=5,fertig_datum=finished)),
            (65003,'NAVARCH-ARCHIVE-TYRE',dict(archiviert=1,status=5,leistungsart='reifenservice',
                schaden_aufnahme_json='{"anliegen":"reifenservice"}')),
            (65004,'NAVARCH-ARCHIVE-OFFER',dict(archiviert=1,angebotsphase=1,angebot_abgesendet=1)),
            (65005,'NAVARCH-ARCHIVE-MECHANIC',dict(archiviert=1,status=5,leistungsart='mechanik',fertig_datum=finished)),
        ]
        for order_id,vehicle,changes in fixtures:
            support.insert_order(order_id,fahrzeug=vehicle,**changes)
        cls.client=support.admin_client()

    def legacy_full(self):
        with p.app.test_request_context('/admin?archiv=1'):
            return [a for a in p.list_auftraege(include_archived=True) if not p.auftrag_ist_reifen(a)]

    def test_default_get_does_not_hydrate_archived_order_rows_and_keeps_null_active(self):
        original=p.row_to_auftrag
        with patch.object(p,'row_to_auftrag',wraps=original) as hydrate:
            with dashboard_context() as captured:
                response=self.client.get('/admin')
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(captured),1)
        context=captured[0]
        self.assertFalse(context['archiv_geladen'])
        self.assertEqual(context['archivierte_auftraege'],[])
        active_ids={row['id'] for row in context['auftraege']}
        self.assertTrue({65000,65001}.issubset(active_ids))
        # Offers retain their independent existing list; only ordinary archived
        # orders must stay outside the default dashboard hydration path.
        hydrated={call.args[0]['id'] for call in hydrate.call_args_list}
        self.assertTrue({65000,65001}.issubset(hydrated))
        self.assertTrue({65002,65003,65005}.isdisjoint(hydrated),hydrated)
        html=response.get_data(as_text=True)
        self.assertIn('/admin?archiv=1#archiv',html)
        for order_id in (65002,65003,65005):
            self.assertNotIn(f'name="auftrag_ids" value="{order_id}"',html)

    def test_default_archive_count_excludes_tyres_and_offers_and_statistics_match_old_full_models(self):
        legacy=self.legacy_full()
        expected_archived=[row for row in legacy if row['archiviert']]
        expected_stats=p.build_lackier_statistik(legacy)
        with dashboard_context() as captured:
            response=self.client.get('/admin')
        self.assertEqual(response.status_code,200)
        context=captured[0]
        self.assertEqual(context['archiv_anzahl'],len(expected_archived))
        fixture_archived={row['id'] for row in expected_archived if 65000<=row['id']<=65005}
        self.assertEqual(fixture_archived,{65002,65005})
        self.assertEqual(context['lackier_statistik'],expected_stats)

    def test_explicit_archive_get_preserves_rows_statistics_and_original_bulk_controls(self):
        legacy=self.legacy_full()
        expected_archived=[row for row in legacy if row['archiviert']]
        expected_stats=p.build_lackier_statistik(legacy)
        with dashboard_context() as captured:
            response=self.client.get('/admin?archiv=1')
        self.assertEqual(response.status_code,200)
        context=captured[0]
        self.assertTrue(context['archiv_geladen'])
        self.assertEqual(context['archivierte_auftraege'],expected_archived)
        self.assertEqual(context['archiv_anzahl'],len(expected_archived))
        self.assertEqual(context['lackier_statistik'],expected_stats)
        html=response.get_data(as_text=True)
        for row in expected_archived:
            self.assertIn(f'name="auftrag_ids" value="{row["id"]}"',html)
        with p.app.test_request_context('/admin?archiv=1'):
            bulk_url=p.url_for('admin_sammelaktion')
        self.assertRegex(html,r'<form[^>]*method="POST"[^>]*action="'+re.escape(bulk_url)+r'"[^>]*data-bulk-form')
        self.assertIn('name="aktion" value="aktivieren"',html)
        self.assertIn('name="aktion" value="loeschen"',html)

    def test_non_explicit_archive_values_keep_default_behavior(self):
        for query in ('archiv=0','archiv=true','archiv='):
            with self.subTest(query=query):
                with dashboard_context() as captured:
                    response=self.client.get('/admin?'+query)
                self.assertEqual(response.status_code,200)
                self.assertFalse(captured[0]['archiv_geladen'])
                self.assertEqual(captured[0]['archivierte_auftraege'],[])

    def test_default_and_explicit_archive_require_admin_authentication(self):
        anonymous=p.app.test_client()
        for path in ('/admin','/admin?archiv=1'):
            with self.subTest(path=path):
                with dashboard_context() as captured:
                    response=anonymous.get(path)
                self.assertEqual(response.status_code,302)
                self.assertIn('/login',response.headers.get('Location',''))
                self.assertEqual(captured,[])
                self.assertNotIn('NAVARCH-ARCHIVE-PAINT',response.get_data(as_text=True))


if __name__=='__main__':
    unittest.main(verbosity=2)
