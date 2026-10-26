"""GET/render lifetime and transaction boundaries for batched badges."""
import unittest
from unittest.mock import Mock, patch
import test_navigation_readmodels as support

p = support.p


def fallback(name, value=77):
    def count():
        count.calls += 1
        return value
    count.__name__ = name
    count.calls = 0
    return count


class NavigationSnapshotIntegrationTests(unittest.TestCase):
    def test_one_fresh_snapshot_after_sync_and_render_memo_keeps_zero(self):
        events = []
        counts = {key:index for index,key in enumerate(p.NAVIGATION_SQL_COUNT_KEYS)}
        db = Mock()
        def load(connection, today):
            events.append('load')
            self.assertIs(connection, db)
            self.assertEqual(today, p.datetime.now().strftime(p.DATE_FMT))
            return dict(counts)
        with patch.object(p, 'USE_POSTGRES', False), patch.object(p, 'get_db', return_value=db), \
             patch.object(p, 'sync_mietwagen_anfragen_to_leads', side_effect=lambda:events.append('sync')), \
             patch.object(p, 'load_navigation_sql_counts', side_effect=load) as loader:
            for offset in (0,10):
                with p.app.test_request_context('/'):
                    p.session['admin'] = True
                    for key in counts:
                        function = fallback(key)
                        self.assertEqual(p.navigation_render_count(function), counts[key])
                        self.assertEqual(function.calls, 0)
                    counts = {key:value+10 for key,value in counts.items()}
            self.assertEqual(loader.call_count, 2)
        self.assertEqual(events, ['sync','load','sync','load'])
        self.assertEqual(db.close.call_count, 2)
        db.commit.assert_not_called()

    def test_post_partner_background_and_nonbatched_helpers_keep_existing_path(self):
        function = fallback('admin_leads_count')
        with patch.object(p, 'load_navigation_sql_counts') as loader, \
             patch.object(p, 'sync_mietwagen_anfragen_to_leads') as sync:
            self.assertEqual(p.navigation_render_count(function), 77)
            for method, admin in (('POST',True), ('GET',False)):
                with p.app.test_request_context('/', method=method):
                    p.session['admin'] = admin
                    self.assertEqual(p.navigation_render_count(function), 77)
            with p.app.test_request_context('/'):
                p.session['admin'] = True
                other = fallback('not_a_navigation_count', 23)
                self.assertEqual(p.navigation_render_count(other), 23)
            loader.assert_not_called()
            sync.assert_not_called()
        self.assertEqual(function.calls, 3)

    def test_failed_pg_batch_rolls_back_only_savepoint_and_retries_legacy_once_per_render(self):
        db = Mock()
        function = fallback('admin_leads_count')
        with p.app.test_request_context('/'):
            p.session['admin'] = True
            with patch.object(p, 'USE_POSTGRES', True), patch.object(p, 'get_db', return_value=db), \
                 patch.object(p, 'sync_mietwagen_anfragen_to_leads'), \
                 patch.object(p, 'load_navigation_sql_counts', side_effect=RuntimeError('optional schema')) as loader, \
                 patch.object(p, 'admin_leads_count', function):
                helpers = p.inject_csrf_helpers()
                self.assertEqual(helpers['admin_leads_count'](), 77)
                self.assertEqual(helpers['admin_leads_count'](), 77)
                self.assertEqual(function.calls, 1)
                loader.assert_called_once()
        self.assertEqual([call.args[0] for call in db.execute.call_args_list], [
            'SAVEPOINT navigation_counts', 'ROLLBACK TO SAVEPOINT navigation_counts',
            'RELEASE SAVEPOINT navigation_counts'])
        db.rollback.assert_not_called()
        db.commit.assert_not_called()
        db.close.assert_called_once()

    def test_successful_pg_batch_releases_without_committing_route_transaction(self):
        db = Mock()
        function = fallback('admin_leads_count')
        with p.app.test_request_context('/'):
            p.session['admin'] = True
            with patch.object(p, 'USE_POSTGRES', True), patch.object(p, 'get_db', return_value=db), \
                 patch.object(p, 'sync_mietwagen_anfragen_to_leads'), \
                 patch.object(p, 'load_navigation_sql_counts', return_value={'admin_leads_count':3}):
                self.assertEqual(p.navigation_render_count(function), 3)
        self.assertEqual([call.args[0] for call in db.execute.call_args_list], [
            'SAVEPOINT navigation_counts', 'RELEASE SAVEPOINT navigation_counts'])
        self.assertEqual(function.calls, 0)
        db.commit.assert_not_called()
        db.rollback.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
