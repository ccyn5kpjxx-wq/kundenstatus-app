"""Exercise the real portal backup/import paths on synthetic, isolated data."""
from contextlib import closing
from copy import deepcopy
from datetime import date
import json
import sqlite3
import unittest
from unittest.mock import MagicMock
from uuid import uuid4
import zipfile

import smoke_test as isolated
from werkstatt_kundenflotten import BACKUP_TABLES, SCHEMA_FEATURE

portal = isolated.portal


class FleetIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        cls.service = portal.kundenflotten
        today = date.today().isoformat()
        cls.company = cls.service.create_company({'name': 'Synthetischer Testbetrieb', 'freigabe_limit': '300'})
        cls.vehicle = cls.service.create_vehicle(cls.company, {'kennzeichen': 'TEST 1', 'modell': 'Testfahrzeug', 'km': '23000', 'km_datum': today})
        cls.service.change_vehicle(cls.company, cls.vehicle, 'fahrer', {'name': 'Testperson', 'email': 'demo@example.invalid', 'kanal': 'email', 'einwilligung': 'Testeinwilligung'})
        cls.service.change_vehicle(cls.company, cls.vehicle, 'reifen', {'saison': 'winter', 'beschreibung': 'Testreifensatz', 'lagerplatz': 'R01-F02', 'bestellbedarf': 'Profil prüfen'})
        cls.service.change_vehicle(cls.company, cls.vehicle, 'plan', {'art': 'service', 'titel': 'Inspektion', 'faellig_datum': today, 'faellig_km': '25000', 'intervall_monate': '12', 'intervall_km': '15000', 'quelle': 'Synthetischer Wartungsplan', 'request_id': uuid4().hex})
        cls.service.change_vehicle(cls.company, cls.vehicle, 'termin', {'datum': today, 'art': 'reifen', 'transport': 'abholen', 'ersatzwagen': '1', 'notiz': 'Nur Test', 'request_id': uuid4().hex})
        cls.backup = portal.create_backup_package(reason='kundenflotten-integrationstest')
        with zipfile.ZipFile(cls.backup) as archive:
            cls.export = json.loads(archive.read('backup.json'))
            cls.source = isolated.TEMP_DIR / 'fleet-source.db'
            cls.source.write_bytes(archive.read('auftraege.db'))
        cls.expected = cls.snapshot()

    @classmethod
    def snapshot(cls):
        with closing(portal.get_db()) as db:
            return {table: sorted([dict(row) for row in db.execute(f'SELECT * FROM {table}')], key=lambda row: json.dumps(row, sort_keys=True)) for table in BACKUP_TABLES}

    def tearDown(self):
        with zipfile.ZipFile(self.backup) as archive:
            portal.import_backup_json_rows_into_current_database(self.export, archive, archive.namelist())

    def test_real_backup_contains_all_eight_tables_and_schema_feature(self):
        self.assertIn(SCHEMA_FEATURE, self.export['schema_features'])
        self.assertTrue(set(BACKUP_TABLES).issubset(portal.BACKUP_TABLES))
        for table in BACKUP_TABLES:
            with self.subTest(table=table):
                self.assertTrue(self.export['tables'][table], 'Every fleet table has a real test record')
        portal.validate_backup_binary_reference_completeness(self.export, portal.backup_binary_reference_map(self.export))

    def test_json_roundtrip_restores_driver_tires_plans_requests_and_history(self):
        self.service.change_vehicle(self.company, self.vehicle, 'km', {'km': '24000', 'km_datum': date.today().isoformat()})
        with zipfile.ZipFile(self.backup) as archive:
            portal.import_backup_json_rows_into_current_database(self.export, archive, archive.namelist())
        self.assertEqual(self.snapshot(), self.expected)

    def test_sqlite_roundtrip_preserves_text_keys_and_foreign_keys(self):
        self.service.change_vehicle(self.company, self.vehicle, 'reifen', {'saison': 'sommer', 'lagerplatz': 'Anderer Platz'})
        portal.import_sqlite_rows_into_current_database(self.source)
        self.assertEqual(self.snapshot(), self.expected)
        with closing(portal.get_db()) as db:
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_old_json_without_fleet_data_is_rejected_before_any_deletion(self):
        old = deepcopy(self.export)
        old['schema_features'].remove(SCHEMA_FEATURE)
        for table in BACKUP_TABLES:
            del old['tables'][table]
        with self.assertRaisesRegex(ValueError, 'Kundenflotten'):
            portal.import_backup_json_rows_into_current_database(old, None, [])
        self.assertEqual(self.snapshot(), self.expected)

    def test_old_sqlite_is_rejected_before_any_deletion(self):
        source = isolated.TEMP_DIR / 'old-fleet-source.db'
        with closing(sqlite3.connect(source)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS old_schema (id INTEGER)')
            db.commit()
        with self.assertRaisesRegex(ValueError, 'Kundenflotten'):
            portal.import_sqlite_rows_into_current_database(source)
        self.assertEqual(self.snapshot(), self.expected)

    def test_legacy_guard_allows_empty_fleet_but_never_removes_existing_data(self):
        with closing(portal.get_db()) as db:
            for table in reversed(BACKUP_TABLES):
                db.execute(f'DELETE FROM {table}')
            db.commit()
        portal.ensure_kundenflotten_state_for_import(export={'tables': {}})

    def test_broken_fleet_row_rolls_back_complete_import(self):
        broken = deepcopy(self.export)
        broken['tables']['kundenflotten_plaene'].append('not a row')
        with zipfile.ZipFile(self.backup) as archive:
            with self.assertRaisesRegex(ValueError, 'fehlerhafte Zeile'):
                portal.import_backup_json_rows_into_current_database(broken, archive, archive.namelist())
        self.assertEqual(self.snapshot(), self.expected)

    def test_feature_marked_backup_cannot_omit_a_fleet_table(self):
        incomplete = deepcopy(self.export)
        del incomplete['tables']['kundenflotten_reifen']
        with self.assertRaises(ValueError):
            portal.validate_backup_binary_reference_completeness(incomplete, {})

    def test_postgres_adapter_does_not_invent_id_for_vehicle_key_tables(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.description = None
        cursor.rowcount = 1
        adapter = portal.PostgresConnection(connection)
        for table in ('kundenflotten_fahrer', 'kundenflotten_reifen'):
            adapter.execute(f'INSERT INTO {table} (fahrzeug_id) VALUES (?)', ('synthetic',))
            self.assertNotIn('RETURNING id', cursor.execute.call_args.args[0])

    def test_real_routes_require_admin_and_demo_requires_local_explicit_opt_in(self):
        client = portal.app.test_client()
        response = client.get('/admin/kundenflotten')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.headers['Location'])
        previous = portal.app.config.get('KUNDENFLOTTEN_DEMO_ENABLED', False)
        try:
            portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = False
            self.assertEqual(client.get('/kunden-app/vorschau').status_code, 404)
            portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = True
            self.assertEqual(client.get('/kunden-app/vorschau', environ_overrides={'REMOTE_ADDR': '198.51.100.10'}).status_code, 404)
            self.assertEqual(client.get('/kunden-app/vorschau').status_code, 200)
        finally:
            portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = previous


if __name__ == '__main__':
    unittest.main()
