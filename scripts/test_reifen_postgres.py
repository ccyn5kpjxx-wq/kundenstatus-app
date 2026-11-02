"""Offline PostgreSQL return-column regression using the actual portal wrapper.

Only selected AST definitions are evaluated: no application import, environment
loading, database connection or network. The cursor checks requested RETURNING
columns against the schemas from the tire modules' actual migration functions.
"""
import ast
from functools import lru_cache
from pathlib import Path
import re
import socket
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def definitions(path, names, namespace):
    parsed = ast.parse(path.read_text(encoding="utf-8-sig"))
    selected = [node for node in parsed.body if getattr(node, "name", None) in names]
    if {node.name for node in selected} != names:
        raise AssertionError("Required real definitions missing")
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


@lru_cache(maxsize=1)
def wrapper_class():
    return definitions(ROOT / "app.py", {
        "DbRow", "PostgresCursor", "PostgresConnection",
        "get_insert_table_name", "convert_sqlite_sql_to_postgres",
    }, {"re": re, "timed_database_call": lambda *args, **kwargs: lambda fn: fn})["PostgresConnection"]


def migration(path, db):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value
    definitions(path, {"init_schema"}, constants)["init_schema"](db)


def literal_sql(path, table):
    """Take the real SQL literal at the call site, rather than recreating it."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "execute" and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and re.match(r"\s*INSERT\s+INTO\s+" + re.escape(table) + r"\b",
                             node.args[0].value, re.I)):
            return node.args[0].value
    raise AssertionError("Required actual insert statement missing")


class SchemaConnection:
    """Small schema-aware cursor fake; it does not implement the wrapper rule."""
    def __init__(self):
        self.tables = {}
        self.primary_keys = {}
        self.statements = []

    def cursor(self):
        return SchemaCursor(self)


class SchemaCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = 0
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        self.connection.statements.append(sql)
        create = re.fullmatch(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*)\)", sql, re.I | re.S)
        if create:
            table, declaration = create.groups()
            parts = [part.strip() for part in declaration.split(",")]
            columns = [part.split()[0] for part in parts]
            self.connection.tables[table] = columns
            self.connection.primary_keys[table] = [
                part.split()[0] for part in parts if "PRIMARY KEY" in part.upper()]
            return
        insert = re.match(r"INSERT INTO (\w+)\b", sql, re.I)
        if not insert:
            return  # Index DDL does not return rows.
        columns = self.connection.tables[insert.group(1)]
        self.rowcount = 1
        returning = re.search(r"\bRETURNING\s+(.+?)\s*$", sql, re.I | re.S)
        if not returning:
            return
        wanted = columns if returning.group(1) == "*" else [
            name.strip() for name in returning.group(1).split(",")]
        for name in wanted:
            if name not in columns:
                raise ValueError("PostgreSQL: requested returning column does not exist")
        self.description = [SimpleNamespace(name=name) for name in wanted]
        self.rows = [tuple(17 if name == "id" else (params[0] if params else None)
                           for name in wanted)]

    def fetchall(self):
        return self.rows


class ReifenPostgresTests(unittest.TestCase):
    def setUp(self):
        blocked = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden"))
        blocked.start()
        self.addCleanup(blocked.stop)
        self.connection = SchemaConnection()
        self.db = wrapper_class()(self.connection)
        for name in ("reifen_kauf.py", "reifen_vertrag.py", "reifen_sms.py"):
            migration(ROOT / name, self.db)
        self.db.execute("CREATE TABLE IF NOT EXISTS leads (id SERIAL PRIMARY KEY, kunde_name TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
        self.connection.statements.clear()

    def test_sms_budget_uses_its_real_natural_key_schema(self):
        self.assertEqual(self.connection.primary_keys["reifen_kauf_sms_budget"], ["tag"])
        result = self.db.execute(literal_sql(ROOT / "reifen_kauf.py", "reifen_kauf_sms_budget"), ("2026-11-02",))
        self.assertEqual(result.rowcount, 1)
        self.assertIsNone(result.fetchone())
        self.assertIsNone(self.db.lastrowid)

    def test_payment_event_does_not_request_a_nonexistent_id(self):
        self.assertEqual(self.connection.primary_keys["reifen_kauf_events"], ["event_id"])
        result = self.db.execute(literal_sql(ROOT / "reifen_kauf.py", "reifen_kauf_events"), ("event", "pay", "order", "time"))
        self.assertEqual(result.rowcount, 1)
        self.assertIsNone(result.fetchone())

    def test_contract_attachment_uses_actual_sha256_schema(self):
        self.assertEqual(self.connection.primary_keys["reifen_vertrag_anhaenge"], ["sha256"])
        result = self.db.execute(literal_sql(ROOT / "reifen_vertrag.py", "reifen_vertrag_anhaenge"), ("hash", "notice.jpg", "image/jpeg", 1, "eA=="))
        self.assertEqual(result.rowcount, 1)
        self.assertIsNone(result.fetchone())

    def test_normal_existing_table_keeps_auto_id_and_sqlite_compatibility(self):
        result = self.db.execute("INSERT INTO leads(kunde_name) VALUES(?)", ("Synthetic",))
        self.assertEqual(result.lastrowid, 17)
        self.assertEqual(self.db.lastrowid, 17)
        self.assertEqual(self.db.execute("SELECT LAST_INSERT_ROWID()").fetchone()[0], 17)

    def test_existing_natural_key_exception_remains_supported(self):
        result = self.db.execute("INSERT INTO app_settings(key,value) VALUES(?,?)", ("demo", "value"))
        self.assertEqual(result.rowcount, 1)
        self.assertIsNone(result.fetchone())

    def test_actual_sms_challenge_returning_is_preserved(self):
        result = self.db.execute(literal_sql(ROOT / "reifen_sms.py", "reifen_sms_challenges"),
                                 ("challenge", "reference", "phone", "service", 1, 2, 1))
        self.assertEqual(result.fetchone()["challenge_id"], "challenge")
        self.assertIsNone(self.db.lastrowid)

    def test_explicit_returning_on_normal_table_returns_rows_without_clobbering_last_id(self):
        self.db.lastrowid = 99
        result = self.db.execute("INSERT INTO leads(kunde_name) VALUES(?) RETURNING id", ("Synthetic",))
        self.assertEqual(result.fetchone()["id"], 17)
        self.assertEqual(self.db.lastrowid, 99)

    def test_explicit_returning_all_is_preserved(self):
        result = self.db.execute("INSERT INTO leads(kunde_name) VALUES(?) RETURNING *", ("Synthetic",))
        self.assertEqual(result.fetchone()["id"], 17)
        self.assertEqual(result.fetchone()["kunde_name"], "Synthetic")


if __name__ == "__main__":
    unittest.main(verbosity=2)
