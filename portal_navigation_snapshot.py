"""Fresh SQL-only navigation counts using one database round trip."""


NAVIGATION_SQL_COUNT_KEYS = (
    'aufgaben_offen_heute_anzahl',
    'admin_leads_count',
    'admin_bewerbungen_count',
    'fahrzeugsuche_count',
    'fahrzeugverkauf_count',
    'fahrzeugeinkauf_count',
    'mietwagen_anfragen_count',
    'admin_versicherung_count',
    'admin_email_count',
)


_NAVIGATION_SQL_COUNTS = """
SELECT
    (SELECT COUNT(*) FROM mitarbeiter_aufgaben
     WHERE datum=? AND COALESCE(status, '') != 'erledigt')
        AS aufgaben_offen_heute_anzahl,
    (SELECT COUNT(*) FROM leads
     WHERE status NOT IN ('gewonnen', 'verloren'))
        AS admin_leads_count,
    (SELECT COUNT(*) FROM bewerbungen WHERE status='neu')
        AS admin_bewerbungen_count,
    (SELECT COUNT(*) FROM fahrzeugsuchen
     WHERE status NOT IN ('abgeschlossen', 'abgelehnt'))
        AS fahrzeugsuche_count,
    (SELECT COUNT(*) FROM fahrzeugverkaeufe
     WHERE COALESCE(status, 'offen') != 'verkauft')
        AS fahrzeugverkauf_count,
    (SELECT COUNT(*) FROM fahrzeugeinkauf_fahrzeuge
     WHERE interessant=1 AND COALESCE(status, 'neu') NOT IN ('gekauft', 'verworfen'))
        AS fahrzeugeinkauf_count,
    (SELECT COUNT(*) FROM mietwagen_anfragen WHERE status='offen')
        AS mietwagen_anfragen_count,
    (SELECT COUNT(*) FROM auftraege
     WHERE COALESCE(versicherung_id, 0) > 0
       AND COALESCE(archiviert, 0) = 0
       AND NOT (COALESCE(quelle, '') = 'autohaus'
                AND COALESCE(angebotsphase, 0) != 0))
        AS admin_versicherung_count,
    (SELECT COUNT(*) FROM werkstatt_emails WHERE status='neu')
        AS admin_email_count
"""


def load_navigation_sql_counts(db, today_text):
    """Return nine legacy scalar counts, keyed by their portal function names.

    ``today_text`` is the caller's existing DATE_FMT value; no clock, timezone or
    date normalization is introduced. SQL NULL/empty/case behavior deliberately
    matches each original predicate, including their different COALESCE rules.

    The caller owns the connection and request-local lifetime. It must retain
    the existing rental-request-to-lead synchronization before loading if needed.
    This helper never synchronizes, commits, rolls back, closes, caches, writes
    or calls providers. Database errors propagate to the caller's error policy.
    """
    row = db.execute(_NAVIGATION_SQL_COUNTS, (today_text,)).fetchone()
    return {key: int(row[key] or 0) for key in NAVIGATION_SQL_COUNT_KEYS}
