"""Read-only navigation counts without constructing full inbox entries."""


# Each source keeps the selection order and 80-row cap of
# app.list_admin_postfach_items. In particular, hidden entries are removed AFTER
# those caps, and complaints intentionally include archived orders. The LEFT
# JOINs used only for display labels are unnecessary here; the inner JOIN for
# framework-contract requests still excludes requests without a dealership.
_ADMIN_POSTFACH_IDS_SQL = """
WITH chat_items AS (
    SELECT c.id
    FROM chat_nachrichten c
    JOIN auftraege a ON a.id = c.auftrag_id
    WHERE c.absender IN ('autohaus', 'versicherung')
      AND c.gelesen_admin=0 AND a.archiviert=0
    ORDER BY c.id DESC
    LIMIT 80
), versicherung_items AS (
    SELECT v.id
    FROM versicherung_aufgaben v
    JOIN auftraege a ON a.id = v.auftrag_id
    WHERE v.status='offen' AND a.archiviert=0
    ORDER BY v.id DESC
    LIMIT 80
), angebot_items AS (
    SELECT a.id
    FROM auftraege a
    WHERE a.angebotsphase=1 AND a.angebot_abgesendet=1
      AND a.angebot_status='angefragt' AND a.archiviert=0
    ORDER BY a.geaendert_am DESC, a.id DESC
    LIMIT 80
), auftrag_items AS (
    SELECT a.id
    FROM auftraege a
    WHERE a.quelle='autohaus' AND a.angebotsphase=0
      AND a.archiviert=0 AND a.status=1
    ORDER BY a.erstellt_am DESC, a.id DESC
    LIMIT 80
), verzoegerung_items AS (
    SELECT v.id
    FROM verzoegerungen v
    JOIN auftraege a ON a.id = v.auftrag_id
    WHERE v.uebernommen=0 AND a.archiviert=0
    ORDER BY v.erstellt_am DESC, v.id DESC
    LIMIT 80
), reklamation_items AS (
    SELECT r.id
    FROM reklamationen r
    JOIN auftraege a ON a.id = r.auftrag_id
    WHERE r.bearbeitet=0
    ORDER BY r.erstellt_am DESC, r.id DESC
    LIMIT 80
), dokument_items AS (
    SELECT a.id
    FROM auftraege a
    WHERE a.analyse_pruefen=1 AND a.analyse_werkstatt_geprueft=0
      AND a.archiviert=0
    ORDER BY a.geaendert_am DESC, a.id DESC
    LIMIT 80
), rahmenvertrag_items AS (
    SELECT r.id
    FROM rahmenvertrag_anfragen r
    JOIN autohaeuser h ON h.id = r.autohaus_id
    WHERE r.status='offen'
    ORDER BY r.id DESC
    LIMIT 80
)
SELECT 'admin-chat-' AS item_prefix, id FROM chat_items
UNION ALL
SELECT 'admin-versicherung-aufgabe-', id FROM versicherung_items
UNION ALL
SELECT 'admin-angebot-', id FROM angebot_items
UNION ALL
SELECT 'admin-auftrag-', id FROM auftrag_items
UNION ALL
SELECT 'admin-verzoegerung-', id FROM verzoegerung_items
UNION ALL
SELECT 'admin-reklamation-', id FROM reklamation_items
UNION ALL
SELECT 'admin-dokument-', id FROM dokument_items
UNION ALL
SELECT 'admin-rahmenvertrag-', id FROM rahmenvertrag_items
"""


def count_admin_postfach_items(db, hidden_keys, limit=200):
    """Return the legacy inbox length using one query and at most 640 IDs.

    The caller owns ``db`` and supplies the same hidden keys used by the inbox.
    Sorting and text/URL hydration cannot change its length, so only identities
    are fetched. ``UNION ALL`` preserves separate entries for the same order in
    different categories. ``limit`` follows the inbox's final Python slice.
    No provider calls, writes, connection closing or cross-request cache occur.
    """
    hidden = set(hidden_keys or ())
    rows = db.execute(_ADMIN_POSTFACH_IDS_SQL).fetchall()
    visible = sum(
        f"{row['item_prefix']}{row['id']}" not in hidden
        for row in rows
    )
    # This also retains zero, None and negative slice behavior without fetching
    # display text or relying on database-specific LIMIT semantics.
    return len(range(visible)[:limit])
