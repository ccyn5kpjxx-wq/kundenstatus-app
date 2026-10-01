"""Bounded, lossy voice preload of an already authorized Cockpit context.

This is not a permission filter or a replacement for the complete read tools.
Only whole scalar values are copied; identifiers, dates, colours and units are
never shortened. Limits apply to compact ensure_ascii=False JSON, not tokens.
"""
import json
import math


MAX_CONTEXT_CHARS = 8000
MAX_CONTEXT_BYTES = 12000
MAX_SELECTED_CHARS = 2400
MAX_MATERIAL_CHARS = 1600
MAX_ORDER_CHARS = 650
_MISSING = object()
_WORK = {'beschreibung', 'analyse_text', 'analyse_hinweis', 'bauteile_override',
         'werkstatt_angebot_text', 'teile', 'dokumente'}
_ORDER = (
    'id', 'auftragsnummer', 'fahrzeug', 'kennzeichen', 'status', 'bankdaten_entfernt', 'produktion_schritt',
    'lackierbereit', 'lackierbereit_am', 'fertig_datum', 'fertig_uhrzeit',
    'abholtermin', 'abhol_uhrzeit', 'transport_art', 'annahme_datum',
    'annahme_uhrzeit', 'start_datum', 'start_uhrzeit', 'farbcode', 'farbton',
    'farbton_2', 'autohaus', 'beschreibung',
    'angebot_status', 'versicherung_freigabe_status', 'analyse_pruefen',
    'analyse_werkstatt_geprueft', 'archiviert', 'geaendert_am',
)
_SOURCE = ('art', 'beleg_id', 'artikel_id', 'seite', 'position', 'zeile', 'datum')
_COVERAGE = ('quellen_gesamt', 'freigegebene_quellen', 'ungeklaerte_quellen',
             'offene_auslese', 'auslese_zu_pruefen', 'positionen',
             'gespeicherte_artikel', 'sichtbare_positionen', 'begrenzt',
             'positionen_begrenzt', 'vollstaendigkeit_bestaetigt')


def _fits(value, chars, byte_limit=None):
    text = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    return len(text) <= chars and len(text.encode('utf-8')) <= (byte_limit or chars * 3 // 2)


def _scalar(value, limit=120):
    if value is None or type(value) is bool:
        return value
    if type(value) is int and abs(value) <= 10**18:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, str) and len(value) <= limit:
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            return _MISSING
        return value
    return _MISSING


def _flat(raw, keys, limit=120):
    if not isinstance(raw, dict):
        return {}
    return {key: value for key in keys if key in raw
            and (value := _scalar(raw[key], limit)) is not _MISSING}


def _marked(data, omitted, *, work=False):
    result = dict(data)
    omitted = list(dict.fromkeys(omitted))
    if omitted:
        result['ausgelassene_felder'] = omitted[:5]
        if len(omitted) > 5:
            result['weitere_felder_ausgelassen'] = len(omitted) - 5
    if work or any(key in _WORK for key in omitted):
        result['arbeitsdetails_abrufen'] = True
    return result


def _bounded(data, omitted, limit, *, work=False):
    """Discard complete lowest-priority fields, reserving room for markers."""
    data, omitted = dict(data), list(omitted)
    while True:
        result = _marked(data, omitted, work=work)
        if _fits(result, limit):
            return result
        key, _ = data.popitem()
        omitted.append(key)


def _order(raw, *, selected=False):
    if not isinstance(raw, dict):
        return None
    ident = _scalar(raw.get('id'), 60)
    if ident is _MISSING or ident is None or type(ident) is bool or ident == '':
        return None
    data, omitted = {}, []
    for key in _ORDER:
        if key not in raw:
            continue
        value = _scalar(raw[key], 300 if key == 'beschreibung' else 120)
        if value is _MISSING:
            omitted.append(key)
        elif value is not None and value != '':
            data[key] = value
    for key in ('analyse_text', 'analyse_hinweis', 'werkstatt_angebot_text', 'bauteile_override'):
        if raw.get(key):
            omitted.append(key)
    if selected:
        for key in ('modus', 'native', 'quelle', 'stand', 'bankdaten_entfernt',
                    'kunde_name', 'kunde_email', 'kontakt_telefon'):
            if key in raw:
                value = _scalar(raw[key], 180)
                if value is _MISSING:
                    omitted.append(key)
                else:
                    data[key] = value
        # Presence is known only for a detail response, never inferred from index rows.
        for key, fields in (
            ('dokumente', ('id', 'original_name', 'dokument_typ', 'analyse_quelle')),
            ('teile', ('bezeichnung', 'status', 'liefertermin')),
        ):
            if key not in raw:
                continue
            rows = raw[key]
            if not isinstance(rows, list):
                omitted.append(key)
                continue
            data[key + '_anzahl'] = len(rows)
            data[key] = [_flat(row, fields, 100) for row in rows[:3]]
            if rows:
                omitted.append(key)  # Text/notes and possible further rows remain in read tools.
    else:
        omitted.extend(key for key in ('dokumente', 'teile') if raw.get(key))
    return _bounded(data, omitted, MAX_SELECTED_CHARS if selected else MAX_ORDER_CHARS)


def _pack(raw):
    if not isinstance(raw, dict):
        return None
    data = _flat(raw, ('menge', 'einheit', 'pro', 'pruefen'), 60)
    source = _flat(raw.get('quelle'), _SOURCE, 100)
    # Keep amount/unit/container and provenance together; never infer pack size.
    if all(key in data and data[key] not in (None, '') for key in ('menge', 'einheit', 'pro')) and source.get('beleg_id'):
        data['quelle'] = source
        return data
    return None


def _variant(raw):
    if not isinstance(raw, dict):
        return None
    fields = ('variante_id', 'id', 'produkt_name', 'lieferant', 'artikelnummer',
              'groesse', 'farbe', 'gebinde', 've', 'pruefen', 'bestellbar')
    data = _flat(raw, fields, 160)
    # Do not turn an incomplete size/SKU into a deceptively exact variant.
    if any(key in raw and key not in data for key in fields):
        return None
    if not data.get('produkt_name') or not data.get('artikelnummer'):
        return None
    color = raw.get('farbabgleich')
    if isinstance(color, dict):
        data['farbabgleich'] = _flat(color, ('basis', 'bestaetigt', 'namenshinweis'), 80)
    pack = _pack(raw.get('packinhalt'))
    if pack:
        data['packinhalt'] = pack
    sources = raw.get('quellen')
    if isinstance(sources, list) and sources:
        data['quellen'] = [_flat(sources[0], _SOURCE, 100)]
    elif isinstance(raw.get('quelle'), dict):
        data['quelle'] = _flat(raw['quelle'], _SOURCE, 100)
    # Histories, usual quantities and prices intentionally stay in artikel_suchen.
    data['details_abrufen'] = True
    return data if _fits(data, 950) else None


def _material(raw):
    if not isinstance(raw, dict):
        return {'details_abrufen': True}
    data = _flat(raw, ('verfuegbar', 'pruefen', 'bestellbar', 'suchstatus'), 100)
    data['details_abrufen'] = True
    data['hinweis'] = 'Begrenzte Belegauswahl; keine vollständige Artikelkenntnis oder Bestellfreigabe. Weitere Varianten und Mengen mit artikel_suchen lesen.'
    coverage = _flat(raw.get('abdeckung'), _COVERAGE, 60)
    if coverage:
        data['abdeckung'] = coverage
    rows = raw.get('varianten')
    if not isinstance(rows, list):
        return data
    data['varianten'] = []
    data['varianten_gekuerzt'] = True  # Reserve marker space during packing.
    for raw_row in rows:
        row = _variant(raw_row)
        if row is None:
            break
        candidate = dict(data, varianten=data['varianten'] + [row])
        if not _fits(candidate, MAX_MATERIAL_CHARS):
            break
        data = candidate
    data['varianten_gekuerzt'] = bool(raw.get('varianten_gekuerzt') or len(data['varianten']) < len(rows))
    return data


def compact_voice_context(context):
    """Return a bounded independent dict; caller supplies authorized data only.

    Order index retains a contiguous prefix. next_offset therefore remains a
    real source-list cursor, even when selected order is also present separately.
    Missing fields are unknown, not evidence of absence or permission to act.
    """
    source = context if isinstance(context, dict) else {}
    header_fields = ('stand', 'modus', 'native', 'source', 'quelle',
                     'max_alter_sekunden', 'bankdaten_entfernt')
    head = _flat(source, header_fields, 200)
    omitted = [key for key in header_fields if key in source and key not in head]
    if isinstance(source.get('kalender'), dict):
        head['kalender'] = _flat(source['kalender'], ('zeitzone', 'heute', 'morgen', 'uebermorgen'), 64)
        if any(key in source['kalender'] and key not in head['kalender']
               for key in ('zeitzone', 'heute', 'morgen', 'uebermorgen')):
            omitted.append('kalender')
    if 'hinweis' in source:
        hint = _scalar(source['hinweis'], 500)
        if hint is not _MISSING:
            head['hinweis'] = hint
        else:
            omitted.append('hinweis')
    head = _bounded(head, omitted, 1400)
    result = dict(head, sprachkontext_gekuerzt=True,
                  kontext_hinweis='Gekürzte Übersicht. Fehlende Angaben gezielt nachladen; Beschreibung/Analyse sind keine Freigabe.',
                  auftraege=[], next_offset=None, gekuerzt=False)
    selected = source.get('ausgewaehlter_auftrag')
    if selected is not None:
        result['ausgewaehlter_auftrag'] = _order(selected, selected=True) or {'arbeitsdetails_abrufen': True}
    if 'materialfoto_auswahl' in source:
        photo = source['materialfoto_auswahl']
        if photo is None:
            result['materialfoto_auswahl'] = None
        else:
            projection = _flat(photo, ('foto_id', 'treffer_id'), 100)
            projection.update(_variant(photo) or {'details_abrufen': True})
            projection['hinweis'] = 'Nur Artikelauswahl; Menge, Dringlichkeit, Preis und Bestellung nicht bestätigt.'
            result['materialfoto_auswahl'] = _bounded(projection, [], 1200)
    if 'materialwissen' in source:
        result['materialwissen'] = _material(source['materialwissen'])
    rows = source.get('auftraege')
    if not isinstance(rows, list):
        rows = []
        result['gekuerzt'] = True
    offset = source.get('offset', 0)
    offset = offset if type(offset) is int and 0 <= offset <= 10**12 else 0
    if 'offset' in source:
        result['offset'] = offset
    original_next = source.get('next_offset')
    original_next = original_next if type(original_next) is int and 0 <= original_next <= 10**12 else None
    result['next_offset'] = original_next
    result['gekuerzt'] = bool(result['gekuerzt'] or source.get('gekuerzt') or original_next is not None)
    for raw_row in rows:
        row = _order(raw_row)
        if row is None:
            break
        # Reserve the cursor/true marker *before* the final limit check.
        candidate = dict(result, auftraege=result['auftraege'] + [row],
                         next_offset=offset + len(result['auftraege']) + 1, gekuerzt=True)
        if not _fits(candidate, MAX_CONTEXT_CHARS, MAX_CONTEXT_BYTES):
            break
        result['auftraege'].append(row)
    if len(result['auftraege']) < len(rows):
        result['gekuerzt'] = True
        result['next_offset'] = offset + len(result['auftraege'])
    # Bounded independent blocks leave room even with no index entries. This
    # fallback also guards future block additions without slicing any values.
    if not _fits(result, MAX_CONTEXT_CHARS, MAX_CONTEXT_BYTES):
        for key in ('materialwissen', 'materialfoto_auswahl'):
            if key in result:
                result[key] = {'details_abrufen': True, 'ausgelassene_felder': [key]}
            if _fits(result, MAX_CONTEXT_CHARS, MAX_CONTEXT_BYTES):
                break
    return result
