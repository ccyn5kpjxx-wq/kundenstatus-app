"""Admin day overview from existing read services; no stamps or dispatch.

Clock state is evidence of recorded work events, never online login presence.
Order amounts are saved snapshots, never a current supplier quotation.
"""
from datetime import date, datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from werkstatt_bestelluebersicht import OrderOverview

BERLIN = ZoneInfo('Europe/Berlin')
_ORDER_GROUPS = {
    'offen': {'draft', 'approved_pending', 'material_open', 'material_review', 'material_inquiry', 'material_ready'},
    'eingeplant': {'queued', 'ready'},
    'versandt': {'sent', 'copy_pending', 'external_sent'},
    'pruefen': {'sending', 'partial', 'uncertain', 'not_sent', 'blocked', 'unknown', 'external_pending', 'material_accepted'},
}


def _group(state):
    return next((key for key, states in _ORDER_GROUPS.items() if state in states), 'pruefen')


def _date_range(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end or start)
    if end < start:
        raise ValueError('Abwesenheitszeitraum prüfen.')
    return start, end


def _date_label(start, end):
    return start.strftime('%d.%m.%Y') if start == end else start.strftime('%d.%m.%Y') + ' – ' + end.strftime('%d.%m.%Y')


def _team(portal):
    rows, counts = [], dict.fromkeys(('arbeitet', 'pause', 'abwesend', 'pruefen'), 0)
    for employee in portal.assistant_time.admin_employees():
        if not employee['aktiv']:
            continue
        state = employee['zeitstatus']
        key = state['key']
        key = 'abwesend' if key in ('beendet', 'nicht_angestempelt') else key
        key = key if key in counts else 'pruefen'
        counts[key] += 1
        rows.append(dict(id=employee['id'], name=employee['name'], key=key,
                         label=state['label'], detail=state['detail']))
    return dict(rows=rows, counts=counts, error='')


def _absences(portal, employees, today):
    active = {person['id']: person for person in employees if person.get('aktiv')}
    rows, invalid = [], False
    # The existing calendar contains legacy entries and approved requests.
    # Do not imply an approval status that a legacy calendar never recorded.
    for mid, person in active.items():
        for item in person.get('urlaube', []):
            try:
                start, end = _date_range(item.get('start_iso', ''), item.get('end_iso', ''))
            except (TypeError, ValueError):
                invalid = True
                continue
            if end < today:
                continue
            rows.append(dict(mitarbeiter_id=mid, name=person['name'], art='urlaub',
                             datum_label=_date_label(start, end), zeit_label='Im Urlaubskalender',
                             notiz=item.get('notiz', ''), heute=start <= today <= end,
                             url=f'/werkstatt/assistent/urlaub/verwaltung#mitarbeiter-{mid}', _start=start))
    seen = set()
    for year in (today.year, today.year + 1):
        for item in portal.employee_school.admin_rows({'actor': 'admin'}, year):
            mid = item['mitarbeiter_id']
            if item['id'] in seen or mid not in active or item['status'] != 'gemeldet':
                continue
            seen.add(item['id'])
            try:
                start, end = _date_range(item['von'], item['bis'])
            except (TypeError, ValueError):
                invalid = True
                continue
            if end < today or start > today + timedelta(days=366):
                continue
            times = 'Ganztägig · gemeldet' if item['ganztag'] else item['start_zeit'] + '–' + item['end_zeit'] + ' Uhr · gemeldet'
            rows.append(dict(mitarbeiter_id=mid, name=active[mid]['name'], art='schule',
                             datum_label=_date_label(start, end), zeit_label=times,
                             notiz=item['notiz'], heute=start <= today <= end,
                             url=f'/werkstatt/assistent/urlaub/verwaltung#mitarbeiter-{mid}', _start=start))
    rows.sort(key=lambda row: (not row['heute'], row['_start'], row['name'], row['art']))
    for row in rows:
        row.pop('_start')
    return dict(rows=rows, error='Ein Abwesenheitsdatum ist ungültig. Bitte im Abwesenheitsbereich prüfen.' if invalid else '')


def _recent(item):
    try:
        return datetime.strptime(item['created'], '%d.%m.%Y %H:%M')
    except (ValueError, TypeError, KeyError):
        return datetime.min


def _orders(portal, now):
    overview = OrderOverview(portal.get_db)
    page = overview.page(now=now)
    # Counts include every saved queue/material item, independent of paging.
    counts = dict.fromkeys(_ORDER_GROUPS, 0)
    for state, number in page['counts'].items():
        if state != 'cancelled':
            counts[_group(state)] += number
    # Drafts are independently paged by the source service. Count their known
    # states explicitly; unseen/unknown draft states remain review work.
    known_drafts = 0
    for state in ('draft', 'approved_pending'):
        number = overview.page({'state': state}, now=now)['draft_count']
        counts['offen'] += number
        known_drafts += number
    counts['pruefen'] += max(0, page['draft_count'] - known_drafts)
    entries = [entry for entry in page['items'] + page['drafts'] if entry['state'] != 'cancelled']
    entries.sort(key=_recent, reverse=True)
    rows = []
    for item in entries[:8]:
        query = {'vorschlag' if item['draft'] else 'bestellung': item['id']}
        price = item.get('total')
        price_label = 'Gespeicherter Betrag: ' + price if price and price != 'nicht belegt' else 'Betrag nicht belegt'
        rows.append(dict(url=item.get('detail_url') or '/admin/assistent-bestellungen?' + urlencode(query),
                         titel=item['product'], mitarbeiter=item['person'], lieferant=item['supplier'],
                         status_key=_group(item['state']), status_label=item['state_label'],
                         menge_label=(item['quantity'] + ' ' + item.get('unit', '')).strip(),
                         preis_label=price_label, datum_label=item['created']))
    return dict(counts=counts, rows=rows, error='')


def chef_overview(portal, who, employees, *, now=None):
    if not isinstance(who, dict) or who.get('actor') != 'admin':
        raise PermissionError('Nur die Werkstattleitung darf die Chefübersicht öffnen.')
    now = (now or datetime.now(BERLIN)).astimezone(BERLIN)
    result = dict(as_of=now.strftime('%d.%m.%Y, %H:%M Uhr'), today_label=now.strftime('%d.%m.%Y'))
    components = (
        ('team', lambda: _team(portal), 'Der Stempelstatus konnte nicht gelesen werden. Bitte die Arbeitszeiten prüfen.'),
        ('abwesenheiten', lambda: _absences(portal, employees, now.date()), 'Abwesenheiten konnten nicht vollständig gelesen werden. Bitte den Abwesenheitsbereich prüfen.'),
        ('bestellungen', lambda: _orders(portal, now), 'Bestellvorgänge konnten nicht gelesen werden. Bitte den Bestellordner prüfen.'),
    )
    for key, read, error in components:
        try:
            result[key] = read()
        except Exception:
            # The UI must never turn an unavailable source into a zero count.
            result[key] = dict(rows=[], counts=None, error=error)
    return result
