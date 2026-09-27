"""Explicit, read-only cockpit comparison snapshot. Never a live-data claim."""
import json
import os
import requests
from datetime import datetime, timezone
from pathlib import Path

ORIGIN = 'https://kundenstatus-app.onrender.com'
token_provider = lambda: ''


def api_token():
    return os.getenv('ASSISTANT_COCKPIT_API_TOKEN','').strip() or token_provider()


def api_enabled():
    return bool(api_token())


def enabled():
    return api_enabled() or bool(os.getenv('ASSISTANT_COCKPIT_SNAPSHOT', '').strip())


def api_read(path, params=None):
    if not api_enabled():
        raise ValueError('Direkter Cockpit-API-Zugang noch nicht eingerichtet.')
    try:
        response = requests.get(ORIGIN + '/api/werkstatt/v1/' + path,
            params=params, headers={'Authorization':'Bearer '+api_token()},
            timeout=(5,20), allow_redirects=False)
        if response.status_code != 200 or len(response.content)>2_000_000:
            raise ValueError('Cockpit-API nicht erreichbar oder Zugang ungültig. Keine Ersatzdaten verwendet.')
        return response.json()
    except (requests.RequestException, ValueError):
        raise ValueError('Cockpit-API nicht erreichbar oder Zugang ungültig. Keine Ersatzdaten verwendet.') from None



def load_snapshot():
    if api_enabled():
        data=api_read('auftraege')
        for order in data['auftraege']:
            order['quelle']=ORIGIN+'/admin/auftrag/'+str(order['id'])
        return {'source':ORIGIN,'modus':'live','stand':datetime.now(timezone.utc).isoformat(),
                'auftraege':data['auftraege'],'next_offset':data.get('next_offset')}
    try:
        path = Path(os.environ['ASSISTANT_COCKPIT_SNAPSHOT'])
        if path.stat().st_size > 2_000_000:
            raise ValueError()
        raw = json.loads(path.read_text(encoding='utf-8'))
        captured = datetime.fromisoformat(raw['captured_at'].replace('Z', '+00:00'))
        age = (datetime.now(timezone.utc) - captured).total_seconds()
        if raw['source'] != ORIGIN or age < -60 or age > 3600:
            raise ValueError()
        if not isinstance(raw['orders'], list) or len(raw['orders']) > 1000:
            raise ValueError()
        orders = []
        seen = set()
        for row in raw['orders']:
            oid = row['id']
            if isinstance(oid, bool) or not isinstance(oid, int) or oid < 1 or oid in seen:
                raise ValueError()
            seen.add(oid)
            source = f'{ORIGIN}/admin/auftrag/{oid}'
            if row['quelle'] != source:
                raise ValueError()
            order = {k: str(row.get(k) or '')[:12000] for k in
                     ('fahrzeug', 'kennzeichen', 'beschreibung', 'uebersicht', 'termine', 'status')}
            order.update(id=oid, quelle=source, stand=raw['captured_at'], modus='lesestand',
                         detail_gelesen=row.get('detail_gelesen') is True,
                         angebot_status='im Lesestand nicht erhoben',
                         versicherung_freigabe_status='im Lesestand nicht erhoben',
                         werkstatt_angebot_text='', teile=[], archiviert=0,
                         hinweis='Cockpit-Lesestand, keine automatische Synchronisierung. Übersicht kann gekürzt sein. '
                         'Freigaben und Teilebestand wurden nicht erhoben; nicht als offen oder freigegeben behaupten. '
                         'Nur angezeigte Arbeiten wiedergeben, keine Arbeiten hinzufügen. Originalquelle zum Prüfen öffnen.')
            orders.append(order)
        return {'source': ORIGIN, 'stand': raw['captured_at'], 'modus': 'lesestand', 'auftraege': orders}
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError('Cockpit-Lesestand fehlt, ist ungültig oder älter als eine Stunde. Bitte neu aus dem Cockpit übernehmen.') from None


def order_context(order_id):
    if api_enabled():
        result=api_read('auftraege/'+str(int(order_id)))
        result['quelle']=ORIGIN+'/admin/auftrag/'+str(int(order_id))
        result['modus']='live'
        return result
    for order in load_snapshot()['auftraege']:
        if order['id'] == order_id:
            return order
    raise ValueError('Auftrag ist nicht im aktuellen Cockpit-Lesestand. Kein Ersatz durch einen Demo-Auftrag.')
