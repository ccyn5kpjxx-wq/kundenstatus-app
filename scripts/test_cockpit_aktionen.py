"""Isolated action summary regression tests, using the established portal fixture."""
import test_lead_kundenportal as flow
p=flow.portal

def seed():
    p.app.config['TESTING']=True
    p.whatsapp_workshop_numbers=lambda: []
    p.send_lead_email=lambda *a, **kw: None
    p.post_whatsapp_payload=lambda *a, **kw:(False,'','local preview')
    p.schedule_change_backup=lambda *a: None
    ids=[]
    for name in ['Beispiel: Angebot vorbereiten','Beispiel: Kundenantwort','Beispiel: wartet auf Angebot','Beispiel: Termin abstimmen']:
        ids.append(p.create_lead({'website':'auto-lackierzentrum','quelle':'website','kunde_name':name,'fahrzeug':'Demo-Fahrzeug','beschreibung':'Lokales Beispiel ohne echte Nachricht','status':'neu'}))
    db=p.get_db()
    db.execute("UPDATE leads SET naechste_aktion='Neue Kundennachricht beantworten' WHERE id=?",(ids[1],))
    db.execute("UPDATE leads SET angebot_status='angebot_abgegeben', status='angebot_offen' WHERE id=?",(ids[2],))
    db.execute("UPDATE leads SET angebot_status='angebot_abgegeben', angebot_text='Lokales Beispiel', angebot_preis='0 EUR' WHERE id=?",(ids[3],))
    db.commit();db.close()
    client=p.app.test_client();lead=p.get_lead(ids[3]);token=lead['kunden_status_token'];client.get('/status/'+token)
    client.post('/status/'+token+'/angebot-annehmen',data={p.CSRF_FIELD_NAME:flow.csrf_token(client),'angebot_annehmen_bestaetigt':'1','wunsch_annahme_datum':(flow.date.today()+flow.timedelta(days=16)).isoformat(),'transport_art':'standard','ersatzfahrzeug':'nein'})
    return ids

def main():
    ids=seed()
    with p.app.test_request_context('/admin/cockpit'):
        orders=p.list_auftraege(include_archived=True)
        result=p.cockpit_aktionsuebersicht(orders)
        assert [len(result['groups'][k]) for k in ('angebote','antworten','termine')]==[1,1,1],result
        assert result['waiting']==1 and result['total']==3
        db=p.get_db()
        db.execute("UPDATE leads SET status='angebot_offen', angebot_status='entwurf' WHERE id=?",(ids[0],));db.commit();db.close()
        legacy=p.cockpit_aktionsuebersicht(orders)
        assert not legacy['groups']['angebote'] and legacy['waiting']==2
        db=p.get_db();db.execute("UPDATE leads SET status='besichtigung_geplant', naechste_aktion='Besichtigung am 7. vorbereiten' WHERE id=?",(ids[0],));db.commit();db.close()
        assert len(p.cockpit_aktionsuebersicht(orders)['groups']['termine'])==2
        db=p.get_db();db.execute("UPDATE leads SET status='unterlagen_fehlen' WHERE id=?",(ids[0],));db.commit();db.close()
        result=p.cockpit_aktionsuebersicht(orders)
        assert not result['groups']['angebote'] and len(result['groups']['antworten'])==2
        order=next(a for a in orders if a['id']==p.get_lead(ids[3])['auftrag_id'])
        order['schaden_aufnahme']['kunden_wunsch_bestaetigt_am']='21.09.2026'
        assert len(p.cockpit_aktionsuebersicht(orders)['groups']['termine'])==0
        order['archiviert']=1
        assert len(p.cockpit_aktionsuebersicht(orders)['groups']['termine'])==0
        db=p.get_db();db.execute("UPDATE leads SET status='verloren' WHERE id=?",(ids[0],));db.commit();db.close()
        assert not p.cockpit_aktionsuebersicht(orders)['groups']['angebote']
    client=p.app.test_client()
    assert client.get('/admin/cockpit').status_code==302
    with client.session_transaction() as session:session['admin']=True
    response=client.get('/admin/cockpit')
    assert response.status_code==200 and 'Das wartet auf dich' in response.get_data(as_text=True)
    print('PASS: Gruppen, Wartezustand, Terminbestaetigung, Archiv/Verlust und Adminschutz')

if __name__=='__main__':main()
