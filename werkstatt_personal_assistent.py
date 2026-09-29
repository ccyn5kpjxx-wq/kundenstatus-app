"""Own employee tools reuse the avatar's separate human confirmation flow."""
import hashlib
import json
import secrets
from werkstatt_arbeitszeit import personal_id

KINDS = frozenset({'urlaub', 'arbeitszeit'})
READ_TOOLS = frozenset({'mein_urlaub_lesen', 'meine_arbeitszeit_lesen'})
PROPOSAL_TOOLS = {'urlaub_beantragen_vorschlagen': 'urlaub', 'arbeitszeit_vorschlagen': 'arbeitszeit'}
TOOLS = [
    {'type':'function','name':'mein_urlaub_lesen','description':'Nur den eigenen Urlaubsstand und eigene Anträge lesen. Identität kommt aus Anmeldung, niemals fremde Mitarbeiter-ID. Unbekannten Reststand nicht schätzen.',
     'parameters':{'type':'object','properties':{'jahr':{'type':'integer','minimum':2000,'maximum':2099}},'additionalProperties':False}},
    {'type':'function','name':'meine_arbeitszeit_lesen','description':'Nur die eigenen erfassten Arbeitszeiten und den aktuellen Stempelstatus lesen; keine Lohnabrechnung.',
     'parameters':{'type':'object','properties':{'monat':{'type':'string','description':'YYYY-MM, leer für aktuellen Monat'}},'additionalProperties':False}},
    {'type':'function','name':'urlaub_beantragen_vorschlagen','description':'Eigenen Urlaub von/bis als Antrag vorbereiten. App liest vor und holt ausdrückliche Bestätigung; erst dann beantragt, niemals automatisch genehmigt.',
     'parameters':{'type':'object','properties':{'start_datum':{'type':'string','description':'YYYY-MM-DD'},'end_datum':{'type':'string','description':'YYYY-MM-DD'}},'required':['start_datum','end_datum'],'additionalProperties':False}},
    {'type':'function','name':'arbeitszeit_vorschlagen','description':'Eigenen Arbeitsbeginn, Arbeitsende oder Pause mit Serverzeit nach Bestätigung erfassen. Keine rückwirkende Uhrzeit. Erst tatsächlichen Stempelwunsch klären; ein Gruß allein ist keine Arbeitsbuchung.',
     'parameters':{'type':'object','properties':{'aktion':{'type':'string','enum':['kommen','gehen','pause','weiter']}},'required':['aktion'],'additionalProperties':False}},
]
for definition in TOOLS:
    definition['strict']=False
    definition['parameters'].setdefault('required',[])
RULES = (
    'Persönliche Mitarbeiterfunktionen nur für das angemeldete eigene Profil verwenden. '
    'Bei Resturlaub mein_urlaub_lesen, bei Arbeitszeiten meine_arbeitszeit_lesen. Fehlendes Urlaubskonto offen nennen, keine Tage erfinden. '
    'Bei Urlaubswunsch von/bis eindeutig klären, mit urlaub_beantragen_vorschlagen vorbereiten. Beantragt ist noch nicht genehmigt. '
    'Bei ausdrücklichem Kommen/Arbeitsbeginn, Gehen/Feierabend oder Pause arbeitszeit_vorschlagen; nur aktuelle Serverzeit nach separater Bestätigung. '
    'Bei Guten Morgen zuerst Tagesübersicht; ein Gruß allein darf nicht einstempeln. Bei Ich bin jetzt hier nachfragen, ob Arbeitsbeginn erfasst werden soll. '
    'Keine Aussagen zu Konten oder Arbeitszeiten anderer Mitarbeiter. Ein Adminzugang ist keinem persönlichen Urlaubskonto zugeordnet. '
)


class PersonalActions:
    def __init__(self, p, leave, time_tracking, db_scope, audit):
        self.p,self.leave,self.time,self.db,self.audit=p,leave,time_tracking,db_scope,audit

    def read(self, who, name, args):
        personal_id(who)
        allowed={'jahr'} if name=='mein_urlaub_lesen' else {'monat'}
        if not isinstance(args,dict) or set(args)-allowed:
            raise ValueError('Nur eigene Mitarbeiterdaten können abgefragt werden.')
        if name=='mein_urlaub_lesen':
            return self.leave.summary(who,args.get('jahr'))
        if name=='meine_arbeitszeit_lesen':
            return self.time.summary(who,args.get('monat') or None)
        raise ValueError('Unbekannte persönliche Auskunft.')

    def propose(self, who, args):
        personal_id(who)
        kind=args.get('art')
        if kind=='urlaub':
            if set(args)-{'art','start_datum','end_datum'}:
                raise ValueError('Urlaubsanträge werden ausschließlich für das eigene Profil vorbereitet.')
            preview=self.leave.preview(who,args.get('start_datum'),args.get('end_datum'))
            text=f"Urlaub vom {preview['von']} bis {preview['bis']} für dein eigenes Profil beantragen. "
            text+=(f"Berechnet: {preview['tage']} Urlaubstage. " if preview.get('tage') is not None else 'Anzahl und Reststand müssen noch geprüft werden. ')
            text+='Der Antrag ist erst nach Entscheidung der Werkstattleitung genehmigt.'
            payload={'start_datum':preview['von'],'end_datum':preview['bis'],'preview':preview,'text':text}
            with self.db() as db:
                previous=db.execute("SELECT id,status,version FROM mitarbeiter_urlaubsantraege WHERE actor=? AND start_datum=? AND end_datum=? AND status IN ('abgelehnt','zurueckgezogen') ORDER BY erstellt_am DESC,id DESC",(who['actor'],preview['von'],preview['bis'])).fetchall()
            # A new application after withdrawal/rejection is distinct, while
            # repeated preparation within the same generation stays idempotent.
            payload['vorherige_antraege']=[dict(row) for row in previous]
        elif kind=='arbeitszeit':
            if set(args)-{'art','aktion'}:
                raise ValueError('Nur der eigene aktuelle Zeitstempel ist möglich.')
            payload=self.time.preview(who,args.get('aktion'))
        else:
            raise ValueError('Unbekannte persönliche Aktion.')
        serialized=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        fingerprint=hashlib.sha256(f"{who['actor']}:{kind}:{serialized}".encode()).hexdigest()
        with self.db() as db:
            db.execute('INSERT INTO assistent_aktionen(id,actor,auftrag_id,art,payload,fingerprint,erstellt_am) VALUES(?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING',
                       (secrets.token_hex(16),who['actor'],0,kind,serialized,fingerprint,self.p.now_str()))
            row=db.execute('SELECT * FROM assistent_aktionen WHERE fingerprint=?',(fingerprint,)).fetchone()
            self.audit(db,who,None,'persoenlicher_vorschlag',row['id'])
        return row

    def confirm(self, who, row):
        personal_id(who)
        if row['actor']!=who['actor'] or row['art'] not in KINDS:
            raise ValueError('Die persönliche Aktion gehört nicht zu deinem Zugang.')
        payload=json.loads(row['payload'])
        request_id=row['id']
        if row['art']=='urlaub':
            result=self.leave.apply(who,payload['start_datum'],payload['end_datum'],request_id)
            result={**result,'hinweis':('Urlaubsantrag eingereicht. Noch nicht genehmigt.' if result['status']=='beantragt' else 'Der vorhandene Urlaubsantrag ist '+result['status_label']+'. Es wurde kein neuer Antrag eingereicht.')}
        else:
            result=self.time.stamp(who,payload['aktion'],request_id,payload['revision'])
        with self.db() as db:
            changed=db.execute("UPDATE assistent_aktionen SET status='dokumentiert' WHERE id=? AND actor=? AND status='vorschlag'",(row['id'],who['actor'])).rowcount
            if changed:self.audit(db,who,None,'persoenlich_bestaetigt',row['id'])
        return {'ok':True,'status':'dokumentiert',**result}
