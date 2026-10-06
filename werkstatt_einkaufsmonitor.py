"""Opt-in supplier invoice polling. Originals and proposals only; never ordering.

Registration performs schema migration only. Network activity requires an
enabled account and an explicit tick/worker. Mailbox and catalog keep their own
leases; the durable account lease also fences concurrent monitor processes.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
import threading
import time
import uuid

import click

TABLES = ('assistent_einkaufsmonitor', 'assistent_einkaufsmonitor_quellen')
LEASE_SECONDS = 1800


def now():
    return datetime.now(timezone.utc).isoformat()


class PurchaseMonitor:
    def __init__(self, portal):
        self.p = portal
        self._worker_lock = threading.Lock()
        self._worker = None
        self._worker_pid = None
        self._worker_stop = threading.Event()
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS assistent_einkaufsmonitor (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, account TEXT UNIQUE NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0, interval_seconds INTEGER NOT NULL DEFAULT 300,
                    next_poll_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                    lease TEXT NOT NULL DEFAULT '', lease_until DOUBLE PRECISION NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1, failures INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '', last_started_at TEXT NOT NULL DEFAULT '',
                    last_finished_at TEXT NOT NULL DEFAULT '', worker_heartbeat_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL, configured_by TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS assistent_einkaufsmonitor_quellen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, account TEXT NOT NULL,
                    file_id INTEGER NOT NULL, beleg_id INTEGER NOT NULL, import_id INTEGER,
                    state TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at DOUBLE PRECISION NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(account,file_id));
                CREATE INDEX IF NOT EXISTS idx_einkaufsmonitor_sources_pending
                    ON assistent_einkaufsmonitor_quellen(account,state,next_retry_at,id);
            ''')

    @property
    def sources(self):
        return self.p.assistant_mail_sources

    @property
    def catalog(self):
        return self.p.cockpit_data.catalog

    def worker_enabled(self):
        configured = self.p.app.config.get('PURCHASE_MONITOR_WORKER_ENABLED')
        if configured is None:
            configured = os.environ.get('PURCHASE_MONITOR_WORKER_ENABLED', '').lower() in {'1','true','yes','on'}
        return configured is True

    def configure(self, enabled, interval_seconds=300, actor='admin'):
        if actor != 'admin':
            raise PermissionError('Nur die Werkstattverwaltung darf den Rechnungsmonitor konfigurieren.')
        if type(enabled) is not bool or type(interval_seconds) is not int or not 60 <= interval_seconds <= 86400:
            raise ValueError('Aktivierung eindeutig angeben; Intervall zwischen 60 und 86400 Sekunden.')
        account, _ = self.sources.identity()
        with self.db() as db:
            db.execute('''INSERT INTO assistent_einkaufsmonitor
                (account,enabled,interval_seconds,next_poll_at,updated_at,configured_by)
                VALUES(?,?,?,?,?,?) ON CONFLICT(account) DO UPDATE SET
                enabled=excluded.enabled,interval_seconds=excluded.interval_seconds,
                next_poll_at=excluded.next_poll_at,revision=assistent_einkaufsmonitor.revision+1,
                lease='',lease_until=0,last_error='',failures=0,updated_at=excluded.updated_at,
                configured_by=excluded.configured_by''',
                (account,int(enabled),interval_seconds,time.time(),now(),actor))
            # Revoke the old mail step's publishing token as well. Already saved
            # originals/cursors remain intact and are resumed after re-enabling.
            db.execute("UPDATE assistent_mailquellen_laeufe SET state='paused',lease='',lease_until=0 WHERE account=? AND state='active'", (account,))
        return self.status()

    def status(self):
        result = dict(configured=False, enabled=False, state='not_configured', interval_seconds=300,
            next_poll_at=0, busy=False, last_started_at='', last_finished_at='', last_error='', failures=0,
            worker_enabled=self.worker_enabled(), worker_heartbeat_at='', worker_recent=False,
            mail_state='new', counts={key:0 for key in ('queued','complete','review','excluded','error')},
            recent_sources=[], hint='Nur Rechnungsvorschläge. Keine Bestellung, Zahlung oder Änderung im Postfach.')
        try:
            account, _ = self.sources.identity()
        except ValueError:
            return result
        result['configured'] = True
        with self.db() as db:
            row = db.execute('SELECT * FROM assistent_einkaufsmonitor WHERE account=?', (account,)).fetchone()
            if row:
                for key in ('interval_seconds','next_poll_at','last_started_at','last_finished_at','last_error','failures','worker_heartbeat_at'):
                    result[key] = row[key]
                result['enabled'] = bool(row['enabled'])
                result['busy'] = bool(row['lease'] and row['lease_until'] > time.time())
                result['state'] = 'disabled' if not row['enabled'] else 'error' if row['last_error'] else 'active' if result['busy'] else 'waiting'
                try:
                    heartbeat = datetime.fromisoformat(row['worker_heartbeat_at']).timestamp()
                    result['worker_recent'] = result['enabled'] and 0 <= time.time()-heartbeat < 120
                except ValueError:
                    pass
            else:
                result['state'] = 'disabled'
            mail = db.execute('SELECT state FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
            result['mail_state'] = mail['state'] if mail else 'new'
            for count in db.execute('SELECT state,COUNT(*) AS n FROM assistent_einkaufsmonitor_quellen WHERE account=? GROUP BY state', (account,)).fetchall():
                result['counts'][count['state']] = count['n']
            result['recent_sources'] = [dict(row) for row in db.execute('''SELECT q.id,q.beleg_id,q.import_id,q.state,q.attempts,q.updated_at,q.last_error,
                f.supplier,f.original_name,f.sha256 FROM assistent_einkaufsmonitor_quellen q
                JOIN assistent_mailquellen_dateien f ON f.id=q.file_id WHERE q.account=? ORDER BY q.id DESC LIMIT 30''', (account,)).fetchall()]
        return result

    @contextmanager
    def owned(self, claim):
        with self.db() as db:
            self._guard(claim,db)
            yield db

    def _guard(self, claim, db):
        # The caller's transaction keeps this row lock until publication. A
        # separate preflight connection would leave a revocation race.
        db.execute('UPDATE assistent_einkaufsmonitor SET account=account WHERE id=?', (claim['id'],))
        row = db.execute('SELECT * FROM assistent_einkaufsmonitor WHERE id=?', (claim['id'],)).fetchone()
        if not row or not row['enabled'] or row['lease'] != claim['lease'] or row['revision'] != claim['revision'] or row['lease_until'] <= time.time():
            raise ValueError('Monitorlauf abgelaufen oder angehalten.')
        current, _ = self.sources.identity()
        if current != claim['account']:
            raise ValueError('Postfachkonfiguration geändert; Monitorlauf angehalten.')

    def _enqueue_saved(self, claim):
        with self.owned(claim) as db:
            rows = db.execute("SELECT id,attachments_json FROM assistent_mailquellen_nachrichten WHERE account=? AND state IN ('files','review_files') AND monitor_catalog_queued=0 ORDER BY id LIMIT 20", (claim['account'],)).fetchall()
            for row in rows:
                attachments = json.loads(row['attachments_json'] or '[]')
                if not isinstance(attachments,list):
                    raise ValueError('Gespeicherte Belegverweise benötigen eine Prüfung.')
                for item in attachments:
                    if not isinstance(item,dict) or item.get('state') not in {'stored','duplicate'}:
                        continue
                    bid = item.get('beleg_id')
                    if type(bid) is not int or bid <= 0:
                        continue
                    file = db.execute('SELECT id,beleg_id FROM assistent_mailquellen_dateien WHERE beleg_id=? AND sha256=?', (bid,item.get('sha256',''))).fetchone()
                    if file:
                        db.execute('''INSERT INTO assistent_einkaufsmonitor_quellen
                            (account,file_id,beleg_id,created_at,updated_at) VALUES(?,?,?,?,?)
                            ON CONFLICT(account,file_id) DO NOTHING''', (claim['account'],file['id'],bid,now(),now()))
                db.execute('UPDATE assistent_mailquellen_nachrichten SET monitor_catalog_queued=1 WHERE id=?', (row['id'],))

    def _next_source(self, claim):
        with self.owned(claim) as db:
            row = db.execute("SELECT * FROM assistent_einkaufsmonitor_quellen WHERE account=? AND state IN ('queued','error') AND next_retry_at<=? ORDER BY id LIMIT 1", (claim['account'],time.time())).fetchone()
            return dict(row) if row else None

    def _refresh_reviewed_sources(self, claim):
        # A newly granted metadata permission resumes previously unread
        # originals. Uncertain extraction results do not retry themselves.
        with self.owned(claim) as db:
            held = [dict(row) for row in db.execute('''SELECT q.id,q.import_id,i.state AS import_state,
                b.beleg_typ,b.lieferant,b.original_name FROM assistent_einkaufsmonitor_quellen q
                LEFT JOIN assistent_rechnungsimporte i ON i.id=q.import_id
                LEFT JOIN einkauf_belege b ON b.id=q.beleg_id WHERE q.account=?
                AND q.state IN ('review','excluded') AND q.next_retry_at<=?
                AND (q.import_id IS NULL OR i.state IN ('offen','ausgeschlossen','zuordnen'))
                ORDER BY q.id LIMIT 20''', (claim['account'],time.time())).fetchall()]
        for row in held:
            allowed = self.catalog.source_rule(row)['allowed']
            with self.owned(claim) as db:
                if allowed:
                    db.execute("UPDATE assistent_einkaufsmonitor_quellen SET state='queued',next_retry_at=0,last_error='',updated_at=? WHERE id=?", (now(),row['id']))
                else:
                    db.execute('UPDATE assistent_einkaufsmonitor_quellen SET next_retry_at=? WHERE id=?', (time.time()+claim['interval_seconds'],row['id']))

    def _extract(self, claim, job):
        with self.owned(claim) as db:
            source = db.execute('''SELECT b.id,b.beleg_typ,b.lieferant,b.original_name FROM einkauf_belege b
                JOIN assistent_mailquellen_dateien f ON f.beleg_id=b.id WHERE f.id=? AND b.id=?''', (job['file_id'],job['beleg_id'])).fetchone()
        if not source:
            raise ValueError('Originalbeleg fehlt; gespeicherten Verweis prüfen.')
        rule = self.catalog.source_rule(dict(source))
        if not rule['allowed']:
            state = 'excluded' if rule['decision'] == 'block' else 'review'
            with self.owned(claim) as db:
                db.execute('UPDATE assistent_einkaufsmonitor_quellen SET state=?,last_error=?,updated_at=? WHERE id=?',
                           (state,'Lieferanten- oder Belegfreigabe fehlt; keine Datei gelesen.',now(),job['id']))
            return
        # Never collect all Lexware invoices or process the first arbitrary open
        # catalog row. This exact persisted mail original defines the work unit.
        self.catalog.prepare({'einkaufsbelege':[dict(source)],'lieferantenrechnungen':[]})
        with self.owned(claim) as db:
            source_row = db.execute('SELECT id FROM assistent_rechnungsimporte WHERE source_key=?', ('einkauf:'+str(job['beleg_id']),)).fetchone()
            if not source_row:
                raise ValueError('Katalogreferenz konnte nicht gespeichert werden.')
            source_id = int(source_row['id'])
            db.execute('UPDATE assistent_einkaufsmonitor_quellen SET import_id=?,attempts=attempts+1,updated_at=? WHERE id=?', (source_id,now(),job['id']))
        self.catalog.process_next(source_id=source_id,guard=lambda db:self._guard(claim,db))
        with self.owned(claim) as db:
            source_row = db.execute('SELECT state FROM assistent_rechnungsimporte WHERE id=?', (source_id,)).fetchone()
            state = {'ausgelesen':'complete','pruefen':'review','ausgeschlossen':'excluded','zuordnen':'review'}.get(source_row['state'],'queued')
            db.execute('UPDATE assistent_einkaufsmonitor_quellen SET state=?,next_retry_at=?,last_error=?,updated_at=? WHERE id=?',
                       (state,time.time()+30 if state=='queued' else 0,'',now(),job['id']))

    def tick(self, max_steps=2, *, force=False, worker=False):
        if type(max_steps) is not int or not 1 <= max_steps <= 10 or type(force) is not bool or type(worker) is not bool:
            raise ValueError('Ein Lauf verarbeitet ein bis zehn begrenzte Schritte.')
        account, _ = self.sources.identity()
        token = uuid.uuid4().hex
        with self.db() as db:
            if worker:
                db.execute('UPDATE assistent_einkaufsmonitor SET worker_heartbeat_at=? WHERE account=? AND enabled=1', (now(),account))
            changed = db.execute('''UPDATE assistent_einkaufsmonitor SET lease=?,lease_until=?,last_started_at=?,updated_at=?
                WHERE account=? AND enabled=1 AND lease_until<? AND (next_poll_at<=? OR ?=1)''',
                (token,time.time()+LEASE_SECONDS,now(),now(),account,time.time(),time.time(),int(force)))
            if not changed.rowcount:
                return self.status()
            claim = dict(db.execute('SELECT * FROM assistent_einkaufsmonitor WHERE account=?', (account,)).fetchone())
        job = None
        try:
            deadline = time.monotonic()+25
            self._refresh_reviewed_sources(claim)
            for _ in range(max_steps):
                self._enqueue_saved(claim)
                job = self._next_source(claim)
                if job:
                    self._extract(claim,job)
                else:
                    with self.owned(claim) as db:
                        run = db.execute('SELECT state FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
                    if not run or run['state'] != 'active':
                        self.sources.start_incremental(guard=lambda db:self._guard(claim,db))
                    report = self.sources.step(guard=lambda db:self._guard(claim,db))
                    if report['state'] == 'paused':
                        raise ValueError('Postfachabruf unterbrochen; gespeicherter Checkpoint bleibt erhalten.')
                    self._enqueue_saved(claim)
                    if report['state'] == 'done' and not self._next_source(claim):
                        break
                if time.monotonic() >= deadline:
                    break
            with self.owned(claim) as db:
                run = db.execute('SELECT state FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
                pending = db.execute("SELECT 1 FROM assistent_einkaufsmonitor_quellen WHERE account=? AND state='queued' LIMIT 1", (account,)).fetchone()
                active = bool((run and run['state']=='active') or pending)
                retry = db.execute("SELECT MIN(next_retry_at) AS due FROM assistent_einkaufsmonitor_quellen WHERE account=? AND state='error'", (account,)).fetchone()
                delay = 1 if active else claim['interval_seconds']
                if retry and retry['due'] is not None:
                    delay = min(delay,max(1,retry['due']-time.time()))
                db.execute('''UPDATE assistent_einkaufsmonitor SET next_poll_at=?,last_finished_at=?,updated_at=?,
                    last_error='',failures=0 WHERE id=?''',
                    (time.time()+delay,now(),now(),claim['id']))
        except Exception:
            with self.db() as db:
                row = db.execute('SELECT failures FROM assistent_einkaufsmonitor WHERE id=? AND lease=? AND revision=?', (claim['id'],token,claim['revision'])).fetchone()
                if row:
                    delay = min(3600,30*(2**min(int(row['failures']),7)))
                    error = 'Abruf oder Auslese unterbrochen. Gespeicherte Originale und Checkpoints bleiben erhalten; erneuter Versuch ist vorgemerkt.'
                    db.execute('UPDATE assistent_einkaufsmonitor SET failures=failures+1,last_error=?,next_poll_at=?,updated_at=? WHERE id=? AND lease=?',
                               (error,time.time()+delay,now(),claim['id'],token))
                    if job:
                        db.execute("UPDATE assistent_einkaufsmonitor_quellen SET state='error',last_error=?,next_retry_at=?,updated_at=? WHERE id=?", (error,time.time()+delay,now(),job['id']))
        finally:
            with self.db() as db:
                db.execute("UPDATE assistent_einkaufsmonitor SET lease='',lease_until=0 WHERE id=? AND lease=?", (claim['id'],token))
        return self.status()


def start_purchase_monitor_worker(portal, *, interval=15):
    """Explicit optional web-process launcher. Durable leases span all processes."""
    if type(interval) is not int or not 5 <= interval <= 60:
        raise ValueError('Workerintervall zwischen fünf und sechzig Sekunden.')
    service = portal.workshop_purchase_monitor
    if portal.app.config.get('TESTING') or not service.worker_enabled():
        return False
    with service._worker_lock:
        if service._worker_pid == os.getpid() and service._worker and service._worker.is_alive():
            return False
        service._worker_stop = threading.Event()
        def work():
            while not service._worker_stop.is_set():
                try:
                    with portal.app.app_context():
                        if service.worker_enabled():
                            service.tick(worker=True)
                except Exception:
                    portal.app.logger.warning('Rechnungsmonitor: Konfiguration oder Verbindung prüfen.')
                service._worker_stop.wait(interval)
        service._worker_pid = os.getpid()
        service._worker = threading.Thread(target=work,name='werkstatt-rechnungsmonitor',daemon=True)
        service._worker.start()
        return True


def register_monitor(portal):
    service = PurchaseMonitor(portal)
    portal.workshop_purchase_monitor = service
    portal.workshop_purchase_monitor_init_schema = service.init_schema
    portal.app.extensions['werkstatt_purchase_monitor'] = service

    @portal.app.cli.command('werkstatt-einkaufsmonitor')
    @click.option('--once',is_flag=True,help='Ein begrenzter Lauf; nur bei aktivierter Kontokonfiguration.')
    @click.option('--status',is_flag=True,help='Nur den gespeicherten Monitorstatus anzeigen.')
    @click.option('--interval',default=15,type=click.IntRange(5,60))
    def monitor_command(once,status,interval):
        if status:
            click.echo(json.dumps(service.status(),ensure_ascii=False))
            return
        try:
            while True:
                result = service.tick(worker=True)
                if once:
                    click.echo(json.dumps(result,ensure_ascii=False))
                    return
                time.sleep(interval)
        except KeyboardInterrupt:
            return
        except Exception:
            raise click.ClickException('Rechnungsmonitor konnte nicht fortgesetzt werden; Konfiguration prüfen.') from None
    return service
