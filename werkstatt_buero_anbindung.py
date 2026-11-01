"""Explicit CEO inbox bindings and chef-only manual reconciliation page."""
from contextlib import closing, contextmanager
import hashlib
import hmac
import io
import json
from itertools import islice
from pathlib import Path
import re
import secrets
import tempfile
import time
from uuid import UUID

from email import policy
from email.parser import BytesParser
from flask import Blueprint, abort, has_request_context, render_template, request, session
from werkzeug.datastructures import FileStorage

from mailbox_client import Mailbox
from werkstatt_buero_analyse import analyse_document
from werkstatt_buero_posteingang import (InboxBlocked, OfficeInbox, ReadonlyIMAP, PortalInventory,
                                       LexwarePurchaseIndex, MAX_ATTACHMENT, document_identity,
                                       confirmed_ceo_mailbox_user)
from werkstatt_mitarbeiter_portal import _document


class InboxDeadline(InboxBlocked):
    pass


class RunBudget:
    """Cooperative deadline: allow only the currently running bounded I/O."""
    def __init__(self, clock=time.monotonic, seconds=60):
        self.clock, self.deadline = clock, clock()+seconds

    def check(self):
        if self.clock() >= self.deadline:
            raise InboxDeadline("Zeitbudget von 60 Sekunden erreicht. Der Abgleich wurde gestoppt; bereits gespeicherte Prüfbelege und Abgleichsnachweise bleiben erhalten. Zeitraum verkleinern und offene Ergebnisse prüfen.")

    def timeout(self, maximum=30):
        self.check()
        return max(0.001, min(maximum, self.deadline-self.clock()))

    def iterate(self, values):
        iterator=iter(values)
        try:
            while True:
                self.check()
                try:value=next(iterator)
                except StopIteration:return
                self.check()
                yield value
        finally:
            close=getattr(iterator,"close",None)
            if callable(close):close()


class BudgetedIMAPConnection:
    def __init__(self,connection,budget):self.connection,self.budget=connection,budget
    def _call(self,name,*args,**kwargs):
        self.budget.check()
        result=getattr(self.connection,name)(*args,**kwargs)
        self.budget.check()
        return result
    def select(self,*args,**kwargs):return self._call("select",*args,**kwargs)
    def response(self,*args,**kwargs):return self._call("response",*args,**kwargs)
    def uid(self,*args,**kwargs):return self._call("uid",*args,**kwargs)


class LegacyDocumentIndex:
    """Hash all bounded purchase originals, order originals and orphan uploads.

    Missing tables/files, unsafe paths and limits are explicit gaps, never an
    empty successful inventory. No file is restored or changed during a scan.
    """
    def __init__(self, p, max_files=3000, max_bytes=256*1024*1024, checkpoint=None):
        self.p, self.max_files, self.max_bytes = p, max_files, max_bytes
        self.records, self.issues, self.complete = [], [], True
        self.ready, self.bytes = False, 0
        self.checkpoint=checkpoint or (lambda:None)

    def gap(self, text):
        self.complete = False
        if text not in self.issues:
            self.issues.append(text)

    def _read(self, name):
        self.checkpoint()
        if (not isinstance(name, str) or not name or name in {".", ".."}
                or re.search(r"[\\/:\x00]", name)):
            self.gap("Ein Altbeleg enthält keinen sicheren Dateinamen."); return None
        path = self.root/name
        if path.is_symlink() or getattr(path, "is_junction", lambda:False)() or path.resolve().parent != self.root:
            self.gap("Ein Altbeleg verweist außerhalb des freigegebenen Belegordners."); return None
        if not path.is_file():
            self.gap("Ein referenziertes Altoriginal fehlt im konfigurierten Belegordner."); return None
        size = path.stat().st_size
        if not 0 < size <= MAX_ATTACHMENT or self.bytes+size > self.max_bytes:
            self.gap("Altoriginal oder Gesamtbestand überschreitet das begrenzte Hash-Prüflimit."); return None
        with path.open('rb') as source:
            raw = source.read(MAX_ATTACHMENT + 1)
        self.checkpoint()
        if len(raw) != size:
            self.gap("Ein Altoriginal wurde während der Prüfung verändert."); return None
        self.bytes += size
        return raw

    def _append(self, ident, raw):
        self.checkpoint()
        if raw is None: return
        if not (raw.startswith(b"%PDF-") or raw.startswith(b"\xff\xd8\xff") or raw.startswith(b"\x89PNG\r\n\x1a\n")):
            self.gap("Ein Altoriginal hat ein nicht unterstütztes oder unklar benanntes Dokumentformat.")
        self.records.append({"id":ident,"sha256":hashlib.sha256(raw).hexdigest(),"identity_verified":False})

    def scan(self):
        self.checkpoint()
        if self.ready: return
        self.ready = True
        value = getattr(self.p,"UPLOAD_DIR",None)
        if value is None:
            self.gap("UPLOAD_DIR ist nicht konfiguriert; alte Scans können nicht vollständig geprüft werden."); return
        configured = Path(value)
        if (not configured.is_absolute() or not configured.is_dir() or configured.is_symlink()
                or getattr(configured,"is_junction",lambda:False)()):
            self.gap("Der konfigurierte Belegordner fehlt oder ist keine sichere direkte Quelle."); return
        self.root = configured.resolve()
        referenced = set()
        with closing(self.p.get_db()) as db:
            required = {"einkauf_belege":{"id","stored_name"},"dateien":{"id","stored_name"},
                        "datei_backups":{"datei_id","file_base64","file_sha256","size"}}
            present = {}
            for table, columns in required.items():
                self.checkpoint()
                try:
                    found = set(self.p.get_table_columns(db,table))
                except Exception:
                    found = set()
                present[table] = columns <= found
                if not present[table]: self.gap("Altbestandsquelle "+table+" fehlt oder hat nicht die erforderlichen Spalten.")
            if present["einkauf_belege"]:
                self.checkpoint()
                rows = db.execute("SELECT id,stored_name FROM einkauf_belege ORDER BY id LIMIT ?",(self.max_files+1,)).fetchall()
                if len(rows)>self.max_files:self.gap("Zu viele ältere Einkaufsbelege für einen vollständigen begrenzten Lauf.")
                for row in rows[:self.max_files]:
                    self.checkpoint()
                    referenced.add(row["stored_name"])
                    self._append("einkauf:"+str(row["id"]),self._read(row["stored_name"]))
            if present["dateien"]:
                self.checkpoint()
                rows = db.execute("SELECT id,stored_name FROM dateien ORDER BY id LIMIT ?",(self.max_files+1,)).fetchall()
                if len(rows)>self.max_files:self.gap("Zu viele Dateioriginale für einen vollständigen begrenzten Lauf.")
                for row in rows[:self.max_files]:
                    self.checkpoint()
                    referenced.add(row["stored_name"])
                    backup = db.execute("SELECT size,file_sha256,LENGTH(file_base64) AS encoded_size FROM datei_backups WHERE datei_id=?",(row["id"],)).fetchone() if present["datei_backups"] else None
                    raw = None
                    try:
                        valid_backup = (backup is not None and 0 < int(backup["size"] or 0) <= MAX_ATTACHMENT
                                        and 0 < int(backup["encoded_size"] or 0) <= MAX_ATTACHMENT*4//3+8)
                    except (TypeError, ValueError):
                        valid_backup = False
                    if backup and not valid_backup:
                        self.gap("Ein Datenbankoriginal ist beschädigt oder überschreitet das Prüflimit.")
                    if valid_backup:
                        if self.bytes+int(backup["size"])>self.max_bytes:
                            self.gap("Die Datenbankoriginale überschreiten das Gesamt-Prüflimit.")
                        else:
                            self.checkpoint()
                            try:raw=self.p.load_datei_backup_bytes(dict(row))
                            except Exception:raw=None
                            self.checkpoint()
                            if (isinstance(raw, bytes) and len(raw) == int(backup["size"])
                                    and hashlib.sha256(raw).hexdigest()==backup["file_sha256"]):
                                self.bytes+=len(raw)
                            else:
                                raw=None;self.gap("Ein Datenbankoriginal fehlt oder seine Integritätsprüfung schlägt fehl.")
                    if raw is None:raw=self._read(row["stored_name"])
                    self._append("datei:"+str(row["id"]),raw)
        children = []
        for child in islice(self.root.iterdir(), self.max_files+1):
            self.checkpoint();children.append(child)
        if len(children)>self.max_files:self.gap("Der Belegordner überschreitet das Dateilimit.")
        for path in children[:self.max_files]:
            self.checkpoint()
            if path.name in referenced:continue
            if not path.is_file() or path.is_symlink():
                self.gap("Ein Unterordner oder verknüpftes Original ist nicht vollständig im Altbestandsindex enthalten.");continue
            self._append("upload:"+path.name,self._read(path.name))

    def find(self,digest,meta):
        self.scan()
        return {"matches":[row for row in self.records if row["sha256"]==digest],"complete":self.complete}


class ReportingMailbox:
    def __init__(self,transport,issues,budget):
        self.transport,self.issues,self.budget=transport,issues,budget
        self.active=None
    def close(self):
        if self.active is not None:self.active.close()
    def messages(self,*args):
        self.active=self.budget.iterate(self.transport.messages(*args))
        for message in self.active:
            parsed=BytesParser(policy=policy.default).parsebytes(message["raw"])
            for part in parsed.walk():
                self.budget.check()
                name=part.get_filename()
                if name and not part.is_multipart():
                    suffix=Path(str(name)).suffix.lower()
                    expected={".pdf":"application/pdf", ".jpg":"image/jpeg", ".jpeg":"image/jpeg", ".png":"image/png"}.get(suffix)
                    if (not expected or part.get_content_disposition()=="inline"
                            or part.get_content_type() not in {expected,"application/octet-stream"}):
                        self.issues.append("Ein Mailanhang wurde nicht automatisch übernommen: Format oder Inline-Anhang benötigt manuelle Prüfung.")
                elif part.get_content_disposition()=="attachment" and not part.is_multipart():
                    self.issues.append("Ein Mailanhang ohne Dateinamen benötigt manuelle Prüfung.")
            yield message


class EvidencePortalInventory(PortalInventory):
    """Binary completeness alone cannot disprove a differently scanned invoice."""
    def __init__(self,p,legacy):
        super().__init__(p,legacy_find=legacy.find)
        self.legacy=legacy

    def find_matches(self,digest,meta):
        self.legacy.checkpoint()
        result=super().find_matches(digest,meta)
        self.legacy.checkpoint()
        if not result["matches"] and document_identity(meta) is None:
            with self.p.office_documents.db() as db:
                portal_exists=db.execute("SELECT id FROM buero_belege LIMIT 1").fetchone() is not None
            if self.legacy.records or portal_exists:
                self.legacy.gap("Alt- oder Portalbelege sind vorhanden, aber eine verifizierte Belegidentität fehlt. Ein anderer Scan derselben Rechnung kann über Dateihashes nicht ausgeschlossen werden; vor Neuanlage manuell prüfen.")
                result["complete"]=False
        return result

    def create_review(self,*args,**kwargs):
        # Once this bounded create starts, its receipt/audit may finish. The
        # next attachment, lookup or create is blocked after the deadline.
        self.legacy.checkpoint()
        return super().create_review(*args,**kwargs)


class OfficeInboxBindings:
    def __init__(self,p,authorize=None,clock=time.monotonic):
        self.p=p
        self.authorize=authorize or (lambda:has_request_context() and bool(session.get("admin")))
        self.pipeline=OfficeInbox(p,authorize=self.authorize)
        self.clock=clock

    def address(self):
        return self.p.get_app_setting("OFFICE_CEO_MAIL_ADDRESS","") or self.p.app.config.get("OFFICE_CEO_MAIL_ADDRESS","")

    def readiness(self):
        problems=[]
        address=self.address()
        mailbox_user=""
        try:
            mailbox_user=confirmed_ceo_mailbox_user(self.p,address)
            self.pipeline._account(address)
        except (InboxBlocked,PermissionError) as exc:problems.append(str(exc))
        config=self.p.get_werkstatt_imap_config()
        if not config.get("configured") or not config.get("ssl") or not config.get("password") or not config.get("host"):
            problems.append("Der vorhandene verschlüsselte IMAP-Zugang ist nicht vollständig eingerichtet.")
        if str(config.get("folder","INBOX")).upper()!="INBOX":
            problems.append("Der konfigurierte Mailordner ist nicht INBOX; die Chef-Posteingangsquelle muss ausdrücklich geklärt werden.")
        if getattr(self.p,"LEXWARE_API_BASE_URL","").rstrip("/")!="https://api.lexware.io" or not getattr(self.p,"LEXWARE_API_KEY",""):
            problems.append("Der vorhandene offizielle Lexware-GET-Zugang ist nicht vollständig eingerichtet.")
        if not getattr(self.p,"office_documents",None):problems.append("Der geschützte Portal-Belegworkflow ist nicht eingerichtet.")
        return {"address":address,"mailbox_user":mailbox_user,
                "shared_mailbox":bool(mailbox_user and mailbox_user.casefold()!=str(address).strip().casefold()),
                "ready":not problems,"problems":problems}

    def _read_official(self,path,*,max_bytes,accept,budget=None):
        if budget:budget.check()
        if self.p.LEXWARE_API_BASE_URL.rstrip("/")!="https://api.lexware.io":raise InboxBlocked("Offizieller Lexware-Endpunkt erforderlich.")
        http=self.p.get_requests()
        if http is None:raise InboxBlocked("Vorhandener HTTP-Client fehlt.")
        response=http.request("GET","https://api.lexware.io"+path,
            headers={"Authorization":"Bearer "+self.p.LEXWARE_API_KEY,"Accept":accept},
            stream=True,timeout=budget.timeout() if budget else 30,allow_redirects=False)
        try:
            if budget:budget.check()
            if response.status_code!=200:raise InboxBlocked("Ein Lexware-Original konnte nicht begrenzt gelesen werden.")
            length=response.headers.get("Content-Length")
            if length and int(length)>max_bytes:raise InboxBlocked("Lexware-Original überschreitet das Prüflimit.")
            content=bytearray()
            chunks=response.iter_content(65536)
            for chunk in budget.iterate(chunks) if budget else chunks:
                content.extend(chunk)
                if len(content)>max_bytes:raise InboxBlocked("Lexware-Original überschreitet das Prüflimit.")
            return bytes(content)
        finally:response.close()

    def _download(self,file_id,*,max_bytes,budget=None):
        key=str(UUID(str(file_id)))
        return self._read_official("/v1/files/"+key,max_bytes=max_bytes,
                                   accept="application/octet-stream",budget=budget)

    def _lexware_get(self,method,path,query="",*,budget):
        budget.check()
        if method!="GET" or not (path=="/v1/voucherlist" or re.fullmatch(r"/v1/vouchers/[a-f0-9-]{36}",path)):
            raise InboxBlocked("Für den Abgleich ist nur der begrenzte offizielle Beleg-GET erlaubt.")
        raw=self._read_official(path+query,max_bytes=4*1024*1024,accept="application/json",budget=budget)
        budget.check()
        try:return json.loads(raw)
        except (ValueError,UnicodeError):raise InboxBlocked("Lexware-Bestandsantwort ist nicht lesbar.") from None

    def _metadata(self,name,mime,raw,*,budget=None):
        # Same source-bound proposals as uploads. Neither local nor optional
        # host analysis establishes a verified matching key or payment release.
        raw, _, validated_mime = _document(
            FileStorage(stream=io.BytesIO(raw), filename=name, content_type=mime), label="Mailbeleg")
        if validated_mime != mime:
            raise InboxBlocked("Mailbeleg hat keinen eindeutig bestätigten Dateityp.")
        parent_value = getattr(self.p,"DATA_DIR",None)
        if parent_value is None:
            upload = getattr(self.p,"UPLOAD_DIR",None)
            parent_value = Path(upload).parent if upload is not None else None
        if parent_value is None:
            raise InboxBlocked("Geschützter lokaler OCR-Arbeitsordner fehlt.")
        parent=Path(parent_value)
        if not parent.is_absolute() or not parent.is_dir() or parent.is_symlink():
            raise InboxBlocked("Geschützter lokaler OCR-Arbeitsordner fehlt.")
        extractor=getattr(self.p,"extract_document_text_local",None)
        if not callable(extractor):raise InboxBlocked("Lokale Texterkennung ist nicht eingerichtet; keine sichere Klassifizierung möglich.")
        with tempfile.TemporaryDirectory(prefix="office-mail-scan-",dir=parent) as folder:
            suffix={"application/pdf":".pdf","image/jpeg":".jpg","image/png":".png"}[mime]
            path=Path(folder)/("scan"+suffix);path.write_bytes(raw)
            text=extractor(path,path.name)
            if budget:budget.check()
            text=str(text or "")[:100000]
            app=getattr(self.p,"app",None)
            suggestion=analyse_document(text,ai_hook=getattr(self.p,"office_extract_fields_ai",None),
                ai_enabled=app is not None and app.config.get("OFFICE_AI_ANALYSIS_ENABLED") is True,
                path=path,filename=path.name)
            if budget:budget.check()
        accepted_evidence="\n".join(item.get("evidence","") for name,item in suggestion["field_sources"].items()
                                    if name in suggestion["fields"])
        private_hint = bool(re.search(
            r"einkommensteuer|kirchensteuer|solidarit[äa]tszuschlag|privatrechnung|privateinkauf|nur\s+privat|privater\s+(?:beleg|einkauf)",
            (text+"\n"+accepted_evidence).casefold()))
        return {"identity_verified":False,"bereich":"unklar","bereich_verified":False,
                "private_review_only":private_hint,"scan_suggestion":suggestion}

    def run(self,*,lookback_days=14):
        if not self.authorize():raise PermissionError("Ein ausdrücklicher Chefauftrag ist erforderlich.")
        budget=RunBudget(self.clock)
        self.last_source_issues=[]
        ready=self.readiness()
        if not ready["ready"]:raise InboxBlocked(" ".join(ready["problems"]))
        config=dict(self.p.get_werkstatt_imap_config())
        config["timeout"]=min(float(config.get("timeout",20)),budget.timeout(20))
        issues=[];legacy=LegacyDocumentIndex(self.p,checkpoint=budget.check);suggestions={}
        # Existing Mailbox.connect uses authenticated IMAP-over-TLS with
        # create_default_context; no credential retrieval beyond existing config.
        factory=getattr(self.p,"office_inbox_connection_factory",None) or (lambda cfg:Mailbox(lambda:cfg).connect())
        @contextmanager
        def bounded_factory(cfg):
            budget.check()
            with factory(cfg) as connection:
                budget.check()
                yield BudgetedIMAPConnection(connection,budget)
        transport=ReportingMailbox(ReadonlyIMAP(bounded_factory,config),issues,budget)
        portal=EvidencePortalInventory(self.p,legacy)
        lexware=LexwarePurchaseIndex(lambda *a,**k:self._lexware_get(*a,**k,budget=budget),
                                    download=lambda *a,**k:self._download(*a,**k,budget=budget))
        def metadata(name,mime,raw):
            budget.check()
            value=self._metadata(name,mime,raw,budget=budget)
            budget.check()
            suggestions[hashlib.sha256(raw).hexdigest()]=value["scan_suggestion"]
            return value
        try:
            report=self.pipeline.run(ready["address"],mailbox=transport,portal=portal,lexware=lexware,
                                     metadata=metadata,lookback_days=lookback_days)
            for row in report["results"]:
                budget.check()
                if row["status"] not in {"PORTAL_NEU_CHEFPRUEFUNG","LEXWARE_BESTEHT_NUR_ZUORDNUNG_PRUEFEN","PRIVAT_ZUR_PRUEFUNG"}:
                    continue
                suggestion=suggestions.get(row["sha256"])
                if suggestion:
                    try:
                        detail,_=self.p.office_documents.intake_snapshot(row["portal_key"])
                        budget.check()
                        revision=self.p.office_documents.record_extracted_fields(row["portal_key"],str(detail["revision"]),
                            suggestion["fields"],suggestion["hint"],suggestion["source"],intake=True)
                        budget.check()
                        self.p.office_documents.record_scan_suggestion(row["portal_key"],str(revision),
                            suggestion["category"],suggestion["hint"],suggestion["suggested_kind"],intake=True)
                        budget.check()
                    except (ValueError,LookupError,PermissionError):
                        issues.append("Ein Analyse-Vorschlag konnte wegen eines geänderten Belegstands nicht gespeichert werden. Bestehendes Original manuell prüfen.")
        except InboxDeadline as exc:
            issues.append(str(exc));raise
        finally:
            transport.close()
            self.last_source_issues=issues+legacy.issues
        report["source_issues"]=list(dict.fromkeys(self.last_source_issues))
        if any("UNVOLLSTAENDIG" in row["status"] for row in report["results"]):
            report["source_issues"].append("Mindestens ein Altbestands- oder Lexware-Abgleich ist unvollständig. Ein begrenzter Teilindex wird nicht als vollständige Quelle behandelt.")
        self.last_source_issues=report["source_issues"]
        report["legacy_complete"]=legacy.complete if legacy.ready else None
        report["legacy_coverage"]="Dateihashes älterer Einkaufsbelege, geschützter Dateioriginale und nicht zugeordneter Uploads; keine erfundenen Altmetadaten."
        return report


def register_office_inbox_bindings(p,*,authorize=None):
    service=OfficeInboxBindings(p,authorize=authorize);p.office_inbox_bindings=service
    bp=Blueprint("office_inbox_bindings",__name__)
    @bp.route("/admin/buero/posteingang-abgleich",methods=["GET","POST"])
    def admin():
        if not service.authorize():abort(403)
        session.setdefault("csrf_token",secrets.token_urlsafe(32))
        report,error=None,""
        if request.method=="POST":
            tokens=request.form.getlist("csrf_token");expected=session.get("csrf_token")
            if (not tokens or not isinstance(expected,str)
                    or any(not hmac.compare_digest(expected,str(token)) for token in tokens)):abort(400)
            if request.form.getlist("confirmed")!=["1"] or request.form.getlist("action")!=["run"]:
                abort(400)
            try:
                days=request.form.getlist("lookback_days")
                if len(days)!=1 or not re.fullmatch(r"[1-9][0-9]?",days[0]) or int(days[0])>90:raise ValueError()
                report=service.run(lookback_days=int(days[0]))
            except InboxBlocked as exc:error=str(exc)
            except ValueError:error="Zeitraum oder Mailanhang ungültig. Zeitraum von 1 bis 90 Tagen wählen und Originaldateien prüfen."
            except Exception:error="Eine konfigurierte Quelle ist nicht verfügbar. Der Lauf wurde gestoppt; vorhandene Abgleichsnachweise und Teilprüfbelege bleiben erhalten. Keine Zugangsdaten werden angezeigt."
        return render_template("buero_posteingang.html",readiness=service.readiness(),report=report,error=error,
                               source_issues=getattr(service,"last_source_issues",[]),csrf_token=session["csrf_token"]),400 if error else 200
    @bp.after_request
    def private(response):
        response.headers.update({"Cache-Control":"private, no-store","Referrer-Policy":"no-referrer","X-Content-Type-Options":"nosniff","X-Robots-Tag":"noindex, nofollow,noarchive"});return response
    p.app.register_blueprint(bp);return service
