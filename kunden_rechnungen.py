"""Read-only invoice views over explicitly released workshop originals.

Register once with ``register_customer_invoices(portal_module)``. No upload,
OCR, publication, notification, invoice creation or customer-name matching.
"""
from datetime import datetime
from pathlib import PurePath

from flask import Blueprint, abort, make_response, render_template, request, url_for


_FIELDS = """d.id, d.auftrag_id, d.original_name, d.stored_name, d.mime_type,
    d.size, d.quelle, d.kategorie, d.sichtbarkeit_geprueft, d.kunde_sichtbar,
    d.partner_sichtbar, d.versicherung_sichtbar, d.hochgeladen_am,
    a.fahrzeug, a.kennzeichen, a.archiviert"""


def register_customer_invoices(p):
    """Keep the existing partner and active-order-token security boundaries."""
    bp = Blueprint("kundenrechnungen", __name__)
    # Template changes remain safe while another task coordinates app.py wiring.
    p.app.jinja_env.globals["kunden_rechnungen_enabled"] = True

    def eligible(document, audience):
        # A customer's/supplier's own upload is not a workshop-issued invoice.
        # A legacy visibility flag alone is not an explicit invoice release.
        return (
            document.get("kategorie") == "rechnung"
            and document.get("quelle") in {"intern", "werkstatt", "lexware"}
            and bool(document.get("sichtbarkeit_geprueft"))
            and PurePath(document.get("original_name") or "").suffix.lower() == ".pdf"
            and (document.get("mime_type") or "").split(";", 1)[0].strip().lower()
            in {"application/pdf", "application/octet-stream", ""}
            and p.document_visible(document, audience)
        )

    def documents(audience, *, partner_id=None, token=None, document_id=None):
        # No get_auftrag()/hydrate_datei(): these can analyse uploads or load
        # originals. One narrowly scoped metadata query is sufficient here.
        clauses = ["d.kategorie='rechnung'", "d.sichtbarkeit_geprueft=1"]
        params = []
        if audience == "partner" and partner_id is not None:
            clauses += ["a.autohaus_id=?", "d.partner_sichtbar=1"]
            params.append(partner_id)
        elif audience == "kunde" and token:
            # Resolve the same single order as the existing status portal.
            # Even malformed legacy data with a duplicate token cannot turn
            # this into a multi-order/customer invoice search.
            order = p.get_auftrag_by_kunden_status_token(token)
            if not order:
                return []
            clauses += ["a.id=?", "a.kunden_status_token=?", "COALESCE(a.kunden_status_aktiv,1)=1", "d.kunde_sichtbar=1"]
            params += [order["id"], token]
        else:
            return []
        if document_id is not None:
            clauses.append("d.id=?")
            params.append(document_id)
        db = p.get_db()
        try:
            rows = db.execute(
                f"SELECT {_FIELDS} FROM dateien d JOIN auftraege a ON a.id=d.auftrag_id "
                "WHERE " + " AND ".join(clauses) + " ORDER BY d.id DESC",
                tuple(params),
            ).fetchall()
        finally:
            db.close()
        return [item for row in rows if eligible(item := dict(row), audience)]

    def card(document, **route):
        endpoint = "partner_pdf" if "slug" in route else "status_pdf"
        date_label = str(document.get("hochgeladen_am") or "").strip()
        try:
            date_label = datetime.fromisoformat(date_label).strftime("%d.%m.%Y")
        except ValueError:
            legacy_date = p.parse_date(date_label.split(" ", 1)[0])
            if legacy_date:
                date_label = legacy_date.strftime("%d.%m.%Y")
        return {
            "auftrag_id": document["auftrag_id"],
            "fahrzeug": document["fahrzeug"],
            "kennzeichen": document["kennzeichen"],
            "archiviert": bool(document["archiviert"]),
            "name": document["original_name"],
            "datum": date_label,
            "open_url": url_for("kundenrechnungen." + endpoint, document_id=document["id"], **route),
            "download_url": url_for("kundenrechnungen." + endpoint + "_download", document_id=document["id"], **route),
        }

    def private(response):
        response = make_response(response)
        response.headers["Cache-Control"] = "private, no-store, max-age=0"
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    def original(found, *, attachment):
        if not found:
            abort(404)
        document = found[0]
        # The generic upload helper downloads PDFs. This narrowly scoped view
        # may display a released invoice, but never treat a renamed HTML upload
        # as a document and never loosen the global inline-upload policy.
        path = p.upload_file_path(document)
        try:
            if path and path.is_file():
                with path.open('rb') as source:
                    header = source.read(1024)
            else:
                header = (p.load_datei_backup_bytes(document) or b'')[:1024]
        except OSError:
            abort(404)
        if b'%PDF-' not in header:
            abort(404)
        response = private(p.send_upload_file(document, as_attachment=attachment))
        if not attachment and response.status_code in {200, 206}:
            response.headers['Content-Disposition'] = response.headers['Content-Disposition'].replace('attachment;', 'inline;', 1)
            response.headers['Content-Security-Policy'] = "sandbox; default-src 'none'; frame-ancestors 'self'"
        return response

    @bp.get("/partner/<slug>/rechnungen")
    def partner_index(slug):
        partner, redirect_response = p.partner_session_required(slug)
        if redirect_response:
            return private(redirect_response)
        items = documents("partner", partner_id=partner["id"])
        try:
            page = max(1, int(request.args.get("seite", "1")))
        except ValueError:
            page = 1
        pages = max(1, (len(items) + 49) // 50)
        page = min(page, pages)
        return private(render_template(
            "partner_rechnungen.html", autohaus=partner,
            kunden_rechnungen=[card(d, slug=slug) for d in items[(page-1)*50:page*50]],
            rechnungen_gesamt=len(items), rechnungen_seite=page, rechnungen_seiten=pages,
        ))

    def partner_original(slug, document_id, attachment):
        partner, redirect_response = p.partner_session_required(slug)
        if redirect_response:
            return private(redirect_response)
        return original(documents("partner", partner_id=partner["id"], document_id=document_id), attachment=attachment)

    @bp.get("/partner/<slug>/rechnungen/<int:document_id>")
    def partner_pdf(slug, document_id):
        return partner_original(slug, document_id, False)

    @bp.get("/partner/<slug>/rechnungen/<int:document_id>/download")
    def partner_pdf_download(slug, document_id):
        return partner_original(slug, document_id, True)

    # Deliberately under /status/: the existing service-token guard still runs
    # first, including revoked assignments and driver finance restrictions.
    @bp.get("/status/<token>/rechnungen/<int:document_id>")
    def status_pdf(token, document_id):
        return original(documents("kunde", token=token, document_id=document_id), attachment=False)

    @bp.get("/status/<token>/rechnungen/<int:document_id>/download")
    def status_pdf_download(token, document_id):
        return original(documents("kunde", token=token, document_id=document_id), attachment=True)

    @p.app.context_processor
    def invoice_context():
        if request.endpoint != "kunden_status":
            return {}
        token = (request.view_args or {}).get("token")
        return {"kunden_rechnungen": [card(d, token=token) for d in documents("kunde", token=token)]}

    p.app.register_blueprint(bp)
    return bp
