"""Read-only rim catalogue and signed selection; no vehicle or order claims."""
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, jsonify, make_response, render_template, request, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from felgen_katalog import FelgenCatalogProvider, FelgenEingabefehler, FelgenKatalogError
from felgen_bilder import FelgenBildStore, MAX_IMAGE_BYTES


def register_felgen_shop(portal):
    app = portal.app
    bp = Blueprint("felgen_shop", __name__)

    def source():
        value = app.config.get("FELGENANGEBOT_PROVIDER")
        if not isinstance(value, FelgenCatalogProvider):
            raise FelgenKatalogError("configuration")
        return value

    def serializer():
        return URLSafeTimedSerializer(app.secret_key, salt="felgen-auswahl-v1")

    def limit(scope):
        blocked, seconds = portal.public_form_rate_limit_status(scope,
            max_attempts=portal.REIFEN_SUCHE_RATE_LIMIT_MAX,
            window_seconds=portal.REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS)
        if not blocked:
            portal.record_public_form_attempt(scope, window_seconds=portal.REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS)
        return blocked, seconds

    @bp.after_request
    def private(response):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @bp.get("/felgen")
    def catalog():
        return render_template("homepage_felgen.html", shop_kategorie="felgen")

    @bp.get("/komplettraeder")
    def komplettraeder():
        return render_template("homepage_komplettraeder.html", shop_kategorie="komplettraeder")

    @bp.get("/api/felgenkatalog/auswahl")
    def facets():
        blocked, seconds = limit("felgen-facets")
        if blocked:
            response = jsonify(status="nicht_verfuegbar", code="rate_limit", hersteller=[], materialien=[])
            response.status_code = 429
            response.headers["Retry-After"] = str(seconds)
            return response
        if request.args:
            return jsonify(status="ungueltige_suche", code="eingabe_ungueltig", hersteller=[], materialien=[]), 400
        try:
            return jsonify(status="ok", **source().facets())
        except FelgenKatalogError as error:
            return jsonify(status="nicht_verfuegbar", code=error.code, hersteller=[], materialien=[]), 503

    @bp.get("/api/felgenangebote/suche")
    def search():
        blocked, seconds = limit("felgen-suche")
        if blocked:
            response = jsonify(status="nicht_verfuegbar", code="rate_limit", produkte=[], verfuegbar=False)
            response.status_code = 429
            response.headers["Retry-After"] = str(seconds)
            return response
        try:
            payload = source().search(request.args)
            for product in payload["produkte"]:
                token = serializer().dumps({"artikel_id": product["artikel_id"], "anzahl": product["anzahl"]})
                product["auswahl_url"] = url_for("felgen_shop.auswahl", token=token)
            return jsonify(payload)
        except FelgenEingabefehler as error:
            return jsonify(status="ungueltige_suche", code=error.code, feld=error.feld, produkte=[], verfuegbar=False), 400
        except FelgenKatalogError as error:
            return jsonify(status="nicht_verfuegbar", code=error.code, produkte=[], verfuegbar=False), 503

    @bp.get("/api/felgenkatalog/bild/<artikel_id>/<digest>.webp")
    def bild(artikel_id, digest):
        if request.args:
            abort(400)
        images = app.config.get("FELGEN_BILD_STORE")
        value = images.read(artikel_id, digest) if isinstance(images, FelgenBildStore) else None
        if not isinstance(value, bytes) or not 1 <= len(value) <= MAX_IMAGE_BYTES:
            abort(404)
        response = make_response(value)
        response.headers["Content-Type"] = "image/webp"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @bp.get("/felgen/auswahl/<token>")
    def auswahl(token):
        blocked, seconds = limit("felgen-auswahl")
        if blocked:
            response = make_response("Bitte warten Sie kurz vor der nächsten Auswahlprüfung.", 429)
            response.headers["Retry-After"] = str(seconds)
            return response
        if request.args:
            abort(400, description="Für diese Felgenauswahl sind keine zusätzlichen Angaben erlaubt.")
        try:
            if len(token) > 2000:
                raise ValueError()
            selected = serializer().loads(token, max_age=900)
            if (not isinstance(selected, dict) or set(selected) != {"artikel_id", "anzahl"}
                    or type(selected["anzahl"]) is not int or not 1 <= selected["anzahl"] <= 8):
                raise ValueError()
            product = source().lookup(selected["artikel_id"], selected["anzahl"])
            if product is None:
                raise ValueError()
        except (BadSignature, SignatureExpired, ValueError, TypeError, FelgenKatalogError):
            abort(409, description="Die Felgenauswahl ist abgelaufen oder nicht mehr verfügbar. Bitte suchen Sie erneut.")
        berlin = ZoneInfo("Europe/Berlin")
        return render_template("felgen_auswahl.html", produkt=product,
            zurueck_url=url_for("felgen_shop.catalog", suche=product["artikel_id"], anzahl=product["anzahl"], anzeigen="1"),
            pruefen_url=url_for("felgen_shop.auswahl", token=token),
            preisstand_text=datetime.fromisoformat(product["preisstand"]).astimezone(berlin).strftime("%d.%m.%Y %H:%M"),
            gueltig_bis_text=datetime.fromisoformat(product["gueltig_bis"]).astimezone(berlin).strftime("%d.%m.%Y %H:%M"))

    app.register_blueprint(bp)
    return bp
