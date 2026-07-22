import os

from flask import Flask, jsonify, redirect, send_from_directory

app = Flask(__name__, static_folder="static")
RENTAL_DIR = os.path.join(app.static_folder, "mietwagen_vorschau")
PORTAL_BASE_URL = (os.environ.get("PORTAL_BASE_URL") or "https://kundenstatus-app.onrender.com").rstrip("/")

@app.get("/")
def homepage():
    return send_from_directory(RENTAL_DIR, "index.html")

@app.get("/mietwagen-vorschau/")
def homepage_legacy():
    return redirect("/", code=301)

@app.get("/mietwagen-vorschau/<path:filename>")
def rental_asset(filename):
    return send_from_directory(RENTAL_DIR, filename)

@app.get("/impressum")
def impressum():
    return redirect(f"{PORTAL_BASE_URL}/impressum", code=302)

@app.get("/datenschutz")
def datenschutz():
    return redirect(f"{PORTAL_BASE_URL}/datenschutz", code=302)

@app.get("/robots.txt")
def robots():
    return "User-agent: *\nAllow: /\nSitemap: https://www.autovermietung-mos.de/sitemap.xml\n", 200, {"Content-Type": "text/plain; charset=utf-8"}

@app.get("/sitemap.xml")
def sitemap():
    body = '<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://www.autovermietung-mos.de/</loc></url></urlset>'
    return body, 200, {"Content-Type": "application/xml; charset=utf-8"}

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "service": "autovermietung-mos"})