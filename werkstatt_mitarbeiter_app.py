"""Public install instructions for the personal employee web app.

The install page is generic. Authentication and all business operations stay
with the existing portal; the worker does not cache or intercept requests.
"""
from flask import Blueprint, render_template, send_from_directory


def register_employee_app(p):
    bp = Blueprint('employee_app', __name__)

    @bp.get('/werkstatt/app')
    def install():
        return render_template('mitarbeiter_app.html')

    @bp.get('/werkstatt/app-sw.js')
    def worker():
        return send_from_directory(
            p.app.static_folder, 'mitarbeiter-app-sw.js',
            mimetype='application/javascript', max_age=0,
        )

    @bp.after_request
    def private_install_headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        return response

    p.app.register_blueprint(bp)
    return bp
