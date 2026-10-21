"""Authenticated service views over existing fleet, leads and workshop records.

No installation, messaging provider or payment processor is introduced here.
The signed form context associates requests only after a fresh role/vehicle check.
"""
from __future__ import annotations

from io import BytesIO
import hmac
import secrets
from uuid import uuid4

from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


class ServiceApp:
    def __init__(self, p, fleet):
        self.p, self.fleet = p, fleet
        self.signer = URLSafeTimedSerializer(p.app.secret_key, salt='service-vehicle-request-v1')

    def principal(self):
        if self.p.app.config.get('SERVICE_ACCESS_ENABLED'):
            driver = self.p.service_access.identity()
            if driver:
                return {**driver, 'role': 'fahrer', 'company_id': ''}
        if self.p.app.config.get('FLOTTENPORTAL_ENABLED'):
            manager = self.p.flottenportal.identity()
            if manager:
                return {**manager, 'role': 'fuhrparkleitung', 'company_id': manager['firma_id'],
                        'name': manager['benutzername']}
        return None

    def vehicles(self, actor):
        if actor['role'] == 'fahrer':
            return self.p.service_access.vehicles(actor)
        company, vehicles = self.fleet.company(actor['company_id'])
        return [{k: car[k] for k in ('id', 'firma_id', 'kennzeichen', 'modell', 'km', 'km_datum')} |
                {'firma_name': company['name']} for car in vehicles]

    def vehicle(self, actor, vehicle_id):
        for vehicle in self.vehicles(actor):
            if vehicle['id'] == vehicle_id:
                return vehicle
        raise LookupError('Fahrzeugzugriff nicht verfügbar.')

    def authorize_again(self, actor, vehicle_id):
        current = self.principal()
        if not current or any(current.get(key) != actor.get(key) for key in ('id', 'role', 'version')):
            raise PermissionError('Der Zugang hat sich geändert. Bitte erneut anmelden.')
        return self.vehicle(current, vehicle_id)

    @staticmethod
    def scoped_actor(actor, vehicle):
        return {**actor, 'company_id': vehicle['firma_id'], 'vehicle_ids': [vehicle['id']]}

    def form_context(self, actor, vehicle):
        return self.signer.dumps({'actor': actor['id'], 'role': actor['role'],
                                  'version': actor['version'], 'vehicle': vehicle['id'],
                                  'company': vehicle['firma_id']})

    def validate_submission(self, token):
        if not token:
            return None
        if not self.p.app.config.get('SERVICE_APP_ENABLED'):
            raise PermissionError('Der Servicezugang ist nicht verfügbar.')
        try:
            context = self.signer.loads(token, max_age=3600)
        except (BadSignature, SignatureExpired):
            raise PermissionError('Die Fahrzeugauswahl ist abgelaufen. Bitte erneut in der Service-App öffnen.') from None
        actor = self.principal()
        if not actor or any(context.get(key) != actor.get(field) for key, field in
                            (('actor', 'id'), ('role', 'role'), ('version', 'version'))):
            raise PermissionError('Die Fahrzeugauswahl gehört nicht zum angemeldeten Zugang.')
        vehicle = self.vehicle(actor, context.get('vehicle'))
        if vehicle['firma_id'] != context.get('company'):
            raise PermissionError('Die Fahrzeugzuordnung hat sich geändert.')
        return {'actor': actor, 'vehicle': vehicle, 'token': token}

    def bind_request(self, lead_id, context):
        with self.p.portal_originals_operation_lock():
            fresh = self.validate_submission(context['token'])
            vehicle = fresh['vehicle']
            return self.p.service_process.bind_case(
                lead_id=lead_id, company_id=vehicle['firma_id'], vehicle_id=vehicle['id'],
                actor={'role': 'werkstatt', 'id': 'service-kontext:' + fresh['actor']['id']},
            )

    def details(self, actor, vehicle_id):
        with self.p.portal_originals_operation_lock():
            vehicle = self.vehicle(actor, vehicle_id)
            company = vehicle['firma_id']
            data = self.fleet.vehicle(company, vehicle_id)
            scoped_actor = self.scoped_actor(actor, vehicle)
            cases = self.p.service_process.for_vehicle(company, vehicle_id, scoped_actor)
            data['service_cases'] = cases
            data['workshop_cases'] = self.p.flottenportal.safe_cases(company, vehicle_id)
            data['documents'] = self.p.flottenportal.documents(company, vehicle_id)
            data['reminders'] = self.p.service_reminders.vehicle_inbox(company, vehicle_id)
            with self.fleet.db() as db:
                data['tire_slots'] = [dict(row) for row in db.execute('''
                    SELECT s.datum,s.uhrzeit,s.lead_id FROM reifen_termin_slots s
                    JOIN serviceprozesse f ON f.lead_id=s.lead_id
                    JOIN leads l ON l.id=s.lead_id
                    WHERE f.firma_id=? AND f.fahrzeug_id=? AND l.status!='verloren'
                    ORDER BY s.datum,s.uhrzeit''', (company, vehicle_id)).fetchall()]
            data['actor'] = scoped_actor
            self.authorize_again(actor, vehicle_id)
            return data


def register_service_app(p, fleet):
    service = ServiceApp(p, fleet)
    p.service_app = service
    bp = Blueprint('serviceapp', __name__)

    def csrf():
        if not session.get('csrf_token'):
            session['csrf_token'] = secrets.token_urlsafe(32)
        return session['csrf_token']

    @bp.before_request
    def guard():
        if not p.app.config.get('SERVICE_APP_ENABLED'):
            abort(404)
        if request.method == 'POST':
            expected = session.get('csrf_token')
            values = request.form.getlist('csrf_token')
            if not expected or not values or any(not hmac.compare_digest(str(expected), str(v)) for v in values):
                abort(400)

    @bp.after_request
    def private(response):
        response.headers.update({'Cache-Control': 'private, no-store', 'Referrer-Policy': 'no-referrer',
                                 'X-Robots-Tag': 'noindex, nofollow, noarchive',
                                 'X-Content-Type-Options': 'nosniff'})
        response.vary.add('Cookie')
        return response

    @bp.context_processor
    def common():
        return {'service_actor': service.principal(), 'service_csrf': csrf(),
                'service_demo': bool(p.app.config.get('SERVICE_APP_DEMO'))}

    def actor_required():
        actor = service.principal()
        if not actor:
            abort(403, description='Bitte über Ihren persönlichen Zugang anmelden.')
        return actor

    def owned(actor, vehicle_id):
        try:
            return service.vehicle(actor, vehicle_id)
        except (LookupError, PermissionError):
            abort(404)

    @bp.get('/service')
    def index():
        actor = service.principal()
        if not actor:
            return render_template('service_start.html')
        try:
            vehicles = service.vehicles(actor)
            cards = []
            for car in vehicles:
                alerts = p.service_reminders.vehicle_inbox(car['firma_id'], car['id'])
                cases = p.service_process.for_vehicle(car['firma_id'], car['id'], service.scoped_actor(actor, car))
                cards.append({**car, 'reminders': alerts, 'cases': cases})
                service.authorize_again(actor, car['id'])
        except (LookupError, PermissionError):
            abort(403)
        return render_template('service_dashboard.html', vehicles=cards)

    @bp.get('/service/fahrzeug/<vehicle_id>')
    def vehicle(vehicle_id):
        actor = actor_required()
        try:
            data = service.details(actor, vehicle_id)
        except (LookupError, PermissionError):
            abort(404)
        return render_template('service_fahrzeug.html', **data,
                               request_ids={case['id']: uuid4().hex for case in data['service_cases']})

    @bp.get('/service/fahrzeug/<vehicle_id>/anfrage')
    def vehicle_request(vehicle_id):
        actor = actor_required()
        intent = request.args.get('anliegen', 'reparaturanfrage')
        if intent not in {'reparaturanfrage', 'reifenservice', 'mechanikanfrage', 'bremsenservice'}:
            abort(400)
        with p.portal_originals_operation_lock():
            car = owned(actor, vehicle_id)
            data = service.fleet.vehicle(car['firma_id'], car['id'])
            contact = data['driver'] if actor['role'] == 'fahrer' else data['company']
            form = {'anliegen': intent, 'name': actor.get('name') or contact.get('name', ''),
                    'email': actor.get('email') or contact.get('email', ''),
                    'telefon': contact.get('telefon', ''), 'fahrzeug': car['modell'],
                    'kennzeichen': car['kennzeichen'], 'service_context': service.form_context(actor, car)}
            if request.args.get('service') == 'inspektion':
                form['mechanik_kategorie'] = ['inspektion']
            response = p.render_website_anfrage(formdata=form)
            try:
                service.authorize_again(actor, vehicle_id)
            except (LookupError, PermissionError):
                abort(403)
            return response

    @bp.get('/service/fahrzeug/<vehicle_id>/beleg/<int:number>')
    def receipt(vehicle_id, number):
        actor = actor_required()
        with p.portal_originals_operation_lock():
            car = owned(actor, vehicle_id)
            try:
                document, raw = p.flottenportal.original(car['firma_id'], vehicle_id, number)
                service.authorize_again(actor, vehicle_id)
            except LookupError:
                abort(404)
            except (ValueError, PermissionError):
                abort(409)
        response = send_file(BytesIO(raw), mimetype=document['mime_type'],
                             download_name=document['original_name'], as_attachment=True, max_age=0)
        response.headers['Content-Security-Policy'] = "default-src 'none'; sandbox"
        return response

    @bp.get('/service/fahrzeug/<vehicle_id>/erinnerung/<path:item_id>')
    def reminder(vehicle_id, item_id):
        actor = actor_required()
        with p.portal_originals_operation_lock():
            car = owned(actor, vehicle_id)
            try:
                preview = p.service_reminders.preview(car['firma_id'], vehicle_id, item_id)
                service.authorize_again(actor, vehicle_id)
            except (LookupError, ValueError, PermissionError):
                abort(404)
        return render_template('service_anfrage.html', car=car, preview=preview, item_id=item_id,
                               test_request_id=uuid4().hex,
                               test_transport=bool(p.app.config.get('TESTING') and p.app.config.get('SERVICE_APP_TEST_TRANSPORT_ENABLED')))

    @bp.post('/service/fahrzeug/<vehicle_id>/erinnerung/<path:item_id>/test')
    def reminder_test(vehicle_id, item_id):
        actor = actor_required()
        car = owned(actor, vehicle_id)
        try:
            p.service_reminders.record_test(
                car['firma_id'], vehicle_id, item_id,
                snapshot=request.form.get('snapshot', ''), request_id=request.form.get('request_id', ''),
                recipient='service-demo@example.invalid',
                authorize=lambda: service.authorize_again(actor, vehicle_id),
            )
            flash('Nur lokal getestet. Es wurde keine Nachricht versendet.', 'success')
        except (LookupError, ValueError, PermissionError) as exc:
            abort(409, description=str(exc))
        return redirect(url_for('serviceapp.reminder', vehicle_id=vehicle_id, item_id=item_id), code=303)

    @bp.post('/service/fall/<case_id>/freigeben')
    def approve(case_id):
        actor = actor_required()
        if actor['role'] != 'fuhrparkleitung':
            abort(403)
        if request.form.get('bestaetigt') != '1':
            abort(400, description='Bitte Umfang, Preis und Zuständigkeit ausdrücklich bestätigen.')
        try:
            case = p.service_process.view(case_id, actor)
            owned(actor, case['vehicle_id'])
            p.service_process.approve(
                case_id, actor, expected_revision=int(request.form.get('revision', '0')),
                snapshot_hash=request.form.get('snapshot_hash', ''),
                request_id=request.form.get('request_id', ''),
            )
            lead = p.get_lead(case['lead_id']) if case.get('lead_id') else None
            if lead and not lead.get('auftrag_id'):
                p.create_order_from_accepted_lead(lead, lead['portal_data'])
            flash('Kostenfreigabe dokumentiert. Die Werkstatt plant den Auftrag ein.', 'success')
        except LookupError:
            abort(404)
        except (ValueError, PermissionError) as exc:
            abort(409, description=str(exc))
        return redirect(url_for('serviceapp.vehicle', vehicle_id=case['vehicle_id']), code=303)

    @bp.get('/admin/service')
    @p.admin_required
    def admin_index():
        entries = []
        for company in fleet.companies():
            _, cars = fleet.company(company['id'])
            for car in cars:
                cases = p.service_process.for_vehicle(company['id'], car['id'], {'role': 'werkstatt', 'id': 'admin'})
                if cases:
                    entries.append({'company': company, 'vehicle': car, 'cases': cases})
        return render_template('service_admin.html', entries=entries, selected=None)

    @bp.route('/admin/service/fall/<case_id>', methods=['GET', 'POST'])
    @p.admin_required
    def admin_case(case_id):
        actor = {'role': 'werkstatt', 'id': 'admin'}
        try:
            if request.method == 'POST':
                action = request.form.get('aktion') or request.args.get('aktion')
                revision = int(request.form.get('revision', '0'))
                if action == 'parteien':
                    fields = request.form
                    parties = {key: {'name': fields.get(key + '_name', '')}
                               for key in ('auftraggeber', 'freigebender', 'rechnungsempfaenger', 'zahler')}
                    parties['freigebender']['identity_id'] = fields.get('freigebender_identity_id', '')
                    parties['rechnungsempfaenger'].update({key: fields.get('rechnungsempfaenger_' + key, '')
                                                          for key in ('strasse', 'plz', 'ort', 'land')})
                    parties.update({key: fields.get(key, '') for key in
                                    ('zahlungsweg', 'fallart', 'selbstbeteiligung', 'selbstbeteiligung_quelle')})
                    p.service_process.save_parties(case_id, parties, actor, expected_revision=revision)
                elif action == 'angebot':
                    p.service_process.sync_offer(case_id, actor, expected_revision=revision)
                elif action == 'extern':
                    p.service_process.approve(case_id, actor, expected_revision=revision,
                        snapshot_hash=request.form.get('snapshot_hash', ''),
                        request_id=request.form.get('request_id', ''),
                        external_reference=request.form.get('external_reference', ''))
                else:
                    abort(400)
                flash('Servicefall gespeichert. Änderungen am Umfang oder Preis erfordern eine neue Freigabe.', 'success')
                return redirect(url_for('serviceapp.admin_case', case_id=case_id), code=303)
            case = p.service_process.view(case_id, actor)
        except LookupError:
            abort(404)
        except (ValueError, PermissionError) as exc:
            abort(409, description=str(exc))
        return render_template('service_admin.html', entries=[], selected=case, request_id=uuid4().hex)

    @p.app.before_request
    def protect_legacy_service_tokens():
        # Older links remain in mailboxes. A bearer must never override current
        # personal/tenant rights or expose the legacy finance view to a driver.
        if not (request.path.startswith('/status/') or request.path.startswith('/kunden-app/fall/')):
            return None
        token = (request.view_args or {}).get('token')
        if not token:
            return None
        with p.portal_originals_operation_lock():
            order = p.get_auftrag_by_kunden_status_token(token)
            lead = None if order else p.get_lead_by_kunden_status_token(token)
            case_id = (p.service_process.case_for_order(order['id']) if order else
                       p.service_process.case_for_lead(lead['id']) if lead else None)
            if not case_id:
                return None
            if not p.app.config.get('SERVICE_APP_ENABLED'):
                abort(403)
            actor = actor_required()
            try:
                case = p.service_process.view(case_id, actor) if actor['role'] != 'fahrer' else None
                if case is None:
                    # The policy view needs the driver's explicitly scoped vehicle.
                    for car in service.vehicles(actor):
                        candidate = p.service_process.for_vehicle(car['firma_id'], car['id'], service.scoped_actor(actor, car))
                        if any(item['id'] == case_id for item in candidate):
                            case = {'vehicle_id': car['id']}
                            break
                if not case:
                    abort(403)
                service.authorize_again(actor, case['vehicle_id'])
            except (LookupError, PermissionError):
                abort(403)
            if request.method not in {'GET', 'HEAD'}:
                abort(409, description='Bitte den aktuellen Servicezugang verwenden.')
            response = redirect(url_for('serviceapp.vehicle', vehicle_id=case['vehicle_id']), code=303)
            response.headers['Cache-Control'] = 'private, no-store'
            response.headers['Referrer-Policy'] = 'no-referrer'
            return response

    p.app.register_blueprint(bp)
    return service
