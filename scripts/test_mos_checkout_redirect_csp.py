"""Regression for a form POST that redirects to Stripe-hosted Checkout.

The offline provider supplies a synthetic external Checkout URL. No Stripe API
or external network connection is made by this test.
"""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_mos_public_booking as public


def form_action_sources(policy):
    for directive in policy.split(';'):
        parts = directive.strip().split()
        if parts and parts[0] == 'form-action':
            return parts[1:]
    return []


class CheckoutRedirectCspTests(unittest.TestCase):
    def setUp(self):
        self.flow = public.PublicTests('test_card_authorization_before_rent_only_checkout')
        self.flow.setUp()

    def test_status_form_allows_redirect_to_stripe_checkout(self):
        with patch.dict(self.flow.cfg, {'deposit_method': 'card_authorization_at_booking'}):
            hold_id, _, _ = self.flow.card_deposit()
            deposit_page = self.flow.client.get('/mietwagen-test/status/' + hold_id + '/kaution')
            self.assertEqual(deposit_page.status_code, 200)
            self.assertEqual(
                form_action_sources(deposit_page.headers.get('Content-Security-Policy', '')),
                ["'self'", 'https://hooks.stripe.com'],
            )
            authorized = self.flow.post('/mietwagen-test/status/' + hold_id + '/kaution-test')
            self.assertEqual(authorized.status_code, 303)

            status = self.flow.client.get('/mietwagen-test/status/' + hold_id)
            self.assertEqual(status.status_code, 200)
            self.assertIn('action="/mietwagen-test/status/' + hold_id + '/retry"',
                          status.get_data(as_text=True))

            gateway = public.portal.app.extensions['mos_public_booking']['gateway']
            real_create = gateway.create

            def synthetic_external_checkout(params, key):
                session = real_create(params, key)
                return {**session, 'url': 'https://checkout.stripe.com/c/pay/synthetic-test'}

            with patch.object(gateway, 'create', side_effect=synthetic_external_checkout):
                checkout = self.flow.post('/mietwagen-test/status/' + hold_id + '/retry')
            self.assertEqual(checkout.status_code, 303)
            self.assertEqual(checkout.location, 'https://checkout.stripe.com/c/pay/synthetic-test')

            policy = status.headers.get('Content-Security-Policy', '')
            self.assertIn('https://checkout.stripe.com', form_action_sources(policy),
                          'The status form POST redirects to Stripe; Chromium can block a '
                          "cross-origin redirect when form-action only allows 'self'.")

            admin = self.flow.client.get('/admin')
            self.assertEqual(form_action_sources(admin.headers.get('Content-Security-Policy', '')),
                             ["'self'"], 'Ordinary portal pages must keep the narrower policy.')


if __name__ == '__main__':
    unittest.main()
