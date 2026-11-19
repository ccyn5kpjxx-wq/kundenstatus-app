"""Submit a verified shop invoice to the order contact through the shop's MailOutbox."""
from collections.abc import Mapping
from datetime import datetime, timezone
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import format_datetime
import re
from uuid import uuid5

from shop_order_receipts import NAMESPACE, _email
from shop_order_store import ORDER_ID
from werkstatt_mail_identity import append_workshop_footer


def _single_address(value):
    # The envelope is derived from To by MailOutbox. Accept one literal mailbox.
    return bool(_email(value)) and Address(addr_spec=value).addr_spec == value


def _token(order_id):
    if not isinstance(order_id, str) or not ORDER_ID.fullmatch(order_id):
        raise ValueError('invalid_invoice_order')
    return str(uuid5(NAMESPACE, 'live:' + order_id + ':invoice'))


def _delivery_state(result, token, recipient):
    if not isinstance(result, Mapping) or result.get('token') != token or result.get('to') != recipient:
        return 'unknown'
    if result.get('state') in ('sent', 'copy_pending') and result.get('accepted') == [recipient]:
        return 'submitted'
    if result.get('state') == 'not_sent' and result.get('accepted') == []:
        return 'not_sent'
    return 'unknown'


class ShopInvoiceMailer:
    def __init__(self, *, outbox, smtp_config, sender_address):
        self.outbox = outbox
        self.smtp_config = smtp_config
        self.sender = sender_address

    def status(self, *, order_id, recipient):
        """Reconcile an interrupted submission without any SMTP call."""
        try:
            token = _token(order_id)
            if not _single_address(recipient):
                return 'unknown'
            return _delivery_state(self.outbox.status(token), token, recipient)
        except Exception:
            return 'unknown'

    def send(self, *, order_id, recipient, voucher_number, content, filename):
        """Use the frozen order contact and the verified original from ShopInvoiceService.pdf."""
        try:
            token = _token(order_id)
            if not _single_address(recipient) or not _single_address(self.sender):
                return 'not_sent'
            if (not isinstance(voucher_number, str) or not 1 <= len(voucher_number) <= 80
                    or not voucher_number.isprintable() or not voucher_number.strip()):
                return 'not_sent'
            name = re.sub(r'[^\w.-]+', '-', voucher_number).strip('.-') or order_id
            if (filename != f'Rechnung-{name}.pdf' or not isinstance(content, bytes)
                    or len(content) > 20 * 1024 * 1024 or not content.startswith(b'%PDF-')
                    or b'%%EOF' not in content[-1024:]):
                return 'not_sent'
            config = self.smtp_config()
            if (not isinstance(config, Mapping) or config.get('from_address') != self.sender
                    or config.get('smtp_configured') is not True):
                return 'not_sent'
            message = EmailMessage()
            message['From'] = self.sender
            message['To'] = recipient
            message['Subject'] = f'Ihre Rechnung {voucher_number} vom Gärtner Shop'
            message['Date'] = format_datetime(datetime.now(timezone.utc))
            message['Message-ID'] = f'<shop-invoice-{token}@{self.sender.rsplit("@", 1)[1]}>'
            message.set_content(append_workshop_footer(
                f'Guten Tag,\n\nvielen Dank für Ihren Einkauf. Im Anhang finden Sie Ihre Rechnung '
                f'{voucher_number} zur Bestellung {order_id}. Der Rechnungsbetrag ist bereits bezahlt.\n\n'
                'Bei Fragen zur Rechnung antworten Sie gerne auf diese E-Mail.\n\n'
                'Freundliche Grüße aus Mosbach'
            ))
            message.add_attachment(content, maintype='application', subtype='pdf', filename=filename)
        except Exception:
            # No call has been made to the outbox for this attempt.
            return 'not_sent'
        try:
            # The durable outbox retries only a previously proven non-acceptance.
            # Its fingerprint also binds the recipient, content and PDF to this token.
            result = self.outbox.send(token, message, dict(config), retry_not_sent=True)
            return _delivery_state(result, token, recipient)
        except Exception:
            return 'unknown'
