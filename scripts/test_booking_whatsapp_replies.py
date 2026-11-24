"""An operator's booking reply must never become a partner's workshop chat."""
import time
import unittest
from unittest.mock import patch

from test_mos_public_booking import portal  # Network-disabled sandbox and temporary DB.
import workshop_booking_whatsapp as notices


class ReplyRouting(unittest.TestCase):
    def test_recent_operator_notices_and_explicit_workshop_context(self):
        phone = "+491234567890"
        stamp = int(time.time())
        db = portal.get_db()
        try:
            notices.enqueue(db, event_key='rental:1', reference='M-1',
                            title='Mietbuchung', message='Test', path='/admin/kalender')
            db.execute("""INSERT INTO workshop_booking_wa_deliveries
                (event_key,phone,state,provider_id,attempted_at,updated_at)
                VALUES (?,?, 'sent', ?,?,?)""",
                       ('rental:1', portal.whatsapp_payload_phone(phone), 'booking-1', stamp, stamp))
            db.commit()
        finally:
            db.close()

        older = {"auftrag_id": 7, "erstellt_am": portal.now_str()}
        with patch.object(portal, "find_recent_whatsapp_thread_for_phone", return_value=older):
            resolve = portal.resolve_whatsapp_reply_auftrag_id
            self.assertEqual(resolve({"context": {"id": "booking-1"}}, "#A7", phone), (0, "booking-1"))
            self.assertEqual(resolve({}, "Danke", phone), (0, ""))
            self.assertEqual(resolve({}, "#A7", phone), (7, ""))
            self.assertEqual(resolve({}, "Danke", "+49999111"), (7, ""))
            with patch.object(portal, "find_whatsapp_context", return_value={"auftrag_id": 9}):
                self.assertEqual(resolve({"context": {"id": "werkstatt-9"}}, "Danke", phone),
                                 (9, "werkstatt-9"))


if __name__ == "__main__":
    unittest.main()
