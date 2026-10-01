"""Only the assistant document may make a direct WebRTC handshake."""
import unittest
from unittest.mock import patch
import test_assistent as fixture


class RealtimeTransportHeaders(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.AssistantTests()
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def test_browser_handshake_is_scoped_to_assistant_document(self):
        target = "https://api.openai.com/v1/realtime/calls"
        client = self.fixture.client
        for path, allowed in (("/werkstatt/assistent", True),
                              ("/partner", False), ("/admin", False)):
            with self.subTest(path=path):
                response = client.get(path)
                policy = response.headers["Content-Security-Policy"]
                sources = next(d for d in policy.split("; ") if d.startswith("connect-src ")).split()[1:]
                self.assertEqual(target in sources, allowed)
                self.assertNotIn("https://api.openai.com", sources)
                self.assertNotIn("*", sources)
                self.assertIn("form-action 'self'", policy)
                self.assertIn("object-src 'none'", policy)

    def test_deployed_revision_is_bounded_and_never_arbitrary_environment_text(self):
        for revision, expected in (("a" * 40, "a" * 12), ("", None),
                                   ("sk-synthetic-secret", None), ("x\r\nInjected: true", None)):
            with self.subTest(revision=revision), patch.dict(fixture.os.environ, {"RENDER_GIT_COMMIT": revision}):
                response = self.fixture.client.get("/healthz")
                self.assertEqual(response.headers.get("X-Kundenstatus-Build"), expected)


if __name__ == "__main__":
    unittest.main()
