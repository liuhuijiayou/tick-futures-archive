import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.common import load_auth


class ExplicitAuthentication(unittest.TestCase):
    def test_environment_credentials_have_priority(self):
        with patch.dict(os.environ, {"TQ_USER": "fixture-user", "TQ_PASSWORD": "fixture-password",
                                     "TQ_AUTH_CONFIG": "/does/not/exist"}, clear=True):
            self.assertEqual(load_auth(), ("fixture-user", "fixture-password", "environment"))

    def test_partial_environment_does_not_fall_back(self):
        with patch.dict(os.environ, {"TQ_USER": "fixture-user"}, clear=True):
            with self.assertRaisesRegex(ValueError, "both"):
                load_auth()

    def test_missing_configuration_is_explicit(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "TQ_AUTH_CONFIG"):
                load_auth()

    def test_config_is_parsed_without_execution(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder) / "auth.py"
            config.write_text('raise AssertionError("must never execute")\nauth=TqAuth(user_name="fixture-user", password="fixture-password")\n')
            with patch.dict(os.environ, {"TQ_AUTH_CONFIG": str(config)}, clear=True):
                user, password, source = load_auth()
            self.assertEqual((user, password), ("fixture-user", "fixture-password"))
            self.assertIn("AST only", source)

    def test_nonliteral_auth_is_rejected_without_execution(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder) / "auth.py"
            config.write_text('auth=TqAuth(load_secret(), "fixture-password")\n')
            with patch.dict(os.environ, {"TQ_AUTH_CONFIG": str(config)}, clear=True):
                with self.assertRaises(ValueError):
                    load_auth()


if __name__ == "__main__":
    unittest.main()
