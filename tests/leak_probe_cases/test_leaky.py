"""Leaky fixture for the env-leak probe self-test.

The first test writes ``ENV_FILE`` and does not restore it. In probe mode the
autouse fixture deliberately stops restoring, so the next test's setup observes
the transition and attributes it to the first test.
"""

import os


def test_leaks_env_file():
    os.environ["ENV_FILE"] = "leak-probe-should-not-survive.env"


def test_after_leak():
    assert True
