"""Clean fixture for the env-leak probe self-test.

This directory is listed in ``norecursedirs`` so the normal suite never collects
it; ``tests/test_env_leak_probe.py`` passes these files explicitly.
"""


def test_clean_one():
    assert True


def test_clean_two():
    assert True
