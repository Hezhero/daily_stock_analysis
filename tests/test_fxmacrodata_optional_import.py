"""The optional FXMacroData client must degrade, never break the tool registry.

The client wheel ships only through GitHub Release, so a checkout that cannot
reach it is a supported state. See
``docs/adr/0005-fxmacrodata-optional-import-with-packaged-probe.md``.
"""
import importlib
import importlib.util
import logging
import sys

import pytest

from src.agent.tools import fxmacrodata_tools

HAS_CLIENT = importlib.util.find_spec("fxmacrodata_public") is not None


def test_missing_client_package_degrades_without_breaking_import(monkeypatch, caplog):
    """Neither an import-time crash nor a silently shortened allowlist."""
    monkeypatch.setitem(sys.modules, "fxmacrodata_public", None)
    try:
        with caplog.at_level(logging.WARNING, logger=fxmacrodata_tools.__name__):
            module = importlib.reload(fxmacrodata_tools)

            assert module.fxmacrodata_availability() == module.PACKAGE_UNAVAILABLE
            assert module.ALL_FXMACRODATA_TOOLS == []
            assert module.build_fxmacrodata_tools() == []
            assert module.fxmacrodata_tool_names() == []

            with pytest.raises(RuntimeError, match="package_unavailable"):
                module.fxmacrodata_tool_names("data_catalogue")

            assert module.fxmacrodata_tool_names(
                "data_catalogue", require_integration=False
            ) == []

            # One startup signal per import, not one per catalogue lookup.
            module._available_operations()
            module._available_operations()

        signals = [
            record.getMessage()
            for record in caplog.records
            if "FXMacroData integration unavailable" in record.getMessage()
        ]
        assert len(signals) == 1
        assert "package_unavailable" in signals[0]
    finally:
        monkeypatch.undo()
        importlib.reload(fxmacrodata_tools)

    expected = (
        fxmacrodata_tools.AVAILABLE if HAS_CLIENT else fxmacrodata_tools.PACKAGE_UNAVAILABLE
    )
    assert fxmacrodata_tools.fxmacrodata_availability() == expected


@pytest.mark.skipif(not HAS_CLIENT, reason="fxmacrodata-public-client is GitHub-Release-only")
def test_available_client_reports_available_and_named_operations():
    assert fxmacrodata_tools.fxmacrodata_availability() == fxmacrodata_tools.AVAILABLE
    assert fxmacrodata_tools.fxmacrodata_tool_names("data_catalogue") == [
        "fxmacrodata_data_catalogue"
    ]
    with pytest.raises(ValueError, match="Unknown FXMacroData operation"):
        fxmacrodata_tools.fxmacrodata_tool_names("not_an_operation")


def test_catalogue_failure_is_distinguishable_without_the_wheel(monkeypatch):
    """The catalogue branch is unreachable via reload when the wheel is absent."""

    def broken_catalogue():
        raise FileNotFoundError("operations.json")

    monkeypatch.setattr(fxmacrodata_tools, "list_operations", broken_catalogue)

    assert fxmacrodata_tools.fxmacrodata_availability() == fxmacrodata_tools.CATALOGUE_UNAVAILABLE
    assert fxmacrodata_tools._available_operations() == []
    assert fxmacrodata_tools.fxmacrodata_tool_names() == []
    with pytest.raises(RuntimeError, match="catalogue_unavailable"):
        fxmacrodata_tools.fxmacrodata_tool_names("data_catalogue")
    assert fxmacrodata_tools.fxmacrodata_tool_names(
        "data_catalogue", require_integration=False
    ) == []
