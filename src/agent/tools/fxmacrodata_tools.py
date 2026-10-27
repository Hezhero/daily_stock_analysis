"""Public macroeconomic tools consumed by the stock analysis agent."""

from __future__ import annotations

from copy import deepcopy
import logging
import os
from typing import Any, Optional

from src.agent.tools.registry import ToolDefinition, ToolParameter, ToolPolicy

logger = logging.getLogger(__name__)

AVAILABLE = "available"
PACKAGE_UNAVAILABLE = "package_unavailable"
CATALOGUE_UNAVAILABLE = "catalogue_unavailable"

_AVAILABILITY_SIGNAL_EMITTED = False

# The client wheel ships only through GitHub Release, so an environment that
# cannot reach it must still import this module: a missing client disables the
# tools, it does not break the agent tool registry. See docs/adr/0005-*.
try:
    from fxmacrodata_public import FXMacroDataClient, list_operations
    _CLIENT_IMPORT_ERROR: Optional[str] = None
except ImportError as exc:  # pragma: no cover - reached only without the wheel
    FXMacroDataClient = None
    list_operations = None
    _CLIENT_IMPORT_ERROR = type(exc).__name__


class FXMacroDataTool(ToolDefinition):
    """Keep nested public JSON Schemas intact in every DSA tool descriptor."""

    input_schema: dict

    def _params_json_schema(self) -> dict:
        return deepcopy(self.input_schema)

    def _descriptor_json_schema(self) -> dict:
        return self._params_json_schema()


def _load_catalogue() -> tuple[list, str]:
    """Read the pinned client's operation catalogue once, with its reason code.

    Two distinct faults leave the tools unavailable without breaking the agent
    tool registry: the client package is not installed at all, or it is installed
    but its operation catalogue cannot be read (for example a frozen build
    without that package data).
    """
    if list_operations is None:
        return [], PACKAGE_UNAVAILABLE
    try:
        catalogue = list(list_operations())
    except Exception:
        return [], CATALOGUE_UNAVAILABLE
    return (catalogue, AVAILABLE) if catalogue else ([], CATALOGUE_UNAVAILABLE)


def _available_operations() -> list:
    """Return the pinned client's operation catalogue, or none if it cannot load."""
    return _load_catalogue()[0]


def fxmacrodata_availability() -> str:
    """Return ``available``, ``package_unavailable`` or ``catalogue_unavailable``.

    The reason-code source behind the single startup warning: it keeps "the wheel
    is missing" distinguishable from "the wheel cannot read its data".
    """
    return _load_catalogue()[1]


def _emit_availability_signal_once() -> None:
    """Log one explicit startup warning when the integration cannot be used."""
    global _AVAILABILITY_SIGNAL_EMITTED
    if _AVAILABILITY_SIGNAL_EMITTED:
        return
    _AVAILABILITY_SIGNAL_EMITTED = True
    reason = fxmacrodata_availability()
    if reason == AVAILABLE:
        return
    logger.warning(
        "[fxmacrodata_tools] FXMacroData integration unavailable (%s%s); "
        "the agent registry continues without its tools. See docs/adr/0005.",
        reason,
        f", {_CLIENT_IMPORT_ERROR}" if _CLIENT_IMPORT_ERROR else "",
    )


def fxmacrodata_tool_names(*operations: str, require_integration: bool = True) -> list[str]:
    """Name tools for native specialist allowlists; no arguments selects all.

    When the integration cannot load, the "select everything" form returns an
    empty list so specialist agents simply carry no FXMacroData tools. A caller
    that names specific operations fails loudly instead, unless it passes
    ``require_integration=False`` to declare that it accepts the same
    degradation — a silently shortened allowlist is indistinguishable from a
    wiring change (ADR 0005).
    """
    catalogue = _available_operations()
    if not catalogue:
        if operations and require_integration:
            raise RuntimeError(
                "FXMacroData integration is unavailable "
                f"({fxmacrodata_availability()}); refusing to return a silently "
                f"shortened allowlist for {len(operations)} requested operation(s)."
            )
        return []
    available = {operation.name for operation in catalogue}

    selected = list(operations) if operations else [operation.name for operation in catalogue]
    if any(operation not in available for operation in selected):
        raise ValueError("Unknown FXMacroData operation in specialist tool selection.")
    return [f"fxmacrodata_{operation}" for operation in selected]


def _unavailable_response(operation_name: str) -> dict:
    return {
        "operation": operation_name,
        "status": "unavailable",
        "records": [],
        "error": "FXMacroData could not complete this request.",
    }


def build_fxmacrodata_tools(client: Any = None) -> list[FXMacroDataTool]:
    """Build read-only tools; the USD baseline works without configuration.

    ``client`` is an optional ``FXMacroDataClient``; it is typed as ``Any``
    because the class itself is only bound when the optional wheel is installed.
    An optional FXMACRODATA_API_KEY is read from the host environment at call
    time, never put into a tool schema, invocation argument or response.
    """
    def make_handler(operation_name):
        def handler(**arguments):
            provider = client
            try:
                if provider is None:
                    client_class = FXMacroDataClient
                    if client_class is None:
                        return _unavailable_response(operation_name)
                    provider = client_class(
                        api_key=os.environ.get("FXMACRODATA_API_KEY") or "",
                        timeout=30,
                    )
                response = provider.execute(operation_name, arguments).as_dict()
                response["provider_url"] = (
                    "https://fxmacrodata.com/?utm_source=daily_stock_analysis"
                    "&utm_medium=integration&utm_campaign=open_source_integrations&utm_content=app"
                )
                return response
            except Exception:
                # DSA logs propagated exceptions; never propagate a client
                # construction or transport exception, or a request which
                # could contain a credential.
                return _unavailable_response(operation_name)
            finally:
                if client is None and provider is not None:
                    try:
                        provider.close()
                    except Exception:
                        # A close failure must not replace the handler result;
                        # the exception text is not logged for the same reason
                        # as above.
                        logger.debug("[fxmacrodata_tools] %s: client close failed", operation_name)

        return handler

    result = []
    for operation in _available_operations():
        required = operation.input_schema.get("required", [])
        parameters = [
            ToolParameter(
                name=name,
                type=schema.get("type", "object"),
                description=schema.get("description", name),
                required=name in required,
            )
            for name, schema in operation.input_schema.get("properties", {}).items()
        ]
        definition = FXMacroDataTool(
            name=f"fxmacrodata_{operation.name}",
            description=f"FXMacroData: {operation.description}",
            parameters=parameters,
            handler=make_handler(operation.name),
            category="market",
            policy=ToolPolicy.declared(
                read_only=True, permissions=["network"], timeout_seconds=35,
            ),
            timeout_seconds=35,
        )
        definition.input_schema = deepcopy(operation.input_schema)
        result.append(definition)
    return result


# Like the host's other tool modules, publish stable definitions for registry
# rebuilds. These handlers obtain a client and optional authorization only when
# invoked; importing this module makes no network requests and reads no key.
ALL_FXMACRODATA_TOOLS = build_fxmacrodata_tools()

_emit_availability_signal_once()
