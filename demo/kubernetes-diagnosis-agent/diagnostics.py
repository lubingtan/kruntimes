"""Allowlisted Kubernetes diagnosis tools used by the example agent."""

from __future__ import annotations

import re
from typing import Any

_NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

TOOL = {
    "type": "function",
    "name": "collect_kubernetes_diagnostics",
    "description": "Collect one read-only Kubernetes diagnostic from the requested namespace.",
    "parameters": {
        "type": "object",
        "properties": {
            "kind": {
                "type": "string",
                "enum": ["pods", "events", "services", "deployments", "replicasets"],
                "description": "Kubernetes resource to inspect.",
            },
            "name": {
                "type": "string",
                "description": "Optional resource name. Omit it to list the resource.",
            },
        },
        "required": ["kind"],
        "additionalProperties": False,
    },
}


def command_for(namespace: str, arguments: dict[str, Any]) -> list[str]:
    """Build one argv-only, namespace-confined read-only kubectl command."""
    _validate_name("namespace", namespace)
    kind = arguments.get("kind")
    if kind not in TOOL["parameters"]["properties"]["kind"]["enum"]:
        raise ValueError("unsupported diagnostic resource")
    command = ["kubectl", "get", str(kind), "--namespace", namespace, "--output", "json"]
    name = arguments.get("name")
    if name is not None:
        if not isinstance(name, str):
            raise ValueError("diagnostic resource name must be a string")
        _validate_name("resource name", name)
        command.insert(3, name)
    return command


def _validate_name(label: str, value: str) -> None:
    if not _NAME.fullmatch(value):
        raise ValueError(f"invalid {label}")
