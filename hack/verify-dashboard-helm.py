#!/usr/bin/env python3
"""Verify Dashboard Helm rendering for every supported TLS source."""

from __future__ import annotations

import subprocess
import sys


def main() -> int:
    app_version = helm_chart_field("charts/kruntimes", "appVersion")
    default = helm_template()
    if "kruntimes-dashboard" in default:
        return fail("Dashboard must be disabled by default")

    generated = helm_template("--set", "dashboard.enabled=true")
    for expected in (
        "kind: Deployment",
        "name: kruntimes-dashboard",
        f"image: ghcr.io/kruntimes/dashboard:{app_version}",
        "scheme: HTTPS",
        "name: kruntimes-dashboard-tls",
    ):
        if expected not in generated:
            return fail(f"self-signed Dashboard render is missing {expected!r}")
    if "kind: Certificate" in generated:
        return fail("self-signed Dashboard render must not create a Certificate")

    existing = helm_template(
        "--set",
        "dashboard.enabled=true",
        "--set",
        "dashboard.tls.selfSigned=false",
        "--set",
        "dashboard.tls.secretName=dashboard-existing",
    )
    if "secretName: dashboard-existing" not in existing:
        return fail("existing-secret Dashboard render must mount the selected Secret")
    if "kind: Certificate" in existing:
        return fail("existing-secret Dashboard render must not create a Certificate")
    if "name: kruntimes-dashboard-tls" in existing:
        return fail("existing-secret Dashboard render must not create a chart TLS Secret")

    cert_manager = helm_template(
        "--set",
        "dashboard.enabled=true",
        "--set",
        "dashboard.tls.selfSigned=false",
        "--set",
        "dashboard.tls.secretName=dashboard-cert-manager",
        "--set",
        "dashboard.tls.certManager.enabled=true",
        "--set",
        "dashboard.tls.certManager.issuerRef.name=platform-ca",
    )
    for expected in (
        "kind: Certificate",
        "name: kruntimes-dashboard",
        "secretName: dashboard-cert-manager",
        "name: platform-ca",
    ):
        if expected not in cert_manager:
            return fail(f"cert-manager Dashboard render is missing {expected!r}")
    if "kind: Secret\nmetadata:\n  name: dashboard-cert-manager" in cert_manager:
        return fail("cert-manager Dashboard render must not create the target Secret")

    return expect_failure(
        "dashboard.tls.selfSigned and dashboard.tls.certManager.enabled are mutually exclusive",
        "--set",
        "dashboard.enabled=true",
        "--set",
        "dashboard.tls.certManager.enabled=true",
        "--set",
        "dashboard.tls.certManager.issuerRef.name=platform-ca",
    )


def helm_template(*args: str) -> str:
    return subprocess.check_output(
        ["helm", "template", "kruntimes", "charts/kruntimes", "--namespace", "default", *args],
        text=True,
    )


def helm_chart_field(chart: str, field: str) -> str:
    chart_yaml = subprocess.check_output(["helm", "show", "chart", chart], text=True)
    for line in chart_yaml.splitlines():
        key, sep, value = line.partition(":")
        if sep and key == field:
            return value.strip().strip('"')
    raise RuntimeError(f"{chart} is missing {field}")


def expect_failure(expected: str, *args: str) -> int:
    result = subprocess.run(
        ["helm", "template", "kruntimes", "charts/kruntimes", "--namespace", "default", *args],
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return fail("ambiguous Dashboard TLS configuration unexpectedly rendered")
    if expected not in result.stderr:
        return fail(f"unexpected validation error: {result.stderr}")
    return 0


def fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
