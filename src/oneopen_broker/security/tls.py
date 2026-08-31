"""TLS context helpers. Certificates are files on disk; nothing is hardcoded."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any


def server_ssl_context(tls: Any) -> ssl.SSLContext | None:
    if not getattr(tls, "enabled", False):
        return None
    certfile = getattr(tls, "certfile", "") or ""
    keyfile = getattr(tls, "keyfile", "") or ""
    if not certfile or not keyfile:
        raise ValueError("security.tls.enabled requires certfile and keyfile")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(str(Path(certfile)), str(Path(keyfile)))
    cafile = getattr(tls, "cafile", "") or ""
    if cafile:
        ctx.load_verify_locations(cafile=str(Path(cafile)))
    if getattr(tls, "require_client_cert", False):
        ctx.verify_mode = ssl.CERT_REQUIRED
        if not cafile:
            raise ValueError("require_client_cert needs security.tls.cafile")
    return ctx


def client_ssl_context(
    *,
    enabled: bool = False,
    cafile: str = "",
    certfile: str = "",
    keyfile: str = "",
    insecure: bool = False,
) -> ssl.SSLContext | None:
    if not enabled:
        return None
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if cafile:
        ctx.load_verify_locations(cafile=str(Path(cafile)))
    if certfile and keyfile:
        ctx.load_cert_chain(str(Path(certfile)), str(Path(keyfile)))
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx
