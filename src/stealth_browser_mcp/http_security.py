"""HTTP transport security helpers (SB-60 forwards to http_auth).

Maintained for backward compatibility. New code should use http_auth.
"""

from __future__ import annotations

from typing import Optional

from fastmcp.server.auth import TokenVerifier

from .http_auth import (
    HTTP_CONTROL_SCOPE,
    LEGACY_HTTP_AUTH_TOKEN_ENV,
    BearerTokenVerifier as EnvironmentBearerTokenVerifier,
    create_http_auth_provider,
    get_mcp_token as get_http_auth_token,
    is_loopback_host,
    validate_bind,
    verify_token,
)

HTTP_AUTH_TOKEN_ENV = "STEALTH_BROWSER_MCP_AUTH_TOKEN"

__all__ = [
    "EnvironmentBearerTokenVerifier",
    "HTTP_AUTH_TOKEN_ENV",
    "HTTP_CONTROL_SCOPE",
    "LEGACY_HTTP_AUTH_TOKEN_ENV",
    "create_http_auth_provider",
    "get_http_auth_token",
    "is_loopback_host",
    "validate_bind",
    "verify_token",
]
