"""Bearer token auth for the pakfindata API.

Single shared secret. Used as both:

1. A `Depends()` dependency on individual routes that explicitly
   declare it.
2. A global middleware that enforces the token on every request
   except a small allowlist (`/health`, `/docs`, `/openapi.json`).

Phase 1.1 wires the middleware globally so all 16 existing routers
get authenticated automatically without per-route changes.

Constant-time comparison via `secrets.compare_digest` to avoid
timing attacks (overkill for localhost but cheap correctness).
"""

from __future__ import annotations

import secrets
from collections.abc import Iterable

from fastapi import Header, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from pakfindata.api.config import get_settings


# Paths that bypass auth entirely. Keep this list small.
PUBLIC_PATHS: tuple[str, ...] = (
    "/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
)


def _extract_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    if not authorization.startswith("Bearer "):
        return None
    return authorization.removeprefix("Bearer ").strip()


def _token_matches(presented: str) -> bool:
    expected = get_settings().api_token
    return secrets.compare_digest(presented, expected)


async def require_auth(
    authorization: str | None = Header(default=None),
) -> None:
    """FastAPI dependency: validate Bearer token on a single route.

    Usage::

        @router.get("/some-endpoint", dependencies=[Depends(require_auth)])
        def some_endpoint(): ...
    """
    presented = _extract_token(authorization)
    if presented is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not _token_matches(presented):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Global auth middleware.

    Every request whose path is NOT in the public allowlist must
    carry a valid Bearer token. WebSocket upgrades are intentionally
    skipped (the WS router has its own auth path planned for Phase 2).
    """

    def __init__(self, app, public_paths: Iterable[str] = PUBLIC_PATHS) -> None:
        super().__init__(app)
        self._public_paths = tuple(public_paths)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Allow public paths
        if path in self._public_paths or any(
            path.startswith(p + "/") for p in self._public_paths
        ):
            return await call_next(request)

        # WebSocket upgrades bypass; handled by their own router.
        if request.scope.get("type") == "websocket":
            return await call_next(request)

        token = _extract_token(request.headers.get("authorization"))
        if token is None:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Missing Bearer token"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not _token_matches(token):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Invalid token"},
            )

        return await call_next(request)
