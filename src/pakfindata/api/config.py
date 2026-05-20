"""Configuration for the pakfindata FastAPI service.

Loading order (later overrides earlier):
    1. Hardcoded defaults.
    2. ~/.config/pakfindata/api.env (if exists), KEY=VALUE per line.
    3. Environment variables.

Settings:
    PAKFINDATA_API_TOKEN — Bearer token. REQUIRED — no default. The
        service refuses to start without it.
    PAKFINDATA_API_HOST — default 127.0.0.1.
    PAKFINDATA_API_PORT — default 8001 (deliberately different from
        the legacy 8000 the smart client used to assume).
    PAKFINDATA_DB_PATH — path to psx.sqlite (default the canonical
        NVMe path).
    PAKFINDATA_LOG_LEVEL — default INFO.

Settings is cached via lru_cache; one Settings instance per process.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


_DEFAULTS = {
    "PAKFINDATA_API_HOST": "127.0.0.1",
    "PAKFINDATA_API_PORT": "8001",
    "PAKFINDATA_DB_PATH": str(Path.home() / "psxdata_rescue" / "psx.sqlite"),
    "PAKFINDATA_LOG_LEVEL": "INFO",
}

_ENV_FILE = Path.home() / ".config" / "pakfindata" / "api.env"


@dataclass(frozen=True)
class Settings:
    """Frozen settings — instantiate via get_settings()."""

    api_token: str
    api_host: str
    api_port: int
    db_path: Path
    log_level: str


def _read_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file. Quotes optional; comments allowed."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if not key:
            continue
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Resolve settings from defaults + ~/.config file + environment.

    Raises RuntimeError if PAKFINDATA_API_TOKEN is unset — fail fast.
    """
    resolved: dict[str, str] = dict(_DEFAULTS)
    resolved.update(_read_env_file(_ENV_FILE))
    for key in (
        "PAKFINDATA_API_TOKEN",
        "PAKFINDATA_API_HOST",
        "PAKFINDATA_API_PORT",
        "PAKFINDATA_DB_PATH",
        "PAKFINDATA_LOG_LEVEL",
    ):
        env_value = os.environ.get(key)
        if env_value is not None:
            resolved[key] = env_value

    token = resolved.get("PAKFINDATA_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "PAKFINDATA_API_TOKEN is required. Set it in "
            f"{_ENV_FILE} or as an environment variable."
        )

    return Settings(
        api_token=token,
        api_host=resolved["PAKFINDATA_API_HOST"],
        api_port=int(resolved["PAKFINDATA_API_PORT"]),
        db_path=Path(resolved["PAKFINDATA_DB_PATH"]),
        log_level=resolved["PAKFINDATA_LOG_LEVEL"].upper(),
    )
