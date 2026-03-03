"""Environment-based configuration via Settings dataclass.

All settings can be overridden with environment variables.
Defaults point to /mnt/e/psxdata/ (external drive in WSL).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    """Application settings, loaded from environment variables with defaults."""

    # Database — SQLite on external drive
    db_path: str = field(default_factory=lambda: os.environ.get(
        "PSX_DB_PATH", "/mnt/e/psxdata/psx.sqlite"))

    # Data directories — all on external drive
    data_root: str = field(default_factory=lambda: os.environ.get(
        "PSX_DATA_ROOT", "/mnt/e/psxdata"))
    backup_dir: str = ""
    csv_dir: str = ""
    logs_dir: str = ""

    # AI Providers
    llm_provider: str = field(default_factory=lambda: os.environ.get(
        "PSX_LLM_PROVIDER", "openai"))
    openai_api_key: str = field(default_factory=lambda: os.environ.get(
        "OPENAI_API_KEY", ""))
    anthropic_api_key: str = field(default_factory=lambda: os.environ.get(
        "ANTHROPIC_API_KEY", ""))

    # Sync settings
    sync_max_concurrent: int = 25
    sync_rate_limit: float = 0.05

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    def __post_init__(self):
        self.backup_dir = os.path.join(self.data_root, "backups")
        self.csv_dir = os.path.join(self.data_root, "csv")
        self.logs_dir = os.path.join(self.data_root, "logs")
        # Ensure directories exist
        for d in [self.backup_dir, self.csv_dir, self.logs_dir]:
            os.makedirs(d, exist_ok=True)


# Module-level singleton
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
