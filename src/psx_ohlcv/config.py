"""Configuration and default paths."""

import logging
from dataclasses import dataclass, fields
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Data directory on persistent storage (E: drive in WSL)
DATA_ROOT = Path("/mnt/e/psxdata")

# Default paths - all data in /mnt/e/psxdata
DEFAULT_DB_PATH = DATA_ROOT / "psx.sqlite"
DEFAULT_LOGS_DIR = DATA_ROOT / "logs"
DEFAULT_LOG_FILE = DEFAULT_LOGS_DIR / "psxsync.log"

# Logging config
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3


@dataclass
class SyncConfig:
    """Configuration options for sync operations."""

    max_retries: int = 3
    delay_min: float = 0.3
    delay_max: float = 0.7
    timeout: int = 30
    incremental: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> "SyncConfig":
        """Create config from dict, ignoring unknown keys."""
        valid_keys = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in d.items() if k in valid_keys}
        return cls(**filtered)


# Global default config
DEFAULT_SYNC_CONFIG = SyncConfig()


def get_db_path(db_path: Path | str | None = None) -> Path:
    """Get database path, using default if not specified."""
    if db_path is None:
        return DEFAULT_DB_PATH
    return Path(db_path)


def ensure_dirs(db_path: Path | None = None) -> None:
    """Ensure data and logs directories exist."""
    # Ensure data root exists
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    DEFAULT_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    # Ensure db parent dir exists (in case custom path)
    path = get_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)


def setup_logging(
    log_file: Path | str | None = None,
    level: int = logging.INFO,
    console: bool = False,
) -> logging.Logger:
    """
    Setup logging with rotating file handler.

    Args:
        log_file: Path to log file. Uses default if None.
        level: Logging level.
        console: If True, also log to console.

    Returns:
        Configured logger instance.
    """
    if log_file is None:
        log_file = DEFAULT_LOG_FILE
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("psx_ohlcv")
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
    )
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler (optional)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger() -> logging.Logger:
    """Get the psx_ohlcv logger."""
    return logging.getLogger("psx_ohlcv")
