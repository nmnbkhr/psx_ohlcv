"""HTTP client with retries, backoff, jitter, and polite delays."""

import random
import ssl
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util.retry import Retry
from urllib3.util.ssl_ import create_urllib3_context

from .config import DEFAULT_SYNC_CONFIG, SyncConfig

DEFAULT_USER_AGENT = "pakfindata/0.1.0 (Python; educational project)"


class LegacySSLAdapter(HTTPAdapter):
    """HTTPS adapter for servers with broken/legacy SSL configurations.

    Handles two common issues with Pakistani bank and government websites:
      1. Missing intermediate certificates ("unable to verify the first certificate")
      2. Unsafe legacy TLS renegotiation (blocked by modern OpenSSL)

    Usage::

        session = create_session(legacy_ssl=True)
        # or manually:
        session = requests.Session()
        session.mount("https://", LegacySSLAdapter())
    """

    def __init__(self, max_retries=None, **kwargs):
        self._ssl_context = self._build_context()
        super().__init__(max_retries=max_retries, **kwargs)

    @staticmethod
    def _build_context() -> ssl.SSLContext:
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Allow unsafe legacy renegotiation (required by some .pk servers)
        ctx.options |= 0x4  # SSL_OP_LEGACY_SERVER_CONNECT
        return ctx

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **kwargs,
        )


def create_session(
    config: SyncConfig | None = None,
    backoff_factor: float = 0.5,
    status_forcelist: tuple[int, ...] = (500, 502, 503, 504),
    user_agent: str = DEFAULT_USER_AGENT,
    legacy_ssl: bool = False,
) -> requests.Session:
    """
    Create a requests Session with retry logic.

    Args:
        config: SyncConfig with max_retries and other options.
        backoff_factor: Exponential backoff factor (delay = factor * 2^retry)
        status_forcelist: HTTP status codes to retry on
        user_agent: User-Agent header value
        legacy_ssl: Use LegacySSLAdapter for servers with broken SSL
                    (e.g. samba.com.pk, some .gov.pk sites). Disables
                    certificate verification and allows legacy renegotiation.

    Returns:
        Configured requests.Session
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    session = requests.Session()

    retry_strategy = Retry(
        total=config.max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )

    if legacy_ssl:
        adapter = LegacySSLAdapter(max_retries=retry_strategy)
    else:
        adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )

    return session


def polite_delay(
    delay_min: float | None = None,
    delay_max: float | None = None,
    config: SyncConfig | None = None,
) -> None:
    """
    Sleep for a polite delay with jitter.

    Args:
        delay_min: Minimum delay in seconds (overrides config).
        delay_max: Maximum delay in seconds (overrides config).
        config: SyncConfig to use for defaults.
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    min_delay = delay_min if delay_min is not None else config.delay_min
    max_delay = delay_max if delay_max is not None else config.delay_max

    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)


def fetch_url(
    session: requests.Session,
    url: str,
    timeout: int | None = None,
    polite: bool = True,
    config: SyncConfig | None = None,
) -> requests.Response:
    """
    Fetch URL with optional polite delay.

    Args:
        session: requests Session to use
        url: URL to fetch
        timeout: Request timeout in seconds (overrides config)
        polite: Whether to add polite delay after request
        config: SyncConfig to use for defaults

    Returns:
        Response object

    Raises:
        requests.RequestException: On request failure
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    actual_timeout = timeout if timeout is not None else config.timeout
    response = session.get(url, timeout=actual_timeout)
    response.raise_for_status()

    if polite:
        polite_delay(config=config)

    return response
