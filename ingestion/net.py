"""HTTP client shared by both collectors: timeouts, bounded retries, exponential backoff.

Rules (issue "Incremental API collector" and brief section 8):

* every request has a timeout (the CFPB API needed ~21 s for 5 records in the probe, so the
  default is 60 s);
* retry **only** what can fix itself: timeouts, connection resets, HTTP 429 and 5xx;
* client errors (400, 403, 404...) fail immediately;
* retries are **bounded** (``max_retries``): there is never an infinite loop;
* the wait grows exponentially (``backoff_base * 2**n``, capped) and honours ``Retry-After``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import requests

LOG = logging.getLogger("ingestion.net")

USER_AGENT = "cfpb-complaints-platform/0.2 (USJ-ESIB student project; batch research use)"
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_BACKOFF_SECONDS = 60.0
MAX_RETRY_AFTER_SECONDS = 120.0


class HttpFailure(RuntimeError):
    """Base class for request failures."""


class HttpStatusError(HttpFailure):
    """A non-retryable HTTP status (e.g. 404). Fails fast."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"HTTP {status} for {url}")
        self.status = status
        self.url = url


class RetriesExhausted(HttpFailure):
    """The request kept failing with retryable errors until the retry budget was spent."""


@dataclass(frozen=True)
class RetryPolicy:
    timeout_seconds: float = 60.0
    max_retries: int = 5
    backoff_base_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.backoff_base_seconds < 0:
            raise ValueError("backoff_base_seconds must be >= 0")

    def delay(self, retry_number: int) -> float:
        """Seconds to wait before retry number ``retry_number`` (1-based)."""
        return min(self.backoff_base_seconds * (2 ** (retry_number - 1)), MAX_BACKOFF_SECONDS)


def _retry_after_seconds(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, min(float(value), MAX_RETRY_AFTER_SECONDS))
    except ValueError:
        return None  # HTTP-date form: ignore, use our own backoff


class HttpClient:
    """``requests`` wrapper that counts retries and remembers the last HTTP status."""

    def __init__(
        self,
        policy: RetryPolicy,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.policy = policy
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        self._sleep = sleep
        self.retry_count = 0
        self.last_status: int | None = None

    def wait_before_retry(self, retry_number: int, retry_after: float | None = None) -> None:
        """Sleep with exponential backoff; ``retry_number`` starts at 1."""
        delay = self.policy.delay(retry_number)
        if retry_after is not None:
            delay = max(delay, retry_after)
        LOG.warning("Retry %d/%d in %.1fs", retry_number, self.policy.max_retries, delay)
        self._sleep(delay)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        headers: Mapping[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        """Send a request; return a response with status 2xx, or raise.

        Raises ``HttpStatusError`` for non-retryable statuses and ``RetriesExhausted`` when the
        retry budget is spent. With ``stream=True`` the caller must close the response.
        """
        last_problem = "no attempt made"
        for attempt in range(self.policy.max_retries + 1):
            retry_after: float | None = None
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.policy.timeout_seconds,
                    stream=stream,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_problem = f"{type(exc).__name__}: {exc}"
                self.last_status = None
            else:
                self.last_status = response.status_code
                if 200 <= response.status_code < 300:
                    return response
                if response.status_code not in RETRYABLE_STATUS:
                    status = response.status_code
                    response.close()
                    raise HttpStatusError(status, url)
                last_problem = f"HTTP {response.status_code}"
                retry_after = _retry_after_seconds(response)
                response.close()

            if attempt >= self.policy.max_retries:
                break
            self.retry_count += 1
            self.wait_before_retry(attempt + 1, retry_after)

        raise RetriesExhausted(
            f"{method} {url} failed after {self.policy.max_retries + 1} attempts "
            f"(last problem: {last_problem})"
        )
