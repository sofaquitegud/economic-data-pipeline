# Import libraries
import time
from typing import Any, Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class APIClient:
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    @staticmethod
    def _is_retryable(exception: requests.RequestException) -> bool:
        if isinstance(exception, (requests.Timeout, requests.ConnectionError)):
            return True
        if hasattr(exception, "response") and exception.response is not None:
            return exception.response.status_code in (429, 500, 502, 503, 504)
        return False

    def fetch(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> tuple[Any, int]:
        """Fetch data from API with retry logic.

        Returns:
            Tuple of (response_data, http_status_code)
        """
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json(), response.status_code
            except requests.RequestException as e:
                logger.warning(
                    "Attempt %d/%d failed: %s",
                    attempt + 1,
                    self.max_retries,
                    e,
                )
                if attempt < self.max_retries - 1 and self._is_retryable(e):
                    delay = self.retry_delay * (2**attempt)
                    logger.info("Retrying in %.1fs...", delay)
                    time.sleep(delay)
                else:
                    raise
        raise RuntimeError("max_retries must be > 0")
