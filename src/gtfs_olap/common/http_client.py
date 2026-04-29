"""Współdzielone helpery HTTP z retry i eksponencjalnym backoffem.

Wydzielone z ckan.py — używane zarówno przez static ETL (pobieranie
paczek z CKAN) jak i RT ETL (pobieranie feedu protobuf).
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx
from loguru import logger

from gtfs_olap.common.errors import ETLError
from gtfs_olap.config.settings import (
    HTTP_DOWNLOAD_TIMEOUT_S,
    HTTP_MAX_RETRIES,
    HTTP_RETRY_DELAY_S,
    HTTP_TIMEOUT_S,
)


def get_with_retry(
    url: str,
    timeout: float = HTTP_TIMEOUT_S,
    max_retries: int = HTTP_MAX_RETRIES,
) -> httpx.Response:
    """GET z eksponencjalnym retry.

    Raises:
        ETLError: po wyczerpaniu prób
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(url, timeout=timeout, follow_redirects=True)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            last_exc = e
            if attempt < max_retries:
                wait = HTTP_RETRY_DELAY_S * (2 ** (attempt - 1))
                logger.warning(
                    f"Próba {attempt}/{max_retries} nieudana ({e}), "
                    f"czekam {wait:.0f}s"
                )
                time.sleep(wait)

    raise ETLError(
        f"Nieudane pobieranie {url} po {max_retries} próbach: {last_exc}"
    )


def stream_to_file(
    url: str,
    dest: Path,
    timeout: float | None = None,
    max_retries: int = HTTP_MAX_RETRIES,
) -> None:
    """Strumieniowe pobranie pliku do dysku z retry.

    Raises:
        ETLError: po wyczerpaniu prób
    """
    if timeout is None:
        timeout = HTTP_DOWNLOAD_TIMEOUT_S

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with httpx.stream(
                "GET", url,
                follow_redirects=True,
                timeout=timeout,
            ) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=64 * 1024):
                        f.write(chunk)
            return
        except (httpx.HTTPError, httpx.TimeoutException, OSError) as e:
            last_exc = e
            if attempt < max_retries:
                wait = HTTP_RETRY_DELAY_S * (2 ** (attempt - 1))
                logger.warning(
                    f"Pobranie {dest.name} - próba {attempt}/{max_retries} "
                    f"nieudana ({e}), czekam {wait:.0f}s"
                )
                # Usuń nieudany plik częściowy, żeby retry zaczął od zera
                dest.unlink(missing_ok=True)
                time.sleep(wait)

    raise ETLError(f"Nieudane pobranie {dest.name}: {last_exc}")