"""Parametry konfiguracyjne ETL, ładowane ze zmiennych środowiskowych.

Zmienne można ustawić w pliku .env (ręcznie, bez python-dotenv)
lub wyeksportować w shellu. Jeśli nie są ustawione, używane są
domyślne wartości - wystarczające dla docker-compose z tego repo.
"""

from __future__ import annotations

import os
from zoneinfo import ZoneInfo

# ============================================================================
# Baza danych
# ============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/gtfs_olap",
)

# ============================================================================
# Źródła danych — CKAN (statyczny GTFS)
# ============================================================================

CKAN_DATASET_ID = (
    "rozklady-jazdy-i-lokalizacja-przystankow-gtfs-wersja-rozszerzona"
)
CKAN_API_URL = (
    f"https://otwartedane.metropoliagzm.pl/api/3/action/package_show"
    f"?id={CKAN_DATASET_ID}"
)

# Wzorzec nazwy paczek: schedule_ZTM_2026.04.27_9487_0048.zip
# (data wytworzenia, numer kolejny, godzina rozpoczęcia tworzenia)
ZIP_NAME_PATTERN = (
    r"schedule_ZTM_(\d{4})\.(\d{2})\.(\d{2})_(\d+)_(\d+)\.zip"
)

# ============================================================================
# Źródło GTFS-RT
# ============================================================================

GTFS_RT_URL = os.environ.get(
    "GTFS_RT_URL",
    "https://gtfsrt.transportgzm.pl:5443/gtfsrt/gzm/tripUpdates",
)

# Strefa czasowa GZM — GTFS rozkładowy publikuje czasy w czasie lokalnym
TZ_LOCAL = ZoneInfo("Europe/Warsaw")

# ============================================================================
# Parametry sieciowe
# ============================================================================

HTTP_TIMEOUT_S = 30.0            # standardowy timeout (np. CKAN API)
HTTP_DOWNLOAD_TIMEOUT_S = 120.0  # dłuższy dla pobierania ZIP-ów
HTTP_MAX_RETRIES = 3
HTTP_RETRY_DELAY_S = 2.0
RT_HTTP_TIMEOUT_S = 30.0         # timeout feedu RT

# ============================================================================
# Zachowanie pętli RT
# ============================================================================

POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "20"))
CACHE_CHECK_EVERY = 20  # co N iteracji sprawdzamy nową paczkę GTFS
