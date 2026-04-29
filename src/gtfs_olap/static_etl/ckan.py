"""Interakcja z CKAN API udostępnianym przez Metropolię GZM.

Wybór paczek GTFS jest tu nietrywialny, bo:
1. Każda paczka pokrywa tylko wycinek kalendarza (1-10 dni)
2. Numery sekwencyjne w nazwach paczek NIE odpowiadają datom obowiązywania
3. Trzeba zaglądać do feed_info.txt żeby poznać prawdziwy okres

Stąd dwustopniowy proces: list_candidates() → find_packages_covering_period().
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

from loguru import logger

from gtfs_olap.common.errors import CkanError, NoMatchingPackage
from gtfs_olap.common.http_client import get_with_retry, stream_to_file
from gtfs_olap.config.settings import CKAN_API_URL, ZIP_NAME_PATTERN

ZIP_NAME_RE = re.compile(ZIP_NAME_PATTERN, re.IGNORECASE)


# ============================================================================
# Listowanie i wybór paczek
# ============================================================================

def list_candidates() -> list[tuple[tuple[int, int, int, int], str, str]]:
    """Pobiera z CKAN API listę paczek ZIP, posortowaną od najnowszej.

    Returns:
        Lista trójek ((rok, miesiąc, dzień, numer_seq), url, nazwa).
        Posortowana malejąco po sort_key.
    """
    logger.info("Odpytuję CKAN API o listę dostępnych paczek...")
    try:
        resp = get_with_retry(CKAN_API_URL)
        data = resp.json()
    except Exception as e:
        raise CkanError(f"Nie udało się pobrać listy paczek z CKAN: {e}")

    if not data.get("success"):
        raise CkanError(f"CKAN zwrócił błąd: {data}")

    candidates = []
    for r in data.get("result", {}).get("resources", []):
        name = r.get("name", "")
        m = ZIP_NAME_RE.match(name)
        if not m:
            continue
        year, month, day, seq, _hour = m.groups()
        sort_key = (int(year), int(month), int(day), int(seq))
        candidates.append((sort_key, r["url"], name))

    if not candidates:
        raise CkanError("W zbiorze CKAN nie znaleziono żadnej paczki ZIP")

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates


def read_feed_period(zip_path: Path) -> tuple[date, date]:
    """Wyciąga feed_start_date i feed_end_date z feed_info.txt
    bez rozpakowywania całego ZIP-a.

    Używamy modułu csv żeby poprawnie obsłużyć pola w cudzysłowach
    (GZM publikuje wartości jako "20260428").
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("feed_info.txt") as f:
                content = f.read().decode("utf-8-sig")  # -sig usuwa BOM

        reader = csv.DictReader(io.StringIO(content))
        record = next(reader)
        start = datetime.strptime(record["feed_start_date"], "%Y%m%d").date()
        end = datetime.strptime(record["feed_end_date"], "%Y%m%d").date()
        return start, end
    except (KeyError, ValueError, zipfile.BadZipFile, StopIteration) as e:
        raise CkanError(
            f"Nie odczytałem feed_info.txt z {zip_path.name}: {e}"
        ) from e


def find_packages_covering_period(
    dest_dir: Path,
    today: date | None = None,
    horizon_days: int = 14,
) -> list[Path]:
    """Pobiera wszystkie paczki potrzebne do pokrycia okresu
    [today, today + horizon_days].

    Algorytm:
    1. Pobierz listę kandydatów z CKAN (od najnowszego)
    2. Dla każdego ściągnij ZIP, odczytaj feed_info.txt
    3. Jeśli paczka pokrywa cokolwiek z naszego horyzontu - zachowaj
    4. Pomijaj paczki "spoza okresu" lub redundantne (już mamy te dni)
    5. Zatrzymaj się, gdy mamy pokryty cały okres

    Args:
        dest_dir: katalog na ZIP-y (typowo tymczasowy)
        today: data referencyjna (domyślnie dziś)
        horizon_days: liczba dni do pokrycia (domyślnie 14)

    Returns:
        Lista ścieżek do pobranych paczek, posortowana po feed_start_date.
    """
    if today is None:
        today = date.today()
    target_end = today + timedelta(days=horizon_days - 1)
    target_days: set[date] = {
        today + timedelta(days=i) for i in range(horizon_days)
    }

    candidates = list_candidates()
    logger.info(
        f"Szukam paczek pokrywających {today} → {target_end} "
        f"({len(candidates)} kandydatów na CKAN)"
    )

    selected: list[tuple[date, Path]] = []
    covered_days: set[date] = set()

    for _sort_key, url, name in candidates:
        if covered_days >= target_days:
            logger.info("Cały okres pokryty, kończę pobieranie")
            break

        zip_path = dest_dir / name
        logger.info(f"Sprawdzam: {name}")

        try:
            stream_to_file(url, zip_path)
        except Exception as e:
            logger.warning(f"Pomijam {name}: {e}")
            continue

        try:
            start, end = read_feed_period(zip_path)
        except CkanError as e:
            logger.warning(f"{e} (pomijam)")
            zip_path.unlink(missing_ok=True)
            continue

        # Dni z tej paczki, które nas interesują
        pkg_days = {
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
            if today <= start + timedelta(days=i) <= target_end
        }

        if not pkg_days:
            logger.info(f"  okres {start} → {end} - poza horyzontem, pomijam")
            zip_path.unlink(missing_ok=True)
            continue

        new_days = pkg_days - covered_days
        if not new_days:
            logger.info(f"  okres {start} → {end} - już pokryty, pomijam")
            zip_path.unlink(missing_ok=True)
            continue

        size_mb = zip_path.stat().st_size / 1024 / 1024
        logger.success(
            f"  okres {start} → {end} - dokłada {len(new_days)} dni "
            f"({size_mb:.1f} MB)"
        )
        selected.append((start, zip_path))
        covered_days |= pkg_days

    if not selected:
        raise NoMatchingPackage(
            f"Nie znalazłem żadnej paczki pokrywającej okres "
            f"{today} → {target_end}. Sprawdź, czy CKAN GZM jest dostępny."
        )

    missing = target_days - covered_days
    if missing:
        logger.warning(
            f"Niepokryte dni: {sorted(missing)} "
            f"(GZM nie opublikował jeszcze paczek dla tych dni)"
        )

    selected.sort(key=lambda t: t[0])  # chronologicznie po starcie
    logger.info(
        f"Wybrano {len(selected)} paczek pokrywających "
        f"{len(covered_days)}/{horizon_days} dni"
    )
    return [path for _start, path in selected]
