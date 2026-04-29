"""Wczytywanie i scalanie plików GTFS.

Każda paczka GZM jest osobno rozpakowywana i parsowana, potem wszystkie
DataFrame'y są scalane tabela-po-tabeli z deduplikacją po naturalnym kluczu.
"""

from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger

from gtfs_olap.common.errors import GtfsParseError
from gtfs_olap.config.gtfs_schema import DEDUP_KEYS, GTFS_FILES
from gtfs_olap.static_etl.ckan import read_feed_period
from gtfs_olap.static_etl.types import FeedMeta


def _load_single_package(extract_dir: Path) -> dict[str, pd.DataFrame]:
    """Wczytuje pliki GTFS z jednej rozpakowanej paczki.

    Wszystkie kolumny jako string, puste wartości → NA (świadoma konwersja
    typów odbywa się dopiero w transform.py).
    """
    out: dict[str, pd.DataFrame] = {}
    for key, fname in GTFS_FILES.items():
        path = extract_dir / fname
        if not path.exists():
            logger.warning(f"Brak pliku: {fname} (pomijam)")
            continue
        try:
            df = pd.read_csv(
                path, dtype=str, encoding="utf-8", keep_default_na=False
            )
        except Exception as e:
            raise GtfsParseError(f"Błąd wczytywania {fname}: {e}") from e

        df.columns = df.columns.str.strip()
        df = df.replace("", pd.NA)
        out[key] = df
        logger.info(f"Wczytano {fname}: {len(df):,} wierszy")
    return out


def _validate_critical_files(dfs: dict[str, pd.DataFrame], pkg_name: str) -> None:
    """Sprawdza, czy paczka ma wszystkie kluczowe pliki z danymi.

    Bez którejkolwiek z tych tabel rozkład nie może być sensownie zbudowany.
    """
    critical = ["agency", "routes", "stops", "trips", "stop_times", "calendar",
                "feed_info"]
    for key in critical:
        if key not in dfs:
            raise GtfsParseError(
                f"Paczka {pkg_name}: brak krytycznego pliku {GTFS_FILES[key]}"
            )
        if dfs[key].empty:
            raise GtfsParseError(
                f"Paczka {pkg_name}: plik {GTFS_FILES[key]} jest pusty"
            )


def load_and_merge_packages(
    zip_paths: list[Path], tmp_root: Path
) -> dict[str, pd.DataFrame]:
    """Wczytuje pliki GTFS z wielu paczek i scala je w jeden zestaw
    DataFrame'ów z deduplikacją per tabela.

    Args:
        zip_paths: lista paczek (posortowana chronologicznie po feed_start_date)
        tmp_root: katalog roboczy do rozpakowania (typowo tymczasowy)

    Returns:
        Słownik {nazwa_pliku_bez_rozszerzenia: scalony_dataframe}
    """
    logger.info(f"Wczytuję i scalam {len(zip_paths)} paczek...")

    # Rozpakowanie i wczytanie każdej paczki osobno
    per_pkg_dfs: list[dict[str, pd.DataFrame]] = []
    for i, zp in enumerate(zip_paths, start=1):
        sub = tmp_root / f"pkg_{i:02d}"
        sub.mkdir(exist_ok=True)
        try:
            with zipfile.ZipFile(zp) as zf:
                zf.extractall(sub)
        except zipfile.BadZipFile as e:
            raise GtfsParseError(f"Uszkodzony ZIP: {zp.name}: {e}") from e

        logger.info(f"  Paczka {i}/{len(zip_paths)}: {zp.name}")
        pkg_dfs = _load_single_package(sub)
        _validate_critical_files(pkg_dfs, zp.name)
        per_pkg_dfs.append(pkg_dfs)

    # Scalanie tabela-po-tabeli
    merged: dict[str, pd.DataFrame] = {}
    all_keys: set[str] = set()
    for d in per_pkg_dfs:
        all_keys.update(d.keys())

    for key in sorted(all_keys):  # posortowane dla powtarzalnych logów
        frames = [d[key] for d in per_pkg_dfs if key in d]
        if not frames:
            continue

        if key == "feed_info":
            # Bierzemy pierwszą - chronologicznie najwcześniejszą
            merged[key] = frames[0]
            continue

        combined = pd.concat(frames, ignore_index=True)
        before = len(combined)

        dedup_cols = DEDUP_KEYS.get(key, [])
        if dedup_cols and all(c in combined.columns for c in dedup_cols):
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")

        after = len(combined)
        if before != after:
            logger.info(
                f"  {key}: {before:,} → {after:,} wierszy "
                f"(usunięto {before - after:,} duplikatów)"
            )
        else:
            logger.info(f"  {key}: {after:,} wierszy")

        merged[key] = combined

    return merged


def build_feed_meta(
    zip_paths: list[Path],
    period_start: date | None = None,
    period_end: date | None = None,
) -> FeedMeta:
    """Buduje obiekt FeedMeta na bazie zestawu paczek.

    Args:
        zip_paths: lista paczek wchodzących w skład rozkładu
        period_start, period_end: jeśli podane, ograniczają okres
            (typowo: dziś, dziś + horizon - 1). Jeśli None - bierzemy
            min/max ze wszystkich paczek.
    """
    if period_start is None or period_end is None:
        starts = []
        ends = []
        for zp in zip_paths:
            s, e = read_feed_period(zp)
            starts.append(s)
            ends.append(e)
        period_start = period_start or min(starts)
        period_end = period_end or max(ends)

    package_names = [zp.name for zp in zip_paths]
    return FeedMeta(
        package_name=" | ".join(package_names),
        feed_start_date=period_start,
        feed_end_date=period_end,
    )