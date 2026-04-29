"""Static ETL: pobiera GTFS z GZM i ładuje do TimescaleDB.

Entry point - tylko orkiestracja, cała logika jest w pakiecie gtfs_olap/.

Sposób użycia:
    python scripts/run_static_etl.py
    python scripts/run_static_etl.py --db postgresql://user:pass@host:5432/gtfs_olap
    python scripts/run_static_etl.py --zip ./paczka1.zip --zip ./paczka2.zip
    python scripts/run_static_etl.py --horizon-days 7
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

from gtfs_olap.common.errors import ETLError
from gtfs_olap.common.logging_setup import setup_logging
from gtfs_olap.config.settings import DATABASE_URL
from gtfs_olap.db.connection import check_connection
from gtfs_olap.static_etl.ckan import find_packages_covering_period
from gtfs_olap.static_etl.extract import build_feed_meta, load_and_merge_packages
from gtfs_olap.static_etl.load import load_all
from gtfs_olap.static_etl.transform import (
    build_dim_data,
    build_dim_linia,
    build_dim_operator,
    build_dim_przystanek,
    build_schedule_lookup,
)

from loguru import logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=DATABASE_URL,
        help="URL połączenia do PostgreSQL/TimescaleDB",
    )
    parser.add_argument(
        "--zip",
        action="append",
        help=(
            "Ścieżka do lokalnej paczki ZIP (pomija pobieranie z CKAN). "
            "Można podać wielokrotnie żeby scalić kilka paczek."
        ),
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=14,
        help="Ile dni od dziś chcemy pokryć rozkładem (domyślnie 14)",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> None:
    """Główny pipeline ETL: 4 kroki z czytelnym podziałem."""

    # Krok 0: walidacja DB - lepiej fail-fast
    check_connection(args.db)

    with tempfile.TemporaryDirectory(prefix="gtfs_etl_") as tmp:
        tmp_path = Path(tmp)

        # Krok 1: zdobycie paczek
        if args.zip:
            zip_paths = [Path(p) for p in args.zip]
            for zp in zip_paths:
                if not zp.exists():
                    raise ETLError(f"Nie znaleziono lokalnej paczki: {zp}")
            logger.info(f"Używam {len(zip_paths)} lokalnych paczek")
        else:
            zip_paths = find_packages_covering_period(
                tmp_path, horizon_days=args.horizon_days
            )

        # Krok 2: wczytanie i scalenie
        merge_root = tmp_path / "merge"
        merge_root.mkdir()
        dfs = load_and_merge_packages(zip_paths, merge_root)

        # Okres ograniczony do horyzontu (dla --zip bierzemy pełny zakres)
        if args.zip:
            meta = build_feed_meta(zip_paths)
        else:
            today = date.today()
            target_end = today + timedelta(days=args.horizon_days - 1)
            meta = build_feed_meta(
                zip_paths,
                period_start=today,
                period_end=target_end,
            )
        logger.info(
            f"Scalony rozkład | okres: {meta.feed_start_date} → "
            f"{meta.feed_end_date} | paczki: {len(zip_paths)}"
        )

        # Krok 3: transformacje
        logger.info("Buduję wymiary i lookup_schedule...")
        dims = {
            "linia": build_dim_linia(dfs),
            "przystanek": build_dim_przystanek(dfs),
            "operator": build_dim_operator(dfs),
            "data": build_dim_data(dfs, meta),
        }
        lookup = build_schedule_lookup(dfs)

        for name, df in dims.items():
            logger.info(f"  dim_{name}: {len(df):,} wierszy")
        logger.info(f"  lookup_schedule: {len(lookup):,} wierszy")

        # Krok 4: load do bazy
        load_all(args.db, dims, lookup, meta)


def main() -> int:
    """Punkt wejścia z czytelną obsługą błędów."""
    setup_logging()
    args = parse_args()

    try:
        run(args)
    except ETLError as e:
        # "Oczekiwane" błędy ETL - wyświetlamy zwięzły komunikat
        logger.error(f"ETL przerwany: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Przerwano przez użytkownika (Ctrl+C)")
        return 130
    except Exception as e:
        # Nieoczekiwany błąd - pełny stack trace dla diagnozy
        logger.exception(f"Nieoczekiwany błąd: {e}")
        return 2

    logger.success("ETL zakończony pomyślnie")
    return 0


if __name__ == "__main__":
    sys.exit(main())
