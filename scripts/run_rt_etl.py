"""RT ETL: pobiera GTFS-RT TripUpdates z GZM/ZTM i ładuje opóźnienia
do hypertable fakt_opoznienia w TimescaleDB.

Entry point - tylko orkiestracja, cała logika jest w pakiecie gtfs_olap/.

Przed pierwszym uruchomieniem upewnij się, że:
1. run_static_etl.py został uruchomiony (są wymiary i lookup_schedule)
2. TimescaleDB jest dostępna (docker compose up -d)

Sposób użycia:
    python scripts/run_rt_etl.py
    python scripts/run_rt_etl.py --interval 30      # poll co 30 sekund
    python scripts/run_rt_etl.py --once             # jeden cykl i koniec (do testów)

Czyste zatrzymanie: Ctrl+C (kończy bieżącą iterację i wychodzi).
"""

from __future__ import annotations

import argparse
import sys

from gtfs_olap.common.errors import ETLError
from gtfs_olap.common.logging_setup import setup_logging
from gtfs_olap.config.settings import CACHE_CHECK_EVERY, DATABASE_URL, POLL_INTERVAL_S
from gtfs_olap.db.connection import check_connection
from gtfs_olap.rt_etl.runner import run_loop

from loguru import logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=DATABASE_URL,
        help="URL połączenia do PostgreSQL/TimescaleDB",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=POLL_INTERVAL_S,
        help=f"Interwał pollingu w sekundach (domyślnie {POLL_INTERVAL_S})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Przetworzy jeden snapshot i wyjdzie (do testów)",
    )
    parser.add_argument(
        "--cache-check-every",
        type=int,
        default=CACHE_CHECK_EVERY,
        help="Co ile iteracji sprawdzać, czy jest nowsza paczka GTFS",
    )
    return parser.parse_args()


def main() -> int:
    """Punkt wejścia z czytelną obsługą błędów."""
    setup_logging()
    args = parse_args()

    logger.info(
        f"RT ETL start | DB: {args.db.split('@')[-1]} | "
        f"interval={args.interval}s | once={args.once}"
    )

    try:
        check_connection(args.db)
        run_loop(
            conn_str=args.db,
            interval_s=args.interval,
            once=args.once,
            cache_check_every=args.cache_check_every,
        )
    except ETLError as e:
        logger.error(f"RT ETL przerwany: {e}")
        return 1
    except KeyboardInterrupt:
        # ShutdownFlag łapie sygnały, ale gdyby coś przeszło niżej...
        logger.warning("Przerwano przez użytkownika (Ctrl+C)")
        return 130
    except Exception as e:
        logger.exception(f"Nieoczekiwany błąd: {e}")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
