"""Pętla główna RT ETL z obsługą sygnałów i reconnectem bazy."""

from __future__ import annotations

import signal
import sys
import time
from datetime import datetime, timezone

import psycopg
from loguru import logger

from gtfs_olap.config.ddl import DDL_FACT
from gtfs_olap.common.errors import DatabaseError, ETLError
from gtfs_olap.rt_etl.pipeline import IterationStats, run_iteration
from gtfs_olap.rt_etl.schedule_cache import ScheduleCache


# ============================================================================
# Schemat - tworzymy hypertable jeśli jeszcze nie ma
# ============================================================================

def ensure_fact_schema(conn_str: str) -> None:
    """Tworzy fakt_opoznienia i hypertable jeśli nie istnieją."""
    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(DDL_FACT)
            conn.commit()
    except psycopg.Error as e:
        raise DatabaseError(f"Błąd tworzenia schematu faktu: {e}") from e
    logger.info("fakt_opoznienia / hypertable: gotowe")


# ============================================================================
# Obsługa sygnałów — czyste wyjście z pętli
# ============================================================================

class ShutdownFlag:
    """Łapie SIGINT/SIGTERM i pozwala na czyste wyjście z pętli."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame) -> None:
        if not self.requested:
            logger.warning(
                "Otrzymano sygnał zamknięcia, kończę bieżącą iterację..."
            )
            self.requested = True
        else:
            logger.error("Drugi sygnał - wymuszam wyjście")
            sys.exit(1)


def _sleep_with_check(seconds: float, shutdown: ShutdownFlag) -> None:
    """Sleep, ale sprawdza co 0.5s flagę shutdown."""
    end = time.monotonic() + seconds
    while time.monotonic() < end and not shutdown.requested:
        time.sleep(min(0.5, end - time.monotonic()))


# ============================================================================
# Pętla główna
# ============================================================================

def run_loop(
    conn_str: str,
    interval_s: int,
    once: bool,
    cache_check_every: int,
) -> None:
    """Główna pętla pollingu RT ETL.

    Args:
        conn_str: URL bazy
        interval_s: czas między iteracjami
        once: jeśli True, wykonuje 1 iterację i wychodzi (do testów)
        cache_check_every: co N iteracji sprawdzać nową paczkę GTFS
    """
    shutdown = ShutdownFlag()
    ensure_fact_schema(conn_str)

    cache = ScheduleCache(conn_str)
    cache.load()
    if len(cache) == 0:
        raise ETLError(
            "Cache jest pusty - czy uruchomiłeś run_static_etl.py? "
            "Bez lookup_schedule nie liczę opóźnień."
        )

    last_snapshot_ts = 0
    iteration = 0

    conn = psycopg.connect(conn_str)
    try:
        while not shutdown.requested:
            iteration += 1
            t_start = time.monotonic()
            stats = IterationStats()

            try:
                # Periodyczny refresh cache
                if iteration % cache_check_every == 0 and cache.needs_refresh():
                    logger.info("Wykryto nową paczkę GTFS - refresh cache")
                    cache.load()

                stats, last_snapshot_ts = run_iteration(
                    conn, cache, last_snapshot_ts
                )

                elapsed = time.monotonic() - t_start
                snap_str = (
                    datetime.fromtimestamp(stats.snapshot_ts, tz=timezone.utc)
                    .strftime("%H:%M:%S")
                    if stats.snapshot_ts else "—"
                )
                logger.info(
                    f"snap={snap_str} "
                    f"updates={stats.n_trip_updates:,} "
                    f"obs={stats.n_observations:,} "
                    f"ins={stats.n_inserted:,} "
                    f"unk={stats.n_unknown_trips:,} "
                    f"noseq={stats.n_no_sequence:,} "
                    f"nodelay={stats.n_no_delay:,} "
                    f"t={elapsed:.2f}s"
                )

            except ETLError as e:
                logger.error(f"Iteracja nieudana: {e}")
            except psycopg.Error as e:
                logger.error(
                    f"Błąd bazy: {e} - próbuję odświeżyć połączenie"
                )
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = psycopg.connect(conn_str)
                except psycopg.Error as e2:
                    logger.error(
                        f"Nie udało się odzyskać połączenia: {e2}"
                    )
            except Exception as e:
                logger.exception(f"Nieoczekiwany błąd w iteracji: {e}")

            if once:
                break

            if not shutdown.requested:
                _sleep_with_check(interval_s, shutdown)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.success("RT ETL zatrzymany")
