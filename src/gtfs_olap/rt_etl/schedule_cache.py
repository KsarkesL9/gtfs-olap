"""In-memory cache lookup_schedule dla RT ETL.

Zamiast joinów RT z lookup_schedule per event (tysiące razy na snapshot),
trzymamy całą tablicę w słowniku (trip_id, stop_id, stop_sequence) → entry.
Cache jest ładowany na starcie i refreshowany gdy w gtfs_meta pojawi się
nowsza paczka GTFS.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import psycopg
from loguru import logger

from gtfs_olap.common.errors import DatabaseError
from gtfs_olap.config.settings import TZ_LOCAL


@dataclass
class ScheduleEntry:
    """Pojedynczy wpis w cache: rozkład dla pary (trip, przystanek, sequence)."""
    stop_id: str                  # potrzebne, bo RT GZM nie podaje stop_id
    sched_arrival: str | None     # HH:MM:SS, możliwe >24:00:00
    sched_departure: str | None
    linia_id: str | None
    operator_id: str | None
    kierunek: str | None


class ScheduleCache:
    """Cache rozkładu jazdy ładowany z lookup_schedule.

    Indeksowany dwoma sposobami, bo różne źródła różnie identyfikują
    przystanek na trasie:
    - po (trip_id, stop_id, stop_sequence) - pełny klucz, używany gdy
      mamy wszystkie trzy wartości
    - po (trip_id, stop_sequence) - GZM RT nie podaje stop_id w feedzie,
      więc wystarczy nam sequence (jest jednoznaczne w obrębie kursu)

    Refresh dzieje się gdy w gtfs_meta pojawi się wpis o nowej paczce
    (typowo po nocnym uruchomieniu static_etl).
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._by_full: dict[tuple[str, str, int], ScheduleEntry] = {}
        self._by_seq: dict[tuple[str, int], ScheduleEntry] = {}
        self._loaded_package: str | None = None

    def __len__(self) -> int:
        return len(self._by_full)

    def get(
        self, trip_id: str, stop_id: str, stop_sequence: int
    ) -> ScheduleEntry | None:
        """Lookup po pełnym kluczu (gdy stop_id jest dostępne)."""
        return self._by_full.get((trip_id, stop_id, stop_sequence))

    def get_by_seq(
        self, trip_id: str, stop_sequence: int
    ) -> ScheduleEntry | None:
        """Lookup po (trip_id, stop_sequence) - dla feedów RT GZM,
        które nie publikują stop_id."""
        return self._by_seq.get((trip_id, stop_sequence))

    def load(self) -> None:
        """Pełne przeładowanie cache z bazy."""
        logger.info("Ładuję schedule cache z lookup_schedule...")
        t0 = time.monotonic()

        try:
            with psycopg.connect(self._conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT package_name FROM gtfs_meta "
                        "ORDER BY loaded_at DESC LIMIT 1"
                    )
                    row = cur.fetchone()
                    self._loaded_package = row[0] if row else None

                new_full: dict[tuple[str, str, int], ScheduleEntry] = {}
                new_seq: dict[tuple[str, int], ScheduleEntry] = {}
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT trip_id, przystanek_id, stop_sequence,
                               rozkladowy_przyjazd, rozkladowy_odjazd,
                               linia_id, operator_id, kierunek
                        FROM lookup_schedule
                    """)
                    for trip_id, stop_id, seq, arr, dep, lin, op, kier in cur:
                        entry = ScheduleEntry(
                            stop_id=stop_id,
                            sched_arrival=arr,
                            sched_departure=dep,
                            linia_id=lin,
                            operator_id=op,
                            kierunek=kier,
                        )
                        new_full[(trip_id, stop_id, seq)] = entry
                        new_seq[(trip_id, seq)] = entry
        except psycopg.Error as e:
            raise DatabaseError(f"Nie udało się załadować cache: {e}") from e

        self._by_full = new_full
        self._by_seq = new_seq
        elapsed = time.monotonic() - t0
        logger.info(
            f"Cache: {len(self._by_full):,} wpisów (full) + "
            f"{len(self._by_seq):,} (by_seq) "
            f"(paczka: {self._loaded_package}, {elapsed:.1f}s)"
        )

    def needs_refresh(self) -> bool:
        """Sprawdza, czy w bazie pojawiła się nowsza paczka GTFS."""
        try:
            with psycopg.connect(self._conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT package_name FROM gtfs_meta "
                        "ORDER BY loaded_at DESC LIMIT 1"
                    )
                    row = cur.fetchone()
                    latest = row[0] if row else None
            return latest is not None and latest != self._loaded_package
        except Exception as e:
            logger.warning(f"Nie udało się sprawdzić wersji paczki: {e}")
            return False


# ============================================================================
# Konwersja czasu GTFS → datetime
# ============================================================================

def gtfs_time_to_datetime(time_str: str, service_date: date) -> datetime:
    """Konwertuje czas rozkładowy GTFS (HH:MM:SS, możliwe >24h) na datetime
    w strefie czasowej Europe/Warsaw.

    GTFS pozwala na czasy >24:00:00 dla kursów przekraczających północ
    (np. nocnych) - takie czasy interpretujemy jako kolejny dzień.

    Przykład:
        gtfs_time_to_datetime("25:30:00", date(2026, 4, 27))
        → datetime(2026, 4, 28, 1, 30, 0, tzinfo=Europe/Warsaw)
    """
    h, m, s = (int(x) for x in time_str.split(":"))
    extra_days, h = divmod(h, 24)
    base = datetime(
        service_date.year, service_date.month, service_date.day,
        tzinfo=TZ_LOCAL,
    )
    return base + timedelta(days=extra_days, hours=h, minutes=m, seconds=s)
