"""RT ETL: pobiera GTFS-RT TripUpdates i ładuje opóźnienia do hypertable."""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone

import httpx
import psycopg
from google.transit import gtfs_realtime_pb2
from loguru import logger

from gtfs_olap.config import DB_URL, DDL, RT_URL, TZ


# ============================================================================
# Cache rozkładu w pamięci
# ============================================================================
#
# UWAGA. Trzymamy cały lookup_schedule w słowniku w pamięci procesu bo:
# 1. Jest go 1.4 mln wierszy ale to około 200 MB więc żaden problem.
# 2. Każde TripUpdate z RT wymaga lookupu rozkładowego czasu. Robić to per
#    wiersz przez SELECT to byłoby 1000+ roundtripów do DB co snapshot.
# 3. RT GZM publikuje TripUpdaty BEZ stop_id - tylko trip_id i sequence.
#    Zwykły lookup po (trip, stop_id, seq) by się nie udał. Indeksujemy więc
#    po (trip_id, stop_sequence) bo to jest jednoznaczne w obrębie kursu,
#    a stop_id wyciągamy z cache. Sprawdzone empirycznie skryptem
#    diagnostycznym - bez tego wszystkie TripUpdaty kończą jako "unknown".

@dataclass
class ScheduleEntry:
    stop_id: str
    sched_arrival: str | None
    linia_id: str | None
    operator_id: str | None
    kierunek: str | None


class ScheduleCache:
    def __init__(self):
        self._cache: dict[tuple[str, int], ScheduleEntry] = {}

    def __len__(self):
        return len(self._cache)

    def get(self, trip_id: str, seq: int) -> ScheduleEntry | None:
        return self._cache.get((trip_id, seq))

    def load(self):
        logger.info("Ładuję schedule cache...")
        t0 = time.monotonic()
        with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT trip_id, przystanek_id, stop_sequence, rozkladowy_przyjazd,
                       linia_id, operator_id, kierunek
                FROM lookup_schedule
            """)
            self._cache = {
                (trip_id, seq): ScheduleEntry(stop_id, arr, lin, op, kier)
                for trip_id, stop_id, seq, arr, lin, op, kier in cur
            }
        logger.info(f"Cache: {len(self._cache):,} wpisów ({time.monotonic() - t0:.1f}s)")


# ============================================================================
# Główna pętla RT
# ============================================================================
#
# UWAGA. RT GZM publikuje MINIMALNE TripUpdate'y:
# - brak start_date -> zakładamy dziś w strefie Europe/Warsaw
# - brak stop_id -> lookup po (trip_id, sequence), stop_id z cache
# - brak fallbacków na arrival.time/departure.* -> tylko arrival.delay
# Sprawdzone empirycznie. Każdy snapshot to około 1000 updateów.

def _process_feed(feed, cache: ScheduleCache) -> list[tuple]:
    """Zamienia FeedMessage na listę krotek do COPY."""
    snapshot_dt = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    today = datetime.now(tz=TZ).date()
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        if not trip_id:
            continue

        for stu in tu.stop_time_update:
            if not stu.HasField("stop_sequence"):
                continue
            entry = cache.get(trip_id, stu.stop_sequence)
            if entry is None:
                continue
            if not stu.HasField("arrival") or not stu.arrival.HasField("delay"):
                continue

            rows.append((
                snapshot_dt, trip_id, entry.stop_id, stu.stop_sequence,
                entry.linia_id, entry.operator_id, entry.kierunek,
                today, stu.arrival.delay,
            ))
    return rows


def _insert_rows(conn, rows: list[tuple]):
    """Bulk insert z deduplikacją: COPY do TEMP -> INSERT ON CONFLICT DO NOTHING."""
    if not rows:
        return 0
    cols = ("ts", "trip_id", "przystanek_id", "stop_sequence", "linia_id",
            "operator_id", "kierunek", "data_kursu", "opoznienie_s")

    buf = io.StringIO()
    for row in rows:
        csv_row = []
        for v in row:
            if v is None:
                csv_row.append("")
            elif isinstance(v, (datetime, date)):
                csv_row.append(v.isoformat())
            else:
                s = str(v)
                if "," in s or '"' in s:
                    s = '"' + s.replace('"', '""') + '"'
                csv_row.append(s)
        buf.write(",".join(csv_row) + "\n")
    buf.seek(0)

    cols_sql = ",".join(cols)
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _stg "
            "(LIKE fakt_opoznienia INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
        )
        with cur.copy(f"COPY _stg ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL '')") as cp:
            while chunk := buf.read(64 * 1024):
                cp.write(chunk)
        cur.execute(
            f"INSERT INTO fakt_opoznienia ({cols_sql}) "
            f"SELECT {cols_sql} FROM _stg ON CONFLICT DO NOTHING"
        )
        n = cur.rowcount
    conn.commit()
    return n


def run_loop(interval_s: int = 20, once: bool = False):
    """Główna pętla pollingu. Ctrl+C kończy."""
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    cache = ScheduleCache()
    cache.load()

    last_snapshot_ts = 0
    conn = psycopg.connect(DB_URL)
    try:
        while True:
            t_start = time.monotonic()
            try:
                raw = httpx.get(RT_URL, timeout=30.0).content
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(raw)

                if feed.header.timestamp <= last_snapshot_ts:
                    logger.debug("Snapshot już przetworzony, pomijam")
                else:
                    rows = _process_feed(feed, cache)
                    n = _insert_rows(conn, rows) if rows else 0
                    last_snapshot_ts = feed.header.timestamp

                    snap = datetime.fromtimestamp(
                        feed.header.timestamp, tz=timezone.utc
                    ).strftime("%H:%M:%S")
                    logger.info(
                        f"snap={snap} obs={len(rows):,} ins={n:,} "
                        f"t={time.monotonic() - t_start:.2f}s"
                    )
            except Exception as e:
                logger.error(f"Iteracja nieudana: {e}")

            if once:
                break
            time.sleep(interval_s)
    finally:
        conn.close()