"""Przetwarzanie feedu RT i insert do hypertable fakt_opoznienia.

Idempotentność na dwóch poziomach:
1. In-memory: pamiętamy ostatni snapshot.timestamp i pomijamy duplikaty
2. DB: PRIMARY KEY na fakcie + ON CONFLICT DO NOTHING przy insercie
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date, datetime, timezone

import psycopg
from google.transit import gtfs_realtime_pb2
from loguru import logger

from gtfs_olap.config.gtfs_schema import TABLE_COLUMNS
from gtfs_olap.config.settings import TZ_LOCAL
from gtfs_olap.common.errors import DatabaseError
from gtfs_olap.rt_etl.feed import extract_delay, fetch_feed, parse_feed
from gtfs_olap.rt_etl.schedule_cache import ScheduleCache, gtfs_time_to_datetime


# ============================================================================
# Statystyki iteracji
# ============================================================================

@dataclass
class IterationStats:
    """Liczniki dla pojedynczego cyklu pollingu - do logowania."""
    snapshot_ts: int = 0
    n_trip_updates: int = 0
    n_observations: int = 0
    n_unknown_trips: int = 0
    n_no_sequence: int = 0
    n_no_delay: int = 0
    n_inserted: int = 0


# ============================================================================
# Przetwarzanie feedu
# ============================================================================

def process_feed(
    feed: gtfs_realtime_pb2.FeedMessage,
    cache: ScheduleCache,
    stats: IterationStats,
) -> list[tuple]:
    """Iteruje po wszystkich TripUpdate w feedzie i buduje listę wierszy
    do wstawienia do fakt_opoznienia.

    Specyfika GZM RT:
    - feed publikuje MINIMALNE TripUpdate'y: tylko trip_id i stop_sequence
    - brak start_date (zakładamy "dziś" w lokalnej strefie czasowej)
    - brak stop_id w stop_time_update (lookup po sequence wystarcza,
      bo sequence jest jednoznaczne w obrębie kursu)

    Wartości w wierszach mają kolejność zgodną z TABLE_COLUMNS["fakt_opoznienia"].
    """
    snapshot_dt = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    rows: list[tuple] = []

    # Service_date dla feedów GZM, które nie publikują trip.start_date.
    # Bierzemy "dziś" w strefie lokalnej - feed RT nadaje aktualne kursy.
    fallback_service_date = datetime.now(tz=TZ_LOCAL).date()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip
        trip_id = trip.trip_id
        if not trip_id:
            continue

        # Service date - jeśli brak w feedzie (GZM), bierzemy dziś
        if trip.start_date:
            try:
                service_date = datetime.strptime(
                    trip.start_date, "%Y%m%d"
                ).date()
            except ValueError:
                service_date = fallback_service_date
        else:
            service_date = fallback_service_date

        stats.n_trip_updates += 1

        for stu in tu.stop_time_update:
            if not stu.HasField("stop_sequence"):
                stats.n_no_sequence += 1
                continue
            stop_sequence = stu.stop_sequence

            # Lookup po (trip_id, sequence) - GZM nie podaje stop_id,
            # ale (trip_id, sequence) jest jednoznaczne w lookup_schedule
            entry = cache.get_by_seq(trip_id, stop_sequence)
            if entry is None:
                stats.n_unknown_trips += 1
                continue

            # Złóż datetime'y rozkładowe
            sched_arr_dt = (
                gtfs_time_to_datetime(entry.sched_arrival, service_date)
                if entry.sched_arrival else None
            )
            sched_dep_dt = (
                gtfs_time_to_datetime(entry.sched_departure, service_date)
                if entry.sched_departure else None
            )

            # Opóźnienie z fallbackami
            result = extract_delay(stu, sched_arr_dt, sched_dep_dt)
            if result is None:
                stats.n_no_delay += 1
                continue
            delay_s, typ_zdarzenia, zrodlo = result

            rows.append((
                snapshot_dt,
                trip_id,
                entry.stop_id,            # z cache, bo RT GZM tego nie podaje
                stop_sequence,
                entry.linia_id,
                entry.operator_id,
                entry.kierunek,
                service_date,
                sched_arr_dt,
                delay_s,
                typ_zdarzenia,
                zrodlo,
            ))
            stats.n_observations += 1

    return rows


# ============================================================================
# Insert do hypertable
# ============================================================================

def insert_rows(conn: psycopg.Connection, rows: list[tuple]) -> int:
    """Bulk-load wierszy z deduplikacją.

    Wzorzec: COPY do tabeli TEMP, potem INSERT...SELECT...ON CONFLICT
    DO NOTHING. Łączy szybkość COPY z bezpieczeństwem upsertu.
    """
    if not rows:
        return 0

    cols = TABLE_COLUMNS["fakt_opoznienia"]
    cols_sql = ", ".join(cols)

    # Bufor CSV dla COPY
    buf = io.StringIO()
    for row in rows:
        csv_row = []
        for val in row:
            if val is None:
                csv_row.append("")
            elif isinstance(val, datetime):
                csv_row.append(val.isoformat())
            elif isinstance(val, date):
                csv_row.append(val.isoformat())
            else:
                s = str(val)
                if "," in s or '"' in s or "\n" in s:
                    s = '"' + s.replace('"', '""') + '"'
                csv_row.append(s)
        buf.write(",".join(csv_row) + "\n")
    buf.seek(0)

    try:
        with conn.cursor() as cur:
            # Tabela tymczasowa o strukturze faktu
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS _stg_opoznienia
                    (LIKE fakt_opoznienia INCLUDING DEFAULTS)
                ON COMMIT DELETE ROWS
            """)

            # COPY do staging
            copy_sql = (
                f"COPY _stg_opoznienia ({cols_sql}) FROM STDIN "
                f"WITH (FORMAT CSV, NULL '')"
            )
            with cur.copy(copy_sql) as cp:
                while chunk := buf.read(64 * 1024):
                    cp.write(chunk)

            # INSERT z dedup
            cur.execute(f"""
                INSERT INTO fakt_opoznienia ({cols_sql})
                SELECT {cols_sql} FROM _stg_opoznienia
                ON CONFLICT DO NOTHING
            """)
            n_inserted = cur.rowcount
        conn.commit()
    except psycopg.Error as e:
        raise DatabaseError(f"Błąd insertu do fakt_opoznienia: {e}") from e

    return n_inserted


# ============================================================================
# Pojedyncza iteracja: fetch → parse → process → insert
# ============================================================================

def run_iteration(
    conn: psycopg.Connection,
    cache: ScheduleCache,
    last_snapshot_ts: int,
) -> tuple[IterationStats, int]:
    """Pojedyncza iteracja: fetch → parse → process → insert.

    Returns:
        (stats, nowy_last_snapshot_ts)
    """
    stats = IterationStats()
    raw = fetch_feed()
    feed = parse_feed(raw)
    stats.snapshot_ts = feed.header.timestamp

    if stats.snapshot_ts <= last_snapshot_ts:
        # Ten sam snapshot co poprzednio - pomijamy bez zapytania do DB
        logger.debug(f"Snapshot {stats.snapshot_ts} już przetworzony, pomijam")
        return stats, last_snapshot_ts

    rows = process_feed(feed, cache, stats)
    if rows:
        stats.n_inserted = insert_rows(conn, rows)
    return stats, stats.snapshot_ts
