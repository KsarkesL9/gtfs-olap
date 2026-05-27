"""RT ETL: pobiera GTFS-RT TripUpdates i ładuje opóźnienia do hypertable."""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import httpx
import psycopg
from google.transit import gtfs_realtime_pb2
from loguru import logger

from gtfs_olap.config import DB_URL, DDL, RT_URL, TZ

TRIP_CANCELED = gtfs_realtime_pb2.TripDescriptor.CANCELED
STOP_SKIPPED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED


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
def _is_alive(conn) -> bool:
    """Szybki sanity check connection. Zwraca True jeśli można pisać do DB."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False


def _connect_with_retry(max_backoff_s: int = 60):
    """Połączenie z DB z exponential backoff. Próbuje w nieskończoność.

    Backoff: 5s, 10s, 20s, 40s, 60s, 60s, 60s... do skutku.
    Każdą próbę logujemy na WARN, sukces na INFO."""
    backoff = 5
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = psycopg.connect(DB_URL, connect_timeout=10)
            if attempt > 1:
                logger.info(f"DB połączone po {attempt} próbach")
            return conn
        except Exception as e:
            logger.warning(f"DB connect próba {attempt} nieudana: {e}. "
                           f"Czekam {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff_s)

@dataclass
class ScheduleEntry:
    stop_id: str
    sched_arrival: str | None
    linia_id: str | None
    operator_id: str | None
    kierunek: str | None
    offset_dnia: int


class ScheduleCache:
    def __init__(self):
        self._cache: dict[tuple[str, int], ScheduleEntry] = {}
        self._by_trip: dict[str, list[tuple[int, ScheduleEntry]]] = {}
        self.wersja_id: int | None = None

    def __len__(self):
        return len(self._cache)

    def get(self, trip_id: str, seq: int) -> ScheduleEntry | None:
        return self._cache.get((trip_id, seq))

    def get_all_stops(self, trip_id: str) -> list[tuple[int, ScheduleEntry]]:
        """Wszystkie zatrzymania kursu - używane przy obsłudze anulacji."""
        return self._by_trip.get(trip_id, [])
    
    def current_version_in_db(self, conn) -> int | None:
        """Sprawdza, czy w DB jest nowsza wersja niż nasz cache.
        Zwraca wersja_id z DB, lub None jeśli nie udało się sprawdzić."""
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT wersja_id FROM dim_wersja_rozkladu "
                    "ORDER BY zaladowano DESC LIMIT 1"
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def load(self):
        logger.info("Ładuję schedule cache...")
        t0 = time.monotonic()
        with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT wersja_id, obowiazuje_od, obowiazuje_do FROM dim_wersja_rozkladu "
                "ORDER BY zaladowano DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(
                    "Brak wersji rozkładu w dim_wersja_rozkladu. "
                    "Uruchom najpierw run_static_etl.py."
                )
            self.wersja_id, od, do = row
            logger.info(f"Aktywna wersja rozkładu: {self.wersja_id} ({od} → {do})")

            cur.execute("""
                SELECT trip_id, przystanek_id, stop_sequence, rozkladowy_przyjazd,
                       linia_id, operator_id, kierunek, offset_dnia
                FROM lookup_schedule WHERE wersja_id = %s
            """, (self.wersja_id,))
            self._cache = {}
            self._by_trip = {}
            for trip_id, stop_id, seq, arr, lin, op, kier, off in cur:
                entry = ScheduleEntry(stop_id, arr, lin, op, kier, off)
                self._cache[(trip_id, seq)] = entry
                self._by_trip.setdefault(trip_id, []).append((seq, entry))
        logger.info(f"Cache: {len(self._cache):,} wpisów, "
                    f"{len(self._by_trip):,} kursów ({time.monotonic() - t0:.1f}s)")


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
    """Zamienia FeedMessage na listę krotek do COPY.

    Trzy ścieżki:
    - Kurs CANCELED: wiersze dla wszystkich planowanych zatrzymań (status='ANULOWANY').
    - Przystanek SKIPPED: pojedynczy wiersz (status='POMINIETY').
    - Normalna obserwacja: wiersz z arrival.delay (status='OBSERWACJA').
    """
    snapshot_dt = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    snapshot_local_date = snapshot_dt.astimezone(TZ).date()
    wersja_id = cache.wersja_id
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        if not trip_id:
            continue

        # 1. Anulowanie całego kursu - generujemy wiersze dla każdego planowanego przystanku
        if tu.trip.schedule_relationship == TRIP_CANCELED:
            for seq, entry in cache.get_all_stops(trip_id):
                data_kursu = snapshot_local_date - timedelta(days=entry.offset_dnia)
                rows.append((
                    snapshot_dt, wersja_id, trip_id, entry.stop_id, seq,
                    entry.linia_id, entry.operator_id, entry.kierunek,
                    data_kursu, None, "ANULOWANY",
                ))
            continue

        # 2. Normalne przetwarzanie zatrzymań
        for stu in tu.stop_time_update:
            if not stu.HasField("stop_sequence"):
                continue
            entry = cache.get(trip_id, stu.stop_sequence)
            if entry is None:
                continue

            data_kursu = snapshot_local_date - timedelta(days=entry.offset_dnia)

            # 2a. Pojedynczy przystanek pominięty
            if stu.schedule_relationship == STOP_SKIPPED:
                rows.append((
                    snapshot_dt, wersja_id, trip_id, entry.stop_id, stu.stop_sequence,
                    entry.linia_id, entry.operator_id, entry.kierunek,
                    data_kursu, None, "POMINIETY",
                ))
                continue

            # 2b. Normalna obserwacja - musi mieć arrival.delay
            if not stu.HasField("arrival") or not stu.arrival.HasField("delay"):
                continue

            rows.append((
                snapshot_dt, wersja_id, trip_id, entry.stop_id, stu.stop_sequence,
                entry.linia_id, entry.operator_id, entry.kierunek,
                data_kursu, stu.arrival.delay, "OBSERWACJA",
            ))
    return rows

def _insert_rows(conn, rows: list[tuple]):
    """Bulk insert z deduplikacją: COPY do TEMP -> INSERT ON CONFLICT DO NOTHING."""
    if not rows:
        return 0
    cols = ("ts", "wersja_id", "trip_id", "przystanek_id", "stop_sequence", "linia_id",
            "operator_id", "kierunek",
            "data_kursu", "opoznienie_s", "status")

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

def _log_etl_run(conn, started_at, snapshot_ts, obserwacje,
                 wstawione, czas_s, status, blad):
    try:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO fakt_etl_run (started_at, snapshot_ts, "
                "obserwacje, wstawione, czas_s, status, blad) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (started_at, snapshot_ts, obserwacje, wstawione,
                 czas_s, status, blad)
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zapisać audit log: {e}")

def run_loop(interval_s: int = 20, once: bool = False):
    """Główna pętla pollingu. Ctrl+C kończy.

    Zabezpieczenia produkcyjne:
    - Reconnect z backoff przy zerwanym connection do DB
    - Auto-reload cache, gdy w DB pojawi się nowsza wersja rozkładu
      (eliminuje konieczność restartu RT po static ETL)
    - Pętla try/except wokół iteracji - żaden nieprzewidziany wyjątek
      jej nie zabije, jedynie zostanie zalogowany"""

    init_conn = _connect_with_retry()
    try:
        with init_conn.cursor() as cur:
            cur.execute(DDL)
        init_conn.commit()
    finally:
        init_conn.close()

    cache = ScheduleCache()
    conn = _connect_with_retry()
    cache.load()  

    last_snapshot_ts = 0
    try:
        while True:
            started_at = datetime.now(tz=timezone.utc)
            t_start = time.monotonic()
            snapshot_ts = None
            obserwacje = 0
            wstawione = 0
            status = "OK"
            blad = None

            if not _is_alive(conn):
                logger.warning("Connection martwy, reconnect...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect_with_retry()
                cache.load()  

            db_version = cache.current_version_in_db(conn)
            if db_version is not None and db_version != cache.wersja_id:
                logger.info(f"Wykryto nową wersję rozkładu w DB: "
                            f"{cache.wersja_id} → {db_version}. Reload cache.")
                cache.load()

            try:
                raw = httpx.get(RT_URL, timeout=30.0).content
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(raw)
                snapshot_ts = datetime.fromtimestamp(
                    feed.header.timestamp, tz=timezone.utc
                )

                if feed.header.timestamp <= last_snapshot_ts:
                    logger.debug("Snapshot już przetworzony, pomijam")
                    status = "SKIPPED"
                else:
                    rows = _process_feed(feed, cache)
                    obserwacje = len(rows)
                    wstawione = _insert_rows(conn, rows) if rows else 0
                    last_snapshot_ts = feed.header.timestamp

                    snap = snapshot_ts.strftime("%H:%M:%S")
                    logger.info(
                        f"snap={snap} obs={obserwacje:,} ins={wstawione:,} "
                        f"t={time.monotonic() - t_start:.2f}s"
                    )
            except Exception as e:
                logger.error(f"Iteracja nieudana: {e}")
                status = "ERROR"
                blad = str(e)[:500]

            czas_s = round(time.monotonic() - t_start, 3)

            try:
                _log_etl_run(conn, started_at, snapshot_ts, obserwacje,
                             wstawione, czas_s, status, blad)
            except Exception as e:
                logger.error(f"Audit log w sekcji nieudany: {e}")

            if once:
                break
            time.sleep(interval_s)
    finally:
        try:
            conn.close()
        except Exception:
            pass