"""Helpery bazy danych: sprawdzanie połączenia, bulk COPY."""

from __future__ import annotations

import io

import pandas as pd
import psycopg
from loguru import logger

from gtfs_olap.common.errors import DatabaseError


def check_connection(conn_str: str) -> None:
    """Sprawdza, czy baza odpowiada i ma TimescaleDB extension.

    Wywoływane na samym początku ETL - lepiej fail-fast jeśli baza
    nie żyje, zamiast tracić czas na parsowanie GTFS i potem crashować.
    """
    try:
        with psycopg.connect(conn_str, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                pg_version = cur.fetchone()[0]

                cur.execute(
                    "SELECT extversion FROM pg_extension "
                    "WHERE extname = 'timescaledb'"
                )
                ts_row = cur.fetchone()
    except psycopg.OperationalError as e:
        raise DatabaseError(
            f"Nie udało się połączyć z bazą: {e}\n"
            f"Sprawdź czy: (1) docker compose działa, "
            f"(2) URL połączenia jest poprawny ({conn_str.split('@')[-1]})"
        ) from e
    except psycopg.Error as e:
        raise DatabaseError(f"Błąd bazy: {e}") from e

    logger.info(f"Połączono z bazą: {pg_version.split(',')[0]}")
    if ts_row:
        logger.info(f"TimescaleDB extension: {ts_row[0]}")
    else:
        logger.warning(
            "TimescaleDB extension nie jest zainstalowane! "
            "Static ETL zadziała, ale RT ETL będzie wymagać hypertable."
        )


def copy_dataframe(
    conn: psycopg.Connection,
    table: str,
    df: pd.DataFrame,
    columns: list[str],
) -> int:
    """Bulk-load DataFrame do tabeli przez COPY ... FROM STDIN (CSV).

    Format CSV (zamiast TEXT) lepiej radzi sobie z polskimi znakami
    i przecinkami w nazwach przystanków.

    Returns: liczba wstawionych wierszy
    """
    if df.empty:
        logger.warning(f"Pusty DataFrame dla {table}, pomijam")
        return 0

    # Zapis do bufora w pamięci
    df = df[columns].copy()
    buf = io.StringIO()
    df.to_csv(buf, header=False, index=False, na_rep="")
    buf.seek(0)

    cols_sql = ", ".join(columns)
    copy_sql = (
        f"COPY {table} ({cols_sql}) FROM STDIN "
        f"WITH (FORMAT CSV, NULL '')"
    )
    try:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as cp:
                while chunk := buf.read(64 * 1024):
                    cp.write(chunk)
    except psycopg.Error as e:
        raise DatabaseError(f"Błąd COPY do {table}: {e}") from e

    n = len(df)
    logger.info(f"Załadowano {n:,} wierszy do {table}")
    return n
