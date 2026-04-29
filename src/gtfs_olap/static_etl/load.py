"""Bulk load DataFrame'ów do PostgreSQL/TimescaleDB.

Wszystkie dimensions są ładowane przez COPY ... FROM STDIN (CSV) -
dramatycznie szybsze niż per-row INSERT.

Idempotencja: TRUNCATE wymiarów przed loadem - nawet po wielokrotnym
uruchomieniu skryptu mamy stan spójny z aktualną paczką GTFS.
"""

from __future__ import annotations

import psycopg
from loguru import logger

from gtfs_olap.config.ddl import DDL_DIMENSIONS
from gtfs_olap.config.gtfs_schema import TABLE_COLUMNS
from gtfs_olap.common.errors import DatabaseError
from gtfs_olap.db.connection import copy_dataframe
from gtfs_olap.static_etl.extract import FeedMeta

import pandas as pd


def ensure_schema(conn: psycopg.Connection) -> None:
    """Tworzy tabele jeśli nie istnieją (idempotentne)."""
    with conn.cursor() as cur:
        cur.execute(DDL_DIMENSIONS)
    conn.commit()
    logger.info("Schemat wymiarów: utworzony / zweryfikowany")


def truncate_dimensions(conn: psycopg.Connection) -> None:
    """Czyści tabele wymiarów (oprócz fakt_opoznienia, który należy do RT ETL).

    TRUNCATE jest dramatycznie szybszy niż DELETE i zwalnia miejsce.
    Idempotentność: po każdym uruchomieniu mamy stan = aktualna paczka GTFS.
    """
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE dim_linia, dim_przystanek, dim_operator, "
            "dim_data, lookup_schedule"
        )
    conn.commit()
    logger.info("Wyczyszczono tabele wymiarów")


def load_all(
    conn_str: str,
    dims: dict[str, pd.DataFrame],
    lookup: pd.DataFrame,
    meta: FeedMeta,
) -> None:
    """Ładuje wszystkie wymiary + lookup + metadane do bazy.

    Sekwencja: schema → truncate → COPY × N → INSERT meta → commit.
    """
    try:
        with psycopg.connect(conn_str) as conn:
            ensure_schema(conn)
            truncate_dimensions(conn)

            copy_dataframe(conn, "dim_linia", dims["linia"],
                           TABLE_COLUMNS["dim_linia"])
            copy_dataframe(conn, "dim_przystanek", dims["przystanek"],
                           TABLE_COLUMNS["dim_przystanek"])
            copy_dataframe(conn, "dim_operator", dims["operator"],
                           TABLE_COLUMNS["dim_operator"])
            copy_dataframe(conn, "dim_data", dims["data"],
                           TABLE_COLUMNS["dim_data"])
            copy_dataframe(conn, "lookup_schedule", lookup,
                           TABLE_COLUMNS["lookup_schedule"])

            # Metadane wgranej paczki
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO gtfs_meta "
                    "(package_name, feed_start_date, feed_end_date) "
                    "VALUES (%s, %s, %s)",
                    (meta.package_name, meta.feed_start_date, meta.feed_end_date),
                )
            conn.commit()
    except psycopg.Error as e:
        raise DatabaseError(f"Błąd ładowania do bazy: {e}") from e
