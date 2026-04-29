"""Pobieranie i parsowanie feedu GTFS-RT (protobuf).

Obsługuje TripUpdates z GZM/ZTM — minimalne snapshoty z delay
lub timestampami dla poszczególnych przystanków.
"""

from __future__ import annotations

from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2

from gtfs_olap.common.errors import ETLError
from gtfs_olap.common.http_client import get_with_retry
from gtfs_olap.config.settings import GTFS_RT_URL, RT_HTTP_TIMEOUT_S


def fetch_feed() -> bytes:
    """Pobiera świeży feed protobuf z retry."""
    return get_with_retry(GTFS_RT_URL, timeout=RT_HTTP_TIMEOUT_S).content


def parse_feed(data: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Parsuje surowe bajty na strukturę protobuf."""
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(data)
    except Exception as e:
        raise ETLError(f"Nie udało się sparsować feedu protobuf: {e}") from e
    return feed


def extract_delay(
    stu, sched_arr_dt: datetime | None, sched_dep_dt: datetime | None,
) -> tuple[int, str, str] | None:
    """Wybiera opóźnienie ze StopTimeUpdate, z fallbackiem przez kilka źródeł.

    Kolejność prób:
      1. arrival.delay (najczęściej publikowane przez ZTM)
      2. arrival.time - rozkładowy_przyjazd (gdy delay nie jest set)
      3. departure.delay
      4. departure.time - rozkładowy_odjazd

    Returns:
        (delay_seconds, typ_zdarzenia, zrodlo_pomiaru) lub None
    """
    if stu.HasField("arrival"):
        if stu.arrival.HasField("delay"):
            return stu.arrival.delay, "arrival", "arrival.delay"
        if stu.arrival.HasField("time") and sched_arr_dt is not None:
            actual = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
            return (
                int((actual - sched_arr_dt).total_seconds()),
                "arrival", "arrival.time",
            )

    if stu.HasField("departure"):
        if stu.departure.HasField("delay"):
            return stu.departure.delay, "departure", "departure.delay"
        if stu.departure.HasField("time") and sched_dep_dt is not None:
            actual = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
            return (
                int((actual - sched_dep_dt).total_seconds()),
                "departure", "departure.time",
            )

    return None