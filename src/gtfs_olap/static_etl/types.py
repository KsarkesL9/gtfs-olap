"""Wspólne typy danych dla static ETL.

Wydzielone z extract.py żeby uniknąć cyklicznych importów -
FeedMeta jest używana w extract, transform i load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class FeedMeta:
    """Metadane scalonego rozkładu."""
    package_name: str          # lista nazw paczek oddzielona ' | '
    feed_start_date: date
    feed_end_date: date