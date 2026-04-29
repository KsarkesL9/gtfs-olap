# GTFS OLAP

Kostka OLAP dla strumieni danych transportu publicznego GZM Metropolia Śląska.
Projekt na *Hurtownie Danych i Systemy Eksploracji Danych*.

## Jak odpalić

```bash
docker compose up -d              # baza
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows PowerShell
pip install -e .

python scripts/run_static_etl.py  # raz - wgrywa wymiary i lookup
python scripts/run_rt_etl.py      # pętla - polluje opóźnienia co 20s
```

## Struktura

```
gtfs_olap/
├── config.py    # stałe, mapowania, DDL
├── static.py    # static ETL (CKAN -> wymiary)
└── rt.py        # RT ETL (protobuf -> fakt)
scripts/
├── run_static_etl.py
└── run_rt_etl.py
```

## Stack

Python, PostgreSQL + TimescaleDB, httpx, pandas, psycopg, loguru, protobuf.