# GTFS OLAP — kostka OLAP dla transportu publicznego GZM

Projekt na przedmiot *Hurtownie i Systemy Eksploracji Danych* (Politechnika Śląska).

Buduje kostkę OLAP z dwóch źródeł danych transportu publicznego GZM/ZTM Katowice:
- **GTFS statyczny** — paczki ZIP z rozkładami jazdy (CKAN, aktualizowane co kilka dni)
- **GTFS-RT** — feed protobuf TripUpdates z opóźnieniami w czasie rzeczywistym (~30s)

Dane lądują w PostgreSQL z rozszerzeniem TimescaleDB: wymiary (linie, przystanki,
operatorzy, kalendarz) + hypertable faktu opóźnień.

## Wymagania

- Python 3.10+
- Docker i Docker Compose (do bazy danych)

## Szybki start

### 1. Baza danych

```bash
docker compose up -d
```

Startuje TimescaleDB na `localhost:5432` (user/pass: `postgres/postgres`, baza: `gtfs_olap`).

### 2. Instalacja pakietu

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -e .
```

### 3. Wgranie danych statycznych (wymiary + lookup)

```bash
python scripts/run_static_etl.py
```

Skrypt pobiera paczki GTFS z CKAN GZM, scala je (pokrywając 14 dni),
buduje wymiary i ładuje do bazy.

Opcje:
- `--horizon-days 7` — zmiana horyzontu (domyślnie 14 dni)
- `--zip paczka.zip` — użycie lokalnej paczki zamiast pobierania z CKAN
- `--db postgresql://...` — inny URL bazy

### 4. Uruchomienie RT ETL (opóźnienia na żywo)

```bash
python scripts/run_rt_etl.py
```

Polluje feed GTFS-RT co 20 sekund i wstawia opóźnienia do `fakt_opoznienia`.

Opcje:
- `--interval 30` — zmiana interwału
- `--once` — jeden cykl i koniec (do testów)
- `Ctrl+C` — czyste zatrzymanie po bieżącej iteracji

### 5. Konfiguracja (opcjonalnie)

Skopiuj `.env.example` do `.env` i dostosuj:

```bash
cp .env.example .env
```

Dostępne zmienne: `DATABASE_URL`, `GTFS_RT_URL`, `POLL_INTERVAL_S`.

## Struktura projektu

```
src/gtfs_olap/
├── config/          # ustawienia, mapowania GTFS, DDL
├── common/          # wyjątki, logowanie, HTTP retry
├── static_etl/      # pobieranie CKAN, parsowanie, transformacje, load
├── rt_etl/          # feed protobuf, cache rozkładu, pipeline, pętla
└── db/              # połączenie z bazą, helpery COPY
```

## Stack technologiczny

Python, PostgreSQL + TimescaleDB, httpx, pandas, psycopg v3, loguru, protobuf.
