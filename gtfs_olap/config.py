"""Stałe, mapowania, DDL - wszystko w jednym miejscu."""

from zoneinfo import ZoneInfo

# Hardcode
DB_URL = "postgresql://postgres:postgres@localhost:5432/gtfs_olap"
CKAN_API = (
    "https://otwartedane.metropoliagzm.pl/api/3/action/package_show"
    "?id=rozklady-jazdy-i-lokalizacja-przystankow-gtfs-wersja-rozszerzona"
)
RT_URL = "https://gtfsrt.transportgzm.pl:5443/gtfsrt/gzm/tripUpdates"
TZ = ZoneInfo("Europe/Warsaw")

# route_type: ZTM używa tylko tych trzech
TRANSPORT_TYPES = {"0": "tramwaj", "3": "autobus", "11": "trolejbus"}

DAYS_PL = ["poniedziałek", "wtorek", "środa", "czwartek",
           "piątek", "sobota", "niedziela"]
WEEKDAY_COLS = ["monday", "tuesday", "wednesday", "thursday",
                "friday", "saturday", "sunday"]

# UWAGA. Klucze dedup dla scalania paczek GZM. Większość tabel ma normalny
# klucz biznesowy ALE calendar i service_ext są popsute - GZM używa tych
# samych liczb service_id w różnych paczkach do oznaczenia zupełnie różnych
# rzeczy (każda paczka ma swój własny zakres dat dla każdego ID). Dlatego
# klucz dedup MUSI obejmować pełen rekord nie tylko service_id - inaczej
# tracimy 7 z 10 wpisów kalendarza i 18 z 21 dni w dim_data zostaje "brak
# rozkładu". Sprawdzone empirycznie.
DEDUP_KEYS = {
    "agency": ["agency_id"],
    "routes": ["route_id"],
    "routes_ext": ["route_id"],
    "stops": ["stop_id"],
    "stops_ext": ["stop_id"],
    "stops_attributes_ext": ["stop_type_id"],
    "communities_ext": ["community_id"],
    "trips": ["trip_id"],
    "trips_ext": ["trip_id"],
    "stop_times": ["trip_id", "stop_id", "stop_sequence"],
    "calendar": ["service_id", "start_date", "end_date",
                 "monday", "tuesday", "wednesday", "thursday",
                 "friday", "saturday", "sunday"],
    "calendar_dates": ["service_id", "date", "exception_type"],
    "service_ext": ["service_id", "name"],
    "operators_ext": ["operator_id"],
    "contracts_ext": ["contract_id"],
}

DDL = """
CREATE TABLE IF NOT EXISTS dim_linia (
    linia_id            TEXT PRIMARY KEY,
    nazwa_krotka        TEXT,
    nazwa_dluga         TEXT,
    srodek_transportu   TEXT,
    typ_linii           TEXT
);
CREATE TABLE IF NOT EXISTS dim_przystanek (
    przystanek_id       TEXT PRIMARY KEY,
    nazwa               TEXT,
    szer_geo            DOUBLE PRECISION,
    dl_geo              DOUBLE PRECISION,
    gmina               TEXT,
    miasto              TEXT,
    typ_przystanku      TEXT
);
CREATE TABLE IF NOT EXISTS dim_operator (
    operator_id         TEXT PRIMARY KEY,
    nazwa               TEXT,
    numer_umowy         TEXT,
    umowa_od            DATE,
    umowa_do            DATE
);
CREATE TABLE IF NOT EXISTS dim_data (
    data                DATE PRIMARY KEY,
    rok                 SMALLINT,
    miesiac             SMALLINT,
    tydzien_iso         SMALLINT,
    dzien_tygodnia      SMALLINT,
    nazwa_dnia          TEXT,
    typ_dnia            TEXT
);
-- Lookup dla RT - NIE wymiar OLAP
CREATE TABLE IF NOT EXISTS lookup_schedule (
    trip_id              TEXT NOT NULL,
    przystanek_id        TEXT NOT NULL,
    stop_sequence        INT  NOT NULL,
    rozkladowy_przyjazd  TEXT,
    linia_id             TEXT,
    kierunek             TEXT,
    operator_id          TEXT,
    PRIMARY KEY (trip_id, przystanek_id, stop_sequence)
);
CREATE INDEX IF NOT EXISTS idx_lookup_trip ON lookup_schedule (trip_id);

CREATE TABLE IF NOT EXISTS gtfs_meta (
    loaded_at         TIMESTAMPTZ DEFAULT NOW(),
    package_name      TEXT,
    feed_start_date   DATE,
    feed_end_date     DATE
);

CREATE TABLE IF NOT EXISTS fakt_opoznienia (
    ts                   TIMESTAMPTZ NOT NULL,
    trip_id              TEXT NOT NULL,
    przystanek_id        TEXT NOT NULL,
    stop_sequence        INT  NOT NULL,
    linia_id             TEXT,
    operator_id          TEXT,
    kierunek             TEXT,
    data_kursu           DATE,
    opoznienie_s         INT NOT NULL,
    PRIMARY KEY (trip_id, przystanek_id, stop_sequence, ts)
);
SELECT create_hypertable('fakt_opoznienia', 'ts',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_fakt_linia_ts
    ON fakt_opoznienia (linia_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fakt_operator_ts
    ON fakt_opoznienia (operator_id, ts DESC);
"""