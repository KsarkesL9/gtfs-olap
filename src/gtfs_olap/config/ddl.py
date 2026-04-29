"""DDL tabel wymiarowych, lookup i hypertable faktu.

Jawny SQL zamiast ORM — w projekcie edukacyjnym czytelność schematu
jest ważniejsza niż abstrakcja.
"""

# ============================================================================
# Wymiary + lookup_schedule + metadane
# ============================================================================
#
# Kolumny TEXT — świadoma decyzja: dane GTFS są tekstowe na wejściu,
# konwersja na sztywne typy odbywa się tylko gdy realnie tego
# potrzebujemy (daty w dim_data, współrzędne w dim_przystanek).

DDL_DIMENSIONS = """
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
    service_id_primary  TEXT,
    typ_dnia            TEXT
);

-- NIE jest wymiarem OLAP - to lookup dla RT ETL
CREATE TABLE IF NOT EXISTS lookup_schedule (
    trip_id              TEXT NOT NULL,
    przystanek_id        TEXT NOT NULL,
    stop_sequence        INT  NOT NULL,
    rozkladowy_przyjazd  TEXT,
    rozkladowy_odjazd    TEXT,
    linia_id             TEXT,
    service_id           TEXT,
    kierunek             TEXT,
    operator_id          TEXT,
    PRIMARY KEY (trip_id, przystanek_id, stop_sequence)
);

CREATE INDEX IF NOT EXISTS idx_lookup_schedule_trip
    ON lookup_schedule (trip_id);
CREATE INDEX IF NOT EXISTS idx_lookup_schedule_service
    ON lookup_schedule (service_id);

-- Historia wgranych paczek
CREATE TABLE IF NOT EXISTS gtfs_meta (
    loaded_at         TIMESTAMPTZ DEFAULT NOW(),
    package_name      TEXT,
    feed_start_date   DATE,
    feed_end_date     DATE
);
"""


# ============================================================================
# Tabela faktów + hypertable (używana przez RT ETL)
# ============================================================================

DDL_FACT = """
CREATE TABLE IF NOT EXISTS fakt_opoznienia (
    -- Wymiar czasu - moment publikacji snapshotu z feedu
    ts                   TIMESTAMPTZ NOT NULL,

    -- Identyfikatory źródłowe (nie-FK, dla audytu i deduplikacji)
    trip_id              TEXT NOT NULL,
    przystanek_id        TEXT NOT NULL,
    stop_sequence        INT  NOT NULL,

    -- Klucze obce do wymiarów (zdenormalizowane z lookup_schedule)
    linia_id             TEXT,
    operator_id          TEXT,
    kierunek             TEXT,
    data_kursu           DATE,           -- service date z TripDescriptor

    -- Czas rozkładowy (referencja, do drążenia w razie wątpliwości)
    rozkladowy_przyjazd  TIMESTAMPTZ,

    -- Pomiary
    opoznienie_s         INT NOT NULL,   -- ujemne = przyspieszenie
    typ_zdarzenia        TEXT,           -- 'arrival' | 'departure'
    zrodlo_pomiaru       TEXT,           -- skąd wzięto opóźnienie (audyt)

    -- Klucz unikalności gwarantujący idempotentność: ten sam (trip,
    -- przystanek, stop_sequence) z tym samym snapshot-ts nie zapisze
    -- się dwa razy.
    -- Hypertable wymaga, by partition key (ts) był częścią PK.
    PRIMARY KEY (trip_id, przystanek_id, stop_sequence, ts)
);

-- Hypertable z chunkami dziennymi - dla naszego strumienia OK
SELECT create_hypertable(
    'fakt_opoznienia', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indeksy pod typowe zapytania kostki
CREATE INDEX IF NOT EXISTS idx_fakt_linia_ts
    ON fakt_opoznienia (linia_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fakt_przystanek_ts
    ON fakt_opoznienia (przystanek_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fakt_operator_ts
    ON fakt_opoznienia (operator_id, ts DESC);
"""
