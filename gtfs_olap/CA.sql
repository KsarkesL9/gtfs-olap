CREATE MATERIALIZED VIEW IF NOT EXISTS ca_opoznienia_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', ts, 'Europe/Warsaw') AS kwadrans,
    wersja_id,
    linia_id,
    operator_id,
    SUM(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS suma_opoznien,
    COUNT(*)          FILTER (WHERE status = 'OBSERWACJA')          AS obserwacje,
    COUNT(*)          FILTER (WHERE status = 'OBSERWACJA'
                              AND opoznienie_s BETWEEN -30 AND 60)  AS punktualne,
    -- Ekstrema
    MIN(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS min_opoznienie,
    MAX(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS max_opoznienie,
    -- Liczniki zdarzeń:
    COUNT(*) FILTER (WHERE status = 'ANULOWANY')                    AS anulowane,
    COUNT(*) FILTER (WHERE status = 'POMINIETY')                    AS pominiete
FROM fakt_opoznienia
GROUP BY kwadrans, wersja_id, linia_id, operator_id
WITH NO DATA;

-- Polityka odświeżania
SELECT add_continuous_aggregate_policy('ca_opoznienia_15min',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => true);

CREATE MATERIALIZED VIEW IF NOT EXISTS ca_opoznienia_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', kwadrans, 'Europe/Warsaw') AS godzina,
    wersja_id,
    linia_id,
    operator_id,
    SUM(suma_opoznien)  AS suma_opoznien,
    SUM(obserwacje)     AS obserwacje,
    SUM(punktualne)     AS punktualne,
    SUM(anulowane)      AS anulowane,
    SUM(pominiete)      AS pominiete,
    MIN(min_opoznienie) AS min_opoznienie,
    MAX(max_opoznienie) AS max_opoznienie
FROM ca_opoznienia_15min
GROUP BY godzina, wersja_id, linia_id, operator_id
WITH NO DATA;
-- polityka odswieżania
SELECT add_continuous_aggregate_policy('ca_opoznienia_1h',
    start_offset      => INTERVAL '14 days',
    end_offset        => INTERVAL '30 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists     => true);

CREATE MATERIALIZED VIEW IF NOT EXISTS ca_opoznienia_dzien
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', godzina, 'Europe/Warsaw') AS data,
    wersja_id,
    linia_id,
    operator_id,
    SUM(suma_opoznien)  AS suma_opoznien,
    SUM(obserwacje)     AS obserwacje,
    SUM(punktualne)     AS punktualne,
    SUM(anulowane)      AS anulowane,
    SUM(pominiete)      AS pominiete,
    MIN(min_opoznienie) AS min_opoznienie,
    MAX(max_opoznienie) AS max_opoznienie
FROM ca_opoznienia_1h
GROUP BY data, wersja_id, linia_id, operator_id
WITH NO DATA;
-- Polityka odświeżania
SELECT add_continuous_aggregate_policy('ca_opoznienia_dzien',
    start_offset      => INTERVAL '60 days',
    end_offset        => INTERVAL '2 hours',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => true);

CREATE MATERIALIZED VIEW IF NOT EXISTS ca_opoznienia_15min_przystanek
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', ts, 'Europe/Warsaw') AS kwadrans,
    wersja_id,
    przystanek_id,
    linia_id,
    SUM(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS suma_opoznien,
    COUNT(*)          FILTER (WHERE status = 'OBSERWACJA')          AS obserwacje,
    COUNT(*)          FILTER (WHERE status = 'OBSERWACJA'
                              AND opoznienie_s BETWEEN -30 AND 60)  AS punktualne,
    MIN(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS min_opoznienie,
    MAX(opoznienie_s) FILTER (WHERE status = 'OBSERWACJA')          AS max_opoznienie,
    COUNT(*) FILTER (WHERE status = 'ANULOWANY')                    AS anulowane,
    COUNT(*) FILTER (WHERE status = 'POMINIETY')                    AS pominiete
FROM fakt_opoznienia
GROUP BY kwadrans, wersja_id, przystanek_id, linia_id
WITH NO DATA;
-- Polityka odświeżania
SELECT add_continuous_aggregate_policy('ca_opoznienia_15min_przystanek',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => true);

-- Polityka retencyjna
SELECT add_retention_policy('fakt_opoznienia', INTERVAL '30 days', if_not_exists => true);

-- Polityka retencyjna logów
SELECT add_retention_policy('fakt_etl_run', INTERVAL '90 days', if_not_exists => true);