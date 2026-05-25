-- Zbiór zapytań testowych --
-- 1. Pokazuje wszystkie tabele w bazie: 5 wymiarów + tablica faktów + tablica przeszukiwań w cache.
SELECT 'dim_linia' AS t, COUNT(*) FROM dim_linia
UNION ALL SELECT 'dim_przystanek',      COUNT(*) FROM dim_przystanek
UNION ALL SELECT 'dim_operator',        COUNT(*) FROM dim_operator
UNION ALL SELECT 'dim_data',            COUNT(*) FROM dim_data
UNION ALL SELECT 'dim_wersja_rozkladu', COUNT(*) FROM dim_wersja_rozkladu
UNION ALL SELECT 'lookup_schedule',     COUNT(*) FROM lookup_schedule
UNION ALL SELECT 'fakt_opoznienia',     COUNT(*) FROM fakt_opoznienia;

-- 2. Pokazuje klasyfikacje dni tygodnia która będzie miała wpływ na rozkład jazdy w danym dniu. Używana często gdyż pojawiał się błąd "brak rozkładu".
SELECT typ_dnia, COUNT(*) FROM dim_data GROUP BY typ_dnia ORDER BY 2 DESC;

-- 3. Sprawdzanie istnienia sierot - po wprowadzeniu FK zwraca same zera, ale zostaje
--    jako sanity check (NULL-e są dozwolone, więc np. fakt bez operator_id leci).
SELECT
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dim_linia dl WHERE dl.linia_id = f.linia_id))
        AS sieroty_linia,
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dim_przystanek dp WHERE dp.przystanek_id = f.przystanek_id))
        AS sieroty_przystanek,
    COUNT(*) FILTER (WHERE f.operator_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM dim_operator d WHERE d.operator_id = f.operator_id))
        AS sieroty_operator
FROM fakt_opoznienia f;

-- 4. Średnie opóźnienie wg typu dnia - dim_data wreszcie podpięty do faktów po data_kursu.
SELECT
    d.typ_dnia,
    COUNT(*)                          AS obserwacje,
    ROUND(AVG(f.opoznienie_s), 1)     AS srednie_opoznienie_s
FROM fakt_opoznienia f
JOIN dim_data d ON d.data = f.data_kursu
WHERE f.status = 'OBSERWACJA'
GROUP BY d.typ_dnia
ORDER BY obserwacje DESC;