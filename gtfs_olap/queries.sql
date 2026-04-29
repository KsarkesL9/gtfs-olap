-- Zbiór zapytań testowych --
-- 1. Pokazuje wszystkie tabele w bazie 5 wymiarów + tablica faktów + tablica przeszukiwań w cache.
SELECT 'dim_linia' AS t, COUNT(*) FROM dim_linia
UNION ALL SELECT 'dim_przystanek', COUNT(*) FROM dim_przystanek
UNION ALL SELECT 'dim_operator', COUNT(*) FROM dim_operator
UNION ALL SELECT 'dim_data', COUNT(*) FROM dim_data
UNION ALL SELECT 'lookup_schedule', COUNT(*) FROM lookup_schedule
UNION ALL SELECT 'fakt_opoznienia', COUNT(*) FROM fakt_opoznienia;

-- 2. Pokazuje klasyfikacje dni tygodnia która będzie miała wpływ na rozkład jazdy w danym dniu. Używana często gdyż pojawiał się błąd "brak rozkładu".
SELECT typ_dnia, COUNT(*) FROM dim_data GROUP BY typ_dnia ORDER BY 2 DESC;

-- 3. Sprawdzanie istnienia sierot czyli pojazdów bez linii, przystanków na których nic się nie zatrzymuje i pojazdów bez operatora
SELECT 
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dim_linia dl WHERE dl.linia_id = l.linia_id)) AS sieroty_linia,
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dim_przystanek dp WHERE dp.przystanek_id = l.przystanek_id)) AS sieroty_przystanek,
    COUNT(*) FILTER (WHERE l.operator_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dim_operator d WHERE d.operator_id = l.operator_id)) AS sieroty_operator
FROM lookup_schedule l;