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

# Wersja

*   **1.1** usunięcie niewypełnionych kolumn w tabeli `dim_operator`
*   **1.2** dodano nową kolumnę do `lookup_schedule` - `offset_dnia` - aby poprawnie obsługiwać kursy nocne wykryczające ponad zapis 24 godzinny w kolumnie rozkładowy przyjazd
*   **1.3** zaimplementowano rozwiazanie kursów anulowanych i pomiętych przystanków w tym celu wprowadzano do tabeli faktów kolumnę `fakt_opoznienia` która informuje o oberwacji pojazdu, pominięciu przystanku oraz anulowaniu kursu. Jest to również pole z RT.
*   **1.4** Dodano nową tabelę `fakt_etl_run` logującą status procesu ETL z strumienia RT.
*   **1.5** Dodano nową kolumnę do obu tabel faktów aby kierunek w raportach był dobrze widoczny.
*   **1.6** Dodano wymiar wersji rozkładu `dim_wersja_rozkladu`. Dzięki temu kostka się nie zepsuje gdy GZM zmieni rozkład.
    
    Wcześniej gdyby linia zmieniła trasę, stare obserwacje stałyby się "sierotami" ich `trip_id` nie istniałby już w `lookup_schedule`. Teraz stary `trip_id` żyje wiecznie w starej wersji, a nowy `trip_id` w nowej. JOIN-y działają w obu kierunkach. Raporty historyczne są spójne.
    
    Po static ETL trzeba zrestartować RT ETL.
*   **1.7** Dodano widoki zmaterializowane
*   **1.8** Naprawiono `dim_data`. Wcześniej wymiar pokrywał tylko [dziś, dziś+13] (horyzont rozkładu) i był `TRUNCATE`-owany przy każdym static ETL, podczas gdy fakty retencjonowane przez 30 dni mają `data_kursu` w przeszłości. Efekt: JOIN `fakt_opoznienia` z `dim_data` po cichu gubił prawie wszystko i `typ_dnia` (dzień roboczy/święto) był analitycznie niedostępny. Teraz `dim_data` jest budowany dla [dziś - 35, dziś + 14] i ładowany przez `UPSERT`, więc historia naturalnie się kumuluje między uruchomieniami. Dodano też FK z `fakt_opoznienia` do `dim_linia`, `dim_przystanek`, `dim_operator`, `dim_wersja_rozkladu`, `dim_data` - integralność referencyjna jest teraz wymuszana przez bazę, a nie ręcznie zapytaniem #3. Usunięto kolumnę `kierunek_opis` z `fakt_opoznienia` - to atrybut kursu, nie obserwacji, niepotrzebnie puchł tabelę faktów; `headsign` nadal jest w `lookup_schedule` i można go wyciągać JOIN-em po (`wersja_id`, `trip_id`, `przystanek_id`, `stop_sequence`).
*   **1.9** `CA.sql` jest teraz idempotentny (`IF NOT EXISTS` na `CREATE MATERIALIZED VIEW`, `if_not_exists => true` na `add_continuous_aggregate_policy` i `add_retention_policy`) i wgrywany automatycznie ze static ETL po głównym DDL. Wcześniej trzeba było ręcznie `psql -f gtfs_olap/CA.sql`, o czym README nie wspominał - czyli agregaty ciągłe i polityki retencji w praktyce nie powstawały. Wykonanie idzie osobnym połączeniem w autocommit, bo TimescaleDB blokuje `CREATE MATERIALIZED VIEW continuous` wewnątrz explicit transaction.
*   **1.10** Dodano `data_kursu` do `SELECT` i `GROUP BY` we wszystkich czterech agregatach ciągłych. Dotąd CA bucketowały tylko po `time_bucket(ts, ...)`, więc natywne powiązanie z `dim_data` (FK na `data_kursu`) ginęło na poziomie zmaterializowanym - dało się je odtworzyć tylko castowaniem `kwadrans`/`godzina` na DATE w zapytaniu, co w narzędziach BI jest niezgrabne, a dla kursów nocnych (`offset_dnia > 0`) wręcz daje błędny dzień rozkładowy. Teraz każdy wiersz agregatu wprost niesie `data_kursu` gotowy do `JOIN dim_data ON data = data_kursu`.