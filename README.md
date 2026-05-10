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
1.1 usunięcie niewypełnionych kolumn w tabeli dim_operator
1.2 dodano nową kolumnę do lookup_schedule - offset_dnia - aby poprawnie obsługiwać kursy nocne wykryczające ponad zapis 24 godzinny
w kolumnie rozkładowy przyjazd
1.3 zaimplementowano rozwiazanie kursów anulowanych i pomiętych przystanków w tym celu wprowadzano do tabeli faktów kolumnę fakt_opoznienia która informuje o oberwacji pojazdu, pominięciu przystanku oraz anulowaniu kursu. Jest to również pole z RT.
1.4 Dodano nową tabelę fakt_etl_run logującą status procesu ETL z strumienia RT.
1.5 Dodano nową kolumnę do obu tabel faktów aby kierunek w raportach był dobrze widoczny.
1.6 Dodano wymiar wersji rozkładu dim_wersja_rozkladu. Dzięki temu kostka się nie zepsuje gdy GZM zmieni rozkład. 
Wcześniej gdyby linia zmieniła trasę, stare obserwacje stałyby się "sierotami" ich trip_id nie istniałby już w lookup_schedule. Teraz stary trip_id żyje wiecznie w starej wersji, a nowy trip_id w nowej. JOIN-y działają w obu kierunkach. Raporty historyczne są spójne.
Po static ETL trzeba zrestartować RT ETL.
1.7 Dodano widoki zmaterializowane