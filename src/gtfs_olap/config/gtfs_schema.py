"""Mapowania i stałe specyficzne dla formatu GTFS używanego przez GZM/ZTM.

Plik czysto deklaratywny — łatwy do edycji bez ruszania logiki ETL.
"""

# ============================================================================
# Pliki GTFS, które wczytujemy
# ============================================================================

GTFS_FILES = {
    "agency": "agency.txt",
    "routes": "routes.txt",
    "routes_ext": "routes_ext.txt",
    "stops": "stops.txt",
    "stops_ext": "stops_ext.txt",
    "stops_attributes_ext": "stops_attributes_ext.txt",
    "communities_ext": "communities_ext.txt",
    "trips": "trips.txt",
    "trips_ext": "trips_ext.txt",
    "stop_times": "stop_times.txt",
    "calendar": "calendar.txt",
    "calendar_dates": "calendar_dates.txt",
    "service_ext": "service_ext.txt",
    "operators_ext": "operators_ext.txt",
    "contracts_ext": "contracts_ext.txt",
    "feed_info": "feed_info.txt",
}


# ============================================================================
# Mapowania kodów GTFS na czytelne wartości
# ============================================================================

# route_type wg specyfikacji GTFS, ZTM używa 0/3/11
TRANSPORT_TYPE_MAP = {
    "0": "tramwaj",
    "3": "autobus",
    "11": "trolejbus",
}

# Polskie nazwy dni tygodnia (indeks 0 = poniedziałek, zgodnie z weekday())
WEEKDAY_NAMES_PL = [
    "poniedziałek", "wtorek", "środa", "czwartek",
    "piątek", "sobota", "niedziela",
]

# Kolumny dni tygodnia w calendar.txt
WEEKDAY_COLUMNS = [
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
]


# ============================================================================
# Klucze deduplikacji przy scalaniu paczek
# ============================================================================
#
# Każda paczka GZM pokrywa wycinek kalendarza (1-10 dni) i ma swój własny
# zestaw service_id. Aby zbudować rozkład na 14 dni, scalamy kilka paczek.
# Przy scalaniu deduplikujemy każdą tabelę po jej naturalnym kluczu.
#
# UWAGA - calendar/service_ext: GZM używa tych samych liczb service_id
# w różnych paczkach do oznaczenia różnych "service'ów" (każda paczka
# ma własny zakres dat dla każdego service_id). Klucz dedup obejmuje
# więc cały rekord, nie tylko service_id.

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
    "calendar": [
        "service_id", "start_date", "end_date",
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
    ],
    "calendar_dates": ["service_id", "date", "exception_type"],
    "service_ext": ["service_id", "name"],
    "operators_ext": ["operator_id"],
    "contracts_ext": ["contract_id"],
    "feed_info": [],  # specjalna obsługa - bierzemy pierwszą
}


# ============================================================================
# Mapowanie tabela → kolumny w kolejności COPY-a
# ============================================================================
#
# Używane przez load.py / pipeline.py do bulk insertu.

TABLE_COLUMNS = {
    "dim_linia": [
        "linia_id", "nazwa_krotka", "nazwa_dluga",
        "srodek_transportu", "typ_linii",
    ],
    "dim_przystanek": [
        "przystanek_id", "nazwa", "szer_geo", "dl_geo",
        "gmina", "miasto", "typ_przystanku",
    ],
    "dim_operator": [
        "operator_id", "nazwa", "numer_umowy", "umowa_od", "umowa_do",
    ],
    "dim_data": [
        "data", "rok", "miesiac", "tydzien_iso", "dzien_tygodnia",
        "nazwa_dnia", "service_id_primary", "typ_dnia",
    ],
    "lookup_schedule": [
        "trip_id", "przystanek_id", "stop_sequence",
        "rozkladowy_przyjazd", "rozkladowy_odjazd",
        "linia_id", "service_id", "kierunek", "operator_id",
    ],
    "fakt_opoznienia": [
        "ts", "trip_id", "przystanek_id", "stop_sequence",
        "linia_id", "operator_id", "kierunek", "data_kursu",
        "rozkladowy_przyjazd", "opoznienie_s",
        "typ_zdarzenia", "zrodlo_pomiaru",
    ],
}
