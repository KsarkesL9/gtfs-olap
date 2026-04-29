"""Transformacja scalonych DataFrame'ów GTFS na tabele wymiarów i lookup.

Każda funkcja build_* przyjmuje słownik DataFrame'ów i zwraca jeden
DataFrame gotowy do załadowania do bazy.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from gtfs_olap.config.gtfs_schema import (
    TRANSPORT_TYPE_MAP,
    WEEKDAY_COLUMNS,
    WEEKDAY_NAMES_PL,
)
from gtfs_olap.static_etl.types import FeedMeta


# ============================================================================
# dim_linia
# ============================================================================

def build_dim_linia(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Z routes + routes_ext.

    Mapowanie:
    - route_short_name → nazwa_krotka
    - route_long_name  → nazwa_dluga
    - route_type (kod) → srodek_transportu (czytelne)
    - routes_ext.route_type_1 → typ_linii (np. "Normalna", "Nocna")
    """
    routes = dfs["routes"]
    routes_ext = dfs.get("routes_ext", pd.DataFrame())

    out = routes[["route_id", "route_short_name", "route_long_name",
                  "route_type"]].copy()
    out.columns = ["linia_id", "nazwa_krotka", "nazwa_dluga", "_type_code"]

    out["srodek_transportu"] = (
        out["_type_code"].map(TRANSPORT_TYPE_MAP).fillna("inne")
    )
    out = out.drop(columns=["_type_code"])

    if not routes_ext.empty and "route_type_1" in routes_ext.columns:
        ext = routes_ext[["route_id", "route_type_1"]].rename(
            columns={"route_id": "linia_id", "route_type_1": "typ_linii"}
        )
        out = out.merge(ext, on="linia_id", how="left")
    else:
        out["typ_linii"] = pd.NA

    out["typ_linii"] = out["typ_linii"].fillna("Normalna")
    return out


# ============================================================================
# dim_przystanek
# ============================================================================

def build_dim_przystanek(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Z stops + stops_ext + communities_ext + stops_attributes_ext.

    Decyzje upraszczające:
    - community_ids może mieć wiele wartości oddzielonych "_" - bierzemy
      pierwszą jako "główną gminę"
    - stop_attribute_ids też wielokrotne - bierzemy pierwszy atrybut
    """
    stops = dfs["stops"]
    stops_ext = dfs.get("stops_ext", pd.DataFrame())
    communities = dfs.get("communities_ext", pd.DataFrame())
    attrs = dfs.get("stops_attributes_ext", pd.DataFrame())

    out = stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
    out.columns = ["przystanek_id", "nazwa", "szer_geo", "dl_geo"]
    out["szer_geo"] = pd.to_numeric(out["szer_geo"], errors="coerce")
    out["dl_geo"] = pd.to_numeric(out["dl_geo"], errors="coerce")

    if stops_ext.empty:
        out["gmina"] = pd.NA
        out["miasto"] = pd.NA
        out["typ_przystanku"] = "Standardowy"
        return out

    ext = stops_ext[["stop_id"]].copy().rename(
        columns={"stop_id": "przystanek_id"}
    )

    # Gmina (z communities_ext)
    if "community_ids" in stops_ext.columns and not communities.empty:
        first_comm = stops_ext["community_ids"].fillna("").str.split("_").str[0]
        comm_map = dict(
            zip(communities["community_id"], communities["community_name"])
        )
        ext["gmina"] = first_comm.map(comm_map)
    else:
        ext["gmina"] = pd.NA

    # Miasto
    ext["miasto"] = (
        stops_ext["city"] if "city" in stops_ext.columns else pd.NA
    )

    # Typ przystanku
    if "stop_attribute_ids" in stops_ext.columns and not attrs.empty:
        first_attr = stops_ext["stop_attribute_ids"].fillna("").str.split("_").str[0]
        attr_map = dict(zip(attrs["stop_type_id"], attrs["stop_attr_name"]))
        ext["typ_przystanku"] = first_attr.map(attr_map)
    else:
        ext["typ_przystanku"] = pd.NA

    out = out.merge(ext, on="przystanek_id", how="left")
    out["typ_przystanku"] = out["typ_przystanku"].fillna("Standardowy")
    return out


# ============================================================================
# dim_operator
# ============================================================================

def build_dim_operator(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Z operators_ext + contracts_ext.

    Operator może mieć wiele umów - bierzemy najnowszą po contract_start_date.
    """
    ops = dfs.get("operators_ext", pd.DataFrame())

    if ops.empty:
        # Awaryjnie: lista organizatorów z agency.txt
        agency = dfs["agency"]
        return pd.DataFrame({
            "operator_id": agency["agency_id"],
            "nazwa": agency["agency_name"],
            "numer_umowy": pd.NA,
            "umowa_od": pd.NaT,
            "umowa_do": pd.NaT,
        })

    out = ops[["operator_id", "operator_name"]].copy()
    out.columns = ["operator_id", "nazwa"]

    contracts = dfs.get("contracts_ext", pd.DataFrame())
    if not contracts.empty:
        c = contracts[["contract_op_id", "contract_number",
                       "contract_start_date", "contract_end_date"]].copy()
        c.columns = ["operator_id", "numer_umowy", "umowa_od", "umowa_do"]
        c["umowa_od"] = pd.to_datetime(
            c["umowa_od"], format="%Y%m%d", errors="coerce"
        )
        c["umowa_do"] = pd.to_datetime(
            c["umowa_do"], format="%Y%m%d", errors="coerce"
        )
        c = c.sort_values("umowa_od", ascending=False).drop_duplicates(
            "operator_id", keep="first"
        )
        out = out.merge(c, on="operator_id", how="left")
        out["umowa_od"] = out["umowa_od"].dt.date
        out["umowa_do"] = out["umowa_do"].dt.date
    else:
        out["numer_umowy"] = pd.NA
        out["umowa_od"] = pd.NaT
        out["umowa_do"] = pd.NaT

    return out


# ============================================================================
# dim_data
# ============================================================================

def _service_priority(sid: str) -> tuple[int, int]:
    """Priorytet wyboru "głównego" service_id dla danego dnia.

    Dni specjalne (id ≥ 9: święta, Wigilia itp.) mają pierwszeństwo
    nad standardowymi (1-8). Dzięki temu np. Boże Narodzenie wygrywa
    nad "niedzielą wakacyjną".
    """
    try:
        n = int(sid)
    except ValueError:
        n = 999
    return (0 if n >= 9 else 1, n)


def build_dim_data(
    dfs: dict[str, pd.DataFrame], meta: FeedMeta
) -> pd.DataFrame:
    """Jeden wiersz na każdy dzień z okresu rozkładu.

    Logika dla typ_dnia:
    1. Z calendar.txt znajdź service_id aktywne dla dnia tygodnia
       w okresie obejmującym datę
    2. Zastosuj wyjątki z calendar_dates.txt (1=add, 2=remove)
    3. Z aktywnych service_id wybierz "główny" (priorytet dla świąt)
    4. service_ext.txt mapuje service_id → nazwę typu dnia
    """
    cal = dfs["calendar"].copy()
    cal_dates = dfs.get("calendar_dates", pd.DataFrame()).copy()
    service_ext = dfs.get("service_ext", pd.DataFrame())

    type_map: dict[str, str] = {}
    if not service_ext.empty:
        type_map = dict(zip(service_ext["service_id"], service_ext["name"]))

    # Konwersja calendar
    cal["start"] = pd.to_datetime(cal["start_date"], format="%Y%m%d").dt.date
    cal["end"] = pd.to_datetime(cal["end_date"], format="%Y%m%d").dt.date
    for c in WEEKDAY_COLUMNS:
        cal[c] = cal[c].astype(str) == "1"

    if not cal_dates.empty:
        cal_dates["date_p"] = pd.to_datetime(
            cal_dates["date"], format="%Y%m%d"
        ).dt.date

    rows = []
    current = meta.feed_start_date
    while current <= meta.feed_end_date:
        wd_idx = current.weekday()  # 0 = poniedziałek
        wd_col = WEEKDAY_COLUMNS[wd_idx]

        # Aktywne service_id z calendar
        mask = (
            (cal["start"] <= current)
            & (cal["end"] >= current)
            & cal[wd_col]
        )
        active: set[str] = set(cal.loc[mask, "service_id"].tolist())

        # Wyjątki z calendar_dates
        if not cal_dates.empty:
            day_excs = cal_dates[cal_dates["date_p"] == current]
            for _, ex in day_excs.iterrows():
                if str(ex["exception_type"]) == "1":
                    active.add(ex["service_id"])
                elif str(ex["exception_type"]) == "2":
                    active.discard(ex["service_id"])

        # Wybór głównego service_id
        primary: str | None = None
        typ_dnia = "brak rozkładu"
        if active:
            primary = sorted(active, key=_service_priority)[0]
            typ_dnia = type_map.get(primary, f"service_{primary}")

        rows.append({
            "data": current,
            "rok": current.year,
            "miesiac": current.month,
            "tydzien_iso": current.isocalendar().week,
            "dzien_tygodnia": wd_idx + 1,
            "nazwa_dnia": WEEKDAY_NAMES_PL[wd_idx],
            "service_id_primary": primary,
            "typ_dnia": typ_dnia,
        })
        current += timedelta(days=1)

    df = pd.DataFrame(rows)

    # Diagnostyka: ile dni "brak rozkładu"
    n_missing = (df["typ_dnia"] == "brak rozkładu").sum()
    if n_missing > 0:
        missing_dates = df.loc[
            df["typ_dnia"] == "brak rozkładu", "data"
        ].tolist()
        logger.warning(
            f"dim_data: {n_missing} dni bez rozkładu: {missing_dates}"
        )
    return df


# ============================================================================
# lookup_schedule (NIE wymiar - pomocnicza tabela dla RT ETL)
# ============================================================================

def build_schedule_lookup(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Denormalizacja stop_times + trips + trips_ext.

    Cel: dla pary (trip_id, stop_id) RT ETL ma w jednym lookupie:
    rozkładowy czas, linię, operatora, kierunek - bez joinów per event.
    """
    trips = dfs["trips"]
    trips_ext = dfs.get("trips_ext", pd.DataFrame())
    stop_times = dfs["stop_times"]

    # Złączenie z trips_ext (operator_id)
    if not trips_ext.empty and "operator_id" in trips_ext.columns:
        trips = trips.merge(
            trips_ext[["trip_id", "operator_id"]], on="trip_id", how="left"
        )
    else:
        trips["operator_id"] = pd.NA

    # Stop_times → trips
    out = stop_times.merge(
        trips[["trip_id", "route_id", "service_id",
               "direction_id", "operator_id"]],
        on="trip_id",
        how="inner",
    )

    out = out[[
        "trip_id", "stop_id", "stop_sequence",
        "arrival_time", "departure_time",
        "route_id", "service_id", "direction_id", "operator_id",
    ]].rename(columns={
        "stop_id": "przystanek_id",
        "route_id": "linia_id",
        "arrival_time": "rozkladowy_przyjazd",
        "departure_time": "rozkladowy_odjazd",
        "direction_id": "kierunek",
    })

    out["stop_sequence"] = pd.to_numeric(
        out["stop_sequence"], errors="coerce"
    ).astype("Int64")

    return out