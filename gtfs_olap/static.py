"""Static ETL: ściąga paczki GTFS z CKAN GZM, scala, ładuje do bazy."""

from __future__ import annotations

import csv
import io
import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd
import psycopg
from loguru import logger

from gtfs_olap.config import (
    CKAN_API, DB_URL, DDL, DEDUP_KEYS, TRANSPORT_TYPES, DAYS_PL, WEEKDAY_COLS,
)

ZIP_RE = re.compile(r"schedule_ZTM_(\d{4})\.(\d{2})\.(\d{2})_(\d+)_(\d+)\.zip")


@dataclass
class FeedMeta:
    package_name: str
    feed_start_date: date
    feed_end_date: date


# ============================================================================
# Pobieranie paczek
# ============================================================================
#
# UWAGA. Z CKAN GZM trzeba ściągnąć WSZYSTKIE paczki które razem pokrywają
# dzień dzisiejszy + 14 dni do przodu. Nie da się tego zrobić "wybierz najnowszą
# paczkę i już" bo:
# 1. Numer sekwencyjny w nazwie ZIP-a NIE odpowiada datom obowiązywania.
#    Najnowsza paczka (najwyższy numer) może obowiązywać dopiero za 2 tygodnie.
# 2. Każda paczka pokrywa tylko WYCINEK kalendarza (1-10 dni). Żeby mieć pełen
#    rozkład na 14 dni trzeba scalić 7-9 paczek.
# 3. Prawdziwy okres obowiązywania jest tylko w feed_info.txt WEWNĄTRZ ZIP-a.
#    Z nazwy pliku tego nie wyczytasz.
# Rozwiązanie: ściągamy wszystko po kolei, sprawdzamy okres,
# zbieramy aż pokryjemy 14 dni. Zazwyczaj 7-9 paczek po ~10 MB.

def _fetch_active_packages(dest: Path, horizon_days: int = 14) -> list[Path]:
    """Pobiera paczki pokrywające okres [dziś, dziś + horizon_days]."""
    today = date.today()
    target_days = {today + timedelta(days=i) for i in range(horizon_days)}

    resp = httpx.get(CKAN_API, timeout=30.0)
    resp.raise_for_status()
    candidates = []
    for r in resp.json()["result"]["resources"]:
        m = ZIP_RE.match(r.get("name", ""))
        if m:
            y, mo, d, seq, _ = m.groups()
            candidates.append(((int(y), int(mo), int(d), int(seq)), r["url"], r["name"]))
    candidates.sort(reverse=True)

    logger.info(f"Szukam paczek na {today} → {today + timedelta(days=13)} "
                f"({len(candidates)} kandydatów)")

    selected: list[tuple[date, Path]] = []
    covered: set[date] = set()
    for _, url, name in candidates:
        if covered >= target_days:
            break
        zip_path = dest / name
        with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_bytes(64 * 1024):
                    f.write(chunk)

        start, end = _read_feed_period(zip_path)
        pkg_days = {start + timedelta(days=i)
                    for i in range((end - start).days + 1)
                    if today <= start + timedelta(days=i) <= today + timedelta(days=13)}
        if not (pkg_days - covered):
            zip_path.unlink()
            continue

        logger.success(f"  {name}: {start} → {end} (+{len(pkg_days - covered)} dni)")
        selected.append((start, zip_path))
        covered |= pkg_days

    selected.sort(key=lambda t: t[0])
    logger.info(f"Wybrano {len(selected)} paczek, pokryto {len(covered)}/{horizon_days} dni")
    return [p for _, p in selected]


def _read_feed_period(zip_path: Path) -> tuple[date, date]:
    # GZM publikuje CSV-y z polami w cudzysłowach, więc nie da się tego prymitywnie
    # splitować po przecinku - trzeba przez moduł csv. Plus utf-8-sig na BOM.
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("feed_info.txt") as f:
            content = f.read().decode("utf-8-sig")
    rec = next(csv.DictReader(io.StringIO(content)))
    return (datetime.strptime(rec["feed_start_date"], "%Y%m%d").date(),
            datetime.strptime(rec["feed_end_date"], "%Y%m%d").date())


# ============================================================================
# Wczytywanie i scalanie paczek
# ============================================================================

GTFS_FILES = [
    "agency", "routes", "routes_ext", "stops", "stops_ext",
    "stops_attributes_ext", "communities_ext", "trips", "trips_ext",
    "stop_times", "calendar", "calendar_dates", "service_ext",
    "operators_ext", "contracts_ext", "feed_info",
]


def _load_and_merge(zip_paths: list[Path], tmp_root: Path) -> dict[str, pd.DataFrame]:
    """Rozpakowuje paczki i scala DataFrame'y z deduplikacją po DEDUP_KEYS."""
    per_pkg: list[dict[str, pd.DataFrame]] = []
    for i, zp in enumerate(zip_paths, 1):
        sub = tmp_root / f"pkg_{i:02d}"
        sub.mkdir()
        with zipfile.ZipFile(zp) as zf:
            zf.extractall(sub)
        logger.info(f"  Paczka {i}/{len(zip_paths)}: {zp.name}")

        dfs = {}
        for name in GTFS_FILES:
            path = sub / f"{name}.txt"
            if path.exists():
                df = pd.read_csv(path, dtype=str, keep_default_na=False)
                df.columns = df.columns.str.strip()
                dfs[name] = df.replace("", pd.NA)
        per_pkg.append(dfs)

    merged = {}
    for key in {k for d in per_pkg for k in d}:
        frames = [d[key] for d in per_pkg if key in d]
        if key == "feed_info":
            merged[key] = frames[0]
            continue
        combined = pd.concat(frames, ignore_index=True)
        if key in DEDUP_KEYS:
            combined = combined.drop_duplicates(subset=DEDUP_KEYS[key], keep="first")
        merged[key] = combined
    return merged


# ============================================================================
# Budowa wymiarów i lookup_schedule
# ============================================================================

def _build_dim_linia(dfs):
    out = dfs["routes"][["route_id", "route_short_name", "route_long_name", "route_type"]].copy()
    out.columns = ["linia_id", "nazwa_krotka", "nazwa_dluga", "_t"]
    out["srodek_transportu"] = out["_t"].map(TRANSPORT_TYPES).fillna("inne")
    out = out.drop(columns=["_t"])
    ext = dfs["routes_ext"][["route_id", "route_type_1"]].rename(
        columns={"route_id": "linia_id", "route_type_1": "typ_linii"})
    out = out.merge(ext, on="linia_id", how="left")
    out["typ_linii"] = out["typ_linii"].fillna("Normalna")
    return out


def _build_dim_przystanek(dfs):
    out = dfs["stops"][["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
    out.columns = ["przystanek_id", "nazwa", "szer_geo", "dl_geo"]
    out["szer_geo"] = pd.to_numeric(out["szer_geo"], errors="coerce")
    out["dl_geo"] = pd.to_numeric(out["dl_geo"], errors="coerce")

    ext = dfs["stops_ext"][["stop_id"]].rename(columns={"stop_id": "przystanek_id"}).copy()
    # community_ids/stop_attribute_ids mogą być wielokrotne (oddzielone "_") bierzemy pierwsze
    comm = dict(zip(dfs["communities_ext"]["community_id"],
                    dfs["communities_ext"]["community_name"]))
    ext["gmina"] = dfs["stops_ext"]["community_ids"].fillna("").str.split("_").str[0].map(comm)
    ext["miasto"] = dfs["stops_ext"]["city"]
    attr = dict(zip(dfs["stops_attributes_ext"]["stop_type_id"],
                    dfs["stops_attributes_ext"]["stop_attr_name"]))
    ext["typ_przystanku"] = (
        dfs["stops_ext"]["stop_attribute_ids"].fillna("").str.split("_").str[0].map(attr)
        .fillna("Standardowy")
    )
    return out.merge(ext, on="przystanek_id", how="left")


def _build_dim_operator(dfs):
    out = dfs["operators_ext"][["operator_id", "operator_name"]].copy()
    out.columns = ["operator_id", "nazwa"]
    contracts = dfs["contracts_ext"]
    if contracts.empty:
        for col in ("numer_umowy", "umowa_od", "umowa_do"):
            out[col] = pd.NA
        return out
    c = contracts[["contract_op_id", "contract_number",
                   "contract_start_date", "contract_end_date"]].copy()
    c.columns = ["operator_id", "numer_umowy", "umowa_od", "umowa_do"]
    c["umowa_od"] = pd.to_datetime(c["umowa_od"], format="%Y%m%d", errors="coerce").dt.date
    c["umowa_do"] = pd.to_datetime(c["umowa_do"], format="%Y%m%d", errors="coerce").dt.date
    c = c.sort_values("umowa_od", ascending=False).drop_duplicates("operator_id")
    return out.merge(c, on="operator_id", how="left")


def _build_dim_data(dfs, meta: FeedMeta):
    """Jeden wiersz na każdy dzień okresu, z typem dnia z service_ext."""
    cal = dfs["calendar"].copy()
    cal["start"] = pd.to_datetime(cal["start_date"], format="%Y%m%d").dt.date
    cal["end"] = pd.to_datetime(cal["end_date"], format="%Y%m%d").dt.date
    for c in WEEKDAY_COLS:
        cal[c] = cal[c].astype(str) == "1"

    type_map = dict(zip(dfs["service_ext"]["service_id"], dfs["service_ext"]["name"]))

    rows = []
    cur = meta.feed_start_date
    while cur <= meta.feed_end_date:
        wd = cur.weekday()
        mask = (cal["start"] <= cur) & (cal["end"] >= cur) & cal[WEEKDAY_COLS[wd]]
        active = set(cal.loc[mask, "service_id"])
        # Dni specjalne (id ≥ 9: święta) wygrywają nad standardowymi (1-8).
        # Boże Narodzenie ma być świętem, nie "niedzielą wakacyjną".
        primary = sorted(active, key=lambda s: (0 if int(s) >= 9 else 1, int(s)))[0] if active else None
        rows.append({
            "data": cur, "rok": cur.year, "miesiac": cur.month,
            "tydzien_iso": cur.isocalendar().week,
            "dzien_tygodnia": wd + 1, "nazwa_dnia": DAYS_PL[wd],
            "typ_dnia": type_map.get(primary, "brak rozkładu") if primary else "brak rozkładu",
        })
        cur += timedelta(days=1)
    return pd.DataFrame(rows)


def _build_lookup_schedule(dfs):
    """Denormalizacja stop_times + trips + trips_ext.

    Cel: w RT ETL jeden lookup po (trip_id, sequence) zwraca rozkładowy czas,
    linię, operatora kierunek - bez joinów per event."""
    trips = dfs["trips"].merge(
        dfs["trips_ext"][["trip_id", "operator_id"]], on="trip_id", how="left")
    out = dfs["stop_times"].merge(
        trips[["trip_id", "route_id", "direction_id", "operator_id"]],
        on="trip_id", how="inner")
    out = out[["trip_id", "stop_id", "stop_sequence", "arrival_time",
               "route_id", "direction_id", "operator_id"]].rename(columns={
        "stop_id": "przystanek_id",
        "route_id": "linia_id",
        "arrival_time": "rozkladowy_przyjazd",
        "direction_id": "kierunek",
    })
    out["stop_sequence"] = pd.to_numeric(out["stop_sequence"], errors="coerce").astype("Int64")
    return out


# ============================================================================
# Load do bazy
# ============================================================================

def _copy_df(conn, table: str, df: pd.DataFrame, cols: list[str]):
    buf = io.StringIO()
    df[cols].to_csv(buf, header=False, index=False, na_rep="")
    buf.seek(0)
    with conn.cursor() as cur, cur.copy(
        f"COPY {table} ({','.join(cols)}) FROM STDIN WITH (FORMAT CSV, NULL '')"
    ) as cp:
        while chunk := buf.read(64 * 1024):
            cp.write(chunk)
    logger.info(f"  {table}: {len(df):,} wierszy")


def _load_to_db(dims: dict, lookup: pd.DataFrame, meta: FeedMeta):
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE dim_linia, dim_przystanek, dim_operator, "
                        "dim_data, lookup_schedule")
        _copy_df(conn, "dim_linia", dims["linia"],
                 ["linia_id", "nazwa_krotka", "nazwa_dluga", "srodek_transportu", "typ_linii"])
        _copy_df(conn, "dim_przystanek", dims["przystanek"],
                 ["przystanek_id", "nazwa", "szer_geo", "dl_geo", "gmina", "miasto", "typ_przystanku"])
        _copy_df(conn, "dim_operator", dims["operator"],
                 ["operator_id", "nazwa", "numer_umowy", "umowa_od", "umowa_do"])
        _copy_df(conn, "dim_data", dims["data"],
                 ["data", "rok", "miesiac", "tydzien_iso", "dzien_tygodnia", "nazwa_dnia", "typ_dnia"])
        _copy_df(conn, "lookup_schedule", lookup,
                 ["trip_id", "przystanek_id", "stop_sequence", "rozkladowy_przyjazd",
                  "linia_id", "kierunek", "operator_id"])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gtfs_meta (package_name, feed_start_date, feed_end_date) "
                "VALUES (%s, %s, %s)",
                (meta.package_name, meta.feed_start_date, meta.feed_end_date))
        conn.commit()


# ============================================================================
# Główna funkcja - łączy wszystko
# ============================================================================

def run(horizon_days: int = 14):
    """Pełen pipeline static ETL."""
    today = date.today()
    end = today + timedelta(days=horizon_days - 1)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_paths = _fetch_active_packages(tmp_path, horizon_days)

        merge_dir = tmp_path / "merge"
        merge_dir.mkdir()
        dfs = _load_and_merge(zip_paths, merge_dir)

        meta = FeedMeta(
            package_name=" | ".join(p.name for p in zip_paths),
            feed_start_date=today,
            feed_end_date=end,
        )
        logger.info(f"Scalony rozkład: {today} → {end}, {len(zip_paths)} paczek")

        dims = {
            "linia": _build_dim_linia(dfs),
            "przystanek": _build_dim_przystanek(dfs),
            "operator": _build_dim_operator(dfs),
            "data": _build_dim_data(dfs, meta),
        }
        lookup = _build_lookup_schedule(dfs)

        logger.info(f"Wymiary: {len(dims['linia'])} linii, "
                    f"{len(dims['przystanek'])} przystanków, "
                    f"{len(dims['operator'])} operatorów, "
                    f"{len(dims['data'])} dni")
        logger.info(f"Lookup: {len(lookup):,} wierszy")

        _load_to_db(dims, lookup, meta)