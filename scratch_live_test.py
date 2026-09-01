"""Live smoke test — fetch small real slices and write visible files.

Run:  venv/bin/python scratch_live_test.py

Writes under ./scratch_output/:
  raw/<source>/...        raw payloads exactly as downloaded (the cache)
  converted/<source>/...  the same data as one master NetCDF and per-station CSV

Kept deliberately small: Vipnet one hour, SINCA/DMC the first 3 stations only.
Vipnet and SINCA need no auth; DMC reads creds from .env.
"""

import traceback
from pathlib import Path

from atmosphere_data_cl.sources import DmcApi, Sinca, Vipnet
from atmosphere_data_cl.sources.dmc_api import STATION_ID_COLUMN
from atmosphere_data_cl.store import OneCsvPerStation, SingleNetcdf, Store

OUT = Path("scratch_output")
RAW = OUT / "raw"
CONV = OUT / "converted"


def _load_env(path=".env"):
    env = {}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def convert(tag, raw):
    """Read the RawStore back, summarize it, and write master.nc + per-station csv."""
    ds = raw.read()
    print(f"  [{tag}] raw dir : {raw.base_dir}")
    if not ds.data_vars:
        print(f"  [{tag}] parsed EMPTY dataset — check raw files above")
        return
    print(f"  [{tag}] dims    : {dict(ds.sizes)}")
    print(f"  [{tag}] vars    : {list(ds.data_vars)[:12]}")
    print(f"  [{tag}] coords  : {[c for c in ds.coords if ds[c].dims == ('station',)]}")
    print(f"  [{tag}] stations: {[str(s) for s in ds['station'].values[:5]]}")
    nc = Store.change_format(raw, SingleNetcdf(CONV / tag))
    csv = Store.change_format(raw, OneCsvPerStation(CONV / tag / "per_station"))
    print(f"  [{tag}] wrote   : {nc[0]}  +  {len(csv)} csv files")


def main():
    env = _load_env()
    OUT.mkdir(exist_ok=True)

    print("== VIPNET (single hour) ==")
    try:
        convert("vipnet", Vipnet(raw_dir=RAW).fetch("Temperatura", "2026-07-20 12:00"))
    except Exception:
        traceback.print_exc()

    print("\n== SINCA (O3, Sep 2022, Santiago stations) ==")
    try:
        s = Sinca(raw_dir=RAW)
        stations = s.discover_stations()
        # Region "M" (Metropolitana / Santiago) reliably measures O3.
        ids = stations[stations["region"] == "M"]["station_id"].astype(str).tolist()[:5]
        print(f"  discovered {len(stations)} stations; using Santiago {ids}")
        convert("sinca", s.fetch("O3", "1/9/2022 to 30/9/2022", stations=ids))
    except Exception:
        traceback.print_exc()

    print("\n== DMC-API (Jan 2024, first 3 stations) ==")
    user, token = env.get("DMC_API_USER"), env.get("DMC_API_TOKEN")
    if not (user and token):
        print("  no DMC creds in .env, skipping")
    else:
        try:
            d = DmcApi(user, token, raw_dir=RAW)
            stations = d.discover_stations()
            print(f"  discovered {len(stations)} stations; columns: {list(stations.columns)}")
            ids = stations[STATION_ID_COLUMN].astype(str).tolist()[:3]
            convert("dmc-api", d.fetch("dmc", "1/2024 to 31/1/2024", stations=ids))
        except Exception:
            traceback.print_exc()

    print(f"\nDone. Browse {OUT.resolve()}")


if __name__ == "__main__":
    main()
