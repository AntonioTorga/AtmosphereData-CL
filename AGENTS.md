# AtmosphereData-CL

Library and CLI for downloading atmospheric data products — station networks today;
model output and satellite retrievals to come — and reshaping them into master files
and master *directories* in arbitrary formats and layouts.

Changing format (`.csv`, `.netcdf`, …) and changing shape (station-per-file, single
master, date-partitioned directory) are **the same operation**: read one store, write
another. The production goal is a crontab-driven daily run that **extends** masters
incrementally rather than rewriting them.

> **Status:** built and live-verified against all three real endpoints. 95 tests
> pass. Reachable end-to-end from the CLI. The old MonetioCL pipeline
> (`data_download/`, `translate/`) has been deleted. See [EXPLANATION.md](EXPLANATION.md)
> for the mechanical deep-dive.

## Identity

| | |
|---|---|
| Distribution name | `AtmosphereData-CL` |
| Import package | `atmosphere_data_cl`, at `src/atmosphere_data_cl/` |
| CLI command | `AtmosphereData-CL` |
| Reference implementations | `REF_ONLY/`, gitignored |

## Architecture

```
Source ──fetch──> RawStore (== cache) ──change_format──> Store(s)
```

Two extensible layers joined by one canonical intermediate — an xarray
`(time, station)` Dataset. `Source` is acquisition; `Store` is shape+format. A
`RawStore` is the raw download presented as a readable Store, so *everything*
downstream is "read one store, write another".

### Source — acquisition (`sources/`)

A Source is **one access method**, not one organisation — the load-bearing decision.
DMC splits into `dmc-api` (the API client, built) and future `dmc-web` (a scraper for
series the API doesn't serve) because they share no auth and no transport.

Implemented: `Sinca`, `Vipnet`, `DmcApi` (import them from
`atmosphere_data_cl.sources`; list with `list_sources()` / the `sources` CLI command).

A Source declares its shape as **data**, and a shared driver runs it:

- `kind` — data family and registry bucket: `PointSurface` (all three today),
  `Gridded`, `Satellite`.
- `discovery` — how station metadata is obtained: `required` (SINCA/DMC scrape or
  list stations first — it yields the routing key), `derived` (Vipnet — metadata
  rides in every response), `none`.
- `time_grain` — the largest span one request covers. `None` = arbitrary from/to in
  one request (SINCA); `"hour"`/`"month"` = a range fans out into buckets.
- `_plan_axes` declares which axes fan out into separate requests and which collapse.
  This is the crux: SINCA and Vipnet decompose the same (station × variable × time)
  cube in *inverted* ways, and the declaration captures that without per-source loops.

The driver owns retries+backoff, connection pooling, and skip-if-cached — so every
source inherits them. `source.fetch(product, period, ...)` returns a **`RawStore`**;
nothing is parsed until you read or convert it.

```python
from atmosphere_data_cl.sources import Vipnet
raw = Vipnet().fetch("Temperatura", "2026-07-20 12:00")   # RawStore, unparsed
```

### Raw store — the download *is* the cache

A Source writes each payload to disk exactly as downloaded, keyed **by path**: the
per-file axes live in the path (no sidecars, no manifest), so a `RawStore` can recover
them later with `parse_template`. A file being present means "already fetched, skip".

The path is a `variable` folder plus station/height/dates in the filename — deep
nesting was deliberately avoided:

```
raw/vipnet/temperatura/20260720T1200.json
raw/sinca/O3/EMA_x__na__20220901-20220930.csv        {variable}/{station}__{height}__{from}-{to}.csv
raw/dmc-api/330020__2024-01.json                     {station}__{time}.json
```

`RawStore(source, base_dir)` reads that tree back into a Dataset by delegating each
file to the bound Source's parser — including the offline case (no live source, no
network): `RawStore(Vipnet, "raw/vipnet").read()`.

### Store — `base_dir` + path template + format (`store/`)

A Store is a **handle bound to a directory** with two symmetric directions:
`read() -> Dataset` and `write(ds) -> paths`. The path template *is* the shape; the
encoder *is* the format. Implemented layouts (list with the `stores` CLI command):

| Store | Shape |
|---|---|
| `single-netcdf` | one compressed `master.nc`, all stations & variables |
| `one-csv-per-station` | one wide `.csv` per station (+ `stations.csv` sidecar) |

Because a `RawStore` is the same kind of handle, one verb covers conversion,
reshaping, and reformatting:

```python
Store.change_format(src, dst)                 # dst.write(src.read())
Store.change_format(src, dst, mode="append")  # grow dst instead of overwriting
```

Two properties worth knowing:

- **Station metadata travels with the data.** `station_metadata()` normalizes each
  source's discovery columns to `station/latitude/longitude/name/...`; it lands as
  **coordinates** in NetCDF/Zarr and as a **`stations.csv` sidecar** for tabular
  layouts.
- **Growing masters are the cron primitive.** `Store.append(ds)` (or
  `change_format(..., mode="append")`) unions times/stations/variables with **new data
  winning on overlap**, is **idempotent** (re-running a period doesn't duplicate), and
  **atomic** (temp-file + rename, so a crashed run can't corrupt the master).

### CLI (`cli.py`)

Generic verbs over the registries — any source/store works without a new command:

- `fetch SOURCE PRODUCT PERIOD [--to STORE --dest DIR --append] [--stations/--variables/--extra ...]`
- `convert FROM_STORE SRC_DIR TO_STORE DEST [--append]`
- `sources` / `stores` — list what's registered.

DMC creds come from `DMC_API_USER`/`DMC_API_TOKEN` (a `.env` is auto-loaded) or
`--user/--token`.

## What's implemented vs. remaining

**Done:** the Source driver + `sinca`/`vipnet`/`dmc-api`; path-encoded raw store;
`RawStore`; `single-netcdf` + `one-csv-per-station`; metadata threading; growing
(appendable) masters; the CLI; 95 tests; live-verified.

**Remaining / future:**

- More output layouts & serializers: variable-per-file, date-partitioned master
  *directories* (`{year}/{month}/{day}.ext`, which append cheaply by writing only the
  new partition), and `.parquet` / `.zarr` / `.json` / `.xlsx` encoders.
- SINCA station lat/lon (needs a per-station-card scrape; name/region work today).
- `dmc-web` scraper for series the API doesn't serve.
- Gridded / Satellite sources, and time/space resampling.

## Notes for whoever picks this up

- Run and test via the repo venv: `venv/bin/python -m pytest -q` (see the
  `use-project-venv` memory). Tests live in `tests/`, scoped in `pyproject.toml` so
  `REF_ONLY/`'s own suites are ignored.
- SINCA's `psgraph:` responses mean "this station has no such series" — kept in the
  raw store (so we don't refetch) and omitted on transform (empty parse). Not an error.
- Adding a source = one module in `sources/` (declare `kind/name/discovery/time_grain`,
  a `raw_template`, and implement `_plan_axes`/`_build_request`/`_parse`/`_identity`);
  `__init_subclass__` registers it. Adding a layout = one `Store` subclass in `store/`.
