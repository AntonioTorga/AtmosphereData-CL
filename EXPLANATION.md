# Source & Store — a deep explanation

This document explains the two core abstractions of AtmosphereData-CL — **Source**
(acquisition) and **Store** (shape + format) — how each base class is built, a full
worked example, and how the two layers meet.

For the high-level architecture and status see [AGENTS.md](AGENTS.md). This is the
*mechanical* companion: how the classes actually work.

---

## 1. The mental model

```
   Source                    RawStore (= cache)               Store / layout
 ─────────                ──────────────────────           ──────────────────
 how you GET data   ──►   every payload as-downloaded,  ──►  how you SHAPE
 (1 class per             its axes encoded in the path       & FORMAT data out
  access method)          raw/<source>/<var>/<...>.<fmt>      single-netcdf
                          (present ⇒ skip refetch)            one-csv-per-station
```

The bridge between the two layers is a **canonical `(time, station)` xarray
Dataset**. A `RawStore` produces it (`read()`), and a `Store` consumes it
(`write(ds)`). Because a `RawStore` is itself a readable handle, conversion is
uniform: **read one handle, write another**.

Internally, the step from raw payload to Dataset goes through a long DataFrame
`[timestamp, station, variable, value, unit]` — that's what a Source's `_parse`
emits and what `long_to_dataset` pivots into the cube. But you rarely touch it; the
public interchange is the Dataset.

---

## 2. Source — the acquisition abstraction

File: [source.py](src/atmosphere_data_cl/sources/source.py)

A Source is **one way of getting data** — not one organisation. DMC splits into
`dmc-api` and (future) `dmc-web` because they share no auth and no transport.

The base class solves one hard problem: different sources decompose the
`(station × variable × time)` cube in *incompatible* ways, so **request construction
can't be shared** — but the **driver** (loop, retries, caching, connection pool)
can. A source declares its shape as *data*; the driver runs it.

### 2a. What a subclass declares

```python
class Vipnet(Source):
    kind = "PointSurface"     # PointSurface | Gridded | Satellite  → registry bucket
    name = "vipnet"           # registry key, and the raw-store subdirectory
    discovery = "derived"     # required | derived | none
    time_grain = "hour"       # one request covers one hour  → time fans out hourly
    native_format = "json"    # what one raw payload is on disk
    raw_template = "{variable}/{time}.{ext}"   # where each raw file lives
    station_meta_map = {...}  # discovery columns → normalized lat/lon/name
```

- **`kind`** — data family + first key of the two-level registry
  ([source.py:124](src/atmosphere_data_cl/sources/source.py#L124)).
- **`discovery`** — how station metadata is obtained: `required` (must run before
  any fetch because it yields the routing key, e.g. SINCA's `airviro_id`), `derived`
  (rides in every response — Vipnet), `none`.
- **`time_grain`** — largest span one request covers. `None` = arbitrary from/to in
  one request (SINCA); `"hour"`/`"month"` = a range is chopped into buckets.
- **`raw_template`** — the per-file path. Its fields *are* the file's identity, so a
  `RawStore` can recover them later with `parse_template`.

### 2b. The hooks a subclass implements

| Hook | Signature | Responsibility |
|---|---|---|
| `_plan_axes` | `(spec) -> {axis: [values]}` | Which axes **fan out**. Axes not returned **collapse** into one request. |
| `_identity` | `(job) -> {str: str}` | The per-file axes as path-safe strings (fills `raw_template`; also the parse context). |
| `_build_request` | `(job) -> Request` | Build the transport-agnostic HTTP call for one job. |
| `_parse` | `(payload, ctx) -> DataFrame` | One payload → canonical long frame. Runs only on read/convert. |
| `discover_stations` | `() -> DataFrame` | Station metadata. Required for `discovery="required"`. |

The clever part: `_parse` takes a flat **context dict** (`ctx`), not a live `Job`.
Live fetch builds it via `_identity(job)`; a disk read builds the *same* dict by
parsing the filename. So parsing works identically whether the payload just arrived
or was picked up off disk with no live source.

### 2c. What the driver owns

Three dataclasses thread state through the pipeline:

- **`FetchSpec`** ([source.py:40](src/atmosphere_data_cl/sources/source.py#L40)) —
  the whole request in one object (`product`, `start`, `end`, `stations`,
  `variables`, `extras`). `extras` is the escape hatch for source-specific knobs
  (SINCA's `min_validation_level`, Vipnet's `mode`) so the base signature never grows.
- **`Job`** ([source.py:61](src/atmosphere_data_cl/sources/source.py#L61)) — one
  request to make.
- **`Request`** ([source.py:78](src/atmosphere_data_cl/sources/source.py#L78)) — a
  transport-agnostic HTTP description. SINCA is a GET; Vipnet a POST — same dataclass.

Driver methods: `plan(spec)`
([source.py:228](src/atmosphere_data_cl/sources/source.py#L228)) expands
`_plan_axes × time buckets` into a flat `list[Job]` (pure, cheap, what the plan tests
assert); `_execute(job)`
([source.py:279](src/atmosphere_data_cl/sources/source.py#L279)) cache-checks then
sends and saves the raw payload; `_send`
([source.py:293](src/atmosphere_data_cl/sources/source.py#L293)) retries with backoff.

### 2d. The `fetch` contract

```python
raw = Vipnet().fetch("Temperatura", "2026-07-20 12:00")   # → a RawStore
```

`fetch` ([source.py:332](src/atmosphere_data_cl/sources/source.py#L332)) plans the
jobs, `_execute`s each (HTTP or cache), and **returns a `RawStore`** bound to the raw
directory. **Nothing is parsed** — parsing is the cost of reading/converting, never
of acquiring. Passing `format=<Store>` + `dest=` converts immediately (sugar over
`change_format`) and returns the written paths.

Station discovery is decoupled from parsing: for `discovery="derived"` the driver
calls `_accumulate_discovery(items)`
([source.py:216](src/atmosphere_data_cl/sources/source.py#L216)) **once per fetch**
(not per request) to union the station table — so `discover_stations()` is populated
by acquisition, independent of whether you ever parse.

### 2e. The registry

`__init_subclass__` auto-registers every subclass; importing `sources` imports every
module so lookups resolve.

```python
from atmosphere_data_cl.sources import get_source, list_sources
list_sources()          # ['dmc-api', 'sinca', 'vipnet']
get_source("vipnet")    # <class Vipnet>
```

---

## 3. Store — the shape + format abstraction

File: [store.py](src/atmosphere_data_cl/store/store.py)

A Store is a **handle bound to a directory** — `OneCsvPerStation("out")` — with two
symmetric directions. The path template *is* the shape; the encoder *is* the format.

```python
class OneCsvPerStation(Store):
    name = "one-csv-per-station"
    template = "{station}.csv"
    def write(self, ds) -> list[Path]: ...   # Dataset → files (+ metadata sidecar)
    def read(self) -> xr.Dataset: ...        # files → canonical (time, station) Dataset
```

- **`write`** ([store.py:187](src/atmosphere_data_cl/store/store.py#L187)) — one wide
  CSV per station (rows=time, cols=variables), each written atomically; station
  metadata coords go to a `stations.csv` sidecar.
- **`read`** ([store.py:214](src/atmosphere_data_cl/store/store.py#L214)) — glob the
  CSVs, rebuild the Dataset, reattach the sidecar metadata.
- **`SingleNetcdf`** ([store.py:238](src/atmosphere_data_cl/store/store.py#L238)) —
  one compressed `master.nc`; metadata stays inline as coords.

`long_to_dataset` ([store.py:36](src/atmosphere_data_cl/store/store.py#L36)) builds
the `(time, station)` cube natively (NaN-filling ragged variable/station coverage),
and `attach_station_metadata`
([store.py:62](src/atmosphere_data_cl/store/store.py#L62)) adds lat/lon/name as
coordinates on the station dim.

### 3a. Growing stores — the cron primitive

```python
Store.append(ds)                              # merge into what's already stored
Store.change_format(raw, master, mode="append")
```

`append` ([store.py:135](src/atmosphere_data_cl/store/store.py#L135)) read-modify-
writes: it `combine_first`s the new data over the existing master, so times/stations/
variables **union** and **new data wins on overlap** (a preliminary→validated
re-fetch takes effect). It's **idempotent** (re-running a period adds no duplicates)
and **atomic** (temp-file + `os.replace`). It also re-attaches station metadata,
which `combine_first` would otherwise drop.

### 3b. `RawStore` — the raw download as a readable handle

File: [raw.py](src/atmosphere_data_cl/store/raw.py)

`RawStore` is deliberately dumb: it knows only "a directory of raw files laid out
under a Source's `raw_template`". It borrows the *interpretation* from the Source it's
bound to — each file's axes are recovered from its path, and the Source's own
`_parse` turns the payload into records. So the same class serves two flows:

```python
raw = Vipnet().fetch("Temperatura", "2026-07-20 12:00")   # bound to a live source
raw.read()

RawStore(Vipnet, "raw/vipnet").read()   # picked straight off disk, no network
```

Because it exposes `read() -> Dataset` like any Store, `change_format` treats it
identically to an on-disk layout.

---

## 4. A full worked example — Vipnet

```python
from atmosphere_data_cl.sources import Vipnet
from atmosphere_data_cl.store import SingleNetcdf, OneCsvPerStation, Store
```

### Fetch

```python
raw = Vipnet(raw_dir="raw").fetch("Temperatura", "2026-07-20")
```

1. `"2026-07-20"` → interval `(00:00, 23:59:59.999)`.
2. `_plan_axes` fans out **variable** = `["Temperatura"]`; stations collapse (every
   request is network-wide). `time_grain="hour"` → **24 jobs**.
3. Each job → a POST to the VipNet endpoint; `_execute` saves the raw JSON.
4. `discovery="derived"` → the station table is unioned into `stations.csv` once.
5. Returns a **`RawStore`** — nothing parsed yet.

**On disk** (raw = as downloaded; axes in the path):

```
raw/vipnet/
  stations.csv                        ← accumulated station union
  temperatura/20260720T0000.json      ← hour 0
  temperatura/20260720T0100.json      ← hour 1
  ...  (24 files)
```

### Read or convert

```python
ds = raw.read()                                    # → (time=24, station=N) Dataset
                                                   #    with lat/lon/name coords

Store.change_format(raw, SingleNetcdf("out"))      # → out/master.nc
Store.change_format(raw, OneCsvPerStation("out2")) # → out2/<station>.csv + stations.csv
```

`read()` parses each cached file (via `Vipnet._parse` on the path-recovered context),
concatenates the long frames, pivots to the cube, and attaches metadata.

### Grow it tomorrow

```python
raw2 = Vipnet(raw_dir="raw").fetch("Temperatura", "2026-07-21")
Store.change_format(raw2, SingleNetcdf("out"), mode="append")   # master now spans 2 days
```

---

## 5. How Source and Store interact

They meet at exactly **two points**:

1. **`RawStore`** — a Source's `fetch` returns one; it's the adapter that makes raw
   downloads readable as a Store, delegating parsing back to the Source.
2. **The canonical `(time, station)` Dataset** — every handle's `read()` produces it
   and every `write(ds)` consumes it. As long as both honour that shape, any source
   composes with any store.

Because the interaction is that thin, the same Store classes serve a Source-free
purpose — **Store-to-Store conversion** — via the identical `change_format` path.

```
 Vipnet().fetch("Temperatura", "2026-07-20")
        │
        ├─ plan() ─► 24 Jobs ─► _execute() ─► raw/vipnet/temperatura/*.json  (cache)
        │                                    └─ _accumulate_discovery ─► stations.csv
        │
        └─ returns RawStore
                 │  .read()                       Store.change_format(raw, dst[, mode="append"])
                 ▼                                          │
        _parse(payload, ctx) per file ─► long frame ─► long_to_dataset ─► (time,station) Dataset
                                                                    │
                                              + attach_station_metadata (coords)
                                                                    │
                                                    dst.write(ds)  ─► master.nc / {station}.csv
```

---

## 6. From the terminal

Everything above is reachable via generic CLI verbs (see [README.md](README.md)):

```
AtmosphereData-CL fetch vipnet Temperatura "2026-07-20" --to single-netcdf --dest out
AtmosphereData-CL fetch vipnet Temperatura "2026-07-21" --to single-netcdf --dest out --append
AtmosphereData-CL convert single-netcdf out one-csv-per-station out_csv
AtmosphereData-CL sources    # dmc-api, sinca, vipnet
AtmosphereData-CL stores     # one-csv-per-station, single-netcdf
```
