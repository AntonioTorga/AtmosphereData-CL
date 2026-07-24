# Source & Store — a deep explanation

This document explains the two core abstractions of AtmosphereData-CL — **Source**
(acquisition) and **Store** (shape + format) — how each abstract base class is
built, a full worked example, and how the two layers meet.

For the high-level architecture and project status see [AGENTS.md](AGENTS.md).
This document is the *mechanical* companion: how the classes actually work.

---

## 1. The mental model

```
   Source                       raw store (= cache)              Store / layout
 ─────────                    ─────────────────────           ──────────────────
 how you GET data     ───►    every payload as-downloaded  ───►  how you SHAPE
 (1 class per                 raw/<source>/<job>.<fmt>           & FORMAT data out
  access method)              (existing files skip refetch)      {station}.csv
                                                                 master.nc
                                                                 {year}/{month}/{day}.parquet
```

The bridge between the two layers is a **canonical long DataFrame**:

```
[ timestamp | station | variable | value | unit ]
```

A Source *can* produce it (via `_parse`), and a Store *consumes* it (via
`write`). Neither layer knows anything else about the other. That is the whole
decoupling.

---

## 2. Source — the acquisition abstraction

File: [src/atmosphere_data_cl/sources/source.py](src/atmosphere_data_cl/sources/source.py)

A Source is **one way of getting data** — not one organisation. DMC splits into
`dmc-api` and (future) `dmc-web` because they share no auth and no transport.

The base class solves one hard problem: different sources decompose the
`(station × variable × time)` cube in *incompatible* ways, so **request
construction cannot be shared** — but the **driver** (the loop that runs
requests, retries them, caches them, pools the connection) can be. So a source
declares its shape as *data*, and the base class runs it.

### 2a. What a subclass declares

```python
class Vipnet(Source):
    kind = "PointSurface"     # PointSurface | Gridded | Satellite  → registry bucket
    name = "vipnet"           # registry key, and the raw-store subdirectory
    discovery = "derived"     # required | derived | none
    time_grain = "hour"       # one request covers one hour  → time fans out hourly
    native_format = "json"    # what one raw payload is on disk
```

- **`kind`** tags the data family and is the first key of the two-level registry
  ([source.py:96](src/atmosphere_data_cl/sources/source.py#L96),
  [source.py:114](src/atmosphere_data_cl/sources/source.py#L114)).
- **`discovery`** says how station metadata is obtained:
  - `required` — must run *before* any fetch because it yields the routing key
    (SINCA's `airviro_id`). The driver enforces this
    ([source.py:280](src/atmosphere_data_cl/sources/source.py#L280)).
  - `derived` — falls out of every data response (Vipnet). Harvested by
    `_accumulate_discovery` on every payload.
  - `none` — the source has no station concept.
- **`time_grain`** is the largest span one request can cover. `None` means the
  source takes an arbitrary from/to range and time *does not fan out at all*
  (SINCA). `"hour"`/`"month"` mean a range is chopped into buckets of that size.

### 2b. The four hooks a subclass implements

| Hook | Signature | Responsibility |
|---|---|---|
| `_plan_axes` | `(spec) -> {axis: [values]}` | Which axes **fan out**. Axes not returned **collapse** into a single request. |
| `_build_request` | `(job) -> Request` | Build the transport-agnostic HTTP description for one job. |
| `_parse` | `(payload, job) -> DataFrame` | One response → canonical long frame. **Only called when converting.** |
| `discover_stations` | `() -> DataFrame` | Station metadata. Required for `discovery="required"`. |

Everything else is the base class's job.

### 2c. What the base class owns (the driver)

Three dataclasses carry state through the pipeline:

- **`FetchSpec`** ([source.py:39](src/atmosphere_data_cl/sources/source.py#L39)) —
  the whole request bundled into one object (`product`, `start`, `end`,
  `stations`, `variables`, `extras`). `extras` is the open-ended escape hatch for
  source-specific knobs (SINCA's `min_validation_level`, Vipnet's `mode`) so the
  base signature never grows.
- **`Job`** ([source.py:60](src/atmosphere_data_cl/sources/source.py#L60)) — one
  request to make, with a filesystem-safe `key` that is *also its raw-store
  filename*.
- **`Request`** ([source.py:77](src/atmosphere_data_cl/sources/source.py#L77)) — a
  transport-agnostic description of one HTTP call (url, method, params, json_body,
  headers, timeout). SINCA returns a GET; Vipnet a POST — same dataclass.

The driver methods:

- **`plan(spec)`** ([source.py:153](src/atmosphere_data_cl/sources/source.py#L153))
  — expands `_plan_axes` × time buckets into a flat `list[Job]`. Pure and cheap;
  this is what the plan tests assert against.
- **`_execute(job)`** ([source.py:201](src/atmosphere_data_cl/sources/source.py#L201))
  — cache-checks (`raw_path.exists()` → load and short-circuit), else builds +
  sends the request and saves the raw payload.
- **`_send(request)`** ([source.py:215](src/atmosphere_data_cl/sources/source.py#L215))
  — retries with exponential backoff. Neither reference implementation had
  retries; every source inherits them here for free.
- **`fetch(...)`** ([source.py:254](src/atmosphere_data_cl/sources/source.py#L254))
  — the public entrypoint. **This is the contract that matters:**

```python
def fetch(self, product, period, stations=None, variables=None,
          format=None, use_cache=True, **extras):
    ...
    for job in self.plan(spec):
        payload = self._execute(job, use_cache=use_cache)   # HTTP or cache
        if self.discovery == "derived":
            self._accumulate_discovery(payload, job)        # station harvest
        collected.append((job, payload))

    if format is None:
        return {job.key: payload for job, payload in collected}   # ← NATIVE

    frames = [self._parse(payload, job) for job, payload in collected ...]
    data = pd.concat(frames, ...)
    return format.write_from_long(data, source=self, spec=spec)   # ← CONVERTED
```

Two things to burn in:

1. **No `format` → returns payloads exactly as downloaded**, keyed by job. No
   parsing, no reshaping. `_parse` is the *cost of converting*, never paid on the
   default path.
2. **Station discovery is separate from parsing.** `_accumulate_discovery`
   ([source.py:150](src/atmosphere_data_cl/sources/source.py#L150)) runs on every
   payload — native or converted — so a native fetch still populates
   `discover_stations()`. (This is why the harvest is *not* inside `_parse`.)

### 2d. The registry

`__init_subclass__` auto-registers every subclass, and importing the `sources`
package imports every module so the registry is populated
([sources/__init__.py](src/atmosphere_data_cl/sources/__init__.py)). Look one up
by name:

```python
from atmosphere_data_cl.sources import get_source, list_sources
list_sources()            # ['dmc-api', 'sinca', 'vipnet']
get_source("vipnet")      # <class Vipnet>
```

---

## 3. Store — the shape + format abstraction

File: [src/atmosphere_data_cl/store/store.py](src/atmosphere_data_cl/store/store.py)

A Store is **base directory + path template + format**. The path template *is*
the shape and the encoder *is* the format, so "convert between shapes" and
"convert between formats" are the same operation: read one Store, write another.

### 3a. What a subclass declares + implements

```python
class OneCsvPerStation(Store):
    name = "one-csv-per-station"
    template = "{station}.csv"        # the shape: one file per station
    suffix = ".csv"                   # the format

    @classmethod
    def write(cls, long, base_dir) -> list[Path]: ...   # long frame → files
    @classmethod
    def read(cls, base_dir) -> xr.Dataset: ...          # files → canonical Dataset
```

The abstract base ([store.py:62](src/atmosphere_data_cl/store/store.py#L62))
declares `write` and `read` as the two required directions, plus
`write_from_long` — the entry point `Source.fetch` calls.

### 3b. Stores own BOTH directions (the part ClimateGraph lacked)

ClimateGraph's reader was a **funnel**: many layouts in, but a single hardcoded
`ds.to_netcdf()` out. It could *read* `{station}.csv` but not *write* it. So the
write half here is new work. `OneCsvPerStation`:

- **write** ([store.py:104](src/atmosphere_data_cl/store/store.py#L104)) —
  `groupby("station")`, pivot each group to wide (rows=time, cols=variables),
  `atomic_write` one CSV per station.
- **read** ([store.py:121](src/atmosphere_data_cl/store/store.py#L121)) — glob the
  CSVs, melt each back to long, `long_to_dataset` into a `(time, station)`
  Dataset — the exact shape ClimateGraph's `station_per_file` reader produces.

`long_to_dataset` ([store.py:34](src/atmosphere_data_cl/store/store.py#L34)) is
the pivot that both Stores share, and the canonical `(time, station)` xarray form.

`SingleNetcdf` ([store.py:137](src/atmosphere_data_cl/store/store.py#L137)) is the
master-file layout: one compressed `.nc` with zlib encoding and a provenance
`history` attribute via `record()`.

### 3c. Shared infrastructure the Stores lean on

From [utils/paths.py](src/atmosphere_data_cl/utils/paths.py):
`render_template` (build `{year}/{month}/{day}.parquet` paths, zero-padding time
fields so lexical order = chronological order) and `atomic_write` (temp file +
`os.replace`, so a crashed 3am cron can't corrupt a master).

---

## 4. A full worked example — Vipnet, native then converted

```python
from atmosphere_data_cl.sources import Vipnet
from atmosphere_data_cl.store import OneCsvPerStation
```

### Step 1 — native fetch

```python
raw = Vipnet(raw_dir="raw").fetch("Temperatura", "9/9/2022")
```

What happens inside:

1. `"9/9/2022"` → `manage_time_interval` → `(2022-09-09 00:00, 2022-09-09 23:59:59.999)`.
2. `_plan_axes` fans out **variable** = `["Temperatura"]`; stations collapse
   (every request is network-wide). `time_grain="hour"` chops the day into **24**
   buckets. → **24 jobs**.
3. For each job `_build_request` builds a POST to
   `https://vipnet.mop.gob.cl/v1/vipnet/estaciones/valor` with
   `{tipoEstacion: 1, fetchHour: h, fetchDay: "2022-09-09", ...}`.
4. `_execute` sends it (or loads the cached file), saving the raw JSON.
5. `discovery == "derived"` → `_accumulate_discovery` unions the station
   metadata from each response into `stations.csv`.
6. `format is None` → return the payloads as-downloaded.

**On disk:**

```
raw/vipnet/
  stations.csv                                           ← accumulated union
  temperatura__end-...t0059__start-...t0000__...json     ← hour 0
  temperatura__end-...t0159__...json                     ← hour 1
  ...  (24 files)
```

**What comes out** — a dict keyed by job, values are the raw JSON payloads
(the self-describing envelope Vipnet wraps):

```python
{
  "temperatura__end-20220909t0059__start-20220909t0000__variable-temperatura": {
      "variable": "Temperatura",
      "unit": "°C",
      "data": [
          {"codigoEstacion": "02113005-2", "nombre": "GUATACONDO",
           "region": 1, "latitud": -20.93, "longitud": -69.05, "value": 12.4},
          ...
      ]
  },
  ...  # 24 entries
}
```

Nothing was parsed or reshaped. `stations.csv` is populated as a side effect.

### Step 2 — converted fetch (same call, one argument added)

```python
paths = Vipnet(raw_dir="raw").fetch("Temperatura", "9/9/2022", format=OneCsvPerStation)
```

The 24 payloads are already cached (step 1), so **no HTTP happens**. This time
`format` is set, so:

1. Each payload → `_parse` → a long frame slice
   `[timestamp, station, variable, value, unit]`.
2. `pd.concat` → one long frame for the whole day.
3. `OneCsvPerStation.write_from_long` → `write` groups by station and writes one
   wide CSV each.

**What comes out** — the list of written paths:

```python
[PosixPath('raw/one-csv-per-station/02113005-2.csv'),
 PosixPath('raw/one-csv-per-station/02120001-K.csv'),
 ...]
```

and each file is wide:

```
timestamp,           temperatura
2022-09-09 00:00:00, 12.4
2022-09-09 01:00:00, 12.1
...
```

> **Known rough edge (roadmap item):** the output currently lands under
> `raw/one-csv-per-station/` because `write_from_long`
> ([store.py:87](src/atmosphere_data_cl/store/store.py#L87)) derives `base_dir`
> from the source's `raw_dir`. The output directory should be a caller-chosen
> argument, not a sibling of the cache. Slated to be fixed when the CLI is wired.

---

## 5. How Source and Store interact

They meet at exactly **two points**, and nowhere else:

1. **`Source.fetch(format=SomeStore)`** is the only place a Source references a
   Store. It hands the Store a long DataFrame via `write_from_long`. The Source
   never imports a Store class — you pass one in.

2. **The canonical long frame** `[timestamp, station, variable, value, unit]` is
   the contract between them. `_parse` promises to produce it; `write` promises
   to consume it. As long as both honour that shape, any Source composes with any
   Store.

Because the interaction is that thin, the same Store classes serve a second,
Source-free purpose — **Store-to-Store conversion**, which is the "reshape a
master" operation:

```python
# Read one layout, write another — shape AND format change in one step.
ds = OneCsvPerStation.read("raw/one-csv-per-station")   # → (time, station) Dataset
SingleNetcdf.write(ds, "out")                           # → out/master.nc
```

That symmetry — `fetch(format=...)` and Store→Store being the *same* write path —
is the point of putting shape and format in one abstraction.

### One-picture summary

```
 Vipnet().fetch("Temperatura", "9/9/2022")
        │
        ├─ plan() ─► 24 Jobs ─► _execute() ─► raw/vipnet/*.json   (cache)
        │                                    │
        │                                    └─ _accumulate_discovery ─► stations.csv
        │
        ├─ format is None ─────────────────► {job_key: payload}   (native, unparsed)
        │
        └─ format=OneCsvPerStation
                 │
                 └─ _parse ─► long frame [timestamp,station,variable,value,unit]
                                    │
                                    └─ OneCsvPerStation.write ─► {station}.csv
                                                                      ▲
                        Store-to-Store conversion reuses this exact write path
```

---

## 6. Where this is going (today's roadmap)

The layers above are built and covered by 84 offline tests, but not yet reachable
from the CLI, and the cron use case isn't implemented. Planned next:

1. **Live-test** the three sources against real endpoints.
2. **Wire the CLI** to Source + Store, then delete the old `data_download/` +
   `translate/` pipeline it still uses.
3. **Add writers**: variable-per-file layout, `.parquet`, `.zarr`, `.json`.
4. **Incremental append** in the Store layer (`_merge_existing` + `_split` for
   partitioned `{year}/{month}/{day}` master directories) — the daily-cron premise.
