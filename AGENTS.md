# AtmosphereData-CL

Library and CLI for downloading atmospheric data products — station networks, model output,
satellite retrievals — and reshaping them into master files and master *directories* in
arbitrary formats and layouts.

The end state is a crontab-driven daily run that extends those masters incrementally.

> **Status:** this document records the agreed architecture. Most of it is **not yet
> implemented** — see [Current state](#current-state) for what actually exists today, and
> [Next steps](#next-steps) for the order of work.

## Identity

The project was previously called *MonetioCL*. The rename has been applied.

| | |
|---|---|
| Distribution name | `AtmosphereData-CL` |
| Import package | `atmosphere_data_cl`, at `src/atmosphere_data_cl/` |
| CLI command | `AtmosphereData-CL` |
| Reference implementations | `REF_ONLY/`, gitignored |

Note: `melodies-monet-format` keys in `src/config/config.json` and the `Melodies-Monet`
mentions in `translate/translator.py` docstrings refer to the **external** NOAA tool, not to
this project. They are not part of the rename. Melodies-MONET is one supported *output
target*, no longer the purpose of the project.

## Architecture

```
Source ──> raw store (== cache) ──> Processor ──> Store(s)
```

Three layers, each independently extensible.

### Source — acquisition

A Source is **one access method**, not one organisation. This is the load-bearing decision:
DMC splits into two separate Sources because they share no auth and no transport, and
therefore should share no class.

- **`dmc-api`** — the httpx client against the DMC API. This is what the current code does.
- **`dmc-web`** — a scraper for series the API does not serve, such as *Precipitación Diaria
  Histórica*. Pending: the user has a working scraper and the URL to provide.

Properties of a Source:

- Registered in a registry, tagged with a **kind**: `PointSurface`, `Gridded`, `Satellite`, …
- Exposes named **products** — the resolutions and series available through that access
  method. Adding a resolution should be a table entry, not a new class.
- Emits data **as downloaded**. It converts only when the native form is hostile to analysis
  (HTML → csv/json). Fidelity to the source is the default; normalization is the exception.
- Canonical emitted formats: `.csv`, `.json`, `.netcdf`. Recommended additions: `.parquet`
  (columnar, typed, compresses well, dask reads it natively) and `.zarr` for the
  gridded/satellite side.
- Importable standalone, so a Source is useful without the CLI:

  ```python
  from atmosphere_data_cl.sources import DMCApi
  ds = DMCApi(user, api_key).fetch("ema_hourly", period="2026-07")
  ```

### Raw store — the output *is* the cache

A Source writes into its own Store. A period's file being present means "already fetched,
skip it." One concept rather than two, browsable on disk, and no bytes duplicated between a
cache and an output directory.

```
raw/dmc-api/ema_hourly/2026/07/20.json   <- exists, skipped
raw/dmc-api/ema_hourly/2026/07/21.json   <- fetched tonight
```

Cache keying is therefore just source + product + period, expressed as a path.

### Store — `base_directory` + formatting rule + format

The key abstraction. It absorbs **both** shape and format transformation, which is why there
is no separate "reshaper" concept.

A *master* is just a Store. It may be a single file or a directory tree, and the path rule
**is** the shape:

| Rule | Result |
|---|---|
| `master.nc` | single master file |
| `{year}/{month}/{day}.parquet` | daily-partitioned master directory |
| `{siteid}.csv` | station-per-file |
| `{variable}.nc` | variable-per-file |

Consequences worth keeping in mind:

- Converting between shapes and converting between formats are **the same operation**: read
  one Store, write another.
- Requesting a format the data already has is a **no-op**.
- A cron append writes only the partition the new data belongs to — nothing large is
  rewritten, which is what makes the daily run cheap.

### Processor

Reads the canonical formats, appends into Stores, transforms between them. Owns resampling
and time-resolution logic — the useful parts of today's `postprocess_xarray_data`.

## Current state

The repo today is a 3-stage funnel hardwired to DMC station observations:
**raw JSON → per-station CSV → one master NetCDF**.

```
src/atmosphere_data_cl/
├── cli.py                     typer app: get_dmc, process_intermediate_data
├── data_download/
│   ├── downloader.py          Downloader (base)
│   └── dmc_downloader.py      DMCDownloader
├── translate/
│   ├── translator.py          Translator (base, also concretely usable)
│   └── dmc_translator.py      DMCTranslator
├── utils/{utils,config}.py
└── config/config.json
```

### Migration map

| Today | Becomes |
|---|---|
| `DMCDownloader` | `dmc-api` Source |
| `{siteid}.csv` intermediate (`translate/translator.py:93`) | Store with rule `{siteid}.csv` |
| single NetCDF output | Store with rule `master.nc` |
| `intermediate_to_xarray` (`translate/translator.py:380`) | reusable as the `PointSurface` canonical representation |

`cli.py:173`'s `process_intermediate_data` is already source-agnostic and maps cleanly onto
Store-to-Store conversion.

## Known broken things

Read this before picking up any task here.

- **`utils/config.py` is dead and crashes on import.** It opens `"..\\config\\config.json"`
  with Windows separators (`utils/config.py:10`), at class-body evaluation time — so importing
  it raises `FileNotFoundError` on Linux. Nothing imports it. Superseded by the Store and
  registry design; the real config today is an inline `file_info` dict at
  `translate/translator.py:82`, hardcoded URLs at `data_download/dmc_downloader.py:90`, and
  hardcoded column names in `cli.py`.

- **No tests exist.** No test directory, no test dependency. Establish tests before moving
  pipeline logic — the restructuring is otherwise unguarded.

- `Downloader` and `Translator` use `@abstractmethod` **without inheriting `ABC`**, so the
  decorators are advisory only and contract violations surface as `None` at runtime rather
  than at instantiation. `cli.py:258` instantiates the bare `Translator` deliberately.

## Next steps

1. ~~**Rename + packaging fix.**~~ Done — tree moved to `src/atmosphere_data_cl/`,
   `[tool.setuptools]` added, entrypoint and deps fixed. `pip install -e .` works.
2. **`REF_ONLY/` reference implementations land**; user provides the DMC-WEB scraper and its
   URL. The Source abstraction should be shaped by these real implementations rather than
   guessed at ahead of them.
3. **Build the Source registry and Store abstraction**, starting with `dmc-api`.
