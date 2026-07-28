"""The Source abstraction: acquisition, split by access method.

A Source is *one way of getting data*, not one organisation. DMC splits into
``dmc-api`` and ``dmc-web`` because they share no auth and no transport.

The central problem this design solves is that sources decompose the
(station x variable x time) cube in incompatible ways. SINCA takes an arbitrary
from/to range in a single request but needs one request per station; Vipnet
takes a single instant but returns every station at once. Which axes fan out
and which collapse is exactly inverted between them, so request *construction*
cannot be shared.

What can be shared is the driver: a Source declares its fan-out as data
(``FetchPlan``), and ``Source.fetch`` expands it into jobs and runs them with
one client, uniform retries, and skip-if-cached. Neither reference
implementation had retries; both get them here for free.
"""

import json
import logging
import threading
import time
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Literal

import httpx
import pandas as pd

from ..utils.time import as_interval, chunk_period

log = logging.getLogger(__name__)

Kind = Literal["PointSurface", "Gridded", "Satellite"]
Discovery = Literal["required", "derived", "none"]


@dataclass
class FetchSpec:
    """Everything a fetch needs, threaded through every hook.

    One bundle rather than growing hook signatures — ``extras`` is the
    open-ended escape hatch for source-specific knobs (SINCA's validation
    level and heights, Vipnet's mode) so they never pollute the base contract.
    """

    product: str
    start: pd.Timestamp
    end: pd.Timestamp
    stations: list[str] | None = None
    variables: list[str] | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def interval(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        return self.start, self.end


@dataclass
class Job:
    """A single request to make. ``key`` identifies it in the raw store."""

    key: str
    axes: dict[str, Any]
    spec: FetchSpec

    @property
    def start(self) -> pd.Timestamp:
        return self.axes.get("start", self.spec.start)

    @property
    def end(self) -> pd.Timestamp:
        return self.axes.get("end", self.spec.end)


@dataclass
class Request:
    """A transport-agnostic description of one HTTP call."""

    url: str
    method: str = "GET"
    params: dict[str, Any] | None = None
    json_body: dict[str, Any] | None = None
    headers: dict[str, str] | None = None
    timeout: float = 60.0


class Source:
    """Base class for all data sources.

    Subclasses declare ``kind``, ``name``, ``discovery`` and ``time_grain``,
    then implement ``_plan_axes``, ``_build_request`` and ``_parse``.
    """

    registry: dict[str, dict[str, type["Source"]]] = defaultdict(dict)

    kind: Kind
    name: str
    #: How station metadata is obtained. ``required`` means discovery must run
    #: before any fetch because it supplies the routing key (SINCA's
    #: airviro_id); ``derived`` means it falls out of every data response
    #: (Vipnet); ``none`` means the source has no station concept.
    discovery: Discovery = "none"
    #: Largest span a single request can cover. ``None`` means the source
    #: accepts an arbitrary from/to range and time does not fan out at all.
    time_grain: str | None = None
    #: Native payload format, as emitted by ``fetch`` when no format is asked for.
    native_format: str = "json"
    #: Maps normalized metadata names -> this source's discovery column names, so
    #: station lat/lon/name/... travel with the data as coordinates. Only columns
    #: actually present in the discovery table are used, so speculative entries
    #: are harmless. Empty means the source carries no station metadata.
    station_meta_map: dict[str, str] = {}
    #: Directory+filename template for one raw file, relative to ``raw_dir``. The
    #: per-file axes live *in the path* (``{variable}/{station}/{start}-{end}``),
    #: so ``RawStore`` can recover them with ``parse_template`` — no sidecars, no
    #: manifest. Fields are supplied by ``_identity``.
    raw_template: str = "{key}.{ext}"

    max_retries: int = 3
    backoff_seconds: float = 1.0
    #: How many requests to run in flight at once. Fetching is almost all network
    #: wait, so a bounded pool cuts wall time ~linearly. Lower it for a fragile or
    #: rate-limiting endpoint; 1 means fully sequential.
    concurrency: int = 8

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Abstract intermediates (e.g. a shared HTML-scraping base) declare no
        # name and are deliberately not registered.
        if getattr(cls, "name", None) is None:
            return
        if not hasattr(cls, "kind"):
            raise TypeError(f"{cls.__name__} must define a 'kind' attribute.")
        Source.registry[cls.kind][cls.name.lower()] = cls

    def __init__(self, raw_dir: str | Path = "raw", client: httpx.Client | None = None):
        self.raw_dir = Path(raw_dir) / self.name
        self._client = client
        self._owns_client = client is None
        self._client_lock = threading.Lock()

    # ---------------------------------------------------------------- hooks

    @abstractmethod
    def _plan_axes(self, spec: FetchSpec) -> dict[str, list[Any]]:
        """Return the axes that fan out, as ``{axis_name: [values]}``.

        Axes not returned collapse into a single request. The time axis is
        handled by ``time_grain`` and need not be returned here.
        """

    @abstractmethod
    def _build_request(self, job: Job) -> Request:
        """Build the HTTP request for one job."""

    @abstractmethod
    def _identity(self, job: Job) -> dict[str, str]:
        """The per-file axes as a flat dict of path-safe strings.

        These fields fill ``raw_template`` to place the raw file, and are exactly
        the context ``_parse`` needs — so a file's identity travels in its path
        and nowhere else. Values must be filesystem-safe (ids/codes/timestamps,
        never display names).
        """

    @abstractmethod
    def _parse(self, payload: Any, ctx: dict[str, str]) -> pd.DataFrame:
        """Parse one response into a long ``[timestamp, station, variable, value, unit]``.

        ``ctx`` is the identity dict (from ``_identity`` live, or recovered from
        the path by ``RawStore`` on disk), plus any read-time options. Parsing
        thus works identically whether the payload just arrived or was picked up
        off disk with no live fetch.
        """

    def discover_stations(self) -> pd.DataFrame:
        """Return station metadata. Required for ``discovery == "required"``."""
        raise NotImplementedError(f"{self.name} does not implement station discovery")

    def _cached_stations(self) -> pd.DataFrame | None:
        """The discovery table if already available *without* a network call.

        Uses an in-memory ``_stations`` if the source keeps one, else a
        ``stations_file`` already on disk. Never fetches — a disk-only
        ``RawStore.read`` must stay offline.
        """
        if getattr(self, "_stations", None) is not None:
            return self._stations
        stations_file = getattr(self, "stations_file", None)
        if stations_file is not None and Path(stations_file).exists():
            return pd.read_csv(stations_file)
        return None

    def station_metadata(self) -> pd.DataFrame:
        """Normalized station metadata: a ``station`` column plus lat/lon/name/...

        Keyed by the same id the value frames use, so it attaches as coordinates.
        Offline — cached discovery only, so metadata is threaded when available
        and simply absent otherwise.
        """
        empty = pd.DataFrame(columns=["station"])
        if not self.station_meta_map:
            return empty
        raw = self._cached_stations()
        if raw is None or raw.empty:
            return empty
        rename = {src: norm for norm, src in self.station_meta_map.items() if src in raw.columns}
        meta = raw[list(rename)].rename(columns=rename)
        if "station" not in meta.columns:
            return empty
        meta["station"] = meta["station"].astype(str)
        # Sources return coordinates as strings ("-18.35555"); make them numeric
        # so they land as float coords rather than string ones.
        for col in ("latitude", "longitude", "altitude"):
            if col in meta.columns:
                meta[col] = pd.to_numeric(meta[col], errors="coerce")
        return meta.drop_duplicates("station")

    def _accumulate_discovery(self, items: list[tuple[Job, Any]]) -> None:
        """Harvest station metadata from a whole fetch's payloads, once.

        Only meaningful for ``discovery == "derived"`` sources (Vipnet), where
        the station set rides along in every response. Called once per fetch with
        every (job, payload) pair, so the ``stations`` union is written a single
        time rather than rebuilt on every request. Default: no-op.
        """
        return None

    # --------------------------------------------------------------- driver

    def plan(self, spec: FetchSpec) -> list[Job]:
        """Expand the declared fan-out into a concrete list of jobs.

        This is deliberately separable from execution: it is pure, cheap, and
        the thing tests assert against to pin down each source's request shape.
        """
        axes = self._plan_axes(spec)
        buckets = chunk_period(spec.start, spec.end, self.time_grain)

        jobs: list[Job] = []
        for start, end in buckets:
            for combo in _product(axes):
                combo = {**combo, "start": start, "end": end}
                jobs.append(Job(key=self._job_key(combo, spec), axes=combo, spec=spec))
        return jobs

    def _job_key(self, axes: dict[str, Any], spec: FetchSpec) -> str:
        """Filesystem-safe identity for a job — also its raw-store filename."""
        parts = [spec.product]
        for key in sorted(axes):
            value = axes[key]
            if isinstance(value, pd.Timestamp):
                value = value.strftime("%Y%m%dT%H%M")
            parts.append(f"{key}-{value}")
        from ..utils.paths import safe_name

        return safe_name("__".join(str(p) for p in parts))

    @property
    def client(self) -> httpx.Client:
        # Double-checked lock: concurrent jobs must not each build their own client.
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = httpx.Client()
        return self._client

    def _map_concurrent(self, fn: Callable[[Any], Any], items: list) -> list:
        """Run ``fn`` over ``items`` with a bounded thread pool, preserving order.

        The shared work-horse for parallel I/O — used by the fetch loop and by
        sources that scrape many pages (SINCA's regions). ``concurrency == 1``
        stays fully sequential.
        """
        items = list(items)
        if not items:
            return []
        if self.concurrency <= 1 or len(items) == 1:
            return [fn(item) for item in items]
        self.client  # force one shared client before the workers race for it
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            return list(pool.map(fn, items))

    def close(self) -> None:
        if self._client is not None and self._owns_client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _raw_path(self, job: Job) -> Path:
        from ..utils.paths import render_template

        fields = {**self._identity(job), "ext": self.native_format}
        return self.raw_dir / render_template(self.raw_template, fields)

    def _execute(self, job: Job, use_cache: bool = True) -> Any:
        """Run one job, returning its payload. Cached payloads short-circuit."""
        raw_path = self._raw_path(job)
        if use_cache and raw_path.exists():
            log.debug("cache hit: %s", raw_path)
            return self._load_raw(raw_path)

        request = self._build_request(job)
        payload = self._send(request)

        raw_path.parent.mkdir(parents=True, exist_ok=True)
        self._save_raw(payload, raw_path)
        return payload

    def _send(self, request: Request) -> Any:
        """Send one request with retries and exponential backoff."""
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = self.client.request(
                    request.method,
                    request.url,
                    params=request.params,
                    json=request.json_body,
                    headers=request.headers,
                    timeout=request.timeout,
                    follow_redirects=True,
                )
                response.raise_for_status()
                return self._decode(response)
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    delay = self.backoff_seconds * (2**attempt)
                    log.warning(
                        "%s: request failed (%s), retrying in %.1fs [%d/%d]",
                        self.name, exc, delay, attempt + 1, self.max_retries,
                    )
                    time.sleep(delay)
        raise last_error

    def _run_jobs(self, jobs: list[Job], use_cache: bool, keep_payloads: bool) -> list[tuple[Job, Any]]:
        """Execute jobs concurrently, dropping (and logging) any that fail permanently.

        A single bad job never sinks the run. Payloads are retained only when a
        caller needs them (derived discovery) — otherwise they are written to the
        raw store and released, so a huge fetch doesn't hold every response in RAM.
        """
        def run(job: Job) -> tuple[Job, Any] | None:
            try:
                payload = self._execute(job, use_cache=use_cache)
            except Exception as exc:  # one bad job must not lose the whole run
                log.warning(f"{self.name}: job {job.key} failed permanently: {exc}")
                return None
            return (job, payload if keep_payloads else None)

        return [r for r in self._map_concurrent(run, jobs) if r is not None]

    def _decode(self, response: httpx.Response) -> Any:
        """Turn a response into the native payload. Override for text formats."""
        return response.json()

    def _save_raw(self, payload: Any, path: Path) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _load_raw(self, path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    # ---------------------------------------------------------------- public

    def fetch(
        self,
        product: str,
        period,
        stations: list[str] | None = None,
        variables: list[str] | None = None,
        format: type | None = None,
        dest: str | Path | None = None,
        use_cache: bool = True,
        **extras,
    ):
        """Acquire data, returning a ``RawStore`` bound to this source.

        Every job's payload lands in the raw store as downloaded (the store *is*
        the cache — a period already on disk is never refetched). The return
        value is a ``RawStore`` handle over that directory, which you can read
        lazily or convert::

            raw = Sinca().fetch("O3", "9/2022")          # RawStore, nothing parsed
            raw.read()                                   # → (time, station) Dataset
            Store.change_format(raw, OneCsvPerStation("out"))

        Passing ``format`` (a Store class) plus ``dest`` converts immediately and
        returns the written paths — sugar over ``change_format``. Parsing only
        ever happens on a read/convert, never on acquisition.
        """
        from ..store.raw import RawStore
        from ..store.store import Store

        start, end = as_interval(period)
        spec = FetchSpec(
            product=product, start=start, end=end,
            stations=stations, variables=variables, extras=extras,
        )

        if self.discovery == "required":
            self.discover_stations()

        # Run every request concurrently (bounded by `concurrency`). Payloads are
        # kept only for derived discovery; otherwise they're written and released.
        keep = self.discovery == "derived"
        collected = self._run_jobs(self.plan(spec), use_cache, keep_payloads=keep)

        # Derived discovery: union the station metadata once, not per request.
        if keep and collected:
            self._accumulate_discovery(collected)

        raw = RawStore(self, read_options={"stations": stations, **extras})
        if format is None:
            return raw
        target = format(dest if dest is not None else self.raw_dir.parent / format.name)
        return Store.change_format(raw, target)


def _product(axes: dict[str, list[Any]]) -> Iterator[dict[str, Any]]:
    """Cartesian product over the fanning-out axes, as dicts."""
    if not axes:
        yield {}
        return
    names = list(axes)
    from itertools import product as _iproduct

    for combo in _iproduct(*(axes[name] for name in names)):
        yield dict(zip(names, combo))


def get_source(name: str, kind: Kind | None = None) -> type[Source]:
    """Look up a registered Source by name."""
    if kind is not None:
        try:
            return Source.registry[kind][name.lower()]
        except KeyError:
            raise KeyError(f"No source {name!r} registered under kind {kind!r}") from None
    for kind_sources in Source.registry.values():
        if name.lower() in kind_sources:
            return kind_sources[name.lower()]
    raise KeyError(f"No source registered as {name!r}. Known: {sorted(list_sources())}")


def list_sources(kind: Kind | None = None) -> list[str]:
    if kind is not None:
        return sorted(Source.registry[kind])
    return sorted(n for sources in Source.registry.values() for n in sources)
