"""Driver tests: caching and retries, which every Source inherits for free.

Neither reference implementation had retries; SINCA retried a whole batch
forever on ConnectTimeout and VipNet aborted a 24-hour backfill on any error.
"""

import json

import httpx
import pandas as pd
import pytest
import xarray as xr

from atmosphere_data_cl.sources import Vipnet
from atmosphere_data_cl.sources.source import FetchSpec
from atmosphere_data_cl.utils.time import manage_time_interval

BODY = {"data": [
    {"codigoEstacion": "X1", "nombre": "Uno", "region": 1,
     "altitud": 10, "latitud": -33.0, "longitud": -70.0, "value": 1.5},
]}


def one_hour_spec():
    start, end = manage_time_interval("2022-09-09 14:00")
    return FetchSpec(product="Temperatura", start=start, end=end, variables=["Temperatura"])


def make_source(tmp_path, handler):
    transport = httpx.MockTransport(handler)
    return Vipnet(raw_dir=tmp_path, client=httpx.Client(transport=transport))


class TestCaching:
    def test_raw_payload_is_written(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        job = source.plan(one_hour_spec())[0]
        source._execute(job)
        assert source._raw_path(job).exists()

    def test_second_run_hits_cache_and_makes_no_request(self, tmp_path):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(200, json=BODY)

        source = make_source(tmp_path, handler)
        job = source.plan(one_hour_spec())[0]
        source._execute(job)
        source._execute(job)
        assert len(calls) == 1  # output IS the cache

    def test_use_cache_false_refetches(self, tmp_path):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(200, json=BODY)

        source = make_source(tmp_path, handler)
        job = source.plan(one_hour_spec())[0]
        source._execute(job)
        source._execute(job, use_cache=False)
        assert len(calls) == 2

    def test_cached_payload_parses_identically(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        job = source.plan(one_hour_spec())[0]
        ctx = source._identity(job)
        fresh = source._parse(source._execute(job), ctx)
        cached = source._parse(source._execute(job), ctx)
        pd.testing.assert_frame_equal(fresh, cached)


class TestRetries:
    def test_transient_failure_is_retried(self, tmp_path):
        attempts = []

        def handler(request):
            attempts.append(request)
            if len(attempts) < 3:
                return httpx.Response(503)
            return httpx.Response(200, json=BODY)

        source = make_source(tmp_path, handler)
        source.backoff_seconds = 0  # don't actually sleep in tests
        job = source.plan(one_hour_spec())[0]
        source._execute(job)
        assert len(attempts) == 3

    def test_retries_are_bounded(self, tmp_path):
        attempts = []

        def handler(request):
            attempts.append(request)
            return httpx.Response(500)

        source = make_source(tmp_path, handler)
        source.backoff_seconds = 0
        job = source.plan(one_hour_spec())[0]
        with pytest.raises(httpx.HTTPError):
            source._execute(job)
        assert len(attempts) == source.max_retries  # not forever

    def test_one_bad_job_does_not_lose_the_run(self, tmp_path):
        """A single failing hour must not abort a 24-hour backfill."""
        seen = []

        def handler(request):
            seen.append(request)
            if json.loads(request.read())["fetchHour"] == 3:
                return httpx.Response(500)
            return httpx.Response(200, json=BODY)

        source = make_source(tmp_path, handler)
        source.backoff_seconds = 0
        raw = source.fetch("Temperatura", "2022-09-09", variables=["Temperatura"])
        # 24 hourly payloads minus the one that failed → 23 files, 23 timestamps.
        assert raw.read().sizes["time"] == 23


class TestConcurrency:
    def test_all_jobs_run(self, tmp_path):
        """Every one of the 24 hourly jobs still executes under the default pool."""
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        source.concurrency = 8
        raw = source.fetch("Temperatura", "2022-09-09", variables=["Temperatura"])
        assert len(list(raw.base_dir.glob("*/*.json"))) == 24

    def test_parallel_matches_sequential(self, tmp_path):
        """Concurrency is a dispatch detail: the resulting Dataset is identical."""
        handler = lambda request: httpx.Response(200, json=BODY)  # noqa: E731

        seq = make_source(tmp_path / "seq", handler)
        seq.concurrency = 1
        par = make_source(tmp_path / "par", handler)
        par.concurrency = 8

        ds_seq = seq.fetch("Temperatura", "2022-09-09", variables=["Temperatura"]).read()
        ds_par = par.fetch("Temperatura", "2022-09-09", variables=["Temperatura"]).read()

        assert dict(ds_seq.sizes) == dict(ds_par.sizes)
        xr.testing.assert_equal(ds_seq, ds_par)

    def test_one_worker_is_sequential(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        source.concurrency = 1
        raw = source.fetch("Temperatura", "2022-09-09 14:00", variables=["Temperatura"])
        assert "X1" in raw.read()["station"].values


class TestFetchShape:
    def test_native_fetch_returns_a_readable_raw_store(self, tmp_path):
        """No format asked for => a RawStore over the payloads as downloaded."""
        from atmosphere_data_cl.store import RawStore

        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        raw = source.fetch("Temperatura", "2022-09-09 14:00", variables=["Temperatura"])
        assert isinstance(raw, RawStore)

        # The raw json is the response as-downloaded — no wrapping envelope.
        (path,) = raw.base_dir.glob("*/*.json")
        assert json.loads(path.read_text())["data"][0]["codigoEstacion"] == "X1"

        # And it reads straight into a canonical (time, station) Dataset.
        ds = raw.read()
        assert "temperatura" in ds.data_vars
        assert list(ds["station"].values) == ["X1"]

    def test_parse_produces_canonical_long_columns(self, tmp_path):
        """The long frame is the conversion interchange — produced only via _parse."""
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        job = source.plan(one_hour_spec())[0]
        parsed = source._parse(source._execute(job), source._identity(job))
        assert list(parsed.columns) == ["timestamp", "station", "variable", "value", "unit"]

    def test_format_argument_converts_on_the_way_out(self, tmp_path):
        from atmosphere_data_cl.store import OneCsvPerStation

        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        written = source.fetch(
            "Temperatura", "2022-09-09 14:00",
            variables=["Temperatura"], format=OneCsvPerStation, dest=tmp_path / "out",
        )
        names = {p.name for p in written}
        assert names == {"X1.csv", "stations.csv"}  # value file + metadata sidecar

    def test_station_filter_is_applied_post_hoc(self, tmp_path):
        """VipNet cannot filter server-side, so selection happens on the parse path.

        Native output is unfiltered (raw as downloaded); the filter bites on the
        conversion path, so a non-matching selection writes nothing.
        """
        from atmosphere_data_cl.store import OneCsvPerStation

        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        written = source.fetch(
            "Temperatura", "2022-09-09 14:00",
            variables=["Temperatura"], stations=["nonexistent"],
            format=OneCsvPerStation, dest=tmp_path / "out",
        )
        assert written == []  # filtered out before any file is written

    def test_metadata_rides_along_as_coords(self, tmp_path):
        """Station lat/lon/name attach to the Dataset as station-dim coordinates."""
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        ds = source.fetch("Temperatura", "2022-09-09 14:00", variables=["Temperatura"]).read()
        assert float(ds["latitude"].sel(station="X1")) == -33.0
        assert float(ds["longitude"].sel(station="X1")) == -70.0
        assert str(ds["name"].sel(station="X1").values) == "Uno"

    def test_tabular_writes_metadata_to_a_sidecar(self, tmp_path):
        """Tabular layouts keep lat/lon out of every value file and in one sidecar,
        and reattach it on read."""
        import pandas as pd_

        from atmosphere_data_cl.store import OneCsvPerStation

        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        out = tmp_path / "out"
        source.fetch("Temperatura", "2022-09-09 14:00", variables=["Temperatura"],
                     format=OneCsvPerStation, dest=out)

        sidecar = pd_.read_csv(out / "stations.csv")
        assert list(sidecar["station"].astype(str)) == ["X1"]
        assert float(sidecar["latitude"].iloc[0]) == -33.0

        # the per-station value file holds only time×variables, no lat/lon columns
        value = pd_.read_csv(out / "X1.csv")
        assert "latitude" not in value.columns

        # and reading the store back reattaches the metadata as coords
        back = OneCsvPerStation(out).read()
        assert float(back["latitude"].sel(station="X1")) == -33.0

    def test_derived_discovery_accumulates_stations(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        source.fetch("Temperatura", "2022-09-09 14:00", variables=["Temperatura"])
        stations = source.discover_stations()
        assert list(stations["codigo"]) == ["X1"]
