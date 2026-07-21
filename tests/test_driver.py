"""Driver tests: caching and retries, which every Source inherits for free.

Neither reference implementation had retries; SINCA retried a whole batch
forever on ConnectTimeout and VipNet aborted a 24-hour backfill on any error.
"""

import json

import httpx
import pandas as pd
import pytest

from atmosphere_data_cl.sources import Vipnet
from atmosphere_data_cl.sources.source import FetchSpec
from atmosphere_data_cl.utils.time import manage_time_interval

BODY = {"data": [
    {"codigoEstacion": "X1", "nombre": "Uno", "region": 1,
     "altitud": 10, "latitud": -33.0, "longitud": -70.0, "value": 1.5},
]}


def one_hour_spec():
    start, end = manage_time_interval("9/9/2022 14:00")
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
        fresh = source._parse(source._execute(job), job)
        cached = source._parse(source._execute(job), job)
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
        data = source.fetch("Temperatura", "9/9/2022", variables=["Temperatura"])
        assert not data.empty
        assert len(data) == 23  # 24 hours minus the one that failed


class TestFetchShape:
    def test_returns_long_frame_natively(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        data = source.fetch("Temperatura", "9/9/2022 14:00", variables=["Temperatura"])
        assert list(data.columns) == ["timestamp", "station", "variable", "value", "unit"]

    def test_format_argument_converts_on_the_way_out(self, tmp_path):
        from atmosphere_data_cl.store import OneCsvPerStation

        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        written = source.fetch(
            "Temperatura", "9/9/2022 14:00",
            variables=["Temperatura"], format=OneCsvPerStation,
        )
        assert [p.name for p in written] == ["X1.csv"]

    def test_station_filter_is_applied_post_hoc(self, tmp_path):
        """VipNet cannot filter server-side, so it must filter after parsing."""
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        data = source.fetch(
            "Temperatura", "9/9/2022 14:00",
            variables=["Temperatura"], stations=["nonexistent"],
        )
        assert data.empty

    def test_derived_discovery_accumulates_stations(self, tmp_path):
        source = make_source(tmp_path, lambda request: httpx.Response(200, json=BODY))
        source.fetch("Temperatura", "9/9/2022 14:00", variables=["Temperatura"])
        stations = source.discover_stations()
        assert list(stations["codigo"]) == ["X1"]
