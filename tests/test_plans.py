"""Request-planning tests.

These pin down the thing the Source abstraction exists to absorb: SINCA and
VipNet decompose the same (station x variable x time) cube in inverted ways.
Planning is pure and hits no network, so the exact request list is assertable.
"""

import httpx
import pandas as pd
import pytest

from atmosphere_data_cl.sources import DmcApi, Sinca, Vipnet, get_source, list_sources
from atmosphere_data_cl.sources.sinca import STATION_META_COLS
from atmosphere_data_cl.sources.source import FetchSpec
from atmosphere_data_cl.utils.time import manage_time_interval


_FIELDS = {"product", "stations", "variables"}


def spec(period="2022-09-09", **kwargs):
    """Build a FetchSpec, routing source-specific knobs into `extras`."""
    start, end = manage_time_interval(period)
    fields = {k: v for k, v in kwargs.items() if k in _FIELDS}
    extras = {k: v for k, v in kwargs.items() if k not in _FIELDS}
    fields.setdefault("product", "test")
    return FetchSpec(start=start, end=end, extras=extras, **fields)


class TestRegistry:
    def test_all_three_register(self):
        assert {"sinca", "vipnet", "dmc-api"} <= set(list_sources())

    def test_lookup_by_name(self):
        assert get_source("sinca") is Sinca

    def test_lookup_is_kind_scoped(self):
        assert get_source("vipnet", kind="PointSurface") is Vipnet

    def test_unknown_name_lists_the_known_ones(self):
        with pytest.raises(KeyError, match="No source registered"):
            get_source("nope")


class TestSincaDiscovery:
    """The scrape+assemble path — bypassed by the planning fixture, so tested here
    with fake region HTML. (A missing DataFrame build once slipped through because
    nothing offline drove this code.)"""

    REGION_HTML = (
        '<table id="tablaRegional"><tbody>'
        '<tr><th><a href="/index.php/estacion/index/id/232">Arica</a></th>'
        '<td><a href="?macropath=./RXV/F01/Cal">macro</a></td></tr>'
        '<tr><th><a href="/index.php/estacion/index/id/157">Alto Hospicio</a></th>'
        '<td><a href="?macropath=./RI/117/Cal">macro</a></td></tr>'
        '</tbody></table>'
    )

    # Coordinates live on each station's own page, keyed by id.
    LATLNG = {"232": (-18.48, -70.30), "157": (-20.27, -70.10)}

    def _handler(self, request):
        url = str(request.url)
        if "/estacion/" in url:
            sid = url.rstrip("/").rsplit("/", 1)[-1]
            lat, lng = self.LATLNG.get(sid, (-33.0, -70.0))
            return httpx.Response(200, text=f"<script>google.maps.LatLng({lat}, {lng})</script>")
        return httpx.Response(200, text=self.REGION_HTML)

    def _source(self, tmp_path):
        return Sinca(raw_dir=tmp_path, client=httpx.Client(transport=httpx.MockTransport(self._handler)))

    def test_scrapes_and_assembles_station_table(self, tmp_path):
        s = self._source(tmp_path)
        stations = s.discover_stations(regions=["XV", "I"])

        assert list(stations.columns) == STATION_META_COLS
        assert set(stations["station_id"]) == {"232", "157"}     # from the <th> anchors
        assert set(stations["airviro_id"]) == {"F01", "117"}     # from the macropath links
        assert set(stations["region"]) == {"XV", "I"}            # one row per region scanned
        assert s.stations_file.exists()                          # cached to disk

    def test_enriches_lat_lon_from_station_page(self, tmp_path):
        """Coordinates come from a second GET per station, not the region listing."""
        s = self._source(tmp_path)
        # one region → each station appears once (the fake page lists both)
        stations = s.discover_stations(regions=["XV"]).set_index("station_id")
        assert stations.loc["232", "latitude"] == -18.48
        assert stations.loc["232", "longitude"] == -70.30
        assert stations.loc["157", "latitude"] == -20.27

        # and they thread through station_metadata() as numeric coords
        meta = s.station_metadata().set_index("station")
        assert meta.loc["232", "latitude"] == -18.48

    def test_concurrency_one_matches_default(self, tmp_path):
        seq = self._source(tmp_path / "seq")
        seq.concurrency = 1
        par = self._source(tmp_path / "par")  # default concurrency
        pd.testing.assert_frame_equal(
            seq.discover_stations(regions=["XV", "I", "II"]).sort_values("region").reset_index(drop=True),
            par.discover_stations(regions=["XV", "I", "II"]).sort_values("region").reset_index(drop=True),
        )


class TestSincaPlan:
    """SINCA: station x variable x height fans out, time collapses."""

    @pytest.fixture
    def sinca(self, tmp_path):
        source = Sinca(raw_dir=tmp_path)
        source._stations = pd.DataFrame([
            {"station_id": "1", "name": "A", "region": "M", "airviro_id": "AAA"},
            {"station_id": "2", "name": "B", "region": "V", "airviro_id": "BBB"},
        ])
        return source

    def test_time_does_not_fan_out(self, sinca):
        """One request covers the whole range, however long."""
        jobs = sinca.plan(spec("2022", variables=["O3"]))
        assert len(jobs) == 2  # 2 stations x 1 variable x 1 time bucket
        assert jobs[0].start == pd.Timestamp("2022-01-01")
        assert jobs[0].end == pd.Timestamp("2022-12-31 23:59:59.999999999")

    def test_stations_fan_out(self, sinca):
        jobs = sinca.plan(spec(variables=["O3"]))
        assert {j.axes["station"]["airviro_id"] for j in jobs} == {"AAA", "BBB"}

    def test_heights_multiply_meteorological_only(self, sinca):
        """WSPD at 2m and 10m are different series; O3 has no height."""
        met = sinca.plan(spec(variables=["WSPD"], heights=["002", "010"]))
        pollutant = sinca.plan(spec(variables=["O3"], heights=["002", "010"]))
        assert len(met) == 4        # 2 stations x 2 heights
        assert len(pollutant) == 2  # 2 stations, height ignored

    def test_station_filter_narrows_the_plan(self, sinca):
        jobs = sinca.plan(spec(variables=["O3"], stations=["1"]))
        assert len(jobs) == 1

    def test_request_uses_the_pollutant_macro(self, sinca):
        job = sinca.plan(spec(variables=["O3"]))[0]
        request = sinca._build_request(job)
        assert "/Cal/" in request.params["macro"]
        assert request.params["from"] == "220909"

    def test_request_uses_the_met_macro_with_height(self, sinca):
        job = sinca.plan(spec(variables=["WSPD"], heights=["010"]))[0]
        request = sinca._build_request(job)
        assert "/Met/" in request.params["macro"]
        assert "_010.ic" in request.params["macro"]

    def test_unknown_variable_rejected(self, sinca):
        with pytest.raises(ValueError, match="Unknown variable"):
            sinca.plan(spec(variables=["NOTAVAR"]))


class TestVipnetPlan:
    """VipNet: variable x hour fans out, stations collapse."""

    @pytest.fixture
    def vipnet(self, tmp_path):
        return Vipnet(raw_dir=tmp_path)

    def test_a_day_is_144_requests(self, vipnet):
        """6 variables x 24 hours, and no station axis at all."""
        jobs = vipnet.plan(spec("2022-09-09"))
        assert len(jobs) == 144
        assert "station" not in jobs[0].axes

    def test_hours_fan_out(self, vipnet):
        jobs = vipnet.plan(spec("2022-09-09", variables=["Temperatura"]))
        assert len(jobs) == 24
        assert {j.start.hour for j in jobs} == set(range(24))

    def test_request_is_a_post_with_json_body(self, vipnet):
        job = vipnet.plan(spec("2022-09-09 14:00", variables=["Temperatura"]))[0]
        request = vipnet._build_request(job)
        assert request.method == "POST"
        assert request.json_body["tipoEstacion"] == 1
        assert request.json_body["fetchHour"] == 14
        assert request.json_body["fetchDay"] == "2022-09-09"

    def test_mode_is_selectable(self, tmp_path):
        source = Vipnet(raw_dir=tmp_path, mode="Más Actual")
        job = source.plan(spec("2022-09-09 14:00", variables=["Temperatura"]))[0]
        assert source._build_request(job).json_body["mapStatistic"] == 4

    def test_unknown_mode_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown mode"):
            Vipnet(raw_dir=tmp_path, mode="nonsense")

    def test_unknown_variable_rejected(self, vipnet):
        with pytest.raises(ValueError, match="Unknown VipNet variables"):
            vipnet.plan(spec(variables=["Nieve", "Bogus"]))


class TestDmcPlan:
    """DMC: station x month."""

    @pytest.fixture
    def dmc(self, tmp_path):
        source = DmcApi(user="u", api_key="k", raw_dir=tmp_path)
        source._stations = pd.DataFrame({"codigoNacional": ["330020", "330021"]})
        return source

    def test_stations_times_months(self, dmc):
        jobs = dmc.plan(spec("2022-01-01 to 2022-03-31"))
        assert len(jobs) == 6  # 2 stations x 3 months

    def test_request_embeds_year_and_month(self, dmc):
        job = dmc.plan(spec("2/2022"))[0]
        request = dmc._build_request(job)
        assert request.url.endswith("/2022/2")
        assert request.params == {"usuario": "u", "token": "k"}


class TestJobKeys:
    """Job keys are raw-store filenames, so they must be unique and stable."""

    def test_keys_are_unique(self, tmp_path):
        jobs = Vipnet(raw_dir=tmp_path).plan(spec("2022-09-09"))
        assert len({j.key for j in jobs}) == len(jobs)

    def test_keys_are_stable_across_planning_runs(self, tmp_path):
        source = Vipnet(raw_dir=tmp_path)
        first = [j.key for j in source.plan(spec("2022-09-09"))]
        second = [j.key for j in source.plan(spec("2022-09-09"))]
        assert first == second

    def test_keys_are_filesystem_safe(self, tmp_path):
        jobs = Vipnet(raw_dir=tmp_path).plan(spec("2022-09-09", variables=["Precipitación"]))
        assert all(c.isalnum() or c == "_" for j in jobs for c in j.key)
