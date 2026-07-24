"""Store round-trip tests: write one shape, read it back, get the same data."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from atmosphere_data_cl.store import OneCsvPerStation, SingleNetcdf, Store, long_to_dataset
from atmosphere_data_cl.utils.paths import atomic_write, parse_template, render_template


@pytest.fixture
def long():
    times = pd.date_range("2022-09-09", periods=3, freq="h")
    rows = []
    for station in ("A", "B"):
        for variable, unit in (("O3", "ppb"), ("TEMP", "deg.C")):
            for i, t in enumerate(times):
                rows.append({"timestamp": t, "station": station, "variable": variable,
                             "value": float(i), "unit": unit})
    return pd.DataFrame(rows)


@pytest.fixture
def ds(long):
    """The canonical (time, station) Dataset — what Stores now read and write."""
    return long_to_dataset(long)


class TestTemplates:
    def test_time_fields_are_zero_padded(self):
        """Without padding, 2026/7 sorts after 2026/12 and breaks glob-and-concat."""
        rendered = render_template("{year}/{month}/{day}.parquet",
                                   {"year": 2026, "month": 7, "day": 5})
        assert str(rendered) == "2026/07/05.parquet"

    def test_round_trip(self):
        template = "{year}/{month}/{day}.parquet"
        rendered = render_template(template, {"year": 2026, "month": 7, "day": 5})
        assert parse_template(template, rendered) == {"year": "2026", "month": "07", "day": "05"}

    def test_missing_field_names_what_is_missing(self):
        with pytest.raises(KeyError, match="day"):
            render_template("{year}/{day}.csv", {"year": 2026})


class TestAtomicWrite:
    def test_no_temp_file_left_behind(self, tmp_path):
        target = tmp_path / "out.txt"
        atomic_write(target, lambda tmp: tmp.write_text("hello"))
        assert target.read_text() == "hello"
        assert list(tmp_path.glob(".*tmp*")) == []

    def test_failure_leaves_target_untouched(self, tmp_path):
        target = tmp_path / "out.txt"
        target.write_text("original")

        def boom(tmp):
            tmp.write_text("partial")
            raise RuntimeError("crashed mid-write")

        with pytest.raises(RuntimeError):
            atomic_write(target, boom)
        assert target.read_text() == "original"  # a crashed cron must not corrupt a master
        assert list(tmp_path.glob(".*tmp*")) == []


class TestLongToDataset:
    def test_canonical_dims(self, long):
        ds = long_to_dataset(long)
        assert set(ds.dims) == {"time", "station"}
        assert set(ds.data_vars) == {"O3", "TEMP"}

    def test_units_land_in_attrs(self, long):
        ds = long_to_dataset(long)
        assert ds["O3"].attrs["units"] == "ppb"

    def test_empty_input(self):
        assert len(long_to_dataset(pd.DataFrame()).data_vars) == 0


class TestOneCsvPerStation:
    def test_one_file_per_station(self, ds, tmp_path):
        written = OneCsvPerStation(tmp_path).write(ds)
        assert {p.name for p in written} == {"A.csv", "B.csv"}

    def test_round_trip_preserves_values(self, ds, tmp_path):
        OneCsvPerStation(tmp_path).write(ds)
        back = OneCsvPerStation(tmp_path).read()
        assert set(back.data_vars) == {"O3", "TEMP"}
        assert back.sizes["station"] == 2
        np.testing.assert_allclose(
            back["O3"].sel(station="A").values, [0.0, 1.0, 2.0]
        )

    def test_empty_writes_nothing(self, tmp_path):
        assert OneCsvPerStation(tmp_path).write(xr.Dataset()) == []


class TestSingleNetcdf:
    def test_round_trip(self, ds, tmp_path):
        SingleNetcdf(tmp_path).write(ds)
        back = SingleNetcdf(tmp_path).read()
        assert set(back.data_vars) == {"O3", "TEMP"}
        np.testing.assert_allclose(back["O3"].sel(station="A").values, [0.0, 1.0, 2.0])

    def test_history_is_a_cf_string_not_a_list(self, ds, tmp_path):
        """ClimateGraph stored history as a list, which doesn't round-trip."""
        SingleNetcdf(tmp_path).write(ds)
        back = SingleNetcdf(tmp_path).read()
        assert isinstance(back.attrs["history"], str)
        assert "atmosphere_data_cl" in back.attrs["history"]

    def test_station_metadata_survives_as_coords(self, ds, tmp_path):
        """In NetCDF, lat/lon stay inline on the station dim — no sidecar needed."""
        from atmosphere_data_cl.store import attach_station_metadata

        meta = pd.DataFrame({"station": ["A", "B"],
                             "latitude": [-33.0, -34.0], "longitude": [-70.0, -71.0]})
        SingleNetcdf(tmp_path).write(attach_station_metadata(ds, meta))
        back = SingleNetcdf(tmp_path).read()
        assert float(back["latitude"].sel(station="A")) == -33.0
        assert float(back["longitude"].sel(station="B")) == -71.0


class TestStoreToStore:
    def test_shape_and_format_conversion_is_one_operation(self, ds, tmp_path):
        """The whole point: read one Store, write another — one verb."""
        csv_dir, nc_dir = tmp_path / "csv", tmp_path / "nc"
        OneCsvPerStation(csv_dir).write(ds)

        Store.change_format(OneCsvPerStation(csv_dir), SingleNetcdf(nc_dir))

        back = SingleNetcdf(nc_dir).read()
        assert set(back.data_vars) == {"O3", "TEMP"}
        assert back.sizes["station"] == 2


def _cube(times, stations, values):
    """Small (time, station) Dataset for append tests."""
    return xr.Dataset(
        {"v": (("time", "station"), np.array(values, dtype=float))},
        coords={"time": pd.to_datetime(times), "station": list(stations)},
    )


class TestGrowingStores:
    def test_single_netcdf_grows_over_time(self, tmp_path):
        """The cron primitive: append extends the master, new data wins on overlap."""
        s = SingleNetcdf(tmp_path)
        s.write(_cube(["2024-01-01", "2024-01-02"], ["A", "B"], [[1, 2], [3, 4]]))
        s.append(_cube(["2024-01-02", "2024-01-03"], ["B", "C"], [[99, 5], [6, 7]]))

        back = s.read()
        assert back.sizes == {"time": 3, "station": 3}       # times + stations unioned
        assert float(back["v"].sel(time="2024-01-02", station="B")) == 99.0  # new wins
        assert float(back["v"].sel(time="2024-01-01", station="A")) == 1.0   # old kept

    def test_append_to_empty_store_just_writes(self, tmp_path):
        s = SingleNetcdf(tmp_path)
        s.append(_cube(["2024-01-01"], ["A"], [[1]]))
        assert float(s.read()["v"].sel(station="A", time="2024-01-01")) == 1.0

    def test_one_csv_per_station_grows(self, tmp_path):
        layout = OneCsvPerStation(tmp_path)
        layout.write(_cube(["2024-01-01", "2024-01-02"], ["A", "B"], [[1, 2], [3, 4]]))
        layout.append(_cube(["2024-01-02", "2024-01-03"], ["B", "C"], [[99, 5], [6, 7]]))

        back = layout.read()
        assert back.sizes["station"] == 3                    # C added as a new file
        assert float(back["v"].sel(time="2024-01-02", station="B")) == 99.0
        assert float(back["v"].sel(time="2024-01-01", station="A")) == 1.0

    def test_change_format_append_accumulates(self, tmp_path):
        src1 = OneCsvPerStation(tmp_path / "s1")
        src1.write(_cube(["2024-01-01"], ["A"], [[1]]))
        src2 = OneCsvPerStation(tmp_path / "s2")
        src2.write(_cube(["2024-01-02"], ["A"], [[2]]))

        dst = SingleNetcdf(tmp_path / "master")
        Store.change_format(src1, dst)
        Store.change_format(src2, dst, mode="append")

        assert dst.read().sizes["time"] == 2

    def test_metadata_coords_survive_append(self, tmp_path):
        from atmosphere_data_cl.store import attach_station_metadata

        meta = pd.DataFrame({"station": ["A", "B", "C"],
                             "latitude": [-33.0, -34.0, -35.0]})
        s = SingleNetcdf(tmp_path)
        s.write(attach_station_metadata(_cube(["2024-01-01"], ["A", "B"], [[1, 2]]), meta))
        s.append(attach_station_metadata(_cube(["2024-01-02"], ["B", "C"], [[3, 4]]), meta))

        back = s.read()
        assert float(back["latitude"].sel(station="C")) == -35.0


class TestRawStore:
    def test_reads_straight_off_disk_with_no_live_source(self, tmp_path):
        """The headline: a RawStore bound to a Source class parses files already
        on disk, recovering each file's axes from its path — no network, no fetch."""
        from atmosphere_data_cl.store import RawStore
        from atmosphere_data_cl.sources import Sinca

        # Lay one raw xcl file exactly where Sinca's raw_template would put it:
        # variable folder, station + height + dates as the filename.
        raw_file = tmp_path / "O3" / "EMA_x__na__20220901-20220901.csv"
        raw_file.parent.mkdir(parents=True)
        raw_file.write_text("FECHA;HORA;val;pre;nov\n220901;1200;12,3;;\n")

        ds = RawStore(Sinca, tmp_path).read()

        assert "O3" in ds.data_vars
        assert list(ds["station"].values) == ["EMA_x"]
        np.testing.assert_allclose(ds["O3"].sel(station="EMA_x").values, [12.3])


class TestRegistry:
    def test_stores_register_by_name(self):
        assert Store.registry["one-csv-per-station"] is OneCsvPerStation
        assert Store.registry["single-netcdf"] is SingleNetcdf
