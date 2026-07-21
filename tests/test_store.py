"""Store round-trip tests: write one shape, read it back, get the same data."""

import numpy as np
import pandas as pd
import pytest

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
    def test_one_file_per_station(self, long, tmp_path):
        written = OneCsvPerStation.write(long, tmp_path)
        assert {p.name for p in written} == {"A.csv", "B.csv"}

    def test_round_trip_preserves_values(self, long, tmp_path):
        OneCsvPerStation.write(long, tmp_path)
        ds = OneCsvPerStation.read(tmp_path)
        assert set(ds.data_vars) == {"O3", "TEMP"}
        assert ds.sizes["station"] == 2
        np.testing.assert_allclose(
            ds["O3"].sel(station="A").values, [0.0, 1.0, 2.0]
        )

    def test_empty_writes_nothing(self, tmp_path):
        assert OneCsvPerStation.write(pd.DataFrame(), tmp_path) == []


class TestSingleNetcdf:
    def test_round_trip(self, long, tmp_path):
        SingleNetcdf.write(long, tmp_path)
        ds = SingleNetcdf.read(tmp_path)
        assert set(ds.data_vars) == {"O3", "TEMP"}
        np.testing.assert_allclose(ds["O3"].sel(station="A").values, [0.0, 1.0, 2.0])

    def test_history_is_a_cf_string_not_a_list(self, long, tmp_path):
        """ClimateGraph stored history as a list, which doesn't round-trip."""
        SingleNetcdf.write(long, tmp_path)
        ds = SingleNetcdf.read(tmp_path)
        assert isinstance(ds.attrs["history"], str)
        assert "atmosphere_data_cl" in ds.attrs["history"]


class TestStoreToStore:
    def test_shape_and_format_conversion_is_one_operation(self, long, tmp_path):
        """The whole point: read one Store, write another."""
        csv_dir, nc_dir = tmp_path / "csv", tmp_path / "nc"
        OneCsvPerStation.write(long, csv_dir)

        ds = OneCsvPerStation.read(csv_dir)
        SingleNetcdf.write(ds, nc_dir)

        back = SingleNetcdf.read(nc_dir)
        assert set(back.data_vars) == {"O3", "TEMP"}
        assert back.sizes["station"] == 2


class TestRegistry:
    def test_stores_register_by_name(self):
        assert Store.registry["one-csv-per-station"] is OneCsvPerStation
        assert Store.registry["single-netcdf"] is SingleNetcdf
