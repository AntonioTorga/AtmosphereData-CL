"""Stores: a base directory, a path template, and a format.

A Store absorbs *both* shape and format transformation, because those are the
same operation: the path template is the shape, and the encoder is the format.

    master.nc                       one master file
    {year}/{month}/{day}.parquet    daily-partitioned master directory
    {station}.csv                   station-per-file
    {variable}.nc                   variable-per-file

Stores own both directions. Reading collapses many files into one canonical
Dataset; writing splits one Dataset back out across many. ClimateGraph's reader
only ever did the first half — it can read ``{siteid}.csv`` but not write it —
so the write side here is new work rather than a port.

Only what ``Source.fetch(format=...)`` needs is implemented so far; ``_split``
for arbitrary templates and ``_merge_existing`` for incremental cron appends
land with the full Store layer.
"""

import logging
from abc import abstractmethod
from pathlib import Path

import pandas as pd
import xarray as xr

from ..utils.paths import atomic_write, render_template
from ..utils.provenance import record

log = logging.getLogger(__name__)


def long_to_dataset(long: pd.DataFrame) -> xr.Dataset:
    """Pivot a long frame into the canonical ``(time, station)`` Dataset.

    This is the shape ClimateGraph's point_surface readers produce, so anything
    written here is readable by them and vice versa.
    """
    if long.empty:
        return xr.Dataset()
    wide = long.pivot_table(
        index="timestamp", columns=["station", "variable"], values="value", aggfunc="first"
    )
    ds = xr.Dataset(
        {
            variable: (("time", "station"), wide.xs(variable, axis=1, level="variable").values)
            for variable in wide.columns.get_level_values("variable").unique()
        },
        coords={
            "time": wide.index.values,
            "station": wide.columns.get_level_values("station").unique().values,
        },
    )
    units = long.dropna(subset=["unit"]).drop_duplicates("variable").set_index("variable")["unit"]
    for variable in ds.data_vars:
        if variable in units.index and units[variable]:
            ds[variable].attrs["units"] = units[variable]
    return ds


class Store:
    """Base class for storage layouts."""

    registry: dict[str, type["Store"]] = {}

    name: str
    template: str
    suffix: str

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, "name", None):
            Store.registry[cls.name] = cls

    @classmethod
    @abstractmethod
    def write(cls, data, base_dir: str | Path) -> list[Path]:
        """Write ``data`` under ``base_dir``, returning the paths written."""

    @classmethod
    @abstractmethod
    def read(cls, base_dir: str | Path) -> xr.Dataset:
        """Read this store back into a canonical Dataset."""

    @classmethod
    def write_from_long(cls, long: pd.DataFrame, source=None, spec=None) -> list[Path]:
        """Entry point used by ``Source.fetch(format=...)``."""
        base_dir = Path(getattr(source, "raw_dir", "data")).parent / cls.name
        return cls.write(long, base_dir)


class OneCsvPerStation(Store):
    """Station-per-file: ``{station}.csv``, wide (rows=time, cols=variables).

    This is SINCA-API's native output layout and the shape ClimateGraph's
    ``station_per_file`` reader expects.
    """

    name = "one-csv-per-station"
    template = "{station}.csv"
    suffix = ".csv"

    @classmethod
    def write(cls, long: pd.DataFrame, base_dir: str | Path) -> list[Path]:
        base_dir = Path(base_dir)
        if long.empty:
            return []

        written = []
        for station, group in long.groupby("station"):
            wide = group.pivot_table(
                index="timestamp", columns="variable", values="value", aggfunc="first"
            ).sort_index()
            target = base_dir / render_template(cls.template, {"station": station})
            atomic_write(target, lambda tmp, w=wide: w.to_csv(tmp, index_label="timestamp"))
            written.append(target)
        log.info("%s: wrote %d files under %s", cls.name, len(written), base_dir)
        return written

    @classmethod
    def read(cls, base_dir: str | Path) -> xr.Dataset:
        frames = []
        for path in sorted(Path(base_dir).glob("*.csv")):
            wide = pd.read_csv(path, index_col="timestamp", parse_dates=["timestamp"])
            long = wide.reset_index().melt(
                id_vars="timestamp", var_name="variable", value_name="value"
            )
            long["station"] = path.stem
            long["unit"] = ""
            frames.append(long)
        if not frames:
            return xr.Dataset()
        return long_to_dataset(pd.concat(frames, ignore_index=True))


class SingleNetcdf(Store):
    """Single master file: one ``.nc`` holding every station and variable."""

    name = "single-netcdf"
    template = "master.nc"
    suffix = ".nc"

    @classmethod
    def write(cls, long: pd.DataFrame, base_dir: str | Path) -> list[Path]:
        ds = long_to_dataset(long) if isinstance(long, pd.DataFrame) else long
        record(ds, f"written by atmosphere_data_cl as {cls.name}")
        target = Path(base_dir) / cls.template
        # Compress: station timeseries are mostly NaN once stations are unioned,
        # and zlib takes those files down by roughly an order of magnitude.
        encoding = {v: {"zlib": True, "complevel": 4} for v in ds.data_vars}
        atomic_write(
            target,
            lambda tmp: ds.to_netcdf(tmp, format="NETCDF4", encoding=encoding, unlimited_dims="time"),
        )
        return [target]

    @classmethod
    def read(cls, base_dir: str | Path) -> xr.Dataset:
        return xr.open_dataset(Path(base_dir) / cls.template, chunks="auto")
