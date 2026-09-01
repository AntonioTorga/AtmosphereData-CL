"""Stores: a base directory, a path template, and a format.

A Store absorbs *both* shape and format transformation, because those are the
same operation: the path template is the shape, and the encoder is the format.

    master.nc                       one master file
    {year}/{month}/{day}.parquet    daily-partitioned master directory
    {station}.csv                   station-per-file
    {variable}.nc                   variable-per-file

A Store is a **handle bound to a directory** — ``OneCsvPerStation("out")`` — with
two directions: ``read()`` collapses the files under it into one canonical
Dataset, and ``write(ds)`` splits a Dataset back out across them. Because a
``RawStore`` (the native download layout) is the same kind of handle, converting
anything to anything is one verb::

    Store.change_format(src, dst)   # dst.write(src.read())

The canonical interchange between every handle is an xarray ``(time, station)``
Dataset — the shape ClimateGraph's point_surface readers already produce.
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
    """Pivot a long frame ``[timestamp, station, variable, value, unit]`` into a
    canonical ``(time, station)`` Dataset.

    This is the shape ClimateGraph's point_surface readers produce, so anything
    written here is readable by them and vice versa.
    """
    if long.empty:
        return xr.Dataset()
    # Build the (time, station, variable) cube natively: stations rarely share the
    # exact same set of variables, so a plain pivot yields ragged per-variable
    # station lengths. Indexing into xarray fills missing combinations with NaN.
    unique = long.drop_duplicates(["timestamp", "station", "variable"], keep="first")
    cube = unique.set_index(["timestamp", "station", "variable"])["value"].to_xarray()
    ds = cube.to_dataset(dim="variable").rename({"timestamp": "time"})
    # Drop variables that are entirely absent for this slice of stations.
    ds = ds[[v for v in ds.data_vars if not bool(ds[v].isnull().all())]]

    if "unit" in long.columns:
        units = long.dropna(subset=["unit"]).drop_duplicates("variable").set_index("variable")["unit"]
        for variable in ds.data_vars:
            if variable in units.index and units[variable]:
                ds[variable].attrs["units"] = units[variable]
    return ds


def attach_station_metadata(ds: xr.Dataset, meta: pd.DataFrame) -> xr.Dataset:
    """Attach normalized station metadata as coordinates on the ``station`` dim.

    lat/lon/name/... ride *with* the data — inline in NetCDF/Zarr, and extracted
    to a sidecar by the tabular layouts. Stations absent from ``meta`` get NaN.
    """
    if "station" not in ds.coords or ds.sizes.get("station", 0) == 0:
        return ds
    if meta is None or meta.empty or "station" not in meta.columns:
        return ds
    indexed = meta.drop_duplicates("station").set_index("station")
    stations = [str(s) for s in ds["station"].values]
    for col in indexed.columns:
        # to_numpy(), not .values: pandas 3.0 hands back Arrow-backed extension
        # arrays that xarray can't index. object dtype keeps strings indexable.
        aligned = indexed[col].reindex(stations)
        if pd.api.types.is_numeric_dtype(aligned):
            values = aligned.to_numpy()
        else:
            values = aligned.to_numpy(dtype=object)
        ds = ds.assign_coords({col: ("station", values)})
    return ds


def _station_meta_coords(ds: xr.Dataset) -> list[str]:
    """Coords carried on the station dim other than the station id itself."""
    return [c for c in ds.coords if c != "station" and ds[c].dims == ("station",)]


def _station_meta_frame(ds: xr.Dataset) -> pd.DataFrame | None:
    """Extract station-dim metadata coords back into a ``station``-keyed frame."""
    coords = _station_meta_coords(ds)
    if "station" not in ds.coords or not coords:
        return None
    df = pd.DataFrame({"station": [str(s) for s in ds["station"].values]})
    for coord in coords:
        df[coord] = ds[coord].values
    return df


def _union_station_meta(new: xr.Dataset, old: xr.Dataset) -> pd.DataFrame | None:
    """Union station metadata from two datasets, new winning on overlap."""
    frames = [f for f in (_station_meta_frame(new), _station_meta_frame(old)) if f is not None]
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True).drop_duplicates("station", keep="first")


class Store:
    """Base class for storage layouts. A handle bound to a ``base_dir``."""

    registry: dict[str, type["Store"]] = {}

    name: str
    template: str
    suffix: str

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, "name", None):
            Store.registry[cls.name] = cls

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)

    @abstractmethod
    def write(self, ds: xr.Dataset) -> list[Path]:
        """Write ``ds`` under ``base_dir``, returning the paths written."""

    @abstractmethod
    def read(self) -> xr.Dataset:
        """Read this store back into a canonical Dataset."""

    def append(self, ds: xr.Dataset) -> list[Path]:
        """Grow the store: merge ``ds`` into whatever is already stored here.

        Times, stations and variables union; **new data wins on overlap** (so a
        re-fetch that upgrades preliminary→validated readings takes effect). This
        is the cron primitive — run daily, extend the master rather than clobber
        it. Read-modify-write, written atomically, so a crashed run can't corrupt
        the existing master.
        """
        if not ds.data_vars:
            return []
        existing = self.read()
        if existing.data_vars:
            loaded = existing.load()  # detach from disk before we overwrite it
            existing.close()
            combined = ds.combine_first(loaded)
            # combine_first drops non-index (station-dim) coords on conflict, so
            # re-attach the unioned station metadata, new winning on overlap.
            meta = _union_station_meta(ds, loaded)
            if meta is not None:
                combined = attach_station_metadata(combined, meta)
            ds = combined
        return self.write(ds)

    @staticmethod
    def change_format(src, dst: "Store", mode: str = "overwrite") -> list[Path]:
        """The single conversion verb: read one handle, write (or grow) another.

        ``src`` is anything with ``.read() -> xr.Dataset`` (a Store or a
        RawStore); ``dst`` is a Store bound to the destination directory. Shape
        and format both change here, in one step. ``mode="append"`` grows an
        existing ``dst`` instead of overwriting it.
        """
        ds = src.read()
        return dst.append(ds) if mode == "append" else dst.write(ds)


class OneCsvPerStation(Store):
    """Station-per-file: ``{station}.csv``, wide (rows=time, cols=variables).

    This is SINCA-API's native output layout and the shape ClimateGraph's
    ``station_per_file`` reader expects.
    """

    name = "one-csv-per-station"
    template = "{station}.csv"
    suffix = ".csv"

    #: The station metadata sidecar — tabular value files hold only time×vars, so
    #: lat/lon/name live once here rather than repeated in every station file.
    stations_sidecar = "stations.csv"

    def write(self, ds: xr.Dataset) -> list[Path]:
        if not ds.data_vars or "station" not in ds.coords:
            return []
        written = []
        variables = list(ds.data_vars)
        for station in ds["station"].values:
            wide = ds.sel(station=station).to_dataframe()[variables].dropna(how="all")
            if wide.empty:
                continue
            target = self.base_dir / render_template(self.template, {"station": str(station)})
            atomic_write(target, lambda tmp, w=wide: w.to_csv(tmp, index_label="timestamp"))
            written.append(target)
        written += self._write_sidecar(ds)
        log.info("%s: wrote %d files under %s", self.name, len(written), self.base_dir)
        return written

    def _write_sidecar(self, ds: xr.Dataset) -> list[Path]:
        meta_coords = _station_meta_coords(ds)
        if not meta_coords:
            return []
        df = pd.DataFrame({"station": [str(s) for s in ds["station"].values]})
        for coord in meta_coords:
            df[coord] = ds[coord].values
        target = self.base_dir / self.stations_sidecar
        atomic_write(target, lambda tmp: df.to_csv(tmp, index=False))
        return [target]

    def read(self) -> xr.Dataset:
        frames = []
        for path in sorted(self.base_dir.glob("*.csv")):
            if path.name == self.stations_sidecar:
                continue
            wide = pd.read_csv(path, index_col="timestamp", parse_dates=["timestamp"])
            long = wide.reset_index().melt(
                id_vars="timestamp", var_name="variable", value_name="value"
            )
            long["station"] = path.stem
            long["unit"] = ""
            frames.append(long)
        if not frames:
            return xr.Dataset()
        ds = long_to_dataset(pd.concat(frames, ignore_index=True))

        sidecar = self.base_dir / self.stations_sidecar
        if sidecar.exists():
            meta = pd.read_csv(sidecar)
            meta["station"] = meta["station"].astype(str)
            ds = attach_station_metadata(ds, meta)
        return ds


class SingleNetcdf(Store):
    """Single master file: one ``.nc`` holding every station and variable."""

    name = "single-netcdf"
    template = "master.nc"
    suffix = ".nc"

    def write(self, ds: xr.Dataset) -> list[Path]:
        record(ds, f"written by atmosphere_data_cl as {self.name}")
        target = self.base_dir / self.template
        # Compress: station timeseries are mostly NaN once stations are unioned,
        # and zlib takes those files down by roughly an order of magnitude.
        encoding = {v: {"zlib": True, "complevel": 4} for v in ds.data_vars}
        atomic_write(
            target,
            lambda tmp: ds.to_netcdf(tmp, format="NETCDF4", encoding=encoding, unlimited_dims="time"),
        )
        return [target]

    def read(self) -> xr.Dataset:
        target = self.base_dir / self.template
        if not target.exists():
            return xr.Dataset()  # nothing stored yet — an append starts fresh
        return xr.open_dataset(target, chunks="auto")
