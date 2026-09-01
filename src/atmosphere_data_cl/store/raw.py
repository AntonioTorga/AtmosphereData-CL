"""RawStore — the native download layout, as a readable handle.

A ``RawStore`` is deliberately dumb: it knows only "a directory of raw files laid
out under a Source's ``raw_template``". It borrows the *interpretation* from the
Source it is bound to — each file's axes are recovered from its path (no sidecar,
no manifest), and the Source's own ``_parse`` turns the payload into canonical
records. So the same class serves two flows with no special-casing:

    raw = Vipnet().fetch("Temperatura", "2022-09-09")   # bound to a live source
    raw.read()

    RawStore(Vipnet, "raw/vipnet").read()             # picked straight off disk,
                                                      # no network, no live fetch

Because it exposes ``read() -> xr.Dataset`` like any Store, ``change_format``
treats it identically to an on-disk layout.
"""

import logging
from pathlib import Path

import pandas as pd
import xarray as xr

from ..utils.paths import parse_template, template_to_glob
from .store import attach_station_metadata, long_to_dataset

log = logging.getLogger(__name__)


class RawStore:
    """A handle over a Source's raw dump. Reads by delegating to the Source."""

    def __init__(self, source, base_dir: str | Path | None = None, read_options: dict | None = None):
        # A class is fine for sources that construct without args (Vipnet, Sinca);
        # pass an instance for those that need credentials (DmcApi(user, key)).
        self.source = source() if isinstance(source, type) else source
        self.base_dir = Path(base_dir) if base_dir is not None else self.source.raw_dir
        self.read_options = read_options or {}

    def read(self, **overrides) -> xr.Dataset:
        """Parse every raw file under ``base_dir`` into one ``(time, station)`` Dataset.

        Per-file axes are recovered from the path; read-time options (a station
        filter, SINCA's ``min_validation_level``) come from ``read_options`` and
        can be overridden here. One unparseable file is skipped, never fatal.
        """
        options = {**self.read_options, **overrides}
        template = self.source.raw_template
        frames: list[pd.DataFrame] = []
        for path in sorted(self.base_dir.glob(template_to_glob(template))):
            rel = path.relative_to(self.base_dir).as_posix()
            fields = parse_template(template, rel)
            if not fields:
                continue
            ctx = {**fields, **options}
            try:
                payload = self.source._load_raw(path)
                parsed = self.source._parse(payload, ctx)
            except Exception as exc:  # one bad file must not lose the read
                log.warning("%s: could not parse %s: %s", self.source.name, path, exc)
                continue
            if parsed is not None and not parsed.empty:
                frames.append(parsed)
        if not frames:
            return xr.Dataset()
        ds = long_to_dataset(pd.concat(frames, ignore_index=True))
        # Thread station metadata (lat/lon/name/...) in as coordinates, when the
        # source's discovery table is available offline.
        return attach_station_metadata(ds, self.source.station_metadata())
