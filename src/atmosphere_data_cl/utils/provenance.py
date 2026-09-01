"""CF-style provenance breadcrumbs on xarray objects.

Lifted from ClimateGraph's ``utils/dataset_utils.py`` (the ``_record`` helper),
with one fix: the original stores ``attrs["history"]`` as a *list*, which does
not round-trip through ``to_netcdf`` as CF-conformant history. Here history is
kept as a newline-delimited string, which is what CF specifies and what other
tools expect to read back.
"""

from datetime import UTC, datetime

import xarray as xr


def record(obj: xr.Dataset | xr.DataArray, entry: str) -> xr.Dataset | xr.DataArray:
    """Append a timestamped entry to ``obj.attrs["history"]``."""
    existing = obj.attrs.get("history", "")
    if isinstance(existing, list):  # tolerate ClimateGraph-written history
        existing = "\n".join(str(item) for item in existing)

    stamp = datetime.now(tz=UTC).isoformat(timespec="seconds")
    line = f"{stamp} {entry}"
    obj.attrs["history"] = f"{existing}\n{line}" if existing else line
    return obj
