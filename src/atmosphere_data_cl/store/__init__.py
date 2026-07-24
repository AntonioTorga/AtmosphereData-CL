"""Stores — base directory + path template + format.

The path template is the shape and the encoder is the format, so converting
between shapes and converting between formats are one operation: read one
Store, write another.
"""

from .raw import RawStore
from .store import (
    OneCsvPerStation,
    SingleNetcdf,
    Store,
    attach_station_metadata,
    long_to_dataset,
)

__all__ = [
    "Store", "RawStore", "OneCsvPerStation", "SingleNetcdf",
    "long_to_dataset", "attach_station_metadata",
]
