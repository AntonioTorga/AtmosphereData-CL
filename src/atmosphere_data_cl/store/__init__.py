"""Stores — base directory + path template + format.

The path template is the shape and the encoder is the format, so converting
between shapes and converting between formats are one operation: read one
Store, write another.
"""

from .store import OneCsvPerStation, SingleNetcdf, Store, long_to_dataset

__all__ = ["Store", "OneCsvPerStation", "SingleNetcdf", "long_to_dataset"]
