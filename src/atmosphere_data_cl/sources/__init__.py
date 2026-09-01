"""Data sources, each one access method.

Importing this package registers every Source in it. Dropping a new module in
this directory is enough to register it — there is no manifest to update.
"""

import importlib
import pkgutil

from .source import FetchSpec, Job, Request, Source, get_source, list_sources

# Import every submodule so __init_subclass__ populates the registry. Lookups
# by name would otherwise silently miss sources nobody had imported yet.
for _finder, _name, _ispkg in pkgutil.iter_modules(__path__):
    if _name != "source":
        importlib.import_module(f"{__name__}.{_name}")

from .dmc_api import DmcApi  # noqa: E402
from .sinca import Sinca  # noqa: E402
from .vipnet import Vipnet  # noqa: E402

__all__ = [
    "Source", "FetchSpec", "Job", "Request",
    "get_source", "list_sources",
    "Sinca", "Vipnet", "DmcApi",
]
