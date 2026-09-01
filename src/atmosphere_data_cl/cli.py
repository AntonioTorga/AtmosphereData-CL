"""AtmosphereData-CL command line interface.

Generic verbs over the Source and Store layers: any registered source or store
works without a new command. Downloading, converting between formats/shapes, and
growing masters over time are all reachable here.
"""

import os
from pathlib import Path

import typer
from rich import print
from typing_extensions import Annotated

from .sources import get_source, list_sources
from .store import RawStore, Store

app = typer.Typer(
    name="AtmosphereData-CL",
    help="Download atmospheric data products and reshape them between formats and layouts.",
    pretty_exceptions_enable=False,
    add_completion=False,
)


# --------------------------------------------------------------------- helpers

def _load_dotenv(path: str = ".env") -> None:
    """Load KEY=VALUE lines from a .env file into the environment (no override).

    Tiny on purpose — avoids a python-dotenv dependency for the one thing we need
    it for (DMC credentials).
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _split_csv(value: str | None) -> list[str] | None:
    """Turn a comma-separated option into a list, or None if unset."""
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_extras(pairs: list[str]) -> dict[str, str]:
    extras: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise typer.BadParameter(f"--extra expects key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        extras[key.strip()] = value.strip()
    return extras


def _build_source(name: str, raw_dir: str, user: str | None, token: str | None, mode: str | None):
    try:
        cls = get_source(name)
    except KeyError:
        raise typer.BadParameter(
            f"unknown source {name!r}. Known: {', '.join(list_sources())}"
        )

    if name == "dmc-api":
        _load_dotenv()
        user = user or os.environ.get("DMC_API_USER")
        token = token or os.environ.get("DMC_API_TOKEN")
        if not (user and token):
            raise typer.BadParameter(
                "dmc-api needs credentials: pass --user/--token or set "
                "DMC_API_USER / DMC_API_TOKEN (a .env file is auto-loaded)."
            )
        return cls(user, token, raw_dir=raw_dir)

    if name == "vipnet" and mode:
        return cls(raw_dir=raw_dir, mode=mode)

    return cls(raw_dir=raw_dir)


def _store(name: str, base_dir: str) -> Store:
    if name not in Store.registry:
        raise typer.BadParameter(
            f"unknown store {name!r}. Known: {', '.join(sorted(Store.registry))}"
        )
    return Store.registry[name](base_dir)


def _summarize(ds, label: str) -> None:
    if not ds.data_vars:
        print(f"[yellow]{label}: no data[/yellow]")
        return
    coords = [c for c in ds.coords if ds[c].dims == ("station",) and c != "station"]
    print(f"[green]{label}[/green]  dims={dict(ds.sizes)}  "
          f"variables={list(ds.data_vars)[:8]}  station-coords={coords}")


# -------------------------------------------------------------------- commands

@app.command()
def fetch(
    source: Annotated[str, typer.Argument(help="Source name (see `sources`).")],
    product: Annotated[str, typer.Argument(
        help="Product/variable, or a comma-separated list: O3 or O3,NO2,PM10.")],
    period: Annotated[str, typer.Argument(
        help='Descending-granularity date/interval: "2024", "2024-01", '
             '"2024-01-15", "2024-01-01 to 2024-01-31", "2026-07-20 12:00".')],
    stations: Annotated[str, typer.Option(help="Comma-separated station ids to keep.")] = None,
    raw_dir: Annotated[str, typer.Option(help="Where raw payloads land (the cache).")] = "raw",
    to: Annotated[str, typer.Option("--to", help="Store to convert into (see `stores`).")] = None,
    dest: Annotated[str, typer.Option("--dest", help="Destination directory for --to.")] = None,
    append: Annotated[bool, typer.Option("--append/--overwrite",
        help="Grow an existing --dest master instead of overwriting it.")] = False,
    user: Annotated[str, typer.Option(help="dmc-api user (else DMC_API_USER).")] = None,
    token: Annotated[str, typer.Option(help="dmc-api token (else DMC_API_TOKEN).")] = None,
    mode: Annotated[str, typer.Option(help="vipnet aggregation mode.")] = None,
    workers: Annotated[int, typer.Option("--workers",
        help="Concurrent requests in flight (lower for a rate-limiting endpoint).")] = None,
    no_cache: Annotated[bool, typer.Option("--no-cache",
        help="Re-download even if the raw file exists. Needed to refresh a still-"
             "accumulating period (e.g. DMC's current month keeps filling in).")] = False,
    extra: Annotated[list[str], typer.Option("--extra",
        help="Extra fetch option key=value (repeatable), e.g. min_validation_level=preliminar.")] = None,
):
    """Download data from a source into the raw store, optionally converting to a master.

    The raw store doubles as the cache: a period already on disk is not refetched
    unless you pass --no-cache. With --to/--dest the raw is converted into the given
    layout; add --append to grow an existing master (the cron pattern).
    """
    if to and not dest:
        raise typer.BadParameter("--to requires --dest")

    src = _build_source(source, raw_dir, user, token, mode)
    if workers is not None:
        src.concurrency = workers
    extras = _parse_extras(extra or [])
    # PRODUCT may be a single value or a comma-separated list; either way it drives
    # the source's variable fan-out.
    products = _split_csv(product)
    raw = src.fetch(
        products[0], period,
        stations=_split_csv(stations),
        variables=products,
        use_cache=not no_cache,
        **extras,
    )
    print(f"[green]fetched[/green] {source}/{','.join(products)} → raw under {raw.base_dir}")

    if not to:
        _summarize(raw.read(), "raw")
        return

    written = Store.change_format(raw, _store(to, dest), mode="append" if append else "overwrite")
    print(f"[green]wrote[/green] {len(written)} file(s) as {to} under {dest}"
          f"{' (appended)' if append else ''}")


@app.command()
def convert(
    from_store: Annotated[str, typer.Argument(help="Source layout (see `stores`).")],
    src_dir: Annotated[str, typer.Argument(help="Directory of the source store.")],
    to_store: Annotated[str, typer.Argument(help="Destination layout.")],
    dest: Annotated[str, typer.Argument(help="Destination directory.")],
    append: Annotated[bool, typer.Option("--append/--overwrite",
        help="Grow an existing destination instead of overwriting it.")] = False,
):
    """Convert one store into another — reshape and reformat in one step.

    Changing shape (station-per-file ↔ master) and changing format are the same
    operation: read one store, write another.
    """
    written = Store.change_format(
        _store(from_store, src_dir), _store(to_store, dest),
        mode="append" if append else "overwrite",
    )
    print(f"[green]wrote[/green] {len(written)} file(s) as {to_store} under {dest}"
          f"{' (appended)' if append else ''}")


@app.command()
def sources():
    """List the registered data sources."""
    for name in list_sources():
        print(name)


@app.command()
def stores():
    """List the registered storage layouts."""
    for name in sorted(Store.registry):
        print(name)


if __name__ == "__main__":
    app()
