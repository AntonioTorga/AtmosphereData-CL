"""DMC — Dirección Meteorológica de Chile, via its public API.

Ported from ``data_download/dmc_downloader.py`` plus the column names that were
stranded as string literals in ``cli.py``. This is ``dmc-api`` specifically:
data reachable through the documented API. Series the API does not serve (such
as Precipitación Diaria Histórica) need scraping and will land as a separate
``dmc-web`` Source, since it shares neither auth nor transport with this one.

Unlike SINCA and VipNet, DMC requires credentials.

The original ``fetch_all_data`` fired every URL for a station concurrently with
``timeout=None`` and no semaphore, and on ``ConnectTimeout`` retried the whole
batch forever. The shared driver's per-request retry with bounded backoff
replaces that.
"""

import logging
from typing import Any

import pandas as pd

from .source import FetchSpec, Job, Request, Source

log = logging.getLogger(__name__)

DATA_URL = "https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesEma/{siteid}/{year}/{month}"
STATIONS_URL = "https://climatologia.meteochile.gob.cl/application/servicios/getEstacionesRedEma"

# These were string literals in cli.py, disconnected from the DMC classes that
# actually knew about them.
TIME_COLUMN = "momento"
STATION_ID_COLUMN = "codigoNacional"
LATITUDE_COLUMN = "latitud"
LONGITUDE_COLUMN = "longitud"

STATION_META_COLS = [STATION_ID_COLUMN, LATITUDE_COLUMN, LONGITUDE_COLUMN]


class DmcApi(Source):
    """DMC automatic weather stations (Red EMA), via the climatologia API.

    Requires ``user`` and ``api_key``. Station discovery is *required* — the
    station list supplies the site ids every data request is keyed by.
    """

    kind = "PointSurface"
    name = "dmc-api"
    discovery = "required"
    #: The endpoint is addressed by year/month, so a range fans out monthly.
    time_grain = "month"
    native_format = "json"
    #: raw/dmc-api/<station>__<YYYY-MM>.json — one payload carries every variable
    #: for a station-month, so there's no variable to split on; station is a
    #: filename prefix, month in the name.
    raw_template = "{station}__{time}.{ext}"
    # Speculative names too — station_metadata() keeps only columns that exist,
    # so the exact DMC schema gets pinned down at live-test time.
    station_meta_map = {
        "station": STATION_ID_COLUMN, "latitude": LATITUDE_COLUMN,
        "longitude": LONGITUDE_COLUMN, "name": "nombreEstacion", "altitude": "altura",
    }

    def __init__(self, user: str, api_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user
        self.api_key = api_key
        self._stations: pd.DataFrame | None = None
        self.stations_file = self.raw_dir / "stations.csv"

    @property
    def _auth(self) -> dict[str, str]:
        return {"usuario": self.user, "token": self.api_key}

    def _identity(self, job: Job) -> dict[str, str]:
        return {"station": str(job.axes["station"]), "time": job.start.strftime("%Y-%m")}

    def discover_stations(self, refresh: bool = False) -> pd.DataFrame:
        """Fetch the Red EMA station list, caching it to ``stations.csv``."""
        if self._stations is not None and not refresh:
            return self._stations
        if self.stations_file.exists() and not refresh:
            self._stations = pd.read_csv(self.stations_file, dtype={STATION_ID_COLUMN: str})
            return self._stations
        payload = self._send(Request(url=STATIONS_URL, params=self._auth))
        self._stations = pd.DataFrame(payload["datosEstacion"])
        self.stations_file.parent.mkdir(parents=True, exist_ok=True)
        self._stations.to_csv(self.stations_file, index=False)
        log.info("dmc-api: discovered %d stations", len(self._stations))
        return self._stations

    def _plan_axes(self, spec: FetchSpec) -> dict[str, list[Any]]:
        """Fan out over stations; time fans out monthly via ``time_grain``."""
        stations = self.discover_stations()
        ids = stations[STATION_ID_COLUMN].astype(str).tolist()
        if spec.stations:
            wanted = {str(s) for s in spec.stations}
            ids = [i for i in ids if i in wanted]
        return {"station": ids}

    def _build_request(self, job: Job) -> Request:
        return Request(
            url=DATA_URL.format(
                siteid=job.axes["station"],
                year=job.start.year,
                month=job.start.month,
            ),
            params=self._auth,
        )

    def _parse(self, payload: dict, ctx: dict[str, str]) -> pd.DataFrame:
        data = payload.get("datosEstaciones", {}).get("datos") if payload else None
        if not data:
            return pd.DataFrame()

        frame = pd.DataFrame(data)
        if TIME_COLUMN not in frame.columns:
            return pd.DataFrame()

        value_columns = [c for c in frame.columns if c != TIME_COLUMN]
        long = frame.melt(
            id_vars=[TIME_COLUMN], value_vars=value_columns,
            var_name="variable", value_name="raw",
        )
        # DMC embeds the unit in the value string, e.g. "20.5 °C" — split the
        # number from the trailing unit rather than coercing the whole thing to
        # NaN (which silently dropped every variable).
        parts = long["raw"].astype(str).str.extract(r"^\s*(-?\d+(?:[.,]\d+)?)\s*(.*?)\s*$")
        value = pd.to_numeric(parts[0].str.replace(",", ".", regex=False), errors="coerce")
        return pd.DataFrame({
            "timestamp": pd.to_datetime(long[TIME_COLUMN], errors="coerce"),
            "station": str(ctx["station"]),
            "variable": long["variable"],
            "value": value,
            "unit": parts[1].fillna(""),
        }).dropna(subset=["timestamp"])
