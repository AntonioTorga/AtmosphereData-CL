"""VipNet — hydro-meteorological stations, via the MOP website's backing JSON API.

Ported from ``REF_ONLY/vipnet-scrapper/``. The endpoint is the one the site's
Angular map calls; a discovery run established that it is public and needs no
auth, no cookies and not even a Referer, so the originally planned Playwright
scraper was deleted before it was built.

VipNet is the mirror image of SINCA: a request carries no station at all and
returns every station in the country, but covers only a single instant — so
stations *collapse* and time *fans out* hourly.
"""

import logging
from typing import Any

import pandas as pd

from ..utils.paths import safe_name
from .source import FetchSpec, Job, Request, Source

log = logging.getLogger(__name__)

API_URL = "https://vipnet.mop.gob.cl/v1/vipnet/estaciones/valor"

# Every variable in the site's dropdown, mapped to its `tipoEstacion` id and
# its unit. The API never returns a unit, so it is pinned here (confirmed by
# triggering one real .xlsx export per variable and reading the "Valor" column).
VARIABLES = {
    "Precipitación": {"tipo_estacion": 0, "unit": "mm"},
    "Temperatura": {"tipo_estacion": 1, "unit": "°C"},
    "Embalse": {"tipo_estacion": 2, "unit": "Mm3"},
    "Nieve": {"tipo_estacion": 3, "unit": "cm"},
    "Humedad": {"tipo_estacion": 4, "unit": "%"},
    "Viento": {"tipo_estacion": 5, "unit": "km/h"},
}

# Aggregation mode. Promoted from a module constant to a product key so both
# are reachable without editing source.
MAP_STATISTIC = {"Más Actual": 4, "Acumulado": 0}
DEFAULT_MODE = "Acumulado"
ACCUM_RANGE_HOURS = 1

STATION_META_COLS = ["codigo", "region", "estacion", "altitud", "latitud", "longitud"]

# The path carries the safe (accent-stripped) variable name; recover the real
# one for unit lookup. safe_name is lossy, so we invert it with a known table.
_SAFE_TO_VAR = {safe_name(v): v for v in VARIABLES}


def parse_api_records(records: list[dict], variable: str, unit: str = "") -> pd.DataFrame:
    """Turn the API's ``data`` list into a long DataFrame.

    A missing ``value`` becomes NaN — the API uses ``null``, which is cleaner
    than SINCA's sentinel-string approach and needs no special-casing.
    """
    var = safe_name(variable)
    recs = []
    for r in records:
        codigo = r.get("codigoEstacion")
        if not codigo or str(codigo).strip() == "":
            continue
        value = r.get("value")
        recs.append({
            "codigo": str(codigo).strip(),
            "variable": var,
            "valor": float(value) if value is not None else float("nan"),
            "unidad": unit,
            "region": r.get("region"),
            "estacion": r.get("nombre"),
            "altitud": r.get("altitud"),
            "latitud": r.get("latitud"),
            "longitud": r.get("longitud"),
        })
    return pd.DataFrame.from_records(recs)


class Vipnet(Source):
    """VipNet hydro-meteorological network (MOP).

    No authentication. Station discovery is *derived*: metadata rides along in
    every data response, so there is no discovery call and the station table is
    an evolving union rather than a fixed list.
    """

    kind = "PointSurface"
    name = "vipnet"
    discovery = "derived"
    #: One request covers one instant, so a range fans out into hourly jobs.
    time_grain = "hour"
    native_format = "json"
    #: raw/vipnet/<variable>/<YYYYMMDDTHHMM>.json — variable + instant in the path.
    raw_template = "{variable}/{time}.{ext}"
    station_meta_map = {
        "station": "codigo", "name": "estacion", "region": "region",
        "latitude": "latitud", "longitude": "longitud", "altitude": "altitud",
    }

    def __init__(self, *args, mode: str = DEFAULT_MODE, **kwargs):
        super().__init__(*args, **kwargs)
        if mode not in MAP_STATISTIC:
            raise ValueError(f"Unknown mode {mode!r}. Valid: {sorted(MAP_STATISTIC)}")
        self.mode = mode
        self.stations_file = self.raw_dir / "stations.csv"

    def _identity(self, job: Job) -> dict[str, str]:
        return {
            "variable": safe_name(job.axes["variable"]),
            "time": job.start.strftime("%Y%m%dT%H%M"),
        }

    def _plan_axes(self, spec: FetchSpec) -> dict[str, list[Any]]:
        """Fan out over variables; stations collapse (every request is network-wide)."""
        variables = spec.variables or ([spec.product] if spec.product in VARIABLES else list(VARIABLES))
        unknown = set(variables) - set(VARIABLES)
        if unknown:
            raise ValueError(f"Unknown VipNet variables {sorted(unknown)}. Valid: {sorted(VARIABLES)}")
        return {"variable": variables}

    def _build_request(self, job: Job) -> Request:
        variable = job.axes["variable"]
        dt = job.start
        return Request(
            url=API_URL,
            method="POST",
            json_body={
                "tipoEstacion": VARIABLES[variable]["tipo_estacion"],
                "mapStatistic": MAP_STATISTIC[job.spec.extras.get("mode", self.mode)],
                "currentTabIndex": 0,
                "fetchHour": int(dt.hour),
                "fetchDay": dt.strftime("%Y-%m-%d"),
                "hoursRange": job.spec.extras.get("hours_range", ACCUM_RANGE_HOURS),
            },
            timeout=30.0,
        )

    def _accumulate_discovery(self, items: list[tuple[Job, dict]]) -> None:
        """Derived discovery: union every payload's station metadata, once.

        The raw JSON is now stored as-downloaded (no envelope), so the variable
        comes from the job, not the payload. Called a single time per fetch.
        """
        frames = []
        for job, payload in items:
            variable = job.axes["variable"]
            unit = VARIABLES.get(variable, {}).get("unit", "")
            long = parse_api_records((payload or {}).get("data", []), variable, unit)
            if not long.empty:
                frames.append(long[STATION_META_COLS])
        if frames:
            self._upsert_stations(pd.concat(frames, ignore_index=True))

    def _parse(self, payload: dict, ctx: dict[str, str]) -> pd.DataFrame:
        variable = _SAFE_TO_VAR.get(ctx["variable"], ctx["variable"])
        unit = VARIABLES.get(variable, {}).get("unit", "")
        long = parse_api_records((payload or {}).get("data", []), variable, unit)
        if long.empty:
            return long

        out = pd.DataFrame({
            "timestamp": pd.to_datetime(ctx["time"], format="%Y%m%dT%H%M"),
            "station": long["codigo"],
            "variable": long["variable"],
            "value": long["valor"],
            "unit": long["unidad"],
        })
        wanted = ctx.get("stations")
        if wanted:
            # VipNet cannot filter server-side, so station selection is post-hoc.
            out = out[out["station"].isin({str(s) for s in wanted})]
        return out

    def _upsert_stations(self, long: pd.DataFrame) -> None:
        """Union new station metadata into stations.csv, newest wins.

        The station set grows and shrinks over time, so this is an accumulating
        union rather than a snapshot.
        """
        incoming = long[STATION_META_COLS].drop_duplicates(subset=["codigo"], keep="last")
        if self.stations_file.exists():
            existing = pd.read_csv(self.stations_file, dtype={"codigo": str})
            incoming = pd.concat([existing, incoming], ignore_index=True)
        incoming = incoming.drop_duplicates(subset=["codigo"], keep="last")
        self.stations_file.parent.mkdir(parents=True, exist_ok=True)
        incoming.to_csv(self.stations_file, index=False)

    def discover_stations(self) -> pd.DataFrame:
        """Return the accumulated station union. Populated as a side effect of fetching."""
        if self.stations_file.exists():
            return pd.read_csv(self.stations_file, dtype={"codigo": str})
        return pd.DataFrame(columns=STATION_META_COLS)
