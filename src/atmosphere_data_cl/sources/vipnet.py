"""VipNet — hydro-meteorological stations, via the MOP website's backing JSON API.

Ported from ``REF_ONLY/vipnet-scrapper/``. The endpoint is the one the site's
Angular map calls; a discovery run established that it is public and needs no
auth, no cookies and not even a Referer, so the originally planned Playwright
scraper was deleted before it was built.

VipNet is the mirror image of SINCA: a request carries no station at all and
returns every station in the country, but covers only a single instant — so
stations *collapse* and time *fans out* hourly.
"""

import json
import logging
from pathlib import Path
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

    def __init__(self, *args, mode: str = DEFAULT_MODE, **kwargs):
        super().__init__(*args, **kwargs)
        if mode not in MAP_STATISTIC:
            raise ValueError(f"Unknown mode {mode!r}. Valid: {sorted(MAP_STATISTIC)}")
        self.mode = mode
        self.stations_file = self.raw_dir / "stations.csv"

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

    def _save_raw(self, payload: Any, path: Path) -> None:
        # Keep the self-describing envelope: the request params travel with the
        # response so a raw file is interpretable without its filename.
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _execute(self, job: Job, use_cache: bool = True):
        payload = super()._execute(job, use_cache=use_cache)
        # Wrap on first fetch; cached files are already wrapped.
        if isinstance(payload, dict) and "variable" not in payload:
            variable = job.axes["variable"]
            wrapped = {
                "variable": variable,
                "unit": VARIABLES[variable]["unit"],
                "data": payload.get("data", []),
            }
            self._save_raw(wrapped, self._raw_path(job))
            return wrapped
        return payload

    def _parse(self, payload: dict, job: Job) -> pd.DataFrame:
        variable = payload.get("variable", job.axes["variable"])
        unit = payload.get("unit", VARIABLES.get(variable, {}).get("unit", ""))
        long = parse_api_records(payload.get("data", []), variable, unit)
        if long.empty:
            return long

        self._upsert_stations(long)

        out = pd.DataFrame({
            "timestamp": job.start,
            "station": long["codigo"],
            "variable": long["variable"],
            "value": long["valor"],
            "unit": long["unidad"],
        })
        if job.spec.stations:
            # VipNet cannot filter server-side, so station selection is post-hoc.
            out = out[out["station"].isin({str(s) for s in job.spec.stations})]
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
