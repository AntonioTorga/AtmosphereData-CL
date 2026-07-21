"""SINCA — Chile's air-quality network (Sistema de Información Nacional de Calidad del Aire).

Ported from ``REF_ONLY/SINCA-API/``. The acquisition logic is kept as-is: the
macro templates, the two-way variable map, the family dispatch, the validation
level degradation and the ``psgraph:`` sentinel are all carried over verbatim.
What changed is structural — the original ``get_resource`` interleaved network,
parsing and CSV writing in one method; here those are the driver's, ``_parse``'s
and the layout's jobs respectively.

Two things make SINCA the awkward one:

* The ``macro`` query parameter is a *server-side filesystem path* encoding
  station, variable and resolution, and its template differs between pollutant
  and meteorological variables.
* Station discovery is a hard prerequisite. It yields ``airviro_id``, the
  routing key, without which no data request can be built at all.
"""

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from .source import FetchSpec, Job, Request, Source

log = logging.getLogger(__name__)

BASE_URL = "https://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi"
PATH = "/usr/airviro/data/CONAMA/"

# The macro encodes station+variable+resolution as a path on SINCA's backend.
# Pollutants take two resolutions and no height; meteorological take one
# resolution and a height, and name the variable once instead of twice.
MACRO = {
    "CONTAMINANTES": "./R{region}/{airviro_id}/Cal/{variable}//{variable}.{time_resolution_register}.{time_resolution_mean}.ic",
    "METEOROLOGICAS": "./R{region}/{airviro_id}/Met/{variable}//{time_resolution}_{height}.ic",
}

VALIDATION_LEVELS = ["validado", "preliminar", "no_validado"]

VARIABLES = {
    "CONTAMINANTES": {
        "PM25": "PM25",   # [ug/m3]
        "PM10": "PM10",   # [ug/m3]
        "0001": "SO2",    # [ppb]
        "0002": "NO",     # [ppb]
        "0003": "NO2",    # [ppb]
        "0004": "CO",     # [ppm]
        "0008": "O3",     # [ppb]
        "00Cu": "Cu",     # [ug/m3]
        "00Pb": "Pb",     # [ug/m3]
        "0CH4": "CH4",    # [ppmC]
        "0HCM": "HCM",    # [ppm]
        "0NOX": "NOx",    # [ppb]
        "ARSE": "As",     # [ug/m3]
        "PMHV": "PM10HV",  # [ug/m3]
    },
    "METEOROLOGICAS": {
        "GLOB": "RAD",    # [W/m2]
        "PRES": "PRES",   # [hPa]
        "RAIN": "RAIN",   # [mm/h]
        "RHUM": "RHUM",   # [%]
        "WDIR": "WDIR",   # [Deg.M]
        "WSPD": "WSPD",   # [m/s]
        "TEMP": "TEMP",   # [deg.C]
    },
}
VARIABLES_INV = {
    category: {v: k for k, v in mapping.items()}
    for category, mapping in VARIABLES.items()
}

UNITS = {
    "PM25": "ug/m3", "PM10": "ug/m3", "SO2": "ppb", "NO": "ppb", "NO2": "ppb",
    "CO": "ppm", "O3": "ppb", "Cu": "ug/m3", "Pb": "ug/m3", "CH4": "ppmC",
    "HCM": "ppm", "NOx": "ppb", "As": "ug/m3", "PM10HV": "ug/m3",
    "RAD": "W/m2", "PRES": "hPa", "RAIN": "mm/h", "RHUM": "%",
    "WDIR": "Deg.M", "WSPD": "m/s", "TEMP": "deg.C",
}

TIME_RESOLUTION = ["diario", "horario", "trimestral", "anual"]
POSSIBLE_HEIGHTS = ["002", "003", "010"]

REGION_URL = "https://sinca.mma.gob.cl/index.php/region/index/id/{region}"
STATION_URL = "https://sinca.mma.gob.cl/index.php/estacion/index/id/{id}"

_AIRVIRO_RE = re.compile(r"macropath=\.?/?R[A-Z]+/([A-Z0-9]+)")
_LATLNG_RE = re.compile(
    r"google\.maps\.LatLng\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\)"
)

REGIONES = ["XV", "I", "II", "III", "IV", "V", "M", "VI",
            "VII", "XVI", "VIII", "IX", "XIV", "X", "XI", "XII"]

STATION_META_COLS = ["station_id", "name", "region", "airviro_id", "comuna", "latitude", "longitude"]


def to_yymmdd(ts: str | datetime | date) -> str:
    """SINCA wants dates as yymmdd."""
    if isinstance(ts, str):
        ts = pd.Timestamp(ts).to_pydatetime()
    return ts.strftime("%y%m%d")


def parse_sinca_response(
    text: str, variable_type: str, min_validation_level: str = "validado"
) -> dict[str, str]:
    """Parse one xcl payload into ``{timestamp_iso: value}``.

    SINCA returns semicolon-separated rows. CONTAMINANTES has columns
    ``[FECHA, HORA, validados, preliminares, no_validados]``; METEOROLOGICAS has
    ``[FECHA, HORA, value]``. FECHA+HORA merge into an ISO timestamp and the
    first non-empty accepted value column wins — so validated data is preferred
    and quality degrades gracefully down to ``min_validation_level``.

    Returns ``{}`` for SINCA's 'no such file' sentinel or a payload with no rows.
    """
    if text.startswith("psgraph:"):
        return {}

    if variable_type == "CONTAMINANTES":
        if min_validation_level not in VALIDATION_LEVELS:
            raise ValueError(
                f"Unknown min_validation_level {min_validation_level}. Valid: {VALIDATION_LEVELS}"
            )
        min_index = VALIDATION_LEVELS.index(min_validation_level)
        # `min_validation_level` is the *lowest* quality accepted, and validated
        # data always wins when present. The original SINCA-API sliced
        # `[2, 3, 4][min_index:]`, which inverted this: "validado" silently
        # admitted unvalidated readings and "preliminar" discarded validated
        # ones outright. Fixed deliberately — see the docstring above.
        candidate_indexes = [2, 3, 4][: min_index + 1]
    else:
        candidate_indexes = [2]

    out: dict[str, str] = {}
    for line in text.splitlines()[1:]:  # skip header row
        if not line.strip():
            continue
        parts = line.split(";")
        if len(parts) < 3:
            continue
        fecha, hora = parts[0].strip(), parts[1].strip()
        if not fecha or not hora:
            continue

        value = ""
        for index in candidate_indexes:
            if index < len(parts) and parts[index].strip():
                value = parts[index].strip()
                break
        if not value:
            continue

        value = value.replace(",", ".")  # Chilean decimal comma
        try:
            ts = datetime.strptime(f"{fecha}{hora.zfill(4)}", "%y%m%d%H%M").isoformat(
                timespec="minutes"
            )
        except ValueError:
            continue
        out[ts] = value
    return out


class Sinca(Source):
    """SINCA air-quality network, via its public CGI endpoint.

    No authentication. Station discovery is *required*: it scrapes the regional
    HTML listings for ``airviro_id``, which routes every data request.
    """

    kind = "PointSurface"
    name = "sinca"
    discovery = "required"
    #: SINCA accepts an arbitrary from/to range, so time never fans out.
    time_grain = None
    native_format = "csv"

    def __init__(self, *args, stations_file: str | Path | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.stations_file = Path(stations_file) if stations_file else self.raw_dir / "stations.csv"
        self._stations: pd.DataFrame | None = None

    # ------------------------------------------------------------ variables

    @classmethod
    def get_type(cls, variable: str) -> str:
        """Which family a friendly variable name belongs to."""
        for category, mapping in VARIABLES_INV.items():
            if variable in mapping:
                return category
        raise ValueError(
            f"Unknown variable {variable!r}. Known: {sorted(UNITS)}"
        )

    # ------------------------------------------------------------ discovery

    def discover_stations(self, regions: list[str] | None = None, refresh: bool = False) -> pd.DataFrame:
        """Scrape station metadata, caching to ``stations.csv``.

        This is a hard prerequisite, not an enrichment: ``airviro_id`` is the
        routing key and a station without one cannot be fetched at all.
        """
        if self._stations is not None and not refresh:
            return self._stations
        if self.stations_file.exists() and not refresh:
            self._stations = pd.read_csv(self.stations_file, dtype={"station_id": str})
            return self._stations

        from bs4 import BeautifulSoup

        rows: list[dict[str, Any]] = []
        for region in regions or REGIONES:
            html = self.client.get(REGION_URL.format(region=region), timeout=30).text
            soup = BeautifulSoup(html, "lxml")
            table = soup.select_one("#tablaRegional")
            if table is None:
                log.warning("sinca: no station table for region %s", region)
                continue
            for link in table.select("a[href]"):
                match = _AIRVIRO_RE.search(link["href"])
                if not match:
                    continue
                rows.append({
                    "station_id": link["href"].rstrip("/").split("/")[-1],
                    "name": link.get_text(strip=True),
                    "region": region,
                    "airviro_id": match.group(1),
                })

        stations = pd.DataFrame(rows, columns=STATION_META_COLS)
        self.stations_file.parent.mkdir(parents=True, exist_ok=True)
        stations.to_csv(self.stations_file, index=False)
        self._stations = stations
        log.info("sinca: discovered %d stations", len(stations))
        return stations

    def _targets(self, spec: FetchSpec) -> list[dict[str, Any]]:
        stations = self.discover_stations()
        if stations.empty:
            return []
        usable = stations[stations["airviro_id"].notna()]
        dropped = len(stations) - len(usable)
        if dropped:
            log.warning("sinca: %d stations have no airviro_id and cannot be fetched", dropped)
        if spec.stations:
            wanted = {str(s) for s in spec.stations}
            usable = usable[usable["station_id"].astype(str).isin(wanted)]
        return usable.to_dict("records")

    # ----------------------------------------------------------------- plan

    def _plan_axes(self, spec: FetchSpec) -> dict[str, list[Any]]:
        """Fan out over station x variable x height; time collapses.

        Heights only multiply meteorological variables — pollutants get a single
        job with a placeholder the macro template ignores.
        """
        heights = spec.extras.get("heights", ["002"])
        if isinstance(heights, str):
            heights = [heights]

        variables = spec.variables or [spec.product]
        jobs = []
        for variable in variables:
            if self.get_type(variable) == "METEOROLOGICAS":
                jobs.extend({"variable": variable, "height": h} for h in heights)
            else:
                jobs.append({"variable": variable, "height": None})

        return {
            "station": self._targets(spec),
            "job": jobs,
        }

    def _build_request(self, job: Job) -> Request:
        station = job.axes["station"]
        variable = job.axes["job"]["variable"]
        height = job.axes["job"]["height"]

        variable_type = self.get_type(variable)
        server_code = VARIABLES_INV[variable_type][variable]
        extras = job.spec.extras

        if variable_type == "CONTAMINANTES":
            macro = MACRO[variable_type].format(
                region=station["region"],
                airviro_id=station["airviro_id"],
                variable=server_code,
                time_resolution_register=extras.get("register_resolution", "horario"),
                time_resolution_mean=extras.get("mean_resolution", "horario"),
            )
        else:
            macro = MACRO[variable_type].format(
                region=station["region"],
                airviro_id=station["airviro_id"],
                variable=server_code,
                time_resolution=extras.get("register_resolution", "horario"),
                height=height,
            )

        return Request(
            url=BASE_URL,
            method="GET",
            params={
                "outtype": "xcl",
                "macro": macro,
                "from": to_yymmdd(job.start),
                "to": to_yymmdd(job.end),
                "path": PATH,
                "lang": "esp",
                "rsrc": "",
                "macropath": "",
            },
        )

    def _decode(self, response: httpx.Response) -> str:
        return response.text

    def _save_raw(self, payload: str, path: Path) -> None:
        path.write_text(payload, encoding="utf-8")

    def _load_raw(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def _parse(self, payload: str, job: Job) -> pd.DataFrame:
        station = job.axes["station"]
        variable = job.axes["job"]["variable"]
        height = job.axes["job"]["height"]
        variable_type = self.get_type(variable)

        series = parse_sinca_response(
            payload,
            variable_type,
            job.spec.extras.get("min_validation_level", "validado"),
        )
        if not series:
            return pd.DataFrame()

        # Height is part of the identity for met variables — WSPD at 2 m and
        # 10 m are different series and must not collide.
        label = f"{variable}_{height}" if height and variable_type == "METEOROLOGICAS" else variable

        return pd.DataFrame({
            "timestamp": pd.to_datetime(list(series)),
            "station": str(station["station_id"]),
            "variable": label,
            "value": pd.to_numeric(list(series.values()), errors="coerce"),
            "unit": UNITS.get(variable, ""),
        })
