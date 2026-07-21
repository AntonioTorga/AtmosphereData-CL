"""Parser tests against the real response shapes, offline.

Following vipnet-scrapper's model: the parsing layer never touches the network,
so these run without credentials or connectivity.
"""

import pandas as pd
import pytest

from atmosphere_data_cl.sources.sinca import parse_sinca_response
from atmosphere_data_cl.sources.vipnet import parse_api_records

# CONTAMINANTES: FECHA;HORA;validados;preliminares;no_validados
POLLUTANT_RESPONSE = """FECHA;HORA;Registros validados;Registros preliminares;Registros no validados
220909;0000;12,5;;
220909;0100;;13,7;
220909;0200;;;14,9
220909;0300;;;
"""

# METEOROLOGICAS: FECHA;HORA;value
MET_RESPONSE = """FECHA;HORA;Valor
220909;0000;3,4
220909;0100;5,6
"""


class TestSincaParsing:
    def test_validated_only_by_default(self):
        """Default floor is 'validado', so lower-quality columns are ignored."""
        out = parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES")
        assert out == {"2022-09-09T00:00": "12.5"}

    def test_degrades_to_preliminar(self):
        out = parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES", "preliminar")
        assert out == {"2022-09-09T00:00": "12.5", "2022-09-09T01:00": "13.7"}

    def test_degrades_to_no_validado(self):
        out = parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES", "no_validado")
        assert len(out) == 3
        assert out["2022-09-09T02:00"] == "14.9"

    def test_validated_still_wins_when_degrading(self):
        """Graceful degradation must not prefer worse data when better exists."""
        out = parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES", "no_validado")
        assert out["2022-09-09T00:00"] == "12.5"

    def test_fully_empty_row_is_dropped(self):
        out = parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES", "no_validado")
        assert "2022-09-09T03:00" not in out

    def test_meteorological_has_no_validation_columns(self):
        out = parse_sinca_response(MET_RESPONSE, "METEOROLOGICAS")
        assert out == {"2022-09-09T00:00": "3.4", "2022-09-09T01:00": "5.6"}

    def test_comma_decimals_become_dots(self):
        out = parse_sinca_response(MET_RESPONSE, "METEOROLOGICAS")
        assert all(float(v) for v in out.values())

    def test_psgraph_sentinel_means_no_data(self):
        """SINCA signals 'no such series' with a string, not an HTTP error."""
        assert parse_sinca_response("psgraph: no such file", "CONTAMINANTES") == {}

    def test_header_row_is_skipped(self):
        out = parse_sinca_response(MET_RESPONSE, "METEOROLOGICAS")
        assert "FECHA" not in str(out)

    def test_bad_validation_level_rejected(self):
        with pytest.raises(ValueError, match="Unknown min_validation_level"):
            parse_sinca_response(POLLUTANT_RESPONSE, "CONTAMINANTES", "invented")


VIPNET_RECORDS = [
    {"codigoEstacion": "02113005-2", "nombre": "GUATACONDO DGA", "region": 1,
     "altitud": 2460, "latitud": -20.93, "longitud": -69.05, "value": 0.0},
    {"codigoEstacion": "02113006-1", "nombre": "OTRA", "region": 1,
     "altitud": 100, "latitud": -21.0, "longitud": -69.1, "value": None},
    {"codigoEstacion": "", "nombre": "SIN CODIGO", "value": 5.0},
]


class TestVipnetParsing:
    def test_records_become_long_rows(self):
        out = parse_api_records(VIPNET_RECORDS, "Precipitación", "mm")
        assert len(out) == 2  # blank codigo dropped
        assert set(out["variable"]) == {"precipitacion"}

    def test_null_becomes_nan(self):
        out = parse_api_records(VIPNET_RECORDS, "Precipitación", "mm")
        assert pd.isna(out.loc[out["codigo"] == "02113006-1", "valor"].iloc[0])

    def test_blank_station_code_dropped(self):
        out = parse_api_records(VIPNET_RECORDS, "Precipitación", "mm")
        assert "" not in set(out["codigo"])

    def test_variable_name_is_accent_stripped(self):
        """Filesystem and column safety: "Precipitación" -> "precipitacion"."""
        out = parse_api_records(VIPNET_RECORDS, "Precipitación", "mm")
        assert out["variable"].iloc[0] == "precipitacion"

    def test_unit_is_carried_through(self):
        """The API never returns a unit, so it must come from config."""
        out = parse_api_records(VIPNET_RECORDS, "Temperatura", "°C")
        assert set(out["unidad"]) == {"°C"}

    def test_station_metadata_rides_along(self):
        out = parse_api_records(VIPNET_RECORDS, "Precipitación", "mm")
        row = out[out["codigo"] == "02113005-2"].iloc[0]
        assert (row["latitud"], row["longitud"]) == (-20.93, -69.05)

    def test_empty_input_gives_empty_frame(self):
        assert parse_api_records([], "Precipitación", "mm").empty
