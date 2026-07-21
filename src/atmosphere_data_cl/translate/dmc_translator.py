from .translator import Translator
from dask import dataframe as dd
import pandas as pd
import json


class DMCTranslator(Translator):
    """
    DMC network data specific Translator, from raw to tabulated to netcdf.
    Inherits from Translator class which implements most of the process.

    Methods
    -------
    _to_pd_dataframe
        Turns raw data into a pandas dataframe

    raw_to_intermediate_station
        Turns raw data of the stations to a dask Dataframe.
    """

    def __init__(self, intermediate_path, output_path, **kwargs):
        """Constructor

        Parameters
        ----------
        intermediate_path : pathlib.Path
            Path pointing to a folder in which to save or retrieve the intermediate (tabulated) data.

        output_path : pathlib.Path
            Path pointing to a folder in which to save the processed data in netcdf (.nc) format.
        """
        super().__init__(intermediate_path, output_path, **kwargs)

    def _to_pd_dataframe(self, data):
        """Turns raw data into a pandas dataframe

        Parameters
        ----------
        data : list<dict>
            List of dicts where each dict represents a moment and its meteo data.

        Returns
        -------
        pandas.Dataframe
            Dataframe containing the a row for each moment and its meteo data.
        """
        _data = data["data"]
        if _data:
            _data = _data["datosEstaciones"]["datos"]
            return pd.DataFrame(_data)
        return pd.DataFrame()

    def raw_to_intermediate_station(self, station_data):
        """Turns raw data of the stations to a dask Dataframe.

        Parameters
        ----------
        station_data : dict
            Raw data of the stations

        Returns
        -------
        dask.dataframe.Dataframe
            dask dataframe containing the data of the stations, each row is a station and contains
            latitude, longitude, id, etc.
        """
        station_ddf = dd.from_pandas(pd.DataFrame(station_data["datosEstacion"]))
        return station_ddf
