from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
import xarray as xr
import numpy as np
import re
import json
from ..utils.utils import to_float, create_data_vars_dict


class Translator:
    """
    Attributes
    ----------
    raw_path : pathlib.Path
        Path pointing to a folder from which to retrieve (if necessary) the raw data downloaded.
    intermediate_path : pathlib.Path
        Path pointing to a folder in which to save (if specified) or retrieve the intermediate
        (tabulated) data.
    output_path : pathlib.Path
        Path pointing to a folder in which to save the resulting netcdf (.nc Melodies-Monet ready)
        file.
    timestep: str
        Data resolution for the netcdf file ('H', 'M', 'Y', etc)
    verbose: bool
        Verbosity of the execution.
    file_info: dict
        Dict storing information for the files which this class interacts with.

    Methods
    -------
    _to_pd_dataframe
        Turns raw data into a pandas dataframe
    raw_to_intermediate_station
        Turns raw data into tabulated data in dask dataframe format.
    raw_to_intermediate_file
        Turns a raw file to an intermediate file in dask dataframe format.
    load_intermediate_data
        Loads the intermediate data into dask dataframes from intermediate_path, also preprocesses it.
    load_raw_data
        Loads the raw data into dicts from the raw_path
    preprocess_intermediate_station_data
        Preprocesses the tabulated stations data changing column names to fit MM format.
    preprocess_intermediate_data
        Preprocesses the tabulated data for a single station, changing column names, changing dtypes, etc.
    postprocess_xarray_data
        Changes data resolution, limits time interval to analysis interval.
    intermediate_to_xarray
        Turns tabulated data in dask dataframe format to xarray Dataset
    xarray_to_netcdf
        Saves an xarray to netcdf (.nc) format to a file named as the output_file
        in the output_path
    from_raw_to_intermediate_format
        Turns the entire raw data into intermediate tabulated data in dask dataframe format.
    from_intermediate_to_netcdf
        Turns the entire intermediate data into xarray and then saves in netcdf format
        (preprocess, turning to xarray and saving to netcdf)
    from_raw_to_netcdf
        Turns the entire raw data into intermediate, then xarray, and then saves to .netcdf file.
        (turn to intermediate, preprocess, turn to xarray and saving to netcdf)
    """

    def __init__(self, intermediate_path, output_path, **kwargs):
        """Constructor

        Parameters
        ----------
        intermediate_path : pathlib.Path
            Path pointing to a folder in which to save (if specified) or retrieve the intermediate
            (tabulated) data.
        output_path : pathlib.Path
            Path pointing to a folder in which to save the resulting netcdf (.nc Melodies-Monet ready) file.
        """
        self.raw_path = kwargs.get("raw_path")
        self.intermediate_path = intermediate_path
        self.output_path = output_path
        self.timestep = kwargs.get("timestep")
        self.verbose = kwargs.get("verbose", False)

        # TODO: move info to a config file
        self.file_info = {
            "input_file": {
                "regex": kwargs.get(
                    "input_file_regex", "(\\d*)-(\\d*)-(\\d*).json"
                )  # Siteid-Year-Month
            },
            "intermediate_file": {
                "regex": kwargs.get(
                    "intermediate_filename_regex", r"(\d+).csv"
                ),  # IDStation
                "format": "{siteid}.csv",  # IDStation-Info
            },
            "output_file": {
                "format": kwargs.get("output_name", "noname_nc_data.nc")  # Network-Type
            },
            "station_file": {
                "raw_regex": kwargs.get("raw_station_filename", "stations.json"),
                "intermediate_format": kwargs.get("station_filename", "stations.csv"),
            },
        }

    @abstractmethod
    def _to_pd_dataframe(self, file):
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
        pass

    @abstractmethod
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
        pass

    def raw_to_intermediate_file(self, files):
        """Turns a raw file to an intermediate file

        Parameters
        ----------
        files : list
            List of all the data (in raw format) to aggregate into a single dask dataframe.

        Returns
        -------
        dask.dataframe.DataFrame
            Dask dataframe containing for each row a moment and the meteo data of said moment.
        """
        ddf = None
        for file in files:
            pd_dataframe = self._to_pd_dataframe(file)
            if not pd_dataframe.empty:
                _ddf = dd.from_pandas(pd_dataframe, npartitions=1)
                ddf = _ddf if ddf is None else dd.concat([ddf, _ddf])
        if ddf is None:
            ddf = dd.from_dict({}, npartitions=1)
        return ddf

    def load_intermediate_data(self, time_name, id_name, lat_name, lon_name):
        """
        Loads the intermediate data into dask dataframes from intermediate_path
        Assumes all the data must be merged into a single dataframe.

        Parameters
        ----------
        time_name : str
            Name of the time attribute in the intermediate data.
        id_name : str
            Name of the id attribute in the intermediate data.
        lat_name : str
            Name of the latitude attribute in the intermediate data.
        lon_name : str
            Name of the longitude attribute in the intermediate data.

        Returns
        -------
        dict
            Dictionary where the keys are station ids and the values are another dict
            with the "site_id" and the "data" in dask dataframe format

        dask.dataframe.DataFrame
            dask dataframe containing the information of the observation sites in the
            network. Each row is a site and contains id, latitude, longitude, etc.
        """
        data = {}

        for f in Path(self.intermediate_path).iterdir():
            match = re.search(self.file_info["intermediate_file"]["regex"], f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                site_id = int(match.group(1))
                if self.verbose:
                    print(f"Loading site {site_id} ...")

                ddf = dd.read_csv(
                    f,
                    sep=",",
                    decimal=".",
                )
                try:
                    ddf = self.preprocess_intermediate_data(ddf, time_name, site_id)
                except ValueError as e:
                    continue
                data[site_id] = {"site_id": site_id, "data": ddf}

        station_ddf = dd.read_csv(
            self.intermediate_path
            / self.file_info["station_file"]["intermediate_format"],
            sep=",",
            decimal=".",
        )
        station_ddf = self.preprocess_intermediate_station_data(
            station_ddf, id_name, lat_name, lon_name
        )

        return data, station_ddf

    def load_raw_data(self):
        """Loads the raw data into dicts from the raw_path

        Returns
        -------
        dict
            Dictionary where the keys are station ids and the value is a list
            of dictionaries with the raw data.
        """
        filename_regex = self.file_info["input_file"]["regex"]
        station_filename_regex = self.file_info["station_file"]["raw_regex"]
        station_file = None
        # get the list of files in the input path that match the regex
        file_list = sorted(
            Path(self.input_path).iterdir(), key=lambda entry: entry.name
        )

        raw_data = (
            {}
        )  # dictionary to store the raw files that compose each intermediate file for a station.
        for f in file_list:
            match = re.search(filename_regex, f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                station_id = match.group(1)
                year = match.group(2)
                month = match.group(3)
                data = None
                with open(f) as fp:
                    data = json.load(fp)

                if raw_data.get(station_id) == None:
                    raw_data[station_id] = []
                raw_data[station_id].append(
                    {
                        "info": {"siteid": station_id, "year": year, "month": month},
                        "data": data,
                    }
                )
            elif match_station := re.search(station_filename_regex, f.name):
                with open(f) as fp:
                    raw_station = json.load(fp)

        return raw_data, raw_station

    def preprocess_intermediate_station_data(
        self, station_ddf, id_name, lat_name, lon_name
    ):
        """Preprocesses the tabulated stations data changing column names to fit MM format.

        Parameters
        ----------
        station_ddf : dask.dataframe.DataFrame
            dask dataframe containing data for each station. Like latitude, longitude, etc.
        id_name : str
            Name of the id attribute in the intermediate data.
        lat_name : str
            Name of the latitude attribute in the intermediate data.
        lon_name : str
            Name of the longitude attribute in the intermediate data.

        Returns
        -------
        dask.dataframe.DataFrame
            dask dataframe containing data for each station. Like latitude, longitude, etc.
            Preprocessed to make it into MM format.

        Raises
        ------
        ValueError
            Raises if the id_name, lat_name or lon_name doesn't exist in the file.
        """
        for column in [id_name, lat_name, lon_name]:
            if column not in station_ddf.columns:
                raise ValueError(f"No {column} column in Station file.")
        station_ddf = station_ddf.rename(
            columns={id_name: "siteid", lat_name: "latitude", lon_name: "longitude"}
        )
        station_ddf = station_ddf.set_index("siteid", sort=True, shuffle_method="tasks")
        return station_ddf

    def preprocess_intermediate_data(self, ddf, time_name, site_id):
        """Preprocesses the tabulated data for a single station, changing column names, changing dtypes, etc.

        Parameters
        ----------
        ddf : dask.dataframe.DataFrame
            Tabulated data of a station's meteo data. Each row is a moment and can contain Rain, Wind, etc.
        time_name : str
            Name of the time attribute in the intermediate data.
        site_id : str
            Id of the site being processed. Important for raising an error.

        Returns
        -------
        dask.dataframe.DataFrame
            Tabulated data of a station's meteo data. Each row is a moment and can contain Rain, Wind, etc.
            Preprocessed into MM format.

        Raises
        ------
        ValueError
            Raised if time_name is not in the columns of the sites dataframe.
        """
        if not time_name in ddf.columns:
            raise ValueError(f"No column {time_name} in file for {site_id} site.")
        rename = {time_name: "time"}
        cols = ddf.columns.to_list()
        cols.remove(time_name)
        if cols:
            for col in cols:
                pos_replacement = col.replace("/", "|")
                if col != pos_replacement:
                    rename[col] = pos_replacement
                ddf[col] = ddf[col].apply(to_float, meta=float)
        ddf = ddf.rename(columns=rename)
        ddf["time"] = dd.to_datetime(ddf["time"])
        ddf = ddf.dropna(subset=["time"])
        ddf = ddf.drop_duplicates(subset=["time"])
        ddf = ddf.set_index("time")
        return ddf

    def postprocess_xarray_data(self, xrds, timestep="N", start=None, end=None):
        """Changes data resolution, limits time interval to analysis interval.

        Parameters
        ----------
        xrds : xarray.Dataset
            Xarray dataset with all the meteo data for all the stations.
        timestep : str, optional
            time resolution for the resulting netcdf file, by default "N"
        start : datetime.datetime, optional
            Start of the analysis window. Used to filter the data, by default None
            If not provided no filtering is performed.
        end : datetime.datetime, optional
            End of the analysis window. Used to filter the data, by default None
            If not provided no filtering is performed.

        Returns
        -------
        xarray.Dataset
            Postprocessed dataset. Very similar to the one received, changed resolution and time interval maybe.
        """
        dict_intervals = {
            "H": "1h",
            "h": "1h",
            "D": "1D",
            "M": "1M",
            "Y": "1Y",
            "N": None,
        }
        timestep = dict_intervals[timestep]
        if timestep != None:
            xrds = xrds.resample(time=timestep).mean()

        xrds = xrds.set_coords(["siteid", "latitude", "longitude"])

        if start != None and end != None:
            xrds = xrds.sel(time=slice(start, end))

        return xrds

    def intermediate_to_xarray(
        self, data, station_ddf, location_attr_names=[], timestamps=None
    ):
        """Converts the intermediate data to xarray format.

        Parameters
        ----------
        data : dict
            dict where each key is a siteid and the value is a dask dataframe with the meteo data.
        station_ddf : dask.dataframe.DataFrame
            Dask dataframe with the info of every observation site.
        location_attr_names : list<str>, optional
            List of strings where every string is an attribute in the station_ddf
            that could be used to select sites. Like region, comune, etc.
            By default []

        Returns
        -------
        xarray.Dataset
            Xarray dataset with all the meteo data for all the stations.
        """
        data_dd = []
        site_id_dim = []
        lat_dim = []
        lon_dim = []
        location_attr_dims = {attribute: [] for attribute in location_attr_names}
        timestamps_set = set()
        columns = set()

        for site_id, data_dict in data.items():
            _ddf = data_dict["data"]
            try:
                id_row = station_ddf.loc[site_id].compute()
            except KeyError:
                if self.verbose:
                    print(f"No row matching the id {site_id} in station file info.")
                continue
            lat, lon = list(id_row[["latitude", "longitude"]].iloc[0])

            data_dd.append(_ddf)
            site_id_dim.append(site_id)
            lat_dim.append(float(lat))
            lon_dim.append(float(lon))

            for attribute in location_attr_dims.keys():
                x = id_row[[attribute]].iloc[0][attribute]
                location_attr_dims[attribute].append(x)

            columns.update(_ddf.columns.to_list())
            if timestamps == None:
                timestamps_set.update(_ddf.index.compute().to_list())

            if self.verbose:
                print(f"Site {site_id} processed.")

        timestamps_list = (
            sorted(list(timestamps_set)) if timestamps == None else sorted(timestamps)
        )

        if self.verbose:
            print(f"Computing dask dataframes into pandas dataframes.")

        data_df = [
            ddf.compute().reindex(timestamps_list, fill_value=np.nan) for ddf in data_dd
        ]

        if self.verbose:
            print("Creating xarray dataset...")
        data_vars = create_data_vars_dict(data_df, timestamps_list, columns)
        coords = {
            "time": ("time", timestamps_list),
            "latitude": ("x", lat_dim),
            "longitude": ("x", lon_dim),
            "siteid": ("x", list(map(str, site_id_dim))),
            "x": ("x", np.arange(len(site_id_dim))),
        }
        if len(location_attr_names) > 0:
            coords.update(
                {
                    attribute: ("x", attribute_dim)
                    for attribute, attribute_dim in location_attr_dims.items()
                }
            )

        xrds = xr.Dataset(
            data_vars,
            coords=coords,
        )

        if self.timestep:
            if self.verbose:
                print("Postprocessing... changing to correct timestep.")
            xrds = self.postprocess_xarray_data(xrds, self.timestep)

        if self.verbose:
            print("Quick result inspection: ")
            print(xrds)

        return xrds

    def xarray_to_netcdf(self, xarray):
        """Saves an xarray to netcdf (.nc) format to a file named as the output_file
            in the output_path

        Parameters
        ----------
        xarray : xarray.Dataset
            Xarray dataset ready to be saved
        """
        file_path = self.output_path / (self.file_info["output_file"]["format"])
        if self.verbose:
            print(f"Writing to resulting netcdf dataset to {file_path}")

        xarray.to_netcdf(file_path, format="NETCDF4", unlimited_dims="time")

    def from_raw_to_intermediate_format(
        self, raw_data=None, raw_station=None, save=False, merge=False, time_name=None
    ):
        """Turns the entire raw data into intermediate tabulated data in dask dataframe format
        If raw_data or raw_station is not provided it is loaded from raw_path using the file_info

        Parameters
        ----------
        raw_data : dict<dict>, optional
            dict where the key is a station id and the value is a dict with the meteo data
            of the station, by default None
        raw_station : dict, optional
            Dict with the data of the stations, by default None
        save : bool, optional
            Wether to save the intermediate data into the intermediate_path, by default False
        merge : bool, optional
            Wether to merge the existing files in intermediate_path with the new data
            , by default False
        time_name : str, optional
            Name of the time attribute in the intermediate data, by default None

        Returns
        -------
        dict
            a dictionary where every key is a station id and the value is a dict
            with the station id and the "data" (meteo data)
        dask.dataframe.DataFrame
            dask dataframe with the stations data.

        Raises
        ------
        FileNotFoundError
            Raised if there is no data provided and no raw_path.
        """
        if raw_data == None or raw_station == None:
            if self.raw_path == None:
                raise FileNotFoundError("No raw path provided to load raw data...")
            raw_data, raw_station = self.load_raw_data()

        ddfs = {}  # dictionary that will hold the dask dataframes for each station.
        for station_id, station_data in raw_data.items():

            data = self.raw_to_intermediate_file(station_data)
            # if there is no data available for a station, we skip said station
            if len(data.index) == 0:
                print(f"No data for station {station_id}")
                continue

            intermediate_filepath = self.intermediate_path / self.file_info[
                "intermediate_file"
            ]["format"].format(**{"siteid": station_id})
            if (
                merge
                and intermediate_filepath.exists()
                and intermediate_filepath.is_file()
            ):
                loaded = dd.read_csv(
                    intermediate_filepath,
                    sep=",",
                    decimal=".",
                    dtype={column: "object" for column in data.columns},
                )
                data = dd.concat([data, loaded])

            if save:
                data.to_csv(intermediate_filepath, single_file=True, index=False)

            ddfs[station_id] = {
                "station_id": station_id,
                "data": data,
            }

        station_ddf = self.raw_to_intermediate_station(raw_station)

        if save:
            station_ddf.to_csv(
                self.intermediate_path
                / self.file_info["station_file"]["intermediate_format"],
                single_file=True,
            )

        return ddfs, station_ddf

    def from_intermediate_to_netcdf(
        self, time_name, id_name, lat_name, lon_name, data=None, station_ddf=None
    ):
        """Turns the entire intermediate data into xarray and then saves in netcdf format
        (preprocess, turning to xarray and saving to netcdf)

        Parameters
        ----------
        time_name : str
            Name of the time attribute in the intermediate data.
        id_name : str
            Name of the id attribute in the intermediate data.
        lat_name : str
            Name of the latitude attribute in the intermediate data.
        lon_name : str
            Name of the longitude attribute in the intermediate data.
        data : dict, optional
            dict where each key is a siteid and the value is a dask dataframe with the meteo data.
            if not provided loaded from intermediate_path
        station_ddf : dask.dataframe.DataFrame, optional
            Dask dataframe with the info of every observation site.
            if not provided loaded from intermediate_path
        """
        if data == None or station_ddf == None:
            data, station_ddf = self.load_intermediate_data(
                time_name, id_name, lat_name, lon_name
            )
        xarray = self.intermediate_to_xarray(
            data, station_ddf, location_attr_names=location_attr_names
        )
        self.xarray_to_netcdf(xarray)

    def from_raw_to_netcdf(
        self,
        raw_data=None,
        raw_station=None,
        time_name="momento",
        lat_name="latitud",
        lon_name="longitud",
        id_name="codigoNacional",
        location_attr_names=[],
        start=None,
        end=None,
        timestep="N",
        save=False,
        merge=False,
    ):
        """Turns the entire raw data into intermediate, then xarray, and then saves to .netcdf file.
        (turn to intermediate, preprocess, turn to xarray and saving to netcdf)
        Want to deprecate this one so every aggregation of functions occurr outside of this class

        Parameters
        ----------
        raw_data : dict<dict>, optional
            dict where the key is a station id and the value is a dict with the meteo data
            of the station, by default None
        raw_station : dict, optional
            Dict with the data of the stations, by default None
        time_name : str
            Name of the time attribute in the intermediate data, by default "momento"
        id_name : str
            Name of the id attribute in the intermediate data, by default "codigoNacional"
        lat_name : str
            Name of the latitude attribute in the intermediate data, by default "latitud"
        lon_name : str
            Name of the longitude attribute in the intermediate data, by default "longitud"
        location_attr_names : list<str>, optional
            List of strings where every string is an attribute in the station_ddf
            that could be used to select sites. Like region, comune, etc.
            By default []
        timestep : str, optional
            time resolution for the resulting netcdf file, by default "N"
        start : datetime.datetime, optional
            Start of the analysis window. Used to filter the data, by default None
            If not provided no filtering is performed.
        end : datetime.datetime, optional
            End of the analysis window. Used to filter the data, by default None
            If not provided no filtering is performed.
        save : bool, optional
            Wether to save the intermediate data into the intermediate_path, by default False
        merge : bool, optional
            Wether to merge the existing files in intermediate_path with the new data
            , by default False
        """
        # load the data from the input path
        ddfs, station_ddf = self.from_raw_to_intermediate_format(
            raw_data, raw_station, save=save, merge=merge, time_name=time_name
        )
        # convert the data to xarray format

        for site_id, ddf in ddfs.items():
            ddfs[site_id]["data"] = self.preprocess_intermediate_data(
                ddf["data"], time_name, site_id=site_id
            )

        station_ddf = self.preprocess_intermediate_station_data(
            station_ddf, id_name, lat_name, lon_name
        )
        xarray = self.intermediate_to_xarray(
            ddfs, station_ddf, location_attr_names=location_attr_names
        )
        xarray = self.postprocess_xarray_data(
            xarray, timestep=timestep, start=start, end=end
        )
        # save the data to netcdf format
        self.xarray_to_netcdf(xarray)
