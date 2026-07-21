import httpx
from abc import abstractmethod
from ..utils.utils import url_creator
import json


class Downloader:
    """Downloader abstract class.

    Abstract class that implements the general logic of downloading the data from an
    observation network.

    Attributes
    ----------

    raw_path : pathlib.Path
        Path pointing to a folder in which to save (if specified) the raw data downloaded.

    station_data : dict
        Dict containing the parsed data of the stations obtained from stations_url

    data : list<dict>
        List of dicts with "info" and "data" items. Where the data is
        identified by the "info". Info serves as metadata for the data.

    station_file: pathlib.Path
        Raw station file path in which to save the station_data.

    data_file_format: str
        Formattable string for saving the data of the stations, meant to
        be formatted with the "info".

    stations_url: str
        String that points to the resource of the data of all stations.

    data_url: str
        Formattable string meant to be formatted with the "info", points
        to the data of certain station with the "info" required.

    client: HTTPX.Client
        Http client used to get the stations data.

    other_data: dict
        dict containing extra information that is added to "info".
        Used to format things, can be things like "username", "subNetwork", etc.

    verbose: bool
        Verbosity of the execution.

    Methods
    -------
    _get_station_ids
        Abstract method. Gets the station ids as a list.

    _get_stations_data
        Abstract method. Gets the station data from the stations_url

    _get_data_for_station
        Abstract method. Gets all the data from URLs provided,
        used for one station at a time

    _get_data
        Manage getting the data of a network for some timestamps.

    _save
        Saves the downloaded data to file.

    download
        Intended as the only exposed endpoint of the Class.
        Main routine managing all the download subprocesses.
    """

    def __init__(self, data_url, stations_url, raw_path, **kwargs):
        """Constructor

        Parameters
        ----------
        data_url : str
            Formattable string meant to be formatted with the "info", points
            to the data of certain station with the "info" required.
        stations_url : str
            String that points to the resource of the data of all stations.
        raw_path : pathlib.Path
            Path pointing to a folder in which to save (if specified) the raw data downloaded.
        """
        self.raw_path = raw_path

        self.station_data = None
        self.data = None

        self.station_file = raw_path / "stations.json" if raw_path != None else None
        self.data_file_format = "{siteid}-{year}-{month}.json"

        self.stations_url = stations_url
        # In format: https(...)/{year}/{month}/{day}/{hour}/{other_info}
        # order not important, and the other info must be provided in a json string
        self.data_url = data_url

        self.client = httpx.Client()
        self.other_data = kwargs.get("other_data", {})
        self.verbose = kwargs.get("verbose", False)

    # just get the stations data in {siteid: station_data} where station_data is a dict from json
    @abstractmethod
    def _get_stations_data(self):
        """Gets the station ids as a list.

        Returns all the station ids if self.station_data is not None. Meant to be run after
        setting self.station_data

        Returns
        -------
        list
            List containing all the station ids.
        """
        pass

    # for a list in [{url, info}] format transform into [{info, data}] | data in dictionary from json
    @abstractmethod
    def _get_data_for_station(self, urls):
        """Gets all the stations data.

        Makes async requests for all the urls contained in station_urls, returns list with
        data paired to information of said data.

        Parameters
        ----------
        station_urls : list<dict>
            List of dictionaries where every dict has a "url" and "info" item. "url"s value is
            a url formatted with the "info", used to identify later the info that
            was received from said url.

        Returns
        -------
        list<dict>
            List of dictionaries where every dict has a "data" and "info" item. "data"s value is
            the data obtained from a "URL" formatted with the corresponding "info".
        """
        pass

    @abstractmethod
    def _get_station_ids(self):
        pass

    # Returns data in {siteid:[{info,data}]} format and stations in {siteid:station_data} format
    def _get_data(self, timestamps):
        """Manage getting the data of a network for some timestamps

        Parameters
        ----------
        timestamps : list(datetime.datetime)
            Timestamps for which the data is obtained.

        Returns
        -------
        dict
            Dictionary with each key being a station, and each value being a list of
            dictionaries with "data" and "info". Where data is the meteo data and info
            is metadata of said info.
        dict
            Dictionary where each key is a station and each value the data of that station.
            Like latitude, longitude, name, region, etc.

        """
        # stations_data: {siteid: ddf}
        self.station_data = self._get_stations_data()

        # data: {siteid: [{info, data}]}
        data = dict()
        # assumption: assuming that data will be available for each site separately and not altogether
        # might need to move this part to an abstract method
        for station in self._get_station_ids():
            if self.verbose:
                print(f"Fetching data for station {station}...")
            # station_urls [{urls, info}]
            station_urls = url_creator(
                self.data_url, timestamps, station, other_data=self.other_data
            )
            station_data = self._get_data_for_station(station_urls)
            data.update({station: station_data})

        self.data = data

        return self.data, self.station_data

    # saves the raw data to file
    def _save(self):
        """Saves raw data to file. Formats the data_file_format with the "info" of each
        data dictionary in self.data.
        """
        data = self.data
        for siteid, data_list in data.items():
            if self.verbose:
                print(f"Writting to file raw data from station {siteid}...")
            for dictionary in data_list:
                dictionary["info"].update({"siteid": siteid})
                filepath = self.raw_path / (
                    self.data_file_format.format(**dictionary["info"])
                )
                with open(filepath, "w") as fp:
                    data_write = (
                        dictionary["data"] if dictionary["data"] != None else {}
                    )
                    json.dump(dictionary["data"], fp)
        with open(self.station_file, "w") as fp:
            json.dump(self.station_data, fp)

    def download(self, timestamps=None, save=False):
        """Intended as the only exposed endpoint of the Class.
        Main routine managing all the download subprocesses.

        Parameters
        ----------
        timestamps : list<datetime.datetime>, optional
            Timestamps for which to get the data, by default None
        save : bool, optional
            Wether to save the data to file or just return, by default False

        Returns
        -------
        dict
            Dictionary with each key being a station, and each value being a list of
            dictionaries with "data" and "info". Where data is the meteo data and info
            is metadata of said info.
        dict
            Dictionary where each key is a station and each value the data of that station.
            Like latitude, longitude, name, region, etc.

        Raises
        ------
        ValueError
            When timestamps is None, nothing to do and misuse of the method.
        """
        if timestamps == None:
            raise ValueError("No timestamps for download.")

        data, stations = self._get_data(timestamps)

        if save:
            self._save()

        return data, stations
