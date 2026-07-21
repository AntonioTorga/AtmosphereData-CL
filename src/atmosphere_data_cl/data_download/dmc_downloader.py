from .downloader import Downloader
from pathlib import Path
from json import JSONDecodeError
import asyncio
import httpx


async def fetch_all_data(urls: list):
    """Fetchs all the data from DMC API.

    Requests all the URLS provided with an HTTPX AsyncClient.
    Gathers all the responses in a tasks list and then returns them parsed.

    Parameters
    ----------
    urls : list
        list of URLS meant to be requested (GET) by HTTPX Async Client.

    Returns
    -------
    list
        List of the same length containing the parsed responses in JSON format.
        If the parsing fails for the ith element, then the ith element of this list will be
        None value.
    """
    async with httpx.AsyncClient(timeout=None) as client:
        ready = False
        while not ready:
            try:
                tasks = [client.get(url) for url in urls]
                responses = await asyncio.gather(*tasks)
                ready = True
            except httpx.ConnectTimeout as e:
                print(e, " ...trying again")
        processed_responses = []
        for response in responses:
            try:
                x = response.json()
            except JSONDecodeError:
                x = None
            processed_responses.append(x)
        return processed_responses


class DMCDownloader(Downloader):
    """Downloader of the DMC network data.

    Class in charge of specifics of the download of the DMC observation network.
    It inherits from Downloader class and provides it with the specifics functions
    needed to interact with that specific network.

    Attributes
    ----------
    data_url : string
        Formattable string of the URL used to get the actual data.
        It contains a series of attributes enclosed in brackets, meant to be filled
        with information provided as a dictionary. The keys of said dictionary must coincide
        with the names of the attributes (there can be extra items in the dict but
        not less that what are needed).
    stations_url
        Same as the data_url but the URL points to data for all the stations in the network,
        such as the name, latitude, longitude, etc.

    Methods
    -------

    _get_station_ids
        Gets the station ids as a list.

    _get_stations_data
        Gets the station data from the stations_url

    _get_data_for_station
        Gets all the data from URLs provided.

    """

    def __init__(self, raw_path: Path = None, **kwargs):
        """
        Constructor

        Parameters
        ----------
        raw_path : pathlib.Path
            Path pointing to a folder in which to save (if specified) the raw data downloaded.

        **kwargs : dictionary
            Passed to Downloader constructor, set verbosity, file name RegEx, etc.
        """
        data_url = r"https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesEma/{siteid}/{year}/{month}?usuario={user}&token={api_key}"
        stations_url = r"https://climatologia.meteochile.gob.cl/application/servicios/getEstacionesRedEma?usuario={user}&token={api_key}"
        super().__init__(data_url, stations_url, raw_path, **kwargs)

    def _get_station_ids(self):
        """Gets the station ids as a list.

        Returns all the station ids if self.station_data is not None. Meant to be run after
        setting self.station_data

        Returns
        -------
        list
            List containing all the station ids.
        """
        return [
            station["codigoNacional"] for station in self.station_data["datosEstacion"]
        ]

    def _get_stations_data(self):
        """Gets the station data from the stations_url

        Makes a GET request to the stations_url, parses the json response and returns it
        as a dictionary.

        Returns
        -------
        dict
            a dict with the data returned by the URL.
        """
        response = self.client.get(self.stations_url.format(**self.other_data))
        # TODO: manage unsuccesful response
        station_data = response.json()
        return station_data

    # for a list in [{url, info}] format transform into [{info, data}] | data in dictionary from json
    def _get_data_for_station(self, station_urls: list):
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
        urls = [url["url"] for url in station_urls]
        info = [_info["info"] for _info in station_urls]

        responses = asyncio.run(fetch_all_data(urls))

        data = []
        for _info, _response in zip(info, responses):
            data.append({"info": _info, "data": _response})

        return data
