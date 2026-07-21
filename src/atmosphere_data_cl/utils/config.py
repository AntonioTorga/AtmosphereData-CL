import json
import os
import pathlib


def read_config_file():
    # read JSON config file
    current_folder = pathlib.Path(__file__).parent.absolute()
    with open(
        os.path.join(current_folder, "..\\config\\config.json")
    ) as json_data_file:
        config_data = json.load(json_data_file)

    return config_data


class Config:
    __config_data = read_config_file()

    def __init__(self, debug=False):
        config_data = Config.__config_data

        self.debug = debug
        self.observation = None
        self.model = None

        self.observations = config_data["observations"]
        self.models = config_data["models"]
        self.defaults = config_data["defaults"]

    def get_station_url(self):
        """
        Returns the URL for the station info to be downloaded.
        """
        if self.observation is None:
            raise ValueError("Observation not set")

        return self.observations[self.observation]["stations"]["url"]

    def get_data_url(self):
        """
        Returns the URL for the data to be downloaded.
        """
        if self.observation is None:
            raise ValueError("Observation not set")

        return self.observations[self.observation]["data"]["url"]
