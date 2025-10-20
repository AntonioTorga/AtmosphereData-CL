from .translate.dmc_translator import DMCTranslator
from .translate.translator import Translator
from .data_download.dmc_downloader import DMCDownloader
from .utils.utils import (
    check_file,
    check_path_exists,
    to_datetime,
    get_timestamps,
    get_existing_timestamps,
)
from enum import StrEnum
from datetime import datetime

import typer
from typing_extensions import Annotated
from pathlib import Path
from rich import print


class Timestep(StrEnum):
    """Enum for the timestep options."""

    H = "H"  # Hourly
    h = "h"  # hourly
    D = "D"  # Daily
    M = "M"  # Monthly
    Y = "Y"  # Yearly
    N = "N"


app = typer.Typer(
    name="MonetioCL",
    help="Monetio Command Line Interface",
    pretty_exceptions_enable=False,
)


@app.command()
def get_dmc(
    start_time: Annotated[
        datetime,
        typer.Argument(
            formats=["%Y-%m-%d", "%d-%m-%Y"], help="Start time for the resulting file"
        ),
    ],
    end_time: Annotated[
        datetime,
        typer.Argument(
            formats=["%Y-%m-%d", "%d-%m-%Y"], help="End time for the resulting file"
        ),
    ],
    user: Annotated[
        str,
        typer.Argument(help="User for meteochile.gob.cl Probably an Email address."),
    ],
    api_key: Annotated[
        str, typer.Argument(help="Api-Key provided by meteochile.gob.cl")
    ],
    intermediate_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
            help="Folder in which to leave the intermediate files if save_intermediate is True. Also to merge with existing files if cache is True",
        ),
    ] = Path("./inter_data"),
    output_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
            help="Folder in which to leave the resulting .netcdf file.",
        ),
    ] = Path("./output_data"),
    output_name: str = typer.Option(
        r"dmc.nc", "--output-name", "-o", help="Name of the resulting .netcdf file"
    ),
    timestep: str = typer.Option(
        Timestep.N,
        "--timestep",
        "-t",
        help="Time resolution for the result. If data doesn't have the resolution it provides the highest possible.",
    ),
    location_attr_names: str = typer.Option(
        None,
        "--location-attribute-names",
        "-l",
        help="Location attributes like 'region', 'comuna',etc. To add to netcdf as coord.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Verbosity of the execution."
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-m",
        help="Merge with existing files in the intermediate folder.",
    ),
    save_intermediate: bool = typer.Option(
        False,
        "--save-intermediate",
        help="Save the intermediate tabulated files used for netcdf composition.",
    ),
):
    """
    Download DMC data from start_time to end_time, and process into Melodies-Monet netcdf format.
    If wanted saves the "intermediate" tabulated data in .csv format.
    Also able to merge downloaded data with existing data in intermediate_path
    """

    timestamps = get_timestamps(start_time, end_time, time_interval=timestep)
    download_timestamps = timestamps

    if intermediate_path != None and merge:
        existing_timestamps = get_existing_timestamps(
            intermediate_path, r"{\d}.csv", time_name="momento"
        )
        download_timestamps = list(set(download_timestamps) - set(existing_timestamps))

    downloader = DMCDownloader(
        other_data={"user": user, "api_key": api_key}, verbose=verbose
    )
    translator = DMCTranslator(
        intermediate_path,
        output_path,
        verbose=verbose,
        output_name=output_name,
        timestep=timestep,
    )

    location_attr_names = (
        location_attr_names.split(",") if location_attr_names != None else []
    )

    print(
        f"Downloading DMC data from {start_time.strftime("%d-%m-%Y")} to {end_time.strftime("%d-%m-%Y")}"
    )
    data, station = downloader.download(download_timestamps)

    ddfs, station_ddf = translator.from_raw_to_intermediate_format(
        data, station, save=save_intermediate, merge=merge, time_name="momento"
    )

    for site_id, ddf in ddfs.items():
        ddfs[site_id]["data"] = translator.preprocess_intermediate_data(
            ddf["data"], "momento", site_id=site_id
        )

    station_ddf = translator.preprocess_intermediate_station_data(
        station_ddf, "codigoNacional", "latitud", "longitud"
    )

    xarray = translator.intermediate_to_xarray(
        ddfs,
        station_ddf,
        location_attr_names=location_attr_names,
        timestamps=timestamps,
    )

    xarray = translator.postprocess_xarray_data(
        xarray, timestep=timestep, start=start_time, end=end_time
    )

    translator.xarray_to_netcdf(xarray)

    return xarray


@app.command()
def process_intermediate_data(
    intermediate_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
            help="Folder with the intermediate files",
        ),
    ] = Path("./inter_data"),
    station_file: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=False,
            file_okay=True,
            resolve_path=True,
            help=".csv file with the station info",
        ),
    ] = Path("./stations.csv"),
    output_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
            help="Path where to leave the netcdf data.",
        ),
    ] = Path("."),
    filename_regex: str = typer.Option(
        r"(\d+).csv",
        "--filename-regex",
        "-r",
        help="Regular expression of the data files.",
    ),
    output_name: str = typer.Option(
        r"data_in_nc.nc", "--output-name", help="Name of the resulting .netcdf file"
    ),
    lat_name: str = typer.Option(
        "Latitud",
        "--lat-name",
        help="Name of the latitude attribute in the station file",
    ),
    lon_name: str = typer.Option(
        "Longitud",
        "--lon-name",
        help="Name of the longitude attribute in the station file",
    ),
    time_name: str = typer.Option(
        "time", "--time-name", help="Name of the time attribute in the data files."
    ),
    location_attr_names: str = typer.Option(
        None,
        "--location-attr-names",
        "-l",
        help="Location attributes like 'region', 'comuna',etc. To add to netcdf as coord.",
    ),
    id_name: str = typer.Option(
        "ID-Stored",
        "--id-name",
        help="Name of the ID attribute of observation sites in the station file",
    ),
    timestep: str = typer.Option(
        Timestep.N,
        "--timestep",
        "-t",
        help="Time resolution for the result. If data doesn't have the resolution it provides the highest possible.",
    ),
    verbose: bool = typer.Option(
        False, "--verbosity", "-v", help="Verbosity of the execution."
    ),
):
    """
    Process a batch of data located in intermediate_path, that is in intermediate format.
    Intermediate format is caracterized by using wide format in tabular data, where each file
    contains the data of 1 (one) station. It can have any number of data variables.
    """

    location_attr_names = (
        location_attr_names.split(",") if location_attr_names != None else []
    )

    translator = Translator(
        intermediate_path,
        output_path,
        raw_path=None,
        verbose=verbose,
        intermediate_filename_regex=filename_regex,
        station_filename=station_file,
        output_name=output_name,
        timestep=timestep,
    )

    data, station_ddf = translator.load_intermediate_data(
        time_name, id_name, lat_name, lon_name
    )

    xarray = translator.intermediate_to_xarray(
        data, station_ddf, location_attr_names=location_attr_names
    )
    translator.xarray_to_netcdf(xarray)


if __name__ == "__main__":
    app()
