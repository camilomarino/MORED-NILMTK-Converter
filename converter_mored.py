import os
import re
import warnings
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import yaml
from nilm_metadata import save_yaml_to_datastore
from nilmtk.datastore import Key
from nilmtk.measurement import LEVEL_NAMES
from nilmtk.utils import get_datastore

warnings.simplefilter(action="ignore", category=FutureWarning)


def convert_mored(mored_path: str, output_filename: str) -> None:
    """ """

    store = get_datastore(output_filename, "HDF", mode="w")

    # Convert raw data to DataStore
    _convert(mored_path, store)

    metadata_path = "metadata"

    # Add metadata
    save_yaml_to_datastore(metadata_path, store)
    store.close()

    print("Done converting MORED to HDF5!")


def _list_premises(input_path: str) -> Dict[int, str]:
    houses = [
        premise
        for premise in os.listdir(input_path)
        if os.path.isdir(Path(input_path, premise)) and premise.startswith("Premises_")
    ]
    houses = {int(house.split("Premises_")[1]): house for house in houses}
    return houses


def _load_data_location_one_building(metadata_yaml_path: str) -> Dict[int, str]:
    with open(metadata_yaml_path, "r") as fp:
        data_location = yaml.load(fp, Loader=yaml.FullLoader)["elec_meters"]
    data_location = {
        key: value["data_location"] for (key, value) in data_location.items()
    }
    return data_location


def _load_data_location(metadata_path: str) -> Dict[int, Dict[int, str]]:
    """
    Rerturn
    -------
    Diccionario de diccionarios. La primer key es ek numero de casa
    y la segunda el numero de electrodomestico. El value es la ruta
    del csv del electrodomestico
    """
    # load yaml
    data_location = {}
    houses = [
        yaml
        for yaml in os.listdir(metadata_path)
        if yaml.endswith(".yaml") and yaml.startswith("building")
    ]
    for house in houses:
        house_number = int(re.search("building(\d+)\.yaml$", house).group(1))
        data_location[house_number] = _load_data_location_one_building(
            Path(metadata_path, house)
        )
    return data_location


def _read_meter_csv(
    csv_path: str, sort_index: bool, drop_duplicates: bool
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d/%m/%Y %H:%M:%S")
    except ValueError:
        # some appliances have a different date format
        # see Premises_10/TV.csv
        print(
            f"{csv_path} has a different timestamp format, trying pd.to_datetime with no arguments"
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    df.dropna(inplace=True)
    df = df.astype(np.float32)
    if drop_duplicates:
        # https://stackoverflow.com/a/34297689/12462703
        df = df[~df.index.duplicated(keep="first")]
    if sort_index:
        df.sort_index(inplace=True)

    df = df.tz_localize("GMT").tz_convert(
        "Africa/Casablanca"
    )  # Maybe you have to localize
    # it in GMT and then convert it
    columns = pd.MultiIndex.from_tuples(
        [
            ("voltage", ""),
            ("power", "active"),
        ]
    )
    df.columns = columns
    df.columns.set_names(LEVEL_NAMES, inplace=True)

    return df


def _convert(
    mored_path: str, store, sort_index: bool = True, drop_duplicates: bool = False
) -> None:
    premises_data_location = _list_premises(mored_path)
    data_location = _load_data_location("metadata")

    # check correct data
    if set(premises_data_location.keys()) != set(data_location.keys()):
        print("The houses in mored_path do not match the houses in the metadata.")
        mored_path_houses = list(premises_data_location.keys())
        mored_path_houses.sort()
        metadata_houses = list(data_location.keys())
        metadata_houses.sort()
        print("Houses in mored_path:\t", mored_path_houses)
        print("Houses in metadata:\t", metadata_houses)
        return
    print(f"The houses found are: {list(premises_data_location.keys())}")

    for house_number, one_house_data_location in data_location.items():
        print(f"Converting house {house_number} ...")
        for elec_numer, csv_path in one_house_data_location.items():
            print(f"Converting elec {elec_numer} from {csv_path} ...")
            df = _read_meter_csv(
                Path(mored_path, csv_path), sort_index, drop_duplicates
            )
            key = Key(building=house_number, meter=elec_numer)
            store.put(str(key), df)

if __name__ == "__main__":
    convert_mored("data", "mored.h5")