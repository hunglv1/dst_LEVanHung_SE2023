"""
This file is created by LE Van Hung
"""

import json
from datetime import date
from time import mktime

import requests


def to_seconds_since_epoch(input_date: str) -> int:
    """
    Converts a given date string to the corresponding number of seconds since the epoch.

    Args:
        input_date (str): The input date string in the format "YYYY-MM-DD".

    Returns:
        int: The number of seconds since the epoch corresponding to the input date.

    Example:
        input_date = "2023-06-01"
        to_seconds_since_epoch(input_date) -> 1698912000
    """
    # Convert the input date string to a date object
    input_date_object = date.fromisoformat(input_date)

    # Convert the date object to a time tuple
    input_time_tuple = input_date_object.timetuple()

    # Convert the time tuple to the number of seconds since the epoch
    seconds_since_epoch = int(mktime(input_time_tuple))

    return seconds_since_epoch


def read_from_open_sky(begin: int, end: int) -> dict:
    """
    Reads data from the OpenSky API and returns it as a dictionary.

    Returns:
        dict: A dictionary containing the data retrieved from the OpenSky API.
    """
    BASE_URL_OPENSKY = "https://opensky-network.org/api/flights/departure"

    params = {
        "airport": "LFPG",  # ICAO code for CDG
        "begin": begin,
        "end": end
    }

    response = requests.get(BASE_URL_OPENSKY, params=params)

    # Get the response content as a json
    data = response.json()

    # Convert the list of states to a dictionary with "states" key
    result = {"states": data}

    return result


def read_from_open_meteo(latitude: float, longitude: float, date: str) -> dict:
    """
    Reads data from the OpenMeteo API and returns it as a dictionary.

    Args:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.
        date (str): The date for which to retrieve the weather data in the format "YYYY-MM-DD".

    Returns:
        dict: A dictionary containing the data retrieved from the OpenMeteo API.
    """
    BASE_URL_OPENMETEO = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": "true",
        "date": date,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
    }

    response = requests.get(BASE_URL_OPENMETEO, params=params)

    # Get the response content as a json
    data = response.json()

    return data


def write_to_json(data: dict, filename: str):
    """
    Writes the given data dictionary to a JSON file.

    Args:
        data (dict): The data to be written to the JSON file.
        filename (str): The name of the JSON file.
    """
    with open(filename, "w") as file:
        json.dump(data, file)  # Write the data dictionary to the JSON file
