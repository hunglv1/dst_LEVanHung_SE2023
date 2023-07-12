"""
This file is created by LE Van Hung
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago

from common import to_seconds_since_epoch, read_from_open_sky, read_from_open_meteo, write_to_json

# dag configurations
default_args = {
    "start_date": days_ago(1),
    "schedule_interval": "0 1 * * *",  # Run at 1 AM every day
    #"schedule_interval": "@once",  # Run only once -> open it for
    "catchup": False,  # Disable catching up on missed runs
    "owner": "LE_Van_Hung",
}


@task
def get_previous_day(ds=None) -> str:
    """
    Returns the previous date in the format YYYY-MM-DD.

    Args:
        ds (str): The execution date passed by Airflow (in the format YYYY-MM-DD).
            Default value is None.

    Returns:
        str: The previous date in the format YYYY-MM-DD.
    """
    if ds is None:
        # If ds is not provided, use the current date
        current_date = datetime.now().date()
    else:
        # Parse the ds parameter as a date
        current_date = datetime.strptime(ds, "%Y-%m-%d").date()

    # Calculate the previous date
    previous_date = current_date - timedelta(days=1)

    # Format the previous date as YYYY-MM-DD
    previous_date_str = previous_date.strftime("%Y-%m-%d")

    return previous_date_str


@task
def transform_data(opensky_data):
    """
    Perform a transformation on the OpenSky data.

    Args:
        opensky_data (dict): The data retrieved from the OpenSky API.

    Returns:
        dict: The transformed data.
    """
    airports = opensky_data.get("states", [])
    max_distance = 0
    airport_with_max_distance = None

    # Find the flight which has the most estArrivalAirportHorizDistance
    for airport in airports:
        distance = airport.get("estArrivalAirportHorizDistance")
        if distance is not None and distance > max_distance:
            max_distance = distance
            airport_with_max_distance = airport

    transformed_data = {
        "airport_with_max_distance": airport_with_max_distance
    }

    return transformed_data


@task
def read_open_sky(previous_date: str):
    """
    Read data from the OpenSky API.

    Returns:
        dict: The data retrieved from the OpenSky API.
    """
    # Calculate begin and end timestamps for the previous day
    begin_timestamp = to_seconds_since_epoch(previous_date)
    end_timestamp = begin_timestamp + 86400  # Add 1 day in seconds

    return read_from_open_sky(begin_timestamp, end_timestamp)


@task
def read_open_meteo():
    """
    Read data from the OpenMeteo API.

    Returns:
        dict: The data retrieved from the OpenMeteo API.
    """
    latitude = 52.52  # Latitude of the location
    longitude = 13.41  # Longitude of the location
    current_date = datetime.now().strftime("%Y-%m-%d")

    return read_from_open_meteo(latitude, longitude, current_date)


@task
def write_data_to_file(data):
    """
    Write data to a JSON file.

    Args:
        data (dict): The data to be written to the JSON file.
    """
    # Get the path to the dags folder
    base_dir = os.path.dirname(__file__)
    json_file_path = os.path.join(base_dir, 'server_data.json')
    print(f"file is stored at: {json_file_path}")

    # Write the data to the JSON file
    write_to_json(data, json_file_path)


@task
def drop_flight_table():
    """
    Drop the 'users' table from the SQLite database.
    """
    drop_table_sql = "DROP TABLE IF EXISTS flights;"
    drop_table_task = SqliteOperator(
        task_id="drop_table_flights",
        sql=drop_table_sql,
        sqlite_conn_id="sqlite_default",
        database="/dags/db/exam.db"
    )
    drop_table_task.execute(context=None)


@task
def create_flight_table():
    """
    Create the 'flights' table in the SQLite database.
    """
    create_table_sql = """
            CREATE TABLE IF NOT EXISTS flights (
                icao24 TEXT,
                firstSeen INTEGER,
                estDepartureAirport TEXT,
                lastSeen INTEGER,
                estArrivalAirport TEXT,
                callsign TEXT,
                estDepartureAirportHorizDistance INTEGER,
                estDepartureAirportVertDistance INTEGER,
                estArrivalAirportHorizDistance INTEGER,
                estArrivalAirportVertDistance INTEGER,
                departureAirportCandidatesCount INTEGER,
                arrivalAirportCandidatesCount INTEGER
            );
            """
    create_table_task = SqliteOperator(
        task_id="create_table_flights",
        sql=create_table_sql,
        sqlite_conn_id="sqlite_default",
        database="/dags/db/exam.db"
    )
    create_table_task.execute(context=None)


@task
def write_data_to_sqlite(data):
    """
    Write data to a SQLite

    Args:
        data : dict
    """
    flights = data.get("states", [])
    hook = SqliteHook(
        sqlite_conn_id='sqlite_default')

    # Insert the data into the table
    # Iterate over each item in the data list and insert the values into the table
    for item in flights:
        values = list(item.values())
        hook.insert_rows(table='flights', rows=[values], target_fields=None, commit_every=1)


# declare dag
with DAG(
        default_args=default_args,
        dag_id="hunglv_dag"
) as dag:
    # dags calling
    previous_date = get_previous_day()
    opensky_data = read_open_sky(previous_date)
    openmeteo_data = read_open_meteo()
    transformed_data = transform_data(opensky_data)

    write_data_to_file({
        "opensky_data": opensky_data,
        "openmeteo_data": openmeteo_data,
        "transformed_data": transformed_data
    })

    drop_table_task = drop_flight_table()
    create_table_task = create_flight_table()
    write_data_sqlite_task = write_data_to_sqlite(opensky_data)
    drop_table_task >> create_table_task >> write_data_sqlite_task  # set order for sql tasks
