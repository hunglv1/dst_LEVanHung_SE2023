# dst_LEVanHung_SE2023

This Airflow DAG retrieves data from the OpenSky and OpenMeteo APIs, performs data transformation, and writes the transformed data to a SQLite database.

## DAG Structure

The DAG consists of the following tasks:

1. 'get_previous_day': Returns the previous date in the format 'YYYY-MM-DD.
2. 'transform_data': Performs a transformation on the OpenSky data.
3. 'read_open_sky': Reads data from the OpenSky API.
4. 'read_open_meteo': Reads data from the OpenMeteo API.
5. 'write_data_to_file': Writes data to a JSON file.
6. 'drop_flight_table': Drops the 'flights' table from the SQLite database.
7. 'create_flight_table': Creates the 'flights' table in the SQLite database.
8. 'write_data_to_sqlite': Writes data to the 'flights' table in the SQLite database.

## Prerequisites
1. Airflow should be properly installed and configured.
2. The SQLite database should be set up with the necessary tables (see the task 'create_flight_table').
3. Run "docker compose up airflow-init" to set-up environments

## Configuration
The DAG can be configured using the 'default_args' configuration in the DAG file ('handle_server_data.py'). The following configuration options are available:

- 'start_date': The start date for the DAG. By default, it is set to 'days_ago(1)'.
- 'schedule_interval': The schedule interval for the DAG. By default, it is set to run at 1 AM every day.
- 'catchup': Whether to catch up on missed runs. By default, it is set to 'False'.
- 'owner': The owner of the DAG.

## Usage
1. Place the DAG file ('handle_server_data.py') in the Airflow dags folder.
2. Update the necessary configurations in the DAG file, such as tag id, schedule, owner...
3. Set up the SQLite database with the required information on Airflow (the SQLite database path, use 'sqlite_default' connection id)
4. Start the Airflow scheduler and webserver.
5. The DAG will be automatically scheduled and executed based on the specified schedule interval.

## License
HungLV

