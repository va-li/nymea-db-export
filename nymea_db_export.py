#!/usr/bin/env python

import pandas as pd
import yaml
import re
from dateutil.parser import parse as parse_timestring
from sqlalchemy import text, create_engine
import pymysql
from getpass import getpass
from pathlib import Path
import argparse
from datetime import datetime, timedelta
import logging
import sys

###############################################################################
# Program configuration, argument parser and log setup
###############################################################################

log = logging.getLogger('mariadb-export.py')
log.setLevel(logging.INFO)
stdout_log_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_log_handler.setFormatter(formatter)
log.addHandler(stdout_log_handler)

# Regex pattern matching anything that could cause problems for windows filenames
WINDOWS_CHARACTER_BLACKLIST = re.compile(r'[^\w\.!@#$^+=-]+')
# Filename of the configuration file for the database connection
DEFAULT_DB_CONNECTION_CONFIG_FILE = './mariadb_connection_config.yml'
# Filename of the configuration file for the metadata about the measurements
DEFAULT_DB_METADATA_CONFIG_FILE = './measurements_config.yml'
# Directory name where the CSV files will be stored
DEFAULT_DATA_DIRECTORY = './exported-data'

parser = argparse.ArgumentParser(description='Export measurements as CSV from MariaDB using nymea\'s databse schema.')
mutex_group_timerange = parser.add_mutually_exclusive_group()

mutex_group_timerange.add_argument('--since', metavar='ISO-TIMESTAMP', type=str, default=None, action='store')
mutex_group_timerange.add_argument('--between', metavar='ISO-TIMESTAMP', nargs=2, type=str, default=None, action='store')

mutex_group_timerange.add_argument('--previous-full-hour', action='store_true')
mutex_group_timerange.add_argument('--previous-full-day', action='store_true')

group_configuration = parser.add_argument_group('configuration')
parser.add_argument('--db-config', type=str, default=DEFAULT_DB_CONNECTION_CONFIG_FILE, action='store')
parser.add_argument('--meta-config', type=str, default=DEFAULT_DB_METADATA_CONFIG_FILE, action='store')
parser.add_argument('--export-directory', type=str, default=DEFAULT_DATA_DIRECTORY, action='store')

###############################################################################
# Main program exporting measurements
###############################################################################

def retrieve_measurements(device_id: str, measurement_id: str, from_timestamp: datetime, to_timestamp: datetime) -> pd.DataFrame:
	sql_query = text(
					'SELECT timestamp, value '
						'FROM entries '
						'WHERE thingId = :thingId '
							'AND typeId = :typeId '
							'AND timestamp >= :unixMillisecondsStart '
							'AND timestamp < :unixMillisecondsEnd')
	parameters = {
		'thingId': '{' + device_id + '}',
		'typeId': '{' + measurement_id + '}',
		'unixMillisecondsStart': int(from_timestamp.timestamp())*1000,
		'unixMillisecondsEnd': int(to_timestamp.timestamp())*1000
	}
	measurements = pd.read_sql(sql_query, params=parameters, con=db_connection, index_col='timestamp')

	return measurements

def process_measurements_for_readability(measurments: pd.DataFrame, measurement_header: str) -> pd.DataFrame:
	# Do it like pandas' default and treat the original DataFrame as immutable
	measurments = measurments.copy()

	# Give the index and measurement column meaningful names
	measurments.index.rename('UNIX-Zeitstempel', inplace=True)
	measurments.rename(columns={'value': measurement_header}, inplace=True)
	
	# Convert the unreadable UNIX-Timestamp to human readable UTC and local timestamps.
	# The `.dt.tz_localize(None)` removes the timezone info without changing the timestamp value,
	# this way we get ISO timestamps when they are saved to a CSV file without the timezone string (e.g. '+00:00'),
	# because many programs cannot interpret this and we leave it to the user to interpret the data using the
	# CSV's column headers.
	measurments['UTC-Zeitstempel'] = pd.to_datetime(data.index.to_series(), unit='ms').dt.tz_localize('UTC').dt.round('s')
	measurments['Lokalzeit-Zeitstempel'] = data['UTC-Zeitstempel'].dt.tz_convert('Europe/Vienna').dt.tz_localize(None)
	measurments['UTC-Zeitstempel'] = data['UTC-Zeitstempel'].dt.tz_localize(None)

	# Reorder the columns so when they are printed the timestamps are all left of the corresponding measurements
	measurments = measurments[['UTC-Zeitstempel','Lokalzeit-Zeitstempel', measurement_header]]
	return measurements

if __name__ == "__main__":

	args = parser.parse_args()

	db_connection_config_file = args.db_config
	log.info('Using database connection config file: ' + db_connection_config_file)
	db_metadata_config_file = args.meta_config
	log.info('Using database metadata config file: ' + db_metadata_config_file)
	export_directory = args.export_directory
	log.info('Using data output directory: ' + export_directory)

	if args.since:
		start_timestamp = parse_timestring(args.start)
		end_timestamp = datetime.now()
	elif args.between:
		end_timestamp = parse_timestring(args.between[0])
		start_timestamp = parse_timestring(args.between[1])
	elif args.previous_full_hour:
		start_previous_full_hour = (datetime.now() - timedelta(hours=1))\
			.replace(minute=0, second=0, microsecond=0)
		start_timestamp = start_previous_full_hour
		start_current_hour = datetime.now()\
			.replace(minute=0, second=0, microsecond=0)
		end_timestamp = start_current_hour
	elif args.previous_full_day:
		start_previous_full_day = (datetime.now() - timedelta(days=1))\
			.replace(hour=0, minute=0, second=0, microsecond=0)
		start_timestamp = start_previous_full_day
		start_current_day = datetime.now()\
			.replace(hour=0, minute=0, second=0, microsecond=0)
		end_timestamp = start_current_day
	else:
		start_timestamp = parse_timestring('2020-09-01')
		end_timestamp = datetime.now()

	log.info('First exported datapoint will be at or after ' + start_timestamp.isoformat())
	log.info('Last exported datapoint will be before ' + end_timestamp.isoformat())

	with open(db_connection_config_file, 'r') as db_config_file:
		db_config = yaml.safe_load(db_config_file)

	with open(db_metadata_config_file, 'r') as metadata_file:
		metadata = yaml.safe_load(metadata_file)

	if not db_config['user']:
		db_config['user'] = input('Enter username for database at ' + db_config['host'] + ': ')

	if not db_config['password']:
		db_config['password'] = getpass('Enter password for database at ' + db_config['host'] + ': ')

	db_server_url = db_config['protocol'] + '://' + db_config['user'] + ':' + db_config['password'] + '@' + db_config['host'] + '/'

	log.info('Number of locations configured: ' + str(len(metadata['locations'])))
	for location_name in metadata['locations']:
		location = metadata['locations'][location_name]
		devices = location['devices']

		db_connection = create_engine(db_server_url + location['database'])
		try:
			log.info('Number of devices at ' + location_name + ' configured: ' + str(len(devices)))
			for device_name in devices:
				device = devices[device_name]
				measurements = device['measurements']

				log.info('Number of measurements of ' + device_name + ' at ' + location_name + ' configured: ' + str(len(measurements)))
				for measurement_name in measurements:
					measurement_info = measurements[measurement_name]
					measurement_column_name = measurement_name + '(' + measurement_info['unit'] + ')'

					log.info('Requesting measurements from database, this migth take some time...')

					data = retrieve_measurements(device['thingId'], measurement_info['typeId'], start_timestamp, end_timestamp)

					log.info('Retrieved ' + str(len(measurements)) + ' datapoints for ' + measurement_name + ' of ' + device_name + ' at ' + location_name)
					
					data = process_measurements_for_readability(data, measurement_column_name)

					log.info('Edited timestamps for datapoints successfully.')
					
					if not Path(export_directory).is_dir():
						Path(export_directory).mkdir()
						log.info('Created directory for exported data: ' + export_directory)
					
					escaped_timestamped_folder_name = re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', start_timestamp.isoformat(' ') + ' bis ' + end_timestamp.isoformat(' '))
					timestamped_folder_path = Path(export_directory) / Path(escaped_timestamped_folder_name)
					if not timestamped_folder_path.is_dir():
						timestamped_folder_path.mkdir()

					location_folder_path = Path(export_directory) / Path(escaped_timestamped_folder_name) / Path(re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', location_name))
					if not location_folder_path.is_dir():
						location_folder_path.mkdir()

					escaped_data_filename = re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', (location_name + '-' + device_name + '-' + measurement_name + '.csv'))
					data.to_csv(location_folder_path / Path(escaped_data_filename), index=False)
					log.info('Saved measurements successfully to CSV (' + str(location_folder_path / Path(escaped_data_filename)) + ')')
		finally:
			db_connection.dispose()
