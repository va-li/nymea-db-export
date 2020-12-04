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
DEFAULT_DATA_DIRECTORY = './data'

parser = argparse.ArgumentParser(description='Export measurements as CSV from MariaDB using nymea\'s databse schema.')
group = parser.add_mutually_exclusive_group()
group.add_argument('--start', metavar='timestamp', type=str, default=None, action='store')
parser.add_argument('--end', metavar='timestamp', type=str, default=None, action='store')
group.add_argument('--previous-full-hour', action='store_true')
group.add_argument('--previous-full-day', action='store_true')
parser.add_argument('db_connection_config', metavar='<database-config>', type=str, default=DEFAULT_DB_CONNECTION_CONFIG_FILE, action='store')
parser.add_argument('db_metadata_config', metavar='<metadata-config>', type=str, default=DEFAULT_DB_METADATA_CONFIG_FILE, action='store')
parser.add_argument('export_directory', metavar='<export-dir>', type=str, default=DEFAULT_DATA_DIRECTORY, action='store')

args = parser.parse_args()

DB_CONNECTION_CONFIG_FILE = args.db_connection_config
log.info('Using database connection config file: ' + DB_CONNECTION_CONFIG_FILE)
DB_METADATA_CONFIG_FILE = args.db_metadata_config
log.info('Using database metadata config file: ' + DB_METADATA_CONFIG_FILE)
DATA_DIRECTORY = args.export_directory
log.info('Using data output directory: ' + DATA_DIRECTORY)

if args.start:
	start_timestamp = parse_timestring(args.start)
	if args.end:
		end_timestamp = parse_timestring(args.end)
	else:
		end_timestamp = datetime.now()
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
log.info('First exported datapoint will be before ' + end_timestamp.isoformat())

with open(DB_CONNECTION_CONFIG_FILE, 'r') as db_config_file:
	db_config = yaml.safe_load(db_config_file)

with open(DB_METADATA_CONFIG_FILE, 'r') as metadata_file:
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

				sql_query = text(
					'SELECT timestamp, value '
						'FROM entries '
						'WHERE thingId = :thingId '
							'AND typeId = :typeId '
							'AND timestamp >= :unixMillisecondsStart '
							'AND timestamp < :unixMillisecondsEnd')
				parameters = {
					'thingId': '{' + device['thingId'] + '}',
					'typeId': '{' + measurement_info['typeId'] + '}',
					'unixMillisecondsStart': int(start_timestamp.timestamp())*1000,
					'unixMillisecondsEnd': int(end_timestamp.timestamp())*1000
				}

				data : pd.DataFrame = pd.read_sql(sql_query, params=parameters, con=db_connection, index_col='timestamp')

				log.info('Retrieved ' + str(len(data)) + ' datapoints for ' + measurement_name + ' of ' + device_name + ' at ' + location_name)
				
				# Give the index and measurement column meaningful names
				data.index.rename('UNIX-Zeitstempel', inplace=True)
				data.rename(columns={'value': measurement_column_name}, inplace=True)

				if not data.index.is_monotonic_increasing:
					raise ValueError('UNIX-Zeitstempel der Daten ist nicht monoton steigend!')
				if data.index.has_duplicates:
					raise ValueError('UNIX-Zeitstempel der Daten hat Duplikate!')
				
				# Convert the unreadable UNIX-Timestamp to human readable UTC and local timestamps
				data['UTC-Zeitstempel'] = pd.to_datetime(data.index.to_series(), unit='ms').dt.tz_localize('UTC').dt.round('s')
				data['Lokalzeit-Zeitstempel'] = data['UTC-Zeitstempel'].dt.tz_convert('Europe/Vienna').dt.tz_localize(None)
				data['UTC-Zeitstempel'] = data['UTC-Zeitstempel'].dt.tz_localize(None)

				# Reorder the columns so when they are printed the timestamps are all left of the corresponding measurements
				data = data[['UTC-Zeitstempel','Lokalzeit-Zeitstempel', measurement_column_name]]

				log.info('Edited timestamps for datapoints successfully.')
				
				if not Path(DATA_DIRECTORY).is_dir():
					Path(DATA_DIRECTORY).mkdir()
				
				escaped_timestamped_folder_name = re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', start_timestamp.isoformat(' ') + ' bis ' + end_timestamp.isoformat(' '))
				timestamped_folder_path = Path(DATA_DIRECTORY) / Path(escaped_timestamped_folder_name)
				if not timestamped_folder_path.is_dir():
					timestamped_folder_path.mkdir()

				folder_path = Path(DATA_DIRECTORY) / Path(escaped_timestamped_folder_name) / Path(re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', location_name))
				if not folder_path.is_dir():
					folder_path.mkdir()

				escaped_data_filename = re.sub(WINDOWS_CHARACTER_BLACKLIST, '_', (location_name + '-' + device_name + '-' + measurement_name + '.csv'))
				data.to_csv(folder_path / Path(escaped_data_filename), index=False)
				log.info('Saved measurements successfully to CSV (' + str(folder_path / Path(escaped_data_filename)) + ')')
	finally:
		db_connection.dispose()
