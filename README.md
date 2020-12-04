# Nymea database export

Script to export data collected by nymea in a MariaDB to CSV files.

## Requirements

- Python 3.7
- pip3

## Setup & Install (Linux)

```shell
$ pwd
/home/vbauer/nymea-mariadb-export
```

Check you're in the repo's root directory

```shell
pip3 -m venv venv
```

Create a virtual environment to store the libraries this project needs in a folder named `venv`.

```shell
source venv/bin/activate
```

Activate the created virtual environment.

```shell
pip3 install -r requirements.txt
```

Install the required libraries (they will only be installed in this particular virtual environment).

## Usage

```shell
python export.py
```

Conncects to the database specified in `mariadb_connection_config.yml` and exports data for every measurement specified in  `measurements_config.yml` into seperate CSV files inside the `data` directory.

By default the script asks for the password to the mysql database and for a timestamp defining the earliest data to export.

## Configuration

### In `export.py`

```python
# ...

# Filename of the configuration file for the database connection
DB_CONNECTION_CONFIG_FILE = 'mariadb_connection_config.yml'
# Filename of the configuration file for the metadata about the measurements
DATABASE_METADATA_FILE = 'measurements_config.yml'
# Directory name where the CSV files will be stored
DATA_DIRECTORY = './data'

# ...
```

### Database connection

In `mariadb_connection_config.yml` the following top level keys are required:

- `host` - The IP or hostname (optionally with port) where the database is running
- `protocol` - The MySQL protocol string accepted by SQLAlchemy (see: [MySQL dialect](https://docs.sqlalchemy.org/en/13/core/engines.html#mysql))
- `user` - The mysql user at `host`

Optionally provide the password for `user`@`host` in the top level field `password`.

### Measurements

In `measurements_config.yml` the following structure is expected:

```yml
locations:
    Bauhof/Kläranlage: # Custom name of the location, used in the CSV filename
        database: client_4_enceladus # Name of the database storing data for this location
        devices:
            Fronius Solar Inverter 1: # Custom name of a device at the location, used in the CSV filename
                thingId: 9a31ba36-cb1e-4c45-8c00-70c0e3ef5176 # Nymea's ThingID corresponding to the device
                measurements:
                    Erzeugung: # Custom name of a measurement taken with the device at the location, used in CSV as column header and in the CSV filename
                        typeId: 788accbc-b86e-471b-b37f-14c9c6411526 # Nymeas TypeID corresponding to the measurement
                        unit: W # Unit used as part of the column name in the CSV
            Fronius Solar Inverter 2: # Another device ...
                # ...
    Gemeindeamt: # Another location ...
        # ...
```
