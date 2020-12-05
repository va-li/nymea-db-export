# Nymea database export

Export [nymea](https://github.com/nymea/nymea)'s collected measurements into human-readable formats.

## Requirements

- Python 3.7+
- pip3

## Setup & Install (Linux)

```shell
$ pwd
/home/vbauer/nymea-mariadb-export
```

Check you're in the repo's root directory.

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

Install the required libraries (they will only be installed in this project's virtual environment folder `venv`).

## Usage

### Get measurements from the last full day

```shell
python nymea_db_export.py --previous-full-day
```

Conncects to the database specified in `mariadb_connection_config.yml` and exports data for every measurement specified in  `measurements_config.yml` into seperate CSV files inside the `./exported-data` directory.

By default the script asks for a user and password that are expected to be authorized in the mysql database.

### Get measurements since 1. Dec. 2020 4:15pm

```shell
python nymea_db_export.py --since 2020-12-01T16:15:00
```

### Get all measurements since the very first timestamp in the database

```shell
python nymea_db_export.py
```

Currently this earliest date is set in the code to `2020-09-01` (for historic reasons :P).

## Configuration

### Database connection

In `mariadb_connection_config.yml` the following top level keys are required:

- `host` - The IP or hostname (optionally with port) where the database is running
- `protocol` - The MySQL protocol string accepted by SQLAlchemy (see: [MySQL dialect](https://docs.sqlalchemy.org/en/13/core/engines.html#mysql))
- `user` - The mysql user at `host`

Optionally provide the password for `user`@`host` in the top level field `password`.

> Be careful to not commit sensitive data (like your password) in `mariadb_connection_config.yml` to Git! Otherwise you might accidentially publish your full database credentials and where to reach it...

### Measurements metadata

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
