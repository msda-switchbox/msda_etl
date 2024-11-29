# MSDA ETL

To run the ETL you will need the following applications on your computer:

1. Git - You can follow [these instructions for installing Git](https://github.com/git-guides/install-git)
2. Docker Desktop - You can follow [these instructions for installing Docker Desktop](https://www.docker.com/products/docker-desktop/). (If you are using Linux you can use Docker engine instead.)

After git is installed, you can create a local copy of the ETL source code repository by running the following command:

```sh
git clone https://github.com/msda-switchbox/msda_etl.git
```

## Running the ETL

Normally we build and run the ETL inside a Docker container, but if you prefer you can instead run it directly via Python in a virtualenv.

### Run ETL using Docker Compose (recommended method)

#### 1. Check / Modify the `.env` file

There is a file in base of the repo with the name `.env` - it contains settings used by the ETL and the CDM database container. Open it in an editor and checks the values therein.

```sh
# db connection info for (dev) CDM database
DB_USER="postgres"
DB_PASSWORD="devdb"
DB_NAME="omopcdm"

# the name of your source data
CDM_SOURCE_NAME="msda"

# an abbreviation of the name of your source data
CDM_SOURCE_ABBREVIATION="msda"

# the owner of your source data
CDM_HOLDER="msda"

# prose description of input dataset
SOURCE_DESCRIPTION="msda dev data"

# the release date of the source data examples: "1700-01-01", "31-12-1700"
SOURCE_RELEASE_DATE="31-12-23"

# the field delimiter used in the CSV input files
INPUT_DELIMITER=";"

# the following TEST_* values are used by pytest to connect to the test db
TEST_POSTGRES_HOST="localhost"
TEST_POSTGRES_PORT="5432"
TEST_POSTGRES_USER="postgres"
TEST_POSTGRES_PASSWORD="devdb"
TEST_POSTGRES_DBNAME="omopcdm"
TEST_POSTGRES_SCHEMA="omopcdm"

```

#### 2. Put your input CSV files in the right place

- Place your input files in the `source_data` directory
- Place your vocabulary files in the `vocab_data` directory

#### 3. Build and run the container

Use the following command to build the ETL container (if needed), then execute it. (The `--rm` flag used here tells docker to clean-up the container after it exits.) This process will also start the database container if necessary. By default the database container will use port `5432` on your computer.

```sh
docker compose run --build --rm etl
```

### Run ETL directly using Python (for developers)

This process is intended for development purposes. You'll need a local Python distribution installed.

#### 1. Setup virtual environment

The ETL has some python pacakge dependencies that need to be installed, we'll manage these using a `virtualenv`.:

```sh
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt -r requirements.dev.txt
```

After this is complete you should see `(venv) ` at the beginning of your command-line prompt.

#### 2. Run the checks & tests

We use pre-commit to manage the linters, checkers, and testing in the codebase. This command should now run all checks on the code.

```sh
pre-commit run --all
```

#### 3. Run the ETL

```sh
export PYTHONPATH=src
```

From the source root, run Python with this command (adjust the directory paths to match your local environment):

```sh
python3 -m etl --log-dir ./log --datadir ./source_data --vocab-dir ./vocab_data --db-host 127.0.0.1 --db-password devdb
```

If you've previously run tests on the data or if you are running the ETL for the first time, you will need to reload the vocab tables. This can be enabled with the `--reload-vocab` flag. For example:

```sh
python3 -m etl --reload-vocab --log-dir ./log --datadir ./source_data --vocab-dir ./vocab_data --db-host 127.0.0.1 --db-password devdb
```

To see other command-line flags and configuration options, use the `-h` or `--help` flags:

```sh
python3 -m etl --help
```

## Usage

When the ETL code is invoked with the `-h` or `--help` arguments the following help text is produced.

```
usage: etl [-h] [--version] [--log-dir LOG_DIR] [--datadir DATADIR]
           [--vocab-dir VOCAB_DIR]
           [--verbosity-level {DEBUG,INFO,WARNING,ERROR}]
           [--input-delimiter INPUT_DELIMITER]
           [--lookup-delimiter LOOKUP_DELIMITER]
           [--lookup-standard-concept-col LOOKUP_STANDARD_CONCEPT_COL]
           [--reload-vocab | --no-reload-vocab]
           [--run-integration-tests | --no-run-integration-tests]
           [--db-dbms DB_DBMS] [--db-host DB_HOST] [--db-port DB_PORT]
           [--db-name DB_NAME] [--db-schema DB_SCHEMA]
           [--db-username DB_USERNAME] [--db-password DB_PASSWORD]
           [--date-format DATE_FORMAT] [--cdm-source-name CDM_SOURCE_NAME]
           [--cdm-source-abbreviation CDM_SOURCE_ABBREVIATION]
           [--cdm-holder CDM_HOLDER]
           [--source-release-date SOURCE_RELEASE_DATE]
           [--cdm-etl-ref CDM_ETL_REF]
           [--source-description SOURCE_DESCRIPTION]
           [--source-doc-reference SOURCE_DOC_REFERENCE]

ETL from MSDA data to OMOP CDM

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --log-dir LOG_DIR     directory where run logs should be written (default:
                        PosixPath('/log'))
  --datadir DATADIR     directory where source data files are located
                        (default: PosixPath('/data'))
  --vocab-dir VOCAB_DIR
                        directory where vocabulary files are located (default:
                        PosixPath('/vocab'))
  --verbosity-level {DEBUG,INFO,WARNING,ERROR}
                        level of log detail that should be written to the
                        console (default: 'INFO')
  --input-delimiter INPUT_DELIMITER
                        delimiter used in the source input csv files (default:
                        ',')
  --lookup-delimiter LOOKUP_DELIMITER
                        delimiter used in the internal lookup csv files
                        (default: ';')
  --lookup-standard-concept-col LOOKUP_STANDARD_CONCEPT_COL
                        name of standard concept_id column in the lookup csv
                        file (default: 'standard_concept_id')
  --reload-vocab, --no-reload-vocab
                        enable vocab load (default: False)
  --run-integration-tests, --no-run-integration-tests
                        run etl integration tests as part of testsuite
                        (default: True)
  --db-dbms DB_DBMS     database management system used on the db_server
                        (default: 'postgresql')
  --db-host DB_HOST     network address of the database server (default:
                        'msda_postgres')
  --db-port DB_PORT     network port number to connect to the db_server on
                        (default: 5432)
  --db-name DB_NAME     name of the database to use on the database server
                        (default: 'postgres')
  --db-schema DB_SCHEMA
                        schema to use with the db_dbname (default: 'omopcdm')
  --db-username DB_USERNAME
                        name of the user account on the database server
                        (default: 'postgres')
  --db-password DB_PASSWORD
                        password associated with the db_username (default: '')
  --date-format DATE_FORMAT
                        date format (default: 'DDMONYYYY')
  --cdm-source-name CDM_SOURCE_NAME
                        name of the CDM instance (default: 'TEST DATA')
  --cdm-source-abbreviation CDM_SOURCE_ABBREVIATION
                        abbreviation of the CDM instance (default: 'TD')
  --cdm-holder CDM_HOLDER
                        holder of the CDM instance (default: 'TEST FACILITY')
  --source-release-date SOURCE_RELEASE_DATE
                        date of last export of the source data (default:
                        '31-12-23')
  --cdm-etl-ref CDM_ETL_REF
                        link to the CDM version used (default:
                        'https://github.com/edencehealth/msda_etl/')
  --source-description SOURCE_DESCRIPTION
                        description of the CDM instance (default: '')
  --source-doc-reference SOURCE_DOC_REFERENCE
                        link to source data documentation (default: '')
```
