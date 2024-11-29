"""declarative configuration for ETL"""

from pathlib import Path
from typing import Any

from basecfg import BaseCfg, opt


def pathparse(val: Any) -> Path:
    """helper which returns a pathlib path based on the given input"""
    return Path(str(val))


class ETLConf(BaseCfg):
    """declarative configuration class for ETL"""

    # directories and paths----------------------------------------------------
    log_dir: Path = opt(
        default=Path("/log"),
        doc="directory where run logs should be written",
        parser=pathparse,
    )
    datadir: Path = opt(
        default=Path("/data"),
        doc="directory where source data files are located",
        parser=pathparse,
    )
    vocab_dir: Path = opt(
        default=Path("/vocab"),
        doc="directory where vocabulary files are located",
        parser=pathparse,
    )

    # general operating params -----------------------------------------------
    verbosity_level: str = opt(
        default="INFO",
        doc="level of log detail that should be written to the console",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    # source-related settings -------------------------------------------------
    input_delimiter: str = opt(
        default=",",
        doc="delimiter used in the source input csv files",
    )
    lookup_delimiter: str = opt(
        default=";",
        doc="delimiter used in the internal lookup csv files",
    )
    lookup_standard_concept_col: str = opt(
        default="standard_concept_id",
        doc="name of standard concept_id column in the lookup csv file",
    )

    # modes of operation ------------------------------------------------------
    reload_vocab: bool = opt(
        default=False,
        doc="enable vocab load",
    )
    run_integration_tests: bool = opt(
        default=True,
        doc="run etl integration tests as part of testsuite",
    )

    # database settings--------------------------------------------------------
    db_dbms: str = opt(
        default="postgresql",
        doc="database management system used on the db_server",
    )
    db_host: str = opt(
        default="msda_postgres",
        doc="network address of the database server",
    )
    db_port: int = opt(
        default=5432,
        doc="network port number to connect to the db_server on",
    )
    db_name: str = opt(
        default="postgres",
        doc="name of the database to use on the database server",
    )
    db_schema: str = opt(
        default="omopcdm",
        doc="schema to use with the db_dbname",
    )
    db_username: str = opt(
        default="postgres",
        doc="name of the user account on the database server",
    )
    db_password: str = opt(
        default="",
        doc="password associated with the db_username",
    )

    # CDM source variables-----------------------------------------------------
    date_format: str = opt(
        default="DDMONYYYY",
        doc="date format",
    )
    cdm_source_name: str = opt(
        default="TEST DATA",
        doc="name of the CDM instance",
    )
    cdm_source_abbreviation: str = opt(
        default="TD",
        doc="abbreviation of the CDM instance",
    )
    cdm_holder: str = opt(
        default="TEST FACILITY",
        doc="holder of the CDM instance",
    )
    source_release_date: str = opt(
        default="31-12-23",
        doc="date of last export of the source data",
    )
    cdm_etl_ref: str = opt(
        default="https://github.com/msda-switchbox/msda_etl/",
        doc="link to the CDM version used",
    )
    source_description: str = opt(
        default="",
        doc="description of the CDM instance",
    )
    source_doc_reference: str = opt(
        default="",
        doc="link to source data documentation",
    )
