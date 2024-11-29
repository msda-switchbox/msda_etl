"""Main program to run the ETL"""

import os

import baselog

from .config import ETLConf
from .process import run_etl
from .util.db import create_engine_from_args
from .util.memory import set_gc_threshold_mult


def main() -> None:
    """
    Main entrypoint for running the ETL
    """
    config = ETLConf(
        prog=__package__,
        prog_description="ETL from MSDA data to OMOP CDM",
        version=f"{os.environ.get('GITHUB_TAG', 'dev')}/{os.environ.get('COMMIT_SHA', 'dev')}",
    )

    logger = baselog.BaseLog(
        __package__,
        log_dir=config.log_dir,
        console_log_level=config.verbosity_level,
    )

    config.logcfg(logger)

    set_gc_threshold_mult(3)

    target_engine = create_engine_from_args(
        config.db_dbms,
        host=config.db_host,
        port=config.db_port,
        username=config.db_username,
        password=config.db_password,
        dbname=config.db_name,
        schema=config.db_schema,
        implicit_returning=False,
    )
    try:
        logger.info("connecting to database...")
        with target_engine.connect() as cnxn:
            with cnxn.begin():
                logger.info("starting ETL run")
                run_etl(config, cnxn)
                logger.info("run complete, beginning database commit")
    except KeyboardInterrupt:
        logger.error("KeyboardInterrupt detected, exiting")
        print("\n")
    finally:
        logger.info("exiting")


if __name__ == "__main__":
    main()
