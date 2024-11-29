"""Logs the environment variable of the ETL"""

import logging
import os

from ..models.omopcdm54.registry import TARGET_SCHEMA

logger = logging.getLogger(__name__)


def log(reload_vocab: bool = False) -> None:
    logger.info("Logging environment variables..")
    logger.info("TARGET_SCHEMA: %s", TARGET_SCHEMA)
    logger.info("RELOAD_VOCAB: %s", reload_vocab)
    logger.info("CDM_SOURCE_NAME: %s", os.environ.get("CDM_SOURCE_NAME"))
    logger.info(
        "CDM_SOURCE_ABBREVIATION: %s", os.environ.get("CDM_SOURCE_ABBREVIATION")
    )
    logger.info("CDM_HOLDER: %s", os.environ.get("CDM_HOLDER"))
    logger.info("SOURCE_DESCRIPTION: %s", os.environ.get("SOURCE_DESCRIPTION"))
    logger.info(
        "SOURCE_DOCUMENTATION_REFERENCE: %s",
        os.environ.get("SOURCE_DOCUMENTATION_REFERENCE"),
    )
    logger.info("SOURCE_RELEASE_DATE: %s", os.environ.get("SOURCE_RELEASE_DATE"))
    logger.info("Environment variables logged!")
