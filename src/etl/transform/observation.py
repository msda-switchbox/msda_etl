"""Observation transformation"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..models.omopcdm54.clinical import Observation
from ..sql.observation_transform import SQL_ENTRIES
from ..transform.etl_logging import (
    OBSERVATION_LOGGER_DICT,
    log_default_date,
    log_invalid_mri_records,
)
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Observation transformation"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing OBSERVATION transformation...")
        current_total = 0
        for source, query in SQL_ENTRIES.items():
            result = cnxn.execute(text(query))
            overview = result.fetchall()
            logger.info(
                "%s Transformation Complete! %s Observation(s) included",
                source,
                (
                    overview[0][0]
                    if source == "OBSERVATION"
                    else overview[0][0] - current_total
                ),
            )
            current_total = overview[0][0]
        log_default_date(ctxt, OBSERVATION_LOGGER_DICT)
        log_invalid_mri_records(ctxt, Observation)
