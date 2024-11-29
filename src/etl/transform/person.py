"""Person transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.person_transform import SQL as person_transform
from ..transform.etl_logging import PATIENT_LOGGER_DICT, log_errors
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Person transform"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing PERSON transformation...")
        result = cnxn.execute(text(person_transform))
        overview = result.fetchall()
        logger.info(
            "PERSON Transformation Complete! %s people included",
            overview[0][0],
        )
    log_errors(ctxt, PATIENT_LOGGER_DICT)
