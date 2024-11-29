"""Condition Occurrence transformation"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.condition_transform import SQL as condition_transform
from ..transform.etl_logging import (
    CONDITION_OCCURRENCE_LOGGER_DICT,
    log_default_date,
)
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Condition Occurrence transformation"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing CONDITION OCCURRENCE transformation...")
        result = cnxn.execute(text(condition_transform))
        overview = result.fetchall()
        logger.info(
            "CONDITION OCCURRENCE Transformation Complete! %s records included",
            overview[0][0],
        )
        log_default_date(ctxt, CONDITION_OCCURRENCE_LOGGER_DICT)
