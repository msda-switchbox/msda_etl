"""Measurement transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.measurement_transform import SQL_ENTRIES
from ..transform.etl_logging import MEASUREMENT_LOGGER_DICT, log_default_date
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Measurement transform"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing MEASUREMENT transformation...")
        current_total = 0
        for source, query in SQL_ENTRIES.items():
            result = cnxn.execute(text(query))
            overview = result.fetchall()
            logger.info(
                "%s Transformation Complete! %s Measurement(s) included",
                source,
                (
                    overview[0][0]
                    if source == "MEASUREMENT"
                    else overview[0][0] - current_total
                ),
            )
            current_total = overview[0][0]
        log_default_date(ctxt, MEASUREMENT_LOGGER_DICT)
