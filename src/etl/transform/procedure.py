"""Procedure Ocurrence transformation"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..models.omopcdm54.clinical import ProcedureOccurrence
from ..sql.procedure_transform import SQL_ENTRIES
from ..transform.etl_logging import (
    PROCEDURE_LOGGER_DICT,
    log_default_date,
    log_invalid_mri_records,
)
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Procedure Occurrence transformation"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing PROCEDURE OCCURRENCE transformation...")
        current_total = 0
        for source, query in SQL_ENTRIES.items():
            result = cnxn.execute(text(query))
            overview = result.fetchall()
            logger.info(
                "%s Transformation Complete! %s people included",
                source,
                (
                    overview[0][0]
                    if source == "PROCEDURE OCCURRENCE"
                    else overview[0][0] - current_total
                ),
            )
            current_total = overview[0][0]
        log_default_date(ctxt, PROCEDURE_LOGGER_DICT)
        log_invalid_mri_records(ctxt, ProcedureOccurrence)
