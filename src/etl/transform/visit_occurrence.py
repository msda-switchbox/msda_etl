"""Visit_occurrence transformation"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.visit_occurrence_transform import (
    MODELS,
    SQL as visit_occurrence_transform,
)
from ..transform.etl_logging import log_default_visit_date
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Visit_occurrence transformation"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing VISIT OCCURRENCE transformation...")
        result = cnxn.execute(text(visit_occurrence_transform))
        overview = result.fetchall()
        logger.info(
            "VISIT OCCURRENCE Transformation Complete! %s records included",
            overview[0][0],
        )
        log_default_visit_date(ctxt, MODELS)
