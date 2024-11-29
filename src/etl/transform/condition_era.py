"""Condition Era transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.condition_era_transform import SQL as condition_era_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Condition Era transforms. It includes Condition Era"""
    with ctxt.transaction() as cnxn:
        logger.info("Performing CONDITION ERA transformation...")
        result = cnxn.execute(text(condition_era_transform))
        overview = result.fetchall()
        logger.info(
            "CONDITION ERA Transformation Complete! %s Condition Era(s) included",
            overview[0][0],
        )
