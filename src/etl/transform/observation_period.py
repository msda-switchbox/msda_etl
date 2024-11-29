"""Observation Period transformations"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.observation_period import SQL as observation_period_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Create the ObservationPeriod tables"""
    with ctxt.transaction() as cnxn:
        logger.info("Performing OBSERVATION PERIOD transformation... ")
        result = cnxn.execute(text(observation_period_transform))
        overview = result.fetchall()
        logger.info(
            "OBSERVATION PERIOD Transformation complete! %s Observation Period(s) "
            "included",
            overview[0][0],
        )
