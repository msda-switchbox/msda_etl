"""Location transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.location_transform import SQL as location_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Location transform"""
    logger.info("Performing LOCATION transformation...")
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(location_transform))
        overview = result.fetchall()
        logger.info(
            "LOCATION Transformation complete! %s Location(s) included",
            overview[0][0],
        )
