"""CDM Source transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.cdm_source_transform import get_transform_sql

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """CDM Source transform"""
    logger.info("Performing CDM SOURCE transformation...")
    logger.info("Gathering vocab info from target database")
    with ctxt.transaction() as cnxn:
        qry = get_transform_sql(ctxt.config)
        result = cnxn.execute(text(qry))
        overview = result.fetchall()
        logger.info(
            "VOCABULARY_VERSION: %s",
            overview[0][1],
        )
        logger.info(
            "CDM SOURCE Transformation Complete! %s CDM Source included",
            overview[0][0],
        )
