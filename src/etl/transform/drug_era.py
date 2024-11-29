"""Drug Era transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.drug_era_transform import SQL as drug_era_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Drug Era transforms. It includes Drug Era"""
    with ctxt.transaction() as cnxn:
        logger.info("Performing DRUG ERA transformation...")
        result = cnxn.execute(text(drug_era_transform))
        overview = result.fetchall()
        logger.info(
            "DRUG ERA Transformation Complete! %s Drug Era(s) included",
            overview[0][0],
        )
