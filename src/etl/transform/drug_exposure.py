"""Drug Exposure transform"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..sql.drug_exposure_transform import SQL as drug_exposure_transform
from ..transform.etl_logging import DRUG_EXPOSURE_LOGGER_DICT, log_default_date
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Drug Exposure transforms"""
    with ctxt.transaction() as cnxn:
        result = cnxn.execute(text(cast_date_format()))
        logger.info("Performing DRUG EXPOSURE transformation...")
        result = cnxn.execute(text(drug_exposure_transform))
        overview = result.fetchall()
        logger.info(
            "DRUG EXPOSURE Transformation Complete! %s Drug Exposure(s) included",
            overview[0][0],
        )
        log_default_date(ctxt, DRUG_EXPOSURE_LOGGER_DICT)
