"""Create the tables needed for the ETL"""

import logging

from ..context import ETLContext
from ..sql.create_omopcdm_tables import SQL
from ..transform.transformutils import execute_sql_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Create the OMOP CDM tables"""
    logger.info("Creating OMOP CDM tables in DB... ")
    execute_sql_transform(ctxt, SQL)
    logger.info("OMOP CDM tables created successfully!")
