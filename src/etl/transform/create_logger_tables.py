"""Create the tables needed for the ETL"""

import logging

from ..context import ETLContext
from ..sql.create_logger_tables import SQL
from ..transform.transformutils import execute_sql_transform

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Create the ETL LOGGER tables"""
    logger.info("Creating ETL LOGGER table in DB... ")
    execute_sql_transform(ctxt, SQL)
    logger.info("ETL LOGGER table created successfully!")
