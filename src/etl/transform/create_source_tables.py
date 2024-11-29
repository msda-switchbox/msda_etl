"""Create the tables with source data"""

import logging
from typing import Final, List

from ..context import ETLContext
from ..models.source import (
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Patient,
    Relapses,
    Symptom,
)
from ..sql.create_source_tables import SQL
from ..transform.transformutils import execute_sql_transform
from ..util.db import df_to_sql

logger = logging.getLogger(__name__)

MODELS: Final[List] = [
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Patient,
    Relapses,
    Symptom,
]


def transform(ctxt: ETLContext) -> None:
    """Create source tables"""
    execute_sql_transform(ctxt, SQL)
    with ctxt.transaction() as cnxn:
        for model in MODELS:
            logger.info("Creating %s table in DB... ", model.__tablename__)
            source_table = ctxt.sources[model.__tablename__]
            df_to_sql(
                cnxn=cnxn,
                dataframe=source_table,
                table=str(model.__table__),
                columns=source_table.columns,
            )
            logger.info("%s table created successfully ", model.__tablename__)
    logger.info("All SOURCE tables created successfully!")
