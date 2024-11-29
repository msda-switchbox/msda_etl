"""Create the tables needed for the ETL"""

import logging

from sqlalchemy import text

from ..context import ETLContext
from ..models.lookupmodels import CodeLogger, ConceptLookup
from ..sql.create_lookup_tables import SQL
from ..util.db import df_to_sql

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """Create lookup tables"""
    logger.info("Creating LOOK UP tables in DB... ")
    with ctxt.transaction() as cnxn:
        cnxn.execute(text(SQL))
        concept_lookup = ctxt.lookups[ConceptLookup.__tablename__]
        df_to_sql(
            cnxn=cnxn,
            dataframe=concept_lookup,
            table=str(ConceptLookup.__table__),
            columns=concept_lookup.columns,
        )
        code_logger = ctxt.lookups[CodeLogger.__tablename__]
        df_to_sql(
            cnxn=cnxn,
            dataframe=code_logger,
            table=str(CodeLogger.__table__),
            columns=code_logger.columns,
        )
        logger.info("LOOK UP tables created successfully!")
