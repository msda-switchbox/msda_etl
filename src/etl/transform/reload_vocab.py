"""The vocabulary reload transform"""

import logging

from ..context import ETLContext
from ..transform.transformutils import execute_sql_file

logger = logging.getLogger(__name__)


def transform(ctxt: ETLContext) -> None:
    """The final load (copy from temp tables to production)"""
    logger.info("".join(["-"] * 93))
    logger.info("Launching Vocab Reloader")
    logger.info("".join(["-"] * 93))
    if ctxt.config.reload_vocab:
        logger.info(
            "Reloading vocabulary files, setting all indexes, " "and constraints..."
        )
        execute_sql_file(ctxt, filename="reload_vocab.sql")
        logger.info("Vocabulary Reload Step Complete!")
    else:
        logger.info("Skipping vocabulary reload!")
