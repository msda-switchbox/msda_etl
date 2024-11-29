"""context classes used to pass data around the ETL"""

import logging
from contextlib import contextmanager
from typing import Dict, Final, Optional

import pandas as pd
from sqlalchemy.engine import Connection

from .config import ETLConf


class ETLContext:
    """context class passed to transforms and other operations"""

    config: ETLConf
    lookups: Dict[str, pd.DataFrame] = {}
    sources: Dict[str, pd.DataFrame] = {}
    cnxn: Connection
    logger: logging.Logger

    def __init__(
        self,
        config: ETLConf,
        cnxn: Optional[Connection] = None,
        lookups: Optional[Dict[str, pd.DataFrame]] = None,
        sources: Optional[Dict[str, pd.DataFrame]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        if cnxn:
            self.cnxn = cnxn
        if lookups:
            self.lookups = lookups
        if sources:
            self.sources = sources
        if logger:
            self.logger = logger

    @contextmanager
    def transaction(self):
        """start a nested transaction and yield the connection object"""

        try:
            if not self.cnxn.in_transaction():
                with self.cnxn.begin():
                    yield self.cnxn
            else:
                yield self.cnxn
        except Exception as error:
            raise error from None

    def log_big(
        self,
        *args,
        sep_before: bool = True,
        sep_after: bool = True,
        **kwargs,
    ):
        """log a message in the process logger with textual emphasis"""
        sep: Final = (
            "-----------------------------------------------------------------"
            "-------------------------------------"
        )
        if sep_before:
            self.logger.info(sep)
        self.logger.info(*args, **kwargs)
        if sep_after:
            self.logger.info(sep)
