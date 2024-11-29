"""Load files into memory"""

import logging
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .util.exceptions import ETLFatalErrorException

logger = logging.getLogger(__name__)


class Loader:
    """An empty loader to load in csv files"""

    def __init__(self, models: Dict) -> None:
        """Does nothing"""
        self.models = models.values()
        self.tables = [m.__tablename__ for m in self.models]
        self.file_data: Dict[str, Any] = {}

    def load(self) -> "Loader":
        """Does nothing"""
        return self

    # pylint: disable=attribute-defined-outside-init
    def reset(self) -> None:
        """Reset the loaded data"""
        pass

    def _update(self, key: str, value: Any) -> None:
        self.file_data[key] = value

    def get(self, key: str) -> Any:
        """Get a loaded entry by key"""
        return self.file_data.get(key)

    @property
    def data(self) -> Dict[str, Any]:
        return self.file_data


EmptyLoader = Loader


class CSVFileLoader(Loader):
    """A loader for CSV inputs"""

    def __init__(
        self,
        directory: Path | Traversable,
        models: Dict,
        delimiter: str = ",",
        extension: str = ".csv",
    ) -> None:
        super().__init__(models)
        self.directory = directory
        self.delimiter = delimiter
        self.extension = extension
        self.encoding = "utf-8"

    def load(self) -> Loader:
        """Load from source csv files"""
        self.reset()
        for model in self.models:
            tablename = model.__tablename__
            input_file = self.directory.joinpath(f"{tablename}{self.extension}")
            if not input_file.is_file():
                logger.error(
                    "The following table is expected but is missing: %s, please check input data",
                    tablename,
                )
                raise ETLFatalErrorException(
                    f"Table: {tablename} missing. Expected file name: {input_file}."
                )
            logger.info(
                "Loading: %s, into memory",
                tablename,
            )
            logger.debug("Using encoding: %s", self.encoding)
            self._update(
                tablename,
                pd.read_csv(
                    input_file,
                    sep=self.delimiter,
                    encoding=self.encoding,
                    low_memory=False,
                ),
            )

        return self
