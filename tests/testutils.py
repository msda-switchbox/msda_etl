"""A module for integration testing with postgres"""

import os
import unittest
from typing import Any, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from etl.config import ETLConf
from etl.loader import Loader
from etl.models.modelutils import ModelBase, create_tables_sql
from etl.util.db import create_engine_from_args, session_context


def make_empty_df_with_cols(model: ModelBase) -> pd.DataFrame:  # type: ignore
    """A simple function that will create an empty dataframe with the
    required columns for the given model"""
    return pd.DataFrame(columns=[c.key for c in model.__table__.columns.values()])


class EmptyLoader(Loader):
    """A fake source loader for testing with empty source data"""

    def load(self) -> Loader:
        self.file_data = {}

        for model in self.models:
            self.file_data[model.__tablename__] = make_empty_df_with_cols(model)
        return self


class PostgresBaseTest(unittest.TestCase):
    """Base class for testing with postgres"""

    # pylint: disable=invalid-envvar-default
    def setUp(self):
        super().setUp()
        self.engine = create_engine_from_args(
            dbms="postgres",
            host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
            dbname=os.getenv("TEST_POSTGRES_DBNAME", "postgres"),
            username=os.getenv("TEST_POSTGRES_USER", "postgres"),
            password=os.getenv("TEST_POSTGRES_PASSWORD", "postgres"),
            port=os.getenv("TEST_POSTGRES_PORT", 5432),
            schema=os.getenv("TEST_POSTGRES_SCHEMA", "omopcdm"),
        )

        self.config = ETLConf(cli_args=[])

    def _drop_tables_and_schema(
        self,
        models: List[ModelBase],
        schema: Optional[str] = None,
    ):
        with session_context(Session(self.engine)) as session:
            schema_str = ""
            if schema is not None:
                schema_str = f"{schema}."

            with session.connection().connection.cursor() as cursor:
                for model in models:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {schema_str}{model.__tablename__} CASCADE;"
                    )

                if schema is not None:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def _create_tables_and_schema(
        self, models: List[Any], schema: Optional[str] = None
    ):
        with session_context(Session(self.engine)) as session:
            with session.connection().connection.cursor() as cursor:
                if schema is not None:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                if models:
                    sql = create_tables_sql(models)
                    cursor.execute(sql)
