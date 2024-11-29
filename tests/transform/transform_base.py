"""Base class for transformation test"""

import os
import unittest
from typing import Callable, Dict, Final, Optional

import pandas as pd
from sqlalchemy import inspect, select

from etl.config import ETLConf
from etl.context import ETLContext
from etl.models.lookupmodels import CodeLogger, ConceptLookup
from etl.models.modelutils import create_tables_sql, drop_tables_sql
from etl.models.omopcdm54.registry import TARGET_SCHEMA
from etl.models.source import SOURCE_SCHEMA
from etl.transform.preprocessing import transform as preprocess_transform
from etl.transform.transformutils import execute_sql_transform
from etl.util.db import create_engine_from_args


class TransformBaseTest(unittest.TestCase):
    """Base class for testing transformations"""

    # Mapping of source tables to csv files
    SOURCE: Dict = {}

    # Mapping of target tables to csv files
    TARGET: Dict = {}

    # Mapping of additional tables to csv files
    OTHER: Dict = {}

    # Mapping to lookup files
    LOOKUPS: Final[Dict] = {
        CodeLogger: "src/etl/csv/code_logger.csv",
        ConceptLookup: "src/etl/csv/concept_lookup.csv",
    }

    @classmethod
    def setUpClass(cls) -> None:
        cls.config = ETLConf(cli_args=[])
        cls.engine = create_engine_from_args(
            dbms="postgres",
            host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
            dbname=os.getenv("TEST_POSTGRES_DBNAME", "postgres"),
            username=os.getenv("TEST_POSTGRES_USER", "postgres"),
            password=os.getenv("TEST_POSTGRES_PASSWORD", "postgres"),
            port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
            schema=os.getenv("TEST_POSTGRES_SCHEMA", "omopcdm"),
        )

        with cls.engine.connect() as cnxn:
            with cnxn.begin():
                ctxt = ETLContext(cls.config, cnxn=cnxn)
                create_schemas_sql = f"""
                        CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA};
                        CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};
                        """
                sql_drop_tables = drop_tables_sql([CodeLogger, ConceptLookup])
                sql_create_tables = create_tables_sql([CodeLogger, ConceptLookup])

                execute_sql_transform(ctxt, create_schemas_sql)
                execute_sql_transform(ctxt, sql_drop_tables)
                execute_sql_transform(ctxt, sql_create_tables)

            for table, csv_file in cls.LOOKUPS.items():
                table_df = pd.read_csv(csv_file, delimiter=";")

                tablename = table.__tablename__

                table_df.to_sql(
                    tablename,
                    cls.engine,
                    index=False,
                    schema=table.metadata.schema,
                    if_exists="replace",
                )

    def setUp(self) -> None:
        sql_1 = drop_tables_sql(
            list(self.SOURCE.keys())
            + list(self.TARGET.keys())
            + list(self.OTHER.keys())
        )
        sql_2 = create_tables_sql(
            list(self.SOURCE.keys())
            + list(self.TARGET.keys())
            + list(self.OTHER.keys())
        )

        with self.engine.connect() as cnxn:
            with cnxn.begin():
                ctxt = ETLContext(self.config, cnxn)
                execute_sql_transform(ctxt, sql_1)
                execute_sql_transform(ctxt, sql_2)

                with ctxt.transaction() as session:
                    for table, _ in self.TARGET.items():
                        session.execute(
                            f"""
                                CREATE TABLE {table.__table__}_expected AS
                                TABLE {table.__table__};
                            """
                        )

        self._load_tables(self.SOURCE, table_type="source")
        self._load_tables(self.TARGET, table_type="target")
        self._load_tables(self.OTHER)

    def tearDown(self) -> None:
        expected_tables = ""
        for table, _ in self.TARGET.items():
            expected_tables += f"DROP TABLE IF EXISTS {table.__table__}_expected;"

        sql = [
            drop_tables_sql(
                list(self.SOURCE.keys())
                + list(self.TARGET.keys())
                + list(self.OTHER.keys())
            ),
            expected_tables,
        ]
        sql_stmt = " ".join(sql).strip().replace("\n", " ")

        if sql_stmt.strip().replace(";", ""):
            with self.engine.connect() as cnxn:
                with cnxn.begin():
                    ctxt = ETLContext(self.config, cnxn)
                    execute_sql_transform(ctxt, sql_stmt)

    @classmethod
    def tearDownClass(cls) -> None:
        return None

    def _load_tables(self, tables: Dict, table_type: str = "") -> None:
        for table, csv_file in tables.items():
            if not csv_file:
                continue

            table_df = pd.read_csv(
                os.path.join("tests/transform/csv", csv_file), delimiter=";"
            )

            tablename = table.__tablename__
            tablename += "_expected" if table_type == "target" else ""
            dtype = (
                {column.name: column.type for column in table.__table__.columns}
                if table_type == "target"
                else {}
            )

            if table_type == "source":
                with self.engine.connect() as cnxn:
                    ctxt = ETLContext(self.config, cnxn, sources={tablename: table_df})
                    preprocess_transform(ctxt)

            table_df.to_sql(
                tablename,
                self.engine,
                index=False,
                schema=table.metadata.schema,
                dtype=dtype,
                if_exists="replace",
            )

    def _test_transformation(self, func: Callable, sort_by: Optional[list] = None):
        with self.engine.connect() as cnxn:
            with cnxn.begin():
                ctxt = ETLContext(self.config, cnxn)
                func(ctxt)

        for table, _ in self.TARGET.items():
            index_col = inspect(table).primary_key[0].key

            actual_df = pd.read_sql(
                select(table),
                self.engine,
                index_col=index_col,
            )
            expected_df = pd.read_sql(
                f"SELECT * FROM {table.__table__}_expected;",
                self.engine,
                index_col=index_col,
            )

            if table.__tablename__ == "etl_logger":
                actual_df.sort_values(
                    by=[
                        "patient_id",
                        "omop_table_name",
                        "source_table_name",
                        "error_code_id",
                    ],
                    inplace=True,
                    ignore_index=True,
                )
                expected_df.sort_values(
                    by=[
                        "patient_id",
                        "omop_table_name",
                        "source_table_name",
                        "error_code_id",
                    ],
                    inplace=True,
                    ignore_index=True,
                )
            else:
                if sort_by:
                    actual_df.sort_values(by=sort_by, inplace=True, ignore_index=True)
                    expected_df.sort_values(by=sort_by, inplace=True, ignore_index=True)

            pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
