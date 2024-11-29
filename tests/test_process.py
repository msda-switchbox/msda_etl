import logging
import unittest
from typing import Any, Final
from unittest.mock import patch

import pandas as pd
from sqlalchemy import func, insert, select

from etl.config import ETLConf
from etl.context import ETLContext
from etl.models.modelutils import (
    CharField,
    FloatField,
    IntField,
    create_tables_sql,
    drop_tables_sql,
    make_model_base,
)
from etl.models.omopcdm54.registry import TARGET_SCHEMA
from etl.models.omopcdm54.vocabulary import Concept, ConceptAncestor, Vocabulary
from etl.process import ModelSummary, StepsDict, run_etl, run_transformations
from etl.transform.transformutils import execute_sql_transform
from etl.util.db import df_to_sql
from etl.util.exceptions import (
    ETLFatalErrorException,
    TransformationErrorException,
)
from tests.testutils import PostgresBaseTest

logger = logging.getLogger(__name__)


class ProcessUnitTests(unittest.TestCase):
    """Unit test process"""

    def setUp(self) -> None:
        super().setUp()

        self.dummy_df = pd.DataFrame(
            {
                "a": [1, 4, 6],
                "b": ["test1", "test2", "dhsjak"],
                "c": [435.34, None, 1432213.2],
            }
        )
        self.dummy_df2 = pd.DataFrame(
            {
                "x": [45, 23],
                "y": ["AB", "CD"],
            }
        )

    @patch("etl.context.ETLContext")
    def test_run_transforms(self, mock_ctxt: Any) -> None:
        """Unit test simple transform"""

        def transform1(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df,
                    table="dummy1",
                    columns=["a", "c"],
                )

        def transform2(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df2,
                    table="dummy2",
                    columns=["x"],
                )

        steps: StepsDict = {
            "transform1": transform1,
            "transform2": transform2,
        }
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        mock_cursor = mock_cnxn.connection.cursor.return_value.__enter__.return_value
        self.assertEqual(
            0,
            len(mock_cursor.execute.call_args_list),
        )
        run_transformations(
            ctxt=mock_ctxt,
            steps=steps,
        )
        # expect 1 delete per table
        self.assertEqual(
            2,
            len(mock_cursor.execute.call_args_list),
        )
        # expect 1 copy per table
        self.assertEqual(
            2,
            len(mock_cursor.copy_expert.call_args_list),
        )


TestModelBase: Any = make_model_base()


class ProcessPostgresTests(PostgresBaseTest):
    """Unit test methods with postgres connection"""

    class DummyTable(TestModelBase):
        __tablename__: Final = "dummy_table"
        __table_args__ = {"schema": "dummy"}

        a: Final = IntField(primary_key=True)
        b: Final = CharField(10)
        c: Final = FloatField()

    class DummyTableTwo(TestModelBase):
        __tablename__: Final = "dummy_table2"
        __table_args__ = {"schema": "dummy"}

        x: Final = IntField(primary_key=True)
        y: Final = CharField(5)

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(
            models=[self.DummyTable, self.DummyTableTwo], schema="dummy"
        )

        self.dummy_df = pd.DataFrame(
            {
                "a": [1, 4, 6],
                "b": ["test1", "test2", "dhsjak"],
                "c": [435.34, None, 1432213.2],
            }
        )
        self.dummy_df2 = pd.DataFrame(
            {
                "x": [45, 23],
                "y": ["AB", "CD"],
            }
        )

    def tearDown(self) -> None:
        self._drop_tables_and_schema(
            models=[self.DummyTable, self.DummyTableTwo],
            schema="dummy",
        )
        super().tearDown()

    def _get_table_count(self, model: Any, ctxt: ETLContext):
        return ctxt.cnxn.execute(select(func.count()).select_from(model))

    def test_run_transforms(self) -> None:
        """Unit test simple postgres transforms"""

        def transform1(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df,
                    table=str(self.DummyTable.__table__),
                    columns=self.dummy_df.columns,
                )

        def transform2(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df2,
                    table=str(self.DummyTableTwo.__table__),
                    columns=self.dummy_df2.columns,
                )

        steps: StepsDict = {
            "transform1": transform1,
            "transform2": transform2,
        }

        with self.engine.connect() as cnxn:
            ctxt = ETLContext(config=self.config, cnxn=cnxn, logger=logger)
            result_dummy_pre = self._get_table_count(self.DummyTable, ctxt)
            self.assertEqual(
                0,
                result_dummy_pre.first().count_1,
                "assert dummy table",
            )
            result_dummy2_pre = self._get_table_count(self.DummyTableTwo, ctxt)
            self.assertEqual(
                0,
                result_dummy2_pre.first().count_1,
                "assert dummy table 2",
            )
            run_transformations(
                ctxt=ctxt,
                steps=steps,
            )
            result_dummy_post = self._get_table_count(self.DummyTable, ctxt)
            self.assertEqual(
                3,
                result_dummy_post.first().count_1,
                "assert dummy table",
            )
            result_dummy2_post = self._get_table_count(self.DummyTableTwo, ctxt)
            self.assertEqual(
                2,
                result_dummy2_post.first().count_1,
                "assert dummy table two",
            )

    def test_run_transformations_with_cnxn_error_check_db(self):
        trans_called = {1: False, 2: False, 3: False}

        def transform1(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df,
                    table=str(self.DummyTable.__table__),
                    columns=self.dummy_df.columns,
                )
                trans_called[1] = True

        def transform2(ctxt: ETLContext):
            trans_called[2] = True
            raise TransformationErrorException(
                "Transform 2 throws a transformation error!"
            )

        def transform3(ctxt: ETLContext):
            with ctxt.transaction() as cnxn:
                df_to_sql(
                    cnxn,
                    self.dummy_df2,
                    table=str(self.DummyTableTwo.__table__),
                    columns=self.dummy_df2.columns,
                )
                trans_called[3] = True

        steps: StepsDict = {
            "transform1": transform1,
            "transform2": transform2,
            "transform3": transform3,
        }

        for _, v in trans_called.items():
            self.assertFalse(v)

        called = False
        with self.assertRaises(Exception):
            with self.engine.connect() as cnxn:
                with cnxn.begin():
                    ctxt = ETLContext(config=self.config, cnxn=cnxn, logger=logger)
                    result_dummy_pre = self._get_table_count(self.DummyTable, ctxt)
                    self.assertEqual(
                        0,
                        result_dummy_pre.first().count_1,
                        "assert dummy table",
                    )
                    result_dummy2_pre = self._get_table_count(self.DummyTableTwo, ctxt)
                    self.assertEqual(
                        0,
                        result_dummy2_pre.first().count_1,
                        "assert dummy table 2",
                    )
                    run_transformations(
                        ctxt=ctxt,
                        steps=steps,
                    )
            for k, v in trans_called.items():
                if k == 3:
                    self.assertFalse(v)
                else:
                    self.assertTrue(v)

        called = False
        with self.engine.connect() as cnxn:
            with cnxn.begin():
                ctxt = ETLContext(config=self.config, cnxn=cnxn, logger=logger)
                result_dummy_post = self._get_table_count(self.DummyTable, ctxt)
                self.assertEqual(
                    0,
                    result_dummy_post.first().count_1,
                    "assert dummy table",
                )
                result_dummy2_post = self._get_table_count(self.DummyTableTwo, ctxt)
                self.assertEqual(
                    0,
                    result_dummy2_post.first().count_1,
                    "assert dummy table two",
                )
                called = True
        self.assertTrue(called)

    def test_model_summary(self):
        with self.engine.connect() as cnxn:
            ctxt = ETLContext(config=self.config, cnxn=cnxn, logger=logger)
            result_dummy_pre = self._get_table_count(self.DummyTable, ctxt)
            self.assertEqual(
                0,
                result_dummy_pre.first().count_1,
                "assert dummy table",
            )
            ctxt.cnxn.execute(insert(self.DummyTable).values(a=4, b="djskal", c=5.6))
            ctxt.cnxn.execute(insert(self.DummyTable).values(a=5, b="ddd", c=7.5))
            result_dummy_post = self._get_table_count(self.DummyTable, ctxt)
            self.assertEqual(
                2,
                result_dummy_post.first().count_1,
                "assert dummy table",
            )
            summary = ModelSummary(self.DummyTable)
            name, count = summary.get(ctxt).split(":")
            self.assertEqual(
                name.strip(),
                self.DummyTable.__tablename__,
                "Assert summary tablename",
            )
            self.assertEqual(int(count), 2, "Assert summary count")


class RunETLPostgresTests(PostgresBaseTest):
    """Unit test to run the ETL"""

    def test_run_etl_with_empty_data(self):
        """Test running an etl with empty data"""
        config = ETLConf(
            cli_args=[
                "--datadir=tests/csv/empty_data",
                "--input-delimiter=;",
            ]
        )
        with self.engine.connect() as cnxn:
            ctxt = ETLContext(config=config, cnxn=cnxn)
            sql = [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                drop_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                create_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                f"INSERT INTO {str(Vocabulary.__table__)} SELECT 'None', 'fake_vocab', 'fake_ref', 'fake_version', 1;",
            ]
            sql_stmt = " ".join(sql).strip().replace("\n", " ")
            execute_sql_transform(ctxt, sql_stmt)
            run_etl(config=config, cnxn=cnxn)
            execute_sql_transform(
                ctxt, drop_tables_sql([Concept, ConceptAncestor, Vocabulary])
            )

    def test_run_etl_with_dummy_data(self):
        """Test running an etl with dummy data"""
        config = ETLConf(
            cli_args=[
                "--datadir=tests/csv/dummy_data",
                "--input-delimiter=;",
            ]
        )
        with self.engine.connect() as cnxn:
            ctxt = ETLContext(config=config, cnxn=cnxn)
            sql = [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                drop_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                create_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                f"INSERT INTO {str(Vocabulary.__table__)} SELECT 'None', 'fake_vocab', 'fake_ref', 'fake_version', 1;",
            ]
            sql_stmt = " ".join(sql).strip().replace("\n", " ")
            execute_sql_transform(ctxt, sql_stmt)
            with self.assertLogs() as captured:
                run_etl(config=config, cnxn=cnxn)
                # check that run was completed successfuly before process exits
                self.assertIn(
                    "ETL completed in",
                    captured.records[-1].getMessage(),
                )

            execute_sql_transform(
                ctxt, drop_tables_sql([Concept, ConceptAncestor, Vocabulary])
            )

    def test_run_etl_with_error(self):
        """Test running an etl that throws an error"""
        config = ETLConf(
            cli_args=[
                "--datadir=tests/csv/",
                "--input-delimiter=;",
            ]
        )
        called = False
        with self.engine.connect() as cnxn:
            ctxt = ETLContext(config=config, cnxn=cnxn)
            sql = [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                drop_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                create_tables_sql([Concept, ConceptAncestor, Vocabulary]),
                f"INSERT INTO {str(Vocabulary.__table__)} SELECT 'None', 'fake_vocab', 'fake_ref', 'fake_version', 1;",
            ]
            sql_stmt = " ".join(sql).strip().replace("\n", " ")
            execute_sql_transform(ctxt, sql_stmt)
            with self.assertRaises(ETLFatalErrorException):
                run_etl(config=config, cnxn=cnxn)
            called = True
        self.assertTrue(called)


__all__ = ["ProcessUnitTests", "ProcessPostgresTests", "RunETLPostgresTests"]
