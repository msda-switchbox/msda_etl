"Test SQL calls of table creations"

import os
import unittest
from unittest.mock import patch

from etl.models.lookupmodels import LOOKUP_MODELS
from etl.models.source import SOURCE_MODELS
from etl.sql.create_logger_tables import SQL as logger_sql_stmt
from etl.sql.create_lookup_tables import SQL as lookup_sql_stmt
from etl.sql.create_omopcdm_tables import SQL as omopcdm_sql_stmt
from etl.sql.create_source_tables import SQL as source_sql_stmt
from etl.transform.create_logger_tables import transform as create_logger_tables
from etl.transform.create_lookup_tables import transform as create_lookup_tables
from etl.transform.create_omopcdm_tables import (
    transform as create_omopcdm_tables,
)
from etl.transform.create_source_tables import transform as create_source_tables
from etl.transform.reload_vocab import transform as reload_vocab
from tests.testutils import EmptyLoader
from tests.transform.utils import get_sql_str_list


class TestCreateOmopTables(unittest.TestCase):
    """Test class: create omop tables"""

    @patch("etl.transform.transformutils.ETLContext")
    def test_create_omop_tables(self, mock_ctxt):
        """Test create omop tables ddl"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        create_omopcdm_tables(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)
        self.assertTrue(
            set([omopcdm_sql_stmt]).issubset(set(actual_str_queries)),
        )


class TestCreateLoggerTable(unittest.TestCase):
    """Test class: create logger table"""

    @patch("etl.transform.transformutils.ETLContext")
    def test_create_logger_table(self, mock_ctxt):
        """Test create logger table ddl"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        create_logger_tables(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)
        self.assertTrue(
            set([logger_sql_stmt]).issubset(set(actual_str_queries)),
        )


class TestCreateLookupTable(unittest.TestCase):
    """Test class: create lookup tables"""

    @patch("etl.transform.transformutils.ETLContext")
    def test_create_lookup_table(self, mock_ctxt):
        """Test create lookup table ddl"""
        lookup_loader = EmptyLoader(LOOKUP_MODELS)
        lookup_loader.load()
        mock_ctxt.lookups = lookup_loader.data
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        create_lookup_tables(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)
        self.assertTrue(
            set([lookup_sql_stmt]).issubset(set(actual_str_queries)),
        )


class TestReloadVocabTables(unittest.TestCase):
    """Test class: reload omop vocab tables"""

    @patch("etl.transform.transformutils.ETLContext")
    def test_reload_vocab_tables(self, mock_ctxt):
        """Test reload vocab tables ddl"""

        def read_sql_file(filename: str = "reload_vocab.sql", encoding="utf-8"):
            parent_dir = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            with open(
                f"{parent_dir}/src/etl/sql/{filename}", "r", encoding=encoding
            ) as fsql:
                sql_statement = fsql.read()
            return sql_statement

        sql_statement = read_sql_file()
        mock_ctxt.config.reload_vocab = True
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        reload_vocab(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)
        self.assertTrue(
            set([sql_statement]).issubset(set(actual_str_queries)),
        )


class TestCreateSourceTables(unittest.TestCase):
    """Test class: create source tables"""

    @patch("etl.transform.transformutils.ETLContext")
    def test_create_source_tables(self, mock_ctxt):
        """Test create source tables ddl"""
        source_loader = EmptyLoader(SOURCE_MODELS)
        source_loader.load()
        mock_ctxt.sources = source_loader.data
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        create_source_tables(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)
        self.assertTrue(
            set([source_sql_stmt]).issubset(set(actual_str_queries)),
        )


__all__ = [
    "TestCreateOmopTables",
    "TestCreateLoggerTable",
    "TestCreateLookupTable",
    "TestReloadVocabTables",
    "TestCreateSourceTables",
]
