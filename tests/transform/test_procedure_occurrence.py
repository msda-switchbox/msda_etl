"""Procedure occurrence transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import (
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)
from etl.models.source import Dmt, Mri
from etl.sql.procedure_transform import SQL_ENTRIES
from etl.transform.procedure import transform as procedure_occurrence_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class ProcedureOccurrenceTransformFunctionalTest(TransformBaseTest):
    """Functional test class for procedure_occurrence transform"""

    SOURCE = {
        Dmt: "procedure_occurrence/input_dmt.csv",
        Mri: "procedure_occurrence/input_mri.csv",
    }
    TARGET = {
        ProcedureOccurrence: "procedure_occurrence/output_procedure_occurrence.csv",
        ETLLogger: "procedure_occurrence/logger_procedure_occurrence.csv",
    }
    OTHER = {
        Person: "procedure_occurrence/input_person.csv",
        VisitOccurrence: "procedure_occurrence/input_visit_occurrence.csv",
    }

    def test_transformation(self):
        """Test for procedure_occurrence transform"""
        super()._test_transformation(
            procedure_occurrence_transform,
            sort_by=["person_id", "procedure_source_value"],
        )


class ProcedureOccurrenceTransformUnitTest(unittest.TestCase):
    """Functional test class for procedure_occurrence transform"""

    @patch("etl.transform.procedure.ETLContext")
    def test_proc_occ_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        procedure_occurrence_transform(mock_ctxt)

        expected_sql_queries = [query for query in SQL_ENTRIES.values()]
        expected_sql_queries.append(cast_date_format())

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(set(expected_sql_queries).issubset(set(actual_str_queries)))
