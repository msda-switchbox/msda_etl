"""Condition occurrence transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import (
    ConditionOccurrence,
    Person,
    VisitOccurrence,
)
from etl.models.source import DiseaseHistory, Relapses
from etl.sql.condition_transform import SQL as condition_occurrence_sql_stmt
from etl.transform.condition import transform as condition_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class ConditionOccurrenceTransformFunctionalTest(TransformBaseTest):
    """Functional test class for condition_occurrence transform"""

    SOURCE = {
        DiseaseHistory: "condition_occurrence/input_disease_history.csv",
        Relapses: "condition_occurrence/input_relapses.csv",
    }
    TARGET = {
        ConditionOccurrence: "condition_occurrence/output_condition_occurrence.csv",
        ETLLogger: "condition_occurrence/logger_condition_occurrence.csv",
    }
    OTHER = {
        Person: "condition_occurrence/input_person.csv",
        VisitOccurrence: "condition_occurrence/input_visit_occurrence.csv",
    }

    def test_transformation(self):
        """Test for condition_occurrence transform"""
        super()._test_transformation(
            condition_transform,
            sort_by=[
                "person_id",
                "condition_start_date",
                "condition_concept_id",
            ],
        )


class ConditionOccurrenceTransformUnitTest(unittest.TestCase):
    """Unit test class for condition_occurrence transform"""

    @patch("etl.transform.condition.ETLContext")
    def test_condition_occurrence_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        condition_transform(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(
            set([condition_occurrence_sql_stmt, cast_date_format()]).issubset(
                set(actual_str_queries)
            ),
        )
