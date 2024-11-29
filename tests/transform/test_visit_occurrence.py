"""Visit occurrence transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import Person, VisitOccurrence
from etl.models.source import (
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Relapses,
)
from etl.sql.visit_occurrence_transform import SQL as visit_occurrence_sql_stmt
from etl.transform.visit_occurrence import (
    transform as visit_occurrence_transform,
)
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class VisitOccurrenceTransformFunctionalTest(TransformBaseTest):
    """Functional test class for visit_occurrence transform"""

    SOURCE = {
        Comorbidities: "visit_occurrence/input_comorbidities.csv",
        DiseaseHistory: "visit_occurrence/input_disease_history.csv",
        DiseaseStatus: "visit_occurrence/input_disease_status.csv",
        Dmt: "visit_occurrence/input_dmt.csv",
        Mri: "visit_occurrence/input_mri.csv",
        Npt: "visit_occurrence/input_npt.csv",
        Relapses: "visit_occurrence/input_relapses.csv",
    }
    TARGET = {
        VisitOccurrence: "visit_occurrence/output_visit_occurrence.csv",
        ETLLogger: "visit_occurrence/logger_visit_occurrence.csv",
    }
    OTHER = {
        Person: "visit_occurrence/input_person.csv",
    }

    def test_transformation(self):
        """Test for visit_occurrence transform"""
        super()._test_transformation(
            visit_occurrence_transform,
            sort_by=["person_id", "visit_start_date"],
        )


class VisitOccurrenceTransformUnitTest(unittest.TestCase):
    """Unit test class for visit_occurrence transform"""

    @patch("etl.transform.visit_occurrence.ETLContext")
    def test_visit_occ_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        visit_occurrence_transform(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(
            set([visit_occurrence_sql_stmt, cast_date_format()]).issubset(
                set(actual_str_queries)
            ),
        )
