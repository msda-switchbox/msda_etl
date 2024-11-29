"""Drug exposure transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import DrugExposure, Person, VisitOccurrence
from etl.models.source import Dmt
from etl.sql.drug_exposure_transform import SQL as drug_exposure_sql_stmt
from etl.transform.drug_exposure import transform as drug_exposure_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class DrugExposureTransformFunctionalTest(TransformBaseTest):
    """Functional test class for drug_exposure transform"""

    SOURCE = {
        Dmt: "drug_exposure/input_dmt.csv",
    }
    TARGET = {
        DrugExposure: "drug_exposure/output_drug_exposure.csv",
        ETLLogger: "drug_exposure/logger_drug_exposure.csv",
    }
    OTHER = {
        Person: "drug_exposure/input_person.csv",
        VisitOccurrence: "drug_exposure/input_visit_occurrence.csv",
    }

    def test_transformation(self):
        """Test for drug_exposure transform"""
        super()._test_transformation(
            drug_exposure_transform,
            sort_by=["person_id", "drug_exposure_start_date"],
        )


class DrugExposureTransformUnitTest(unittest.TestCase):
    """Unit test class for drug_exposure transform"""

    @patch("etl.transform.drug_exposure.ETLContext")
    def test_drug_exp_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        drug_exposure_transform(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(
            set([drug_exposure_sql_stmt, cast_date_format()]).issubset(
                set(actual_str_queries)
            ),
        )
