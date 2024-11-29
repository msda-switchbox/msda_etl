"""Person transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import Person
from etl.models.omopcdm54.health_systems import Location
from etl.models.source import DiseaseHistory, Patient
from etl.sql.person_transform import SQL as person_sql_stmt
from etl.transform.person import transform as person_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class PersonTransformFunctionalTest(TransformBaseTest):
    """Functional test class for person transform"""

    SOURCE = {
        DiseaseHistory: "person/input_disease_history.csv",
        Patient: "person/input_patient.csv",
    }
    TARGET = {
        Person: "person/output_person.csv",
        ETLLogger: "person/logger_person.csv",
    }
    OTHER = {
        Location: "person/input_location.csv",
    }

    def test_transformation(self):
        """Test for person transform"""
        super()._test_transformation(
            person_transform,
            sort_by=["person_id"],
        )


class PersonTransformUnitTest(unittest.TestCase):
    """Unit test class for person transform"""

    @patch("etl.transform.person.ETLContext")
    def test_person_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        person_transform(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(
            set([person_sql_stmt, cast_date_format()]).issubset(
                set(actual_str_queries)
            ),
        )
