"""Measurement transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import Measurement, Observation, Person
from etl.models.source import DiseaseHistory, DiseaseStatus
from etl.sql.measurement_transform import SQL_ENTRIES
from etl.transform.measurement import transform as measurement_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class MeasurementTransformFunctionalTest(TransformBaseTest):
    """Functional test for measurement transform"""

    SOURCE = {
        DiseaseHistory: "measurement/input_disease_history.csv",
        DiseaseStatus: "measurement/input_disease_status.csv",
    }
    TARGET = {
        Measurement: "measurement/output_measurement.csv",
        ETLLogger: "measurement/logger_measurement.csv",
    }
    OTHER = {
        Person: "measurement/input_person.csv",
        Observation: "measurement/input_observation.csv",
    }

    def test_transformation(self):
        super()._test_transformation(
            measurement_transform,
            sort_by=["person_id", "measurement_source_value"],
        )


class MeasurementTransformUnitTest(unittest.TestCase):
    """Unit test for measurement transform"""

    @patch("etl.transform.measurement.ETLContext")
    def test_meas_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        measurement_transform(mock_ctxt)

        expected_sql_queries = [query for query in SQL_ENTRIES.values()]
        expected_sql_queries.append(cast_date_format())

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(set(expected_sql_queries).issubset(set(actual_str_queries)))
