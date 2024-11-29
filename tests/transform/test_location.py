"""Location transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.omopcdm54.health_systems import Location
from etl.models.source import Patient
from etl.sql.location_transform import SQL as location_sql_stmt
from etl.transform.location import transform as location_transform
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class LocationTransformFunctionalTest(TransformBaseTest):
    """Functional test class for location transform"""

    SOURCE = {
        Patient: "location/input_patient.csv",
    }
    TARGET = {
        Location: "location/output_location.csv",
    }

    def test_transformation(self):
        """Test for location transform"""
        super()._test_transformation(
            location_transform,
            sort_by=["location_source_value"],
        )


class LocationTransformUnitTest(unittest.TestCase):
    """Unit test class for location transform"""

    @patch("etl.transform.location.ETLContext")
    def test_location_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        location_transform(mock_ctxt)

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertIn(location_sql_stmt, set(actual_str_queries))


__all__ = ["LocationTransformFunctionalTest", "LocationTransformUnitTest"]
