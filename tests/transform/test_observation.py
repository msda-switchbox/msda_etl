"""Observation transformation tests"""

import unittest
from unittest.mock import patch

from etl.models.etl_logger import ETLLogger
from etl.models.omopcdm54.clinical import Observation, Person, VisitOccurrence
from etl.models.source import (
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Patient,
    Relapses,
    Symptom,
)
from etl.sql.observation_transform import SQL_ENTRIES
from etl.transform.observation import transform as observation_transform
from etl.util.sql import cast_date_format
from tests.transform.transform_base import TransformBaseTest
from tests.transform.utils import get_sql_str_list


class ObservationTransformFunctionalTest(TransformBaseTest):
    """Functional test class for observation transform"""

    SOURCE = {
        Comorbidities: "observation/input_comorbidities.csv",
        DiseaseHistory: "observation/input_disease_history.csv",
        DiseaseStatus: "observation/input_disease_status.csv",
        Dmt: "observation/input_dmt.csv",
        Mri: "observation/input_mri.csv",
        Npt: "observation/input_npt.csv",
        Patient: "observation/input_patient.csv",
        Relapses: "observation/input_relapses.csv",
        Symptom: "observation/input_symptom.csv",
    }
    TARGET = {
        Observation: "observation/output_observation.csv",
        ETLLogger: "observation/logger_observation.csv",
    }
    OTHER = {
        Person: "observation/input_person.csv",
        VisitOccurrence: "observation/input_visit_occurrence.csv",
    }

    def test_transformation(self):
        """Test for observation transform"""
        super()._test_transformation(
            observation_transform,
            sort_by=[
                "person_id",
                "observation_concept_id",
                "observation_source_value",
            ],
        )


class ObservationTransformUnitTest(unittest.TestCase):
    """Unit test class for observation transform"""

    @patch("etl.transform.observation.ETLContext")
    def test_obs_transform_query(self, mock_ctxt):
        """Mock sql calls"""
        mock_cnxn = mock_ctxt.transaction.return_value.__enter__.return_value
        observation_transform(mock_ctxt)

        expected_sql_queries = [query for query in SQL_ENTRIES.values()]
        expected_sql_queries.append(cast_date_format())

        actual_str_queries = get_sql_str_list(mock_cnxn.execute.call_args_list)

        self.assertTrue(set(expected_sql_queries).issubset(set(actual_str_queries)))
