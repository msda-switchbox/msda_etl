"""Target models unit test"""

import unittest

from etl.models.omopcdm54 import (
    OMOPCDM_MODEL_NAMES as TARGET_MODEL_NAMES,
)
from etl.models.omopcdm54 import (
    OMOPCDM_MODELS as TARGET_MODELS,
)


class TargetModelsUnitTest(unittest.TestCase):
    """Class for target models unit tests"""

    def test_models(self):
        self.assertTrue(len(TARGET_MODELS) > 0)
        self.assertTrue(len(TARGET_MODEL_NAMES) > 0)

        good_bad_names = [
            ("CareSite", "CareeSite"),
            ("CDMSource", "CDMsSource"),
            ("ConditionEra", "ConditionEta"),
            ("ConditionOccurrence", "ConditionOcurrence"),
            ("Death", "Deaths"),
            ("DrugEra", "DrugEta"),
            ("DrugExposure", "DrugExposures"),
            ("Episode", "Epipsode"),
            ("EpisodeEvent", "EpipsodeEvent"),
            ("Location", "Locations"),
            ("Measurement", "Measurements"),
            ("Observation", "Observations"),
            ("ObservationPeriod", "ObservatiomPeriod"),
            ("Person", "Persons"),
            ("ProcedureOccurrence", "ProcedureOccurrences"),
            ("Provider", "Providers"),
            ("VisitDetail", "VisitDetails"),
            ("VisitOccurrence", "VisitOccurrences"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in TARGET_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in TARGET_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )


__all__ = ["TargetModelsUnitTest"]
