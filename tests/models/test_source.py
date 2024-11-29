"""Source models unit test"""

import unittest

from etl.models.source import SOURCE_MODEL_NAMES, SOURCE_MODELS


class SourceModelsUnitTest(unittest.TestCase):
    """Class for source models unit tests"""

    def test_models(self):
        self.assertTrue(len(SOURCE_MODELS) > 0)
        self.assertTrue(len(SOURCE_MODEL_NAMES) > 0)

        good_bad_names = [
            ("Comorbidities", "Comorbidites"),
            ("DiseaseHistory", "Diseaseistory"),
            ("DiseaseStatus", "DiseaseStatis"),
            ("Dmt", "Dnt"),
            ("Mri", "Nri"),
            ("Npt", "Mpt"),
            ("Patient", "Patients"),
            ("Relapses", "Relapse"),
            ("Symptom", "Symptoms"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in SOURCE_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in SOURCE_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )


__all__ = ["SourceModelsUnitTest"]
