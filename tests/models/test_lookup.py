"""Lookups unit test"""

import unittest

from etl.models.lookupmodels import LOOKUP_MODEL_NAMES, LOOKUP_MODELS


class LookupModelsUnitTest(unittest.TestCase):
    """Class for lookups unit tests"""

    def test_models(self):
        self.assertTrue(len(LOOKUP_MODELS) > 0)
        self.assertTrue(len(LOOKUP_MODEL_NAMES) > 0)

        good_bad_names = [
            ("ConceptLookup", "ConceptsLookup"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in LOOKUP_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in LOOKUP_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )


__all__ = ["LookupModelsUnitTest"]
