"""Logger unit tests"""

import unittest

from etl.models.etl_logger import LOGGER_MODEL_NAMES, LOGGER_MODELS


class LoggerModelsUnitTest(unittest.TestCase):
    """Class for logger unit tests"""

    def test_models(self):
        self.assertTrue(len(LOGGER_MODELS) > 0)
        self.assertTrue(len(LOGGER_MODEL_NAMES) > 0)

        good_bad_names = [
            ("ETLLogger", "ETLogger"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in LOGGER_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in LOGGER_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )


__all__ = ["LoggerModelsUnitTest"]
