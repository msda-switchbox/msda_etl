"""Collect all model submodules here"""

# flake8: noqa
from typing import Dict, Final, List

# Clinical Data Tables
from ..omopcdm54.clinical import *

# Health Economics Data Tables
from ..omopcdm54.health_economics import *

# Health System Data Tables
from ..omopcdm54.health_systems import *

# Vocabulary Tables
from ..omopcdm54.metadata import *

# Module utils
from ..omopcdm54.registry import OmopCdmModelBase, OmopCdmModelRegistry

# Standardized Derived Elements
from .standardized_derived_elements import *

# Vocabulary Tables
from .vocabulary import *

OMOPCDM_VERSION: Final[str] = "5.4"

# pylint: disable=no-member
OMOPCDM_REGISTRY: Final[Dict[str, OmopCdmModelBase]] = (  # type: ignore
    OmopCdmModelRegistry().registered
)

# pylint: disable=no-member
OMOPCDM_MODELS: Final[List[OmopCdmModelBase]] = (  # type: ignore
    OmopCdmModelRegistry().registered.values()
)

# pylint: disable=no-member
OMOPCDM_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in OmopCdmModelRegistry().registered.items()
]

__all__ = OMOPCDM_MODEL_NAMES
