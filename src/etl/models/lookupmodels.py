"""Temporary table data models"""

# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from ..models.modelutils import FK, CharField, Column, IntField, make_model_base
from ..models.omopcdm54.registry import TARGET_SCHEMA
from ..models.omopcdm54.vocabulary import Concept

LookupModelBase: Any = make_model_base()


class LookupModelRegistry:
    """A simple global registry for lookup tables"""

    __shared_state: Dict[str, Dict[str, Any]] = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_lookup_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = LookupModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls


@register_lookup_model
class ConceptLookup(LookupModelBase):
    """Lookup table to map source concepts to target concept_ids"""

    __tablename__: Final = "concept_lookup"
    __table_args__ = {"schema": TARGET_SCHEMA}

    lookup_id: Final[Column] = IntField(primary_key=True)
    concept_string: Final[Column] = CharField(200)
    standard_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    domain: Final[Column] = CharField(20)
    filter: Final[Column] = CharField(50)


@register_lookup_model
class CodeLogger(LookupModelBase):
    """Lookup table to map error code to error description"""

    __tablename__: Final = "code_logger"
    __table_args__ = {"schema": TARGET_SCHEMA}

    error_code_id: Final[Column] = IntField(primary_key=True)
    error_code_description: Final[Column] = CharField(200)
    error_code_level: Final[Column] = CharField(200)


# pylint: disable=no-member
LOOKUP_MODELS: Final[Dict[str, LookupModelBase]] = (  # type: ignore
    LookupModelRegistry().registered
)

# pylint: disable=no-member
LOOKUP_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in LookupModelRegistry().registered.items()
]
