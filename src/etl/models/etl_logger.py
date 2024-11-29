"""Temporary table data models"""

# pylint: disable=too-many-lines
# pylint: disable=invalid-name
from typing import Any, Dict, Final, List

from ..models.lookupmodels import CodeLogger
from ..models.modelutils import FK, CharField, Column, IntField, make_model_base
from ..models.omopcdm54.clinical import Person
from ..models.omopcdm54.registry import TARGET_SCHEMA

LoggerModelBase: Any = make_model_base()


class LoggerModelRegistry:
    """A simple global registry for logger tables"""

    __shared_state: Dict[str, Dict[str, Any]] = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_logger_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = LoggerModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls


@register_logger_model
class ETLLogger(LoggerModelBase):
    """
    The etl logging table
    """

    __tablename__ = "etl_logger"
    __table_args__ = {"schema": TARGET_SCHEMA}

    id: Final[Column] = IntField(primary_key=True, index=True)
    patient_id: Final[Column] = IntField(FK(Person.person_id), nullable=False)
    omop_table_name: Final[Column] = CharField(50, nullable=False)
    source_table_name: Final[Column] = CharField(50, nullable=False)
    error_code_id: Final[Column] = IntField(
        FK(CodeLogger.error_code_id), nullable=False
    )
    error_code_description: Final[Column] = CharField(200)
    error_code_level: Final[Column] = CharField(200)


# pylint: disable=no-member
LOGGER_MODELS: Final[Dict[str, LoggerModelBase]] = (  # type: ignore
    LoggerModelRegistry().registered
)

# pylint: disable=no-member
LOGGER_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in LoggerModelRegistry().registered.items()
]
