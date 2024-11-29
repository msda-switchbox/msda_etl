"""Create the omopcdm tables"""

from typing import Final, List

from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
    set_constraints_sql,
    set_indexes_sql,
)
from ..models.omopcdm54.clinical import (
    CareSite,
    ConditionOccurrence,
    Death,
    DrugExposure,
    Measurement,
    Observation,
    ObservationPeriod,
    Person,
    ProcedureOccurrence,
    Provider,
    VisitDetail,
    VisitOccurrence,
)
from ..models.omopcdm54.health_systems import Location
from ..models.omopcdm54.metadata import CDMSource
from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra,
    DrugEra,
    Episode,
    EpisodeEvent,
)

MODELS: Final[List] = [
    CareSite,
    CDMSource,
    ConditionEra,
    ConditionOccurrence,
    Death,
    DrugEra,
    DrugExposure,
    Episode,
    EpisodeEvent,
    Location,
    Measurement,
    Observation,
    ObservationPeriod,
    Person,
    ProcedureOccurrence,
    Provider,
    VisitDetail,
    VisitOccurrence,
]


def _ddl_sql() -> str:
    statements = [
        drop_tables_sql(MODELS, cascade=True),
        create_tables_sql(MODELS, dialect=DIALECT_POSTGRES),
        set_indexes_sql(MODELS, dialect=DIALECT_POSTGRES),
        set_constraints_sql(MODELS, dialect=DIALECT_POSTGRES),
    ]
    return " ".join(statements)


SQL: Final[str] = _ddl_sql()
