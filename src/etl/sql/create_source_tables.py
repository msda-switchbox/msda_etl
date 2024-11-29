"""Create the source tables"""

from typing import Final, List

from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
)
from ..models.source import (
    SOURCE_SCHEMA,
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

MODELS: Final[List] = [
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Patient,
    Relapses,
    Symptom,
]

SQL_CREATE_SCHEMA: Final[str] = f"CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA};"

_SQL_ENTRIES: Final[List[str]] = [
    SQL_CREATE_SCHEMA,
    drop_tables_sql(MODELS),
    create_tables_sql(MODELS, dialect=DIALECT_POSTGRES),
]

SQL = " ".join(_SQL_ENTRIES).strip().replace("\n", " ")
