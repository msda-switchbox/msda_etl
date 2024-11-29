"""Create the concept_lookup tables"""

from typing import Final, List

from ..models.lookupmodels import CodeLogger, ConceptLookup
from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
    set_constraints_sql,
    set_indexes_sql,
)

_SQL_ENTRIES: Final[List[str]] = [
    drop_tables_sql([CodeLogger, ConceptLookup]),
    create_tables_sql([CodeLogger, ConceptLookup], dialect=DIALECT_POSTGRES),
    set_indexes_sql([CodeLogger, ConceptLookup], dialect=DIALECT_POSTGRES),
    set_constraints_sql([CodeLogger, ConceptLookup], dialect=DIALECT_POSTGRES),
]

SQL = " ".join(_SQL_ENTRIES).strip().replace("\n", " ")
