"""Create the omopcdm tables"""

from typing import Final, List

from ..models.etl_logger import ETLLogger
from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
)

MODELS: Final[List] = [ETLLogger]


def _ddl_sql() -> str:
    statements = [
        drop_tables_sql(MODELS, cascade=True),
        create_tables_sql(MODELS, dialect=DIALECT_POSTGRES),
    ]
    return " ".join(statements)


SQL: Final[str] = _ddl_sql()
