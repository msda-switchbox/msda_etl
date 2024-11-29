"""Utilities specific for the data models"""

# pylint: disable=invalid-name
from typing import Any, Final, List, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_mixin
from sqlalchemy.schema import AddConstraint, CreateIndex, CreateTable

DIALECT_POSTGRES: Final = postgresql.dialect()

ConstraintPK: Final = PrimaryKeyConstraint
# A simple alias for sqlalchemy's ForiegnKey
FK: Final = ForeignKey


def CharField(x, *args, **kwargs) -> Column:
    return Column(String(x), *args, **kwargs)


def EnumField(x, *args, **kwargs) -> Column:
    return Column(Enum(x), *args, **kwargs)


def DateField(*args, **kwargs) -> Column:
    return Column(Date, *args, **kwargs)


def DateTimeField(*args, **kwargs) -> Column:
    return Column(DateTime, *args, **kwargs)


def IntField(*args, **kwargs) -> Column:
    return Column(Integer, *args, **kwargs)


def BigIntField(*args, **kwargs) -> Column:
    return Column(BigInteger, *args, **kwargs)


def FloatField(*args, **kwargs) -> Column:
    return Column(Float, *args, **kwargs)


def NumericField(*args, **kwargs) -> Column:
    return Column(Numeric, *args, **kwargs)


def TextField(*args, **kwargs) -> Column:
    return Column(Text, *args, **kwargs)


def JSONField(*args, **kwargs) -> Column:
    return Column(JSON, *args, **kwargs)


def BoolField(*args, **kwargs) -> Column:
    return Column(Boolean, *args, **kwargs)


def make_model_base(schema: Optional[str] = None):
    """Dynamically create a new model base"""
    return declarative_base(
        metadata=MetaData(schema=schema),
    )


def drop_tables_sql(models: List[Any], cascade=True) -> str:
    cascade_str = ""
    if cascade:
        cascade_str = "CASCADE"
    drop_sql = (
        "; ".join(
            [f"DROP TABLE IF EXISTS {str(m.__table__)} {cascade_str}" for m in models]
        )
        + ";"
    )
    return drop_sql


def create_tables_sql(models: List[Any], dialect=DIALECT_POSTGRES) -> str:
    sql = []
    for model in models:
        sql.append(
            str(
                CreateTable(
                    model.__table__,
                    include_foreign_key_constraints=[],
                    if_not_exists=True,
                ).compile(dialect=dialect)
            )
        )
    return "; ".join(sql) + ";"


def set_indexes_sql(
    models: List[Any], dialect: Optional[Any] = DIALECT_POSTGRES
) -> str:
    sql = []
    for model in models:
        for index in model.__table__.indexes:
            sql.append(
                str(CreateIndex(index, if_not_exists=True).compile(dialect=dialect))
            )
    return "; ".join(sql) + ";"


ModelBase: Final[Any] = make_model_base()


# pylint: disable=unexpected-keyword-arg
def set_constraints_sql(
    models: List[ModelBase],  # type: ignore
    dialect: Optional[Any] = DIALECT_POSTGRES,
    fk_only: Optional[bool] = True,
) -> str:
    sql = []
    for model in models:
        for constraint in model.__table__.constraints:
            if fk_only and isinstance(constraint, FK):
                sql.append(
                    str(
                        AddConstraint(constraint, if_not_exists=True).compile(
                            dialect=dialect
                        )
                    )
                )
    return "; ".join(sql) + ";"


@declarative_mixin
class PKIdMixin:
    """A mixin"""

    _id = IntField(primary_key=True)
