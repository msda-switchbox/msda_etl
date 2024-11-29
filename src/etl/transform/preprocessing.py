"""Preprocessing the source and lookup data"""

import logging
from typing import Final

import numpy as np
import pandas as pd
from sqlalchemy import and_, func, select

from ..context import ETLContext
from ..models.omopcdm54.vocabulary import Concept
from ..models.source import SOURCE_MODELS_FILENAME_KEY
from ..transform.transformutils import try_parsing_date
from ..util.random import generate_int_primary_key

logger = logging.getLogger(__name__)

LOOKUP_DATA: Final[str] = "lookup_data"
SOURCE_DATA: Final[str] = "source_data"


def format_dates(input_df: pd.DataFrame) -> pd.DataFrame:
    """This function will convert date columns to the desired format
    Defined in the try_parsing_date function"""
    for column in input_df.select_dtypes(exclude=["float64", "int64"]):
        try:
            input_df[column] = input_df[column].apply(try_parsing_date)
        except AttributeError:
            continue
    return input_df


def all_object_columns_lower_case(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df.select_dtypes(
        include=object, exclude=["datetime", "timedelta"]
    ):
        try:
            input_df[column] = input_df[column].str.lower()
        # skip non confirming object fields
        except AttributeError:
            continue
    return input_df


def replace_to_nan(input_df: pd.DataFrame) -> pd.DataFrame:
    for column in input_df:
        input_df[column].replace(
            ["nan", "none", "<not performed>"], np.nan, inplace=True
        )
    return input_df


def set_columns_to_lowercase(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df.columns = map(str.lower, input_df.columns)
    return input_df


def log_missing_columns(tablename: str, input_df: pd.DataFrame) -> None:
    # Construct a dictionary with expected columns for each table based on the source/temp models
    # Note: when no primary key is available in the source table, a "virtual" primary key is added: _id
    # (lookup_id for TEMP ConceptLookup model)
    # hence the filter in the list comprehension to avoid erroneous logging

    # pylint: disable=protected-access
    expected_columns = list(
        k
        for k, v in SOURCE_MODELS_FILENAME_KEY[
            tablename
        ]._sa_class_manager.local_attrs.items()
        if k not in ("_id", "lookup_id")
    )

    table_set = set(input_df.columns)
    exp_set = set(expected_columns)

    if not exp_set.issubset(table_set):
        diff = exp_set - table_set
        logger.warning(
            """Source table %s is missing some expected columns: %s""",
            tablename,
            diff,
        )


def _validate_concept_id(concept_id: int, ctxt: ETLContext) -> int:
    with ctxt.transaction() as cnxn:
        qry = (
            select(func.count())
            .select_from(Concept)
            .where(
                and_(
                    Concept.concept_id == concept_id,
                    Concept.standard_concept == "S",
                )
            )
        )
        result = cnxn.execute(qry)
        if result.first().count_1 == 0:
            logger.debug(
                """Concept id %s is missing in the concept table of OMOP CDM database. It has been set to 0.""",
                concept_id,
            )
            return 0
    return concept_id


def validate_concept_ids(input_df: pd.DataFrame, ctxt: ETLContext) -> pd.DataFrame:
    # Validates concept ids. If they are not present in the existing concept ids, it will log
    # the concept_id and set it to 0.
    concept_column = ctxt.config.lookup_standard_concept_col
    input_df[concept_column] = input_df.apply(
        lambda x: _validate_concept_id(x[concept_column], ctxt),
        axis=1,
    )
    return input_df


def set_pk(source_df: pd.DataFrame) -> pd.DataFrame:
    source_df["_id"] = source_df.apply(lambda _: generate_int_primary_key(), axis=1)
    return source_df


def transform(ctxt: ETLContext) -> None:
    for key, value in ctxt.sources.items():
        logger.debug("preprocessing %s", key)
        ctxt.sources[key] = all_object_columns_lower_case(value)
        ctxt.sources[key] = replace_to_nan(value)
        ctxt.sources[key] = set_columns_to_lowercase(value)
        ctxt.sources[key] = set_pk(value)
        ctxt.sources[key] = format_dates(value)
        log_missing_columns(key, value)
    for key, value in ctxt.lookups.items():
        logger.debug("preprocessing %s", key)
        if key == "concept_lookup":
            ctxt.lookups[key] = validate_concept_ids(value, ctxt)
