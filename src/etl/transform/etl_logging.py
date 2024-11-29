"""Log dropped patients to ETL Logger"""

import logging
from typing import Dict, Final

from ..context import ETLContext
from ..models.etl_logger import ETLLogger
from ..models.lookupmodels import CodeLogger
from ..models.omopcdm54.clinical import (
    ConditionOccurrence,
    DrugExposure,
    Measurement,
    Observation,
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)
from ..models.source import (
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
from ..transform.transformutils import execute_sql_transform
from ..util.sql import cast_date_format

logger = logging.getLogger(__name__)


def _log_errors_sql(
    patient_id_col, omop_table, source_table, error_code, join="", where=""
) -> str:
    return f"""
            INSERT INTO {str(ETLLogger.__table__)}
            (
                {ETLLogger.patient_id.key},
                {ETLLogger.omop_table_name.key},
                {ETLLogger.source_table_name.key},
                {ETLLogger.error_code_id.key},
                {ETLLogger.error_code_description.key},
                {ETLLogger.error_code_level.key}
            )
            SELECT
                {patient_id_col},
                '{omop_table.__tablename__}',
                '{source_table.__tablename__}',
                {error_code},
                {CodeLogger.error_code_description.key},
                {CodeLogger.error_code_level.key}
            FROM {str(source_table.__table__)}
            {join}
            LEFT JOIN {str(CodeLogger.__table__)}
                ON {CodeLogger.error_code_id.key} = {error_code}
            {where}
        """


def _log_default_date(model, omop_table, where) -> str:
    return _log_errors_sql(model.patient_id.key, omop_table, model, 4, "", where)


PATIENT_LOGGER_DICT = {
    "Logging patients with missing or incorrectly formatted DOB": [
        Patient.patient_id.key,
        Person,
        Patient,
        1,
        "",
        f"WHERE cast_date({Patient.date_birth.key}::VARCHAR) IS NULL",
    ],
    "Logging patients without a (correctly formatted) gender variable": [
        DiseaseHistory.patient_id.key,
        Person,
        Patient,
        2,
        "",
        f"WHERE {Patient.sex.key} NOT IN ('female', 'male');",
    ],
    "Logging patients without a (correctly formatted) date of diagnosis or date of onset": [
        DiseaseHistory.patient_id.key,
        Person,
        DiseaseHistory,
        3,
        f"""LEFT JOIN {str(Person.__table__)} ON {DiseaseHistory.patient_id.key} = {Person.person_id.key}""",
        f"""WHERE {Person.person_id.key} IS NULL
            AND cast_date({DiseaseHistory.date_diagnosis.key}::VARCHAR) IS NULL
            AND cast_date({DiseaseHistory.date_onset.key}::VARCHAR) IS NULL""",
    ],
    "Logging patients without an entry in the Disease History table": [
        f"patient.{Patient.patient_id.key}",
        Person,
        Patient,
        6,
        f"""LEFT JOIN {str(DiseaseHistory.__table__)} d
            ON patient.{Patient.patient_id.key} = d.{DiseaseHistory.patient_id.key}""",
        f"""WHERE d.{DiseaseHistory.patient_id.key} IS NULL""",
    ],
}

DRUG_EXPOSURE_LOGGER_DICT: Final[Dict] = {
    "drug_exposure_start_date": [
        Dmt,
        DrugExposure,
        f"WHERE cast_date({Dmt.dmt_start.key}::VARCHAR) IS NULL",
    ],
    "drug_exposure_end_date|stop_key": [
        Dmt,
        DrugExposure,
        f"WHERE {Dmt.dmt_stop.key} IS NOT NULL AND cast_date({Dmt.dmt_stop.key}::VARCHAR) IS NULL",
    ],
    "drug_exposure_end_date|dmt_status=yes": [
        Dmt,
        DrugExposure,
        f"WHERE {Dmt.dmt_status.key} = 'yes' AND cast_date({Dmt.date_visit.key}::VARCHAR) IS NULL",
    ],
    "drug_exposure_end_date|dmt_status=no": [
        Dmt,
        DrugExposure,
        f"WHERE {Dmt.dmt_status.key} = 'no' AND cast_date({Dmt.dmt_stop.key}::VARCHAR) IS NULL",
    ],
}


CONDITION_OCCURRENCE_LOGGER_DICT: Final[Dict] = {
    "condition_occurrence_start_date|relapses": [
        Relapses,
        ConditionOccurrence,
        f"WHERE cast_date({Relapses.date_relapse.key}::VARCHAR) IS NULL",
    ],
    "condition_occurrence_start_date|disease_history": [
        DiseaseHistory,
        ConditionOccurrence,
        f"WHERE cast_date({DiseaseHistory.date_visit.key}::VARCHAR) IS NULL",
    ],
}

OBSERVATION_LOGGER_DICT: Final[Dict] = {
    f"observation_date|{model.__tablename__}": [
        model,
        Observation,
        f"WHERE cast_date({model.date_visit.key}::VARCHAR) IS NULL",
    ]
    for model in [
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
}

MEASUREMENT_LOGGER_DICT: Final[Dict] = {
    f"date_diagnosis|{DiseaseHistory.__tablename__}": [
        DiseaseHistory,
        Measurement,
        f"""WHERE cast_date({DiseaseHistory.date_diagnosis.key}::VARCHAR) IS NULL
            AND {DiseaseHistory.csf_olib.key} IS NOT NULL""",
    ],
    f"date_visit|{DiseaseStatus.__tablename__}": [
        DiseaseStatus,
        Measurement,
        f"""WHERE cast_date({DiseaseStatus.date_visit.key}::VARCHAR) IS NULL
            AND (
                {DiseaseStatus.edss_score.key} IS NOT NULL
                OR {DiseaseStatus.pdds_score.key} IS NOT NULL
                OR {DiseaseStatus.t25fw.key} IS NOT NULL
                OR {DiseaseStatus.ninehpt_right.key} IS NOT NULL
                OR {DiseaseStatus.ninehpt_left.key} IS NOT NULL
                OR {DiseaseStatus.vib_sense.key} IS NOT NULL
                OR {DiseaseStatus.sdmt.key} IS NOT NULL
            )""",
    ],
}


PROCEDURE_LOGGER_DICT: Final[Dict] = {
    f"mri_date|{Mri.__tablename__}": [
        Mri,
        ProcedureOccurrence,
        f"""WHERE cast_date({Mri.mri_date.key}::VARCHAR) IS NULL
                AND ({Mri.mri.key} = 'yes'
                OR ({Mri.mri.key} = 'no' AND {Mri.mri_region.key} IS NOT NULL)
                OR ({Mri.mri.key} IS NULL AND {Mri.mri_region.key} IS NOT NULL))""",
    ],
    f"date_visit, dmt_stop|{Dmt.__tablename__}": [
        Dmt,
        ProcedureOccurrence,
        f"""WHERE ({Dmt.dmt_status.key} ='yes' AND {Dmt.dmt_stop.key} IS NULL
                AND cast_date({Dmt.date_visit.key}::VARCHAR) IS NULL)
                OR ({Dmt.dmt_stop.key} IS NOT NULL
                    AND cast_date({Dmt.dmt_stop.key}::VARCHAR) IS NULL)
                OR ({Dmt.dmt_status.key} !='yes' AND {Dmt.dmt_stop.key} IS NULL)
        """,
    ],
}


def log_errors(ctxt: ETLContext, logger_dict: dict) -> None:
    """Log all dropped rows into the ETL Logger"""
    execute_sql_transform(ctxt, cast_date_format())
    for log_message, args in logger_dict.items():
        logger.info(log_message)
        execute_sql_transform(ctxt, _log_errors_sql(*args))


def log_default_date(ctxt: ETLContext, logger_dict: dict) -> None:
    """Log all rows with default dates into the ETL Logger"""
    execute_sql_transform(ctxt, cast_date_format())
    for log_message, args in logger_dict.items():
        logger.info("Logging rows with default date: %s", log_message)
        execute_sql_transform(ctxt, _log_default_date(*args))


def log_default_visit_date(ctxt: ETLContext, models: list) -> None:
    """Log all rows with default date_visit into the ETL Logger"""
    execute_sql_transform(ctxt, cast_date_format())
    logger.info(
        "Logging patients with missing or incorrectly formatted date: visit_start_date"
    )
    for model in models:
        execute_sql_transform(
            ctxt,
            _log_default_date(
                model=model,
                omop_table=VisitOccurrence,
                where=f"WHERE cast_date({model.date_visit.key}::VARCHAR) IS NULL",
            ),
        )


def log_invalid_mri_records(ctxt: ETLContext, table) -> None:
    """Log all rows with invalid mri records into the ETL Logger"""
    logger.info("Logging rows with invalid mri records")
    execute_sql_transform(
        ctxt,
        _log_errors_sql(
            Mri.patient_id.key,
            table,
            Mri,
            5,
            where=f"""
        WHERE ({Mri.mri.key} IS NULL OR {Mri.mri.key} = 'no')
            AND (
                {Mri.mri_new_les_t1.key} IS NOT NULL
                OR
                {Mri.mri_new_les_t2.key} IS NOT NULL
                OR
                {Mri.mri_gd_les.key} IS NOT NULL
            )
        """,
        ),
    )
