"""SQL query string definition for the measurement transformation"""

from typing import Dict, Final

from ..common import (
    CONCEPT_ID_KURTZKE,
    CONCEPT_ID_NEGATIVE,
    CONCEPT_ID_NHPT,
    CONCEPT_ID_NOT_KNOWN,
    CONCEPT_ID_OBS_CID,
    CONCEPT_ID_OLIGOCLONAL_BANDS,
    CONCEPT_ID_POSITIVE,
    CONCEPT_ID_REGISTRY,
    CONCEPT_ID_SDMT,
    CONCEPT_ID_SECOND,
    DEFAULT_DATE,
)
from ..models.omopcdm54.clinical import Measurement, Observation, Person
from ..models.source import DiseaseHistory, DiseaseStatus


# pylint: disable=too-many-arguments
def create_source_sql(
    measurement_cid,
    measurement_date,
    value_as_concept_id,
    value_source_value,
    measurement_source_value,
    source_table,
    join_clause="",
    where_clause="",
    cte="",
    value_as_number="NULL::NUMERIC",
    unit_concept_id="NULL::INTEGER",
    measurement_event_id="NULL::INTEGER",
    meas_event_field_concept_id="NULL::INTEGER",
) -> str:
    return f"""
{cte}
INSERT INTO {str(Measurement.__table__)}
(
    {Measurement.person_id.key},
    {Measurement.measurement_concept_id.key},
    {Measurement.measurement_date.key},
    {Measurement.measurement_datetime.key},
    {Measurement.measurement_time.key},
    {Measurement.measurement_type_concept_id.key},
    {Measurement.operator_concept_id.key},
    {Measurement.value_as_number.key},
    {Measurement.value_as_concept_id.key},
    {Measurement.unit_concept_id.key},
    {Measurement.range_low.key},
    {Measurement.range_high.key},
    {Measurement.provider_id.key},
    {Measurement.visit_occurrence_id.key},
    {Measurement.visit_detail_id.key},
    {Measurement.measurement_source_value.key},
    {Measurement.measurement_source_concept_id.key},
    {Measurement.unit_source_value.key},
    {Measurement.unit_source_concept_id.key},
    {Measurement.value_source_value.key},
    {Measurement.measurement_event_id.key},
    {Measurement.meas_event_field_concept_id.key}
)
SELECT
    p.{Person.person_id.key},
    {measurement_cid},
    (CASE
        WHEN cast_date(s.{measurement_date}::VARCHAR) IS NOT NULL THEN s.{measurement_date}::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END),
    NULL::DATE,
    NULL::VARCHAR,
    {CONCEPT_ID_REGISTRY},
    NULL::INTEGER,
    {value_as_number},
    {value_as_concept_id},
    {unit_concept_id},
    NULL::NUMERIC,
    NULL::NUMERIC,
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::INTEGER,
    {measurement_source_value},
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER,
    {value_source_value},
    {measurement_event_id},
    {meas_event_field_concept_id}
FROM {source_table} s
INNER JOIN {str(Person.__table__)} p
    ON s.patient_id = p.{Person.person_id.key}
{join_clause}
{where_clause}
;

SELECT COUNT(*) FROM {str(Measurement.__table__)};
""".strip().replace("\n", " ")


SQL_COUNT: Final[str] = f"""SELECT COUNT(*) FROM {str(Measurement.__table__)};"""

DISEASE_HISTORY_SQL: Final[str] = create_source_sql(
    measurement_cid=CONCEPT_ID_OLIGOCLONAL_BANDS,
    measurement_date=DiseaseHistory.date_diagnosis.key,
    value_as_concept_id=f"""
    (CASE
        WHEN s.{DiseaseHistory.csf_olib.key} = 'yes' THEN {CONCEPT_ID_POSITIVE}
        WHEN s.{DiseaseHistory.csf_olib.key} = 'no' THEN {CONCEPT_ID_NEGATIVE}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)""",
    value_source_value=f"s.{DiseaseHistory.csf_olib.key}",
    measurement_source_value=f"'csf_olib_'||s.{DiseaseHistory.csf_olib.key}",
    source_table=str(DiseaseHistory.__table__),
    where_clause=f"WHERE s.{DiseaseHistory.csf_olib.key} IS NOT NULL",
)

DISEASE_STATUS_SQL: Final[str] = create_source_sql(
    measurement_cid=f"""
    (CASE
        WHEN s.table = 'edss_score' THEN {CONCEPT_ID_KURTZKE}
        WHEN s.table in ('pdds_score', 't25fw') THEN {CONCEPT_ID_NOT_KNOWN}
        WHEN s.table in ('ninehpt_right', 'ninehpt_left') THEN {CONCEPT_ID_NHPT}
        WHEN s.table = 'sdmt' THEN {CONCEPT_ID_SDMT}
    END)
    """,
    measurement_date=DiseaseStatus.date_visit.key,
    value_as_concept_id="NULL::INTEGER",
    value_source_value="s.source_value",
    measurement_source_value="s.table||'_'||s.source_value",
    source_table="disease_status_cte",
    cte=f"""WITH disease_status_cte AS(
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.edss_score.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'edss_score' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.edss_score.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.pdds_score.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'pdds_score' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.pdds_score.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.t25fw.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            't25fw' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.t25fw.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.ninehpt_right.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ninehpt_right' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ninehpt_right.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.ninehpt_left.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ninehpt_left' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ninehpt_left.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.sdmt.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'sdmt' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.sdmt.key} IS NOT NULL
    )
    """,
    value_as_number="""
    (CASE
        WHEN s.table = 'pdds_score' AND s.source_value = 'normal' THEN 0
        WHEN s.table = 'pdds_score' AND s.source_value = 'mild disability' THEN 1
        WHEN s.table = 'pdds_score' AND s.source_value = 'moderate disability' THEN 2
        WHEN s.table = 'pdds_score' AND s.source_value = 'gait disability' THEN 3
        WHEN s.table = 'pdds_score' AND s.source_value = 'early cane' THEN 4
        WHEN s.table = 'pdds_score' AND s.source_value = 'late cane' THEN 5
        WHEN s.table = 'pdds_score' AND s.source_value = 'bilateral support' THEN 6
        WHEN s.table = 'pdds_score' AND s.source_value = 'wheelchair/scooter' THEN 7
        WHEN s.table = 'pdds_score' AND s.source_value = 'bedridden' THEN 8
        ELSE s.source_value::NUMERIC
    END)
    """,
    unit_concept_id=f"""
    (CASE
        WHEN s.table in ('ninehpt_right', 'ninehpt_left', 't25fw') THEN {CONCEPT_ID_SECOND}
        ELSE NULL::INTEGER
    END)
    """,
    measurement_event_id=f"""
    (CASE
        WHEN (s.table = 'ninehpt_right' AND o.{Observation.value_as_concept_id.key} = 4080761)
            OR (s.table = 'ninehpt_left' AND o.{Observation.value_as_concept_id.key} = 4300877)
            THEN o.{Observation.observation_id.key}
        ELSE NULL::INTEGER
    END)
    """,
    meas_event_field_concept_id=f"""
    (CASE
        WHEN s.table in ('ninehpt_right', 'ninehpt_left')
            AND o.{Observation.value_as_concept_id.key} in (4080761, 4300877) THEN {CONCEPT_ID_OBS_CID}
        ELSE NULL::INTEGER
    END)
    """,
    join_clause=f"""
    LEFT JOIN {str(Observation.__table__)} o
        ON s.{DiseaseStatus.patient_id.key} = o.{Observation.person_id.key}
            AND s.{DiseaseHistory.date_visit.key} = o.{Observation.observation_date.key}
            AND o.{Observation.observation_concept_id.key} = 4112563
    """,
)


SQL_ENTRIES: Final[Dict[str, str]] = {
    "DISEASE_HISTORY": DISEASE_HISTORY_SQL,
    "DISEASE_STATUS": DISEASE_STATUS_SQL,
    "MEASUREMENT": SQL_COUNT,
}
