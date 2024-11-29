"""SQL query string definition for the condition_occurrence transformation"""

from typing import Final

from ..common import (
    CONCEPT_ID_EXACERBATION_MS,
    CONCEPT_ID_MS,
    CONCEPT_ID_NOT_KNOWN,
    CONCEPT_ID_REGISTRY,
    DEFAULT_DATE,
)
from ..models.lookupmodels import ConceptLookup
from ..models.omopcdm54.clinical import (
    ConditionOccurrence,
    Person,
    VisitOccurrence,
)
from ..models.source import DiseaseHistory, Relapses

SQL: Final[str] = f"""
WITH stacked_table AS (
    SELECT
        {DiseaseHistory.patient_id.key},
        {DiseaseHistory.date_diagnosis.key} AS start_date,
        NULL AS condition,
        NULL AS stop_reason,
        {DiseaseHistory.date_visit.key} AS date_visit,
        'date_diagnosis' AS table
    FROM {str(DiseaseHistory.__table__)}
    WHERE {DiseaseHistory.date_diagnosis.key} IS NOT NULL
    UNION
    SELECT
        {DiseaseHistory.patient_id.key},
        {DiseaseHistory.date_visit.key} AS start_date,
        {DiseaseHistory.ms_course.key} AS condition,
        NULL AS stop_reason,
        {DiseaseHistory.date_visit.key} AS date_visit,
        'ms_course' AS table
    FROM {str(DiseaseHistory.__table__)}
    WHERE {DiseaseHistory.date_diagnosis.key} IS NOT NULL AND {DiseaseHistory.ms_course.key} IS NOT NULL
    UNION
    SELECT
        {Relapses.patient_id.key},
        {Relapses.date_relapse.key} AS start_date,
        {Relapses.relapse.key} AS condition,
        {Relapses.relapse_recovery.key} AS stop_reason,
        {Relapses.date_visit.key} AS date_visit,
        'relapses' AS table
    FROM {str(Relapses.__table__)}
    WHERE {Relapses.relapse.key} = 'yes'
)
INSERT INTO {str(ConditionOccurrence.__table__)}
(
    {ConditionOccurrence.person_id.key},
    {ConditionOccurrence.condition_concept_id.key},
    {ConditionOccurrence.condition_start_date.key},
    {ConditionOccurrence.condition_start_datetime.key},
    {ConditionOccurrence.condition_end_date.key},
    {ConditionOccurrence.condition_end_datetime.key},
    {ConditionOccurrence.condition_type_concept_id.key},
    {ConditionOccurrence.condition_status_concept_id.key},
    {ConditionOccurrence.stop_reason.key},
    {ConditionOccurrence.provider_id.key},
    {ConditionOccurrence.visit_occurrence_id.key},
    {ConditionOccurrence.visit_detail_id.key},
    {ConditionOccurrence.condition_source_value.key},
    {ConditionOccurrence.condition_source_concept_id.key},
    {ConditionOccurrence.condition_status_source_value.key}
)
SELECT
    p.{Person.person_id.key},
    (CASE
        WHEN s.table = 'relapses' THEN {CONCEPT_ID_EXACERBATION_MS}
        WHEN s.table = 'date_diagnosis' THEN {CONCEPT_ID_MS}
        WHEN s.table = 'ms_course' AND c.{ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN c.{ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END),
    (CASE
        WHEN cast_date(s.start_date::VARCHAR) IS NOT NULL THEN s.start_date::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END),
    NULL::DATE,
    NULL::DATE,
    NULL::DATE,
    {CONCEPT_ID_REGISTRY},
    NULL::INTEGER,
    (CASE
        WHEN s.stop_reason = 'compl_recovery' THEN s.stop_reason
    END),
    NULL::INTEGER,
    v.{VisitOccurrence.visit_occurrence_id.key},
    NULL::INTEGER,
    (CASE
        WHEN s.table = 'relapses' THEN 'relapse_'||s.condition
        WHEN s.table = 'date_diagnosis' THEN NULL::VARCHAR
        WHEN s.table = 'ms_course' THEN 'ms_course_'||s.condition
    END),
    NULL::INTEGER,
    NULL::VARCHAR
FROM stacked_table s
INNER JOIN {str(Person.__table__)} p
    ON s.patient_id = p.{Person.person_id.key}
LEFT JOIN {str(VisitOccurrence.__table__)} v
    ON s.patient_id = v.{VisitOccurrence.person_id.key}
        AND s.date_visit = v.{VisitOccurrence.visit_start_date.key}
LEFT JOIN {str(ConceptLookup.__table__)} c
    ON 'ms_course_'||s.condition = c.{ConceptLookup.concept_string.key}
;

DELETE
FROM {str(ConditionOccurrence.__table__)} a USING (
    SELECT
        MIN(ctid) as ctid,
        {ConditionOccurrence.person_id.key},
        {ConditionOccurrence.condition_start_date.key},
        {ConditionOccurrence.condition_concept_id.key}
    FROM {str(ConditionOccurrence.__table__)}
    GROUP BY
        {ConditionOccurrence.person_id.key},
        {ConditionOccurrence.condition_start_date.key},
        {ConditionOccurrence.condition_concept_id.key}
    HAVING COUNT(*) > 1
) b
WHERE a.{ConditionOccurrence.person_id.key} = b.{ConditionOccurrence.person_id.key}
    AND a.{ConditionOccurrence.condition_concept_id.key} = b.{ConditionOccurrence.condition_concept_id.key}
    AND a.ctid <> b.ctid;

SELECT COUNT(*)
FROM {str(ConditionOccurrence.__table__)};
""".strip().replace("\n", " ")
