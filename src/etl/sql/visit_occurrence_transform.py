"""SQL query string definition for the visit_occurrence transformation"""

from typing import Final, List

from ..common import (
    CONCEPT_ID_OUTPATIENT_VISIT,
    CONCEPT_ID_REGISTRY,
    DEFAULT_DATE,
)
from ..models.omopcdm54.clinical import Person, VisitOccurrence
from ..models.source import (
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Relapses,
)

MODELS: Final[List] = [
    Comorbidities,
    DiseaseHistory,
    DiseaseStatus,
    Dmt,
    Mri,
    Npt,
    Relapses,
]


def create_cte():
    return " ".join(
        [
            "WITH union_visits AS(",
            "UNION".join(
                [
                    f"""
                    SELECT
                        {m.patient_id.key},
                        (CASE
                            WHEN cast_date({m.date_visit.key}::VARCHAR) IS NOT NULL THEN {m.date_visit.key}::DATE
                            ELSE '{DEFAULT_DATE}'::DATE
                        END) AS date_visit
                    FROM {str(m.__table__)}
                    """
                    for m in MODELS
                ]
            ),
            ")",
        ]
    )


SQL_CTE: Final[str] = create_cte()

SQL_INSERT: Final[str] = f"""
INSERT INTO {str(VisitOccurrence.__table__)}
(
    {VisitOccurrence.person_id.key},
    {VisitOccurrence.visit_concept_id.key},
    {VisitOccurrence.visit_start_date.key},
    {VisitOccurrence.visit_start_datetime.key},
    {VisitOccurrence.visit_end_date.key},
    {VisitOccurrence.visit_end_datetime.key},
    {VisitOccurrence.visit_type_concept_id.key},
    {VisitOccurrence.provider_id.key},
    {VisitOccurrence.care_site_id.key},
    {VisitOccurrence.visit_source_value.key},
    {VisitOccurrence.visit_source_concept_id.key},
    {VisitOccurrence.admitted_from_concept_id.key},
    {VisitOccurrence.admitted_from_source_value.key},
    {VisitOccurrence.discharged_to_concept_id.key},
    {VisitOccurrence.discharged_to_source_value.key},
    {VisitOccurrence.preceding_visit_occurrence_id.key}
)
SELECT DISTINCT ON (u.patient_id, u.date_visit)
    p.{Person.person_id.key},
    {CONCEPT_ID_OUTPATIENT_VISIT},
    u.date_visit::DATE,
    NULL::DATE,
    u.date_visit::DATE,
    NULL::DATE,
    {CONCEPT_ID_REGISTRY},
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER
FROM union_visits u
INNER JOIN {str(Person.__table__)} p
    ON u.patient_id = p.{Person.person_id.key}
;
"""

SQL_COUNT: Final[str] = f"""SELECT COUNT(*) FROM {str(VisitOccurrence.__table__)};"""

_SQL_ENTRIES: Final[List[str]] = [
    SQL_CTE,
    SQL_INSERT,
    SQL_COUNT,
]


SQL: Final[str] = " ".join(_SQL_ENTRIES).strip().replace("\n", " ")
