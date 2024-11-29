"""SQL query string definition for the procedure_occurrence transformation"""

from typing import Dict, Final

from ..common import (
    CONCEPT_ID_MRI,
    CONCEPT_ID_NOT_KNOWN,
    CONCEPT_ID_REGISTRY,
    CONCEPT_ID_TRANSPLANTATION,
    DEFAULT_DATE,
)
from ..models.lookupmodels import ConceptLookup
from ..models.omopcdm54.clinical import (
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)
from ..models.source import Dmt, Mri


# pylint: disable=too-many-arguments
def create_source_sql(
    proc_cid,
    proc_date,
    proc_end_date,
    proc_source_value,
    source_table,
    where,
    join="",
) -> str:
    return f"""
INSERT INTO {str(ProcedureOccurrence.__table__)}
(
    {ProcedureOccurrence.person_id.key},
    {ProcedureOccurrence.procedure_concept_id.key},
    {ProcedureOccurrence.procedure_date.key},
    {ProcedureOccurrence.procedure_datetime.key},
    {ProcedureOccurrence.procedure_end_date.key},
    {ProcedureOccurrence.procedure_end_datetime.key},
    {ProcedureOccurrence.procedure_type_concept_id.key},
    {ProcedureOccurrence.modifier_concept_id.key},
    {ProcedureOccurrence.quantity.key},
    {ProcedureOccurrence.provider_id.key},
    {ProcedureOccurrence.visit_occurrence_id.key},
    {ProcedureOccurrence.visit_detail_id.key},
    {ProcedureOccurrence.procedure_source_value.key},
    {ProcedureOccurrence.procedure_source_concept_id.key},
    {ProcedureOccurrence.modifier_source_value.key}
)

SELECT
    p.{Person.person_id.key},
    {proc_cid},
    {proc_date},
    NULL::DATE,
    {proc_end_date},
    NULL::DATE,
    {CONCEPT_ID_REGISTRY},
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::INTEGER,
    v.{VisitOccurrence.visit_occurrence_id.key},
    NULL::INTEGER,
    {proc_source_value},
    NULL::INTEGER,
    NULL::VARCHAR
FROM {str(source_table.__table__)} s
INNER JOIN {str(Person.__table__)} p
    ON s.patient_id = p.{Person.person_id.key}
LEFT JOIN {str(VisitOccurrence.__table__)} v
    ON s.patient_id = v.{VisitOccurrence.person_id.key}
        AND s.date_visit::DATE = v.{VisitOccurrence.visit_start_date.key}::DATE
{join}
{where}
;

SELECT COUNT(*) FROM {str(ProcedureOccurrence.__table__)};
""".strip().replace("\n", " ")


MRI_SQL: Final[str] = create_source_sql(
    proc_cid=f"""
    (CASE
        WHEN s.{Mri.mri.key} = 'yes' AND s.{Mri.mri_region.key} IS NULL THEN {CONCEPT_ID_MRI}
        WHEN s.{Mri.mri_region.key} IS NOT NULL THEN c.{ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    proc_date=f"""
    (CASE
        WHEN cast_date(s.{Mri.mri_date.key}::VARCHAR) IS NOT NULL THEN s.{Mri.mri_date.key}::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END)
    """,
    proc_end_date="NULL::DATE",
    proc_source_value=f"""
    (CASE
        WHEN s.{Mri.mri_region.key} IS NOT NULL THEN s.{Mri.mri_region.key}||'_done'
        ELSE 'mri_done_'||s.{Mri.mri.key}
    END)
    """,
    source_table=Mri,
    where=f"""
    WHERE s.{Mri.mri.key} = 'yes'
    OR ((s.{Mri.mri.key} = 'no' OR s.{Mri.mri.key} IS NULL)
        AND s.{Mri.mri_region.key} IS NOT NULL)
    """,
    join=f"""
    LEFT JOIN {str(ConceptLookup.__table__)} c
    ON 'mri_region_'||s.{Mri.mri_region.key} = c.{ConceptLookup.concept_string.key}
        AND LOWER(c.{ConceptLookup.filter.key}) = 'procedure'
    """,
)

DMT_SQL: Final[str] = create_source_sql(
    proc_cid=CONCEPT_ID_TRANSPLANTATION,
    proc_date=f"""s.{Dmt.dmt_start.key}::DATE""",
    proc_end_date=f"""
    (CASE
        WHEN cast_date(s.{Dmt.dmt_stop.key}::VARCHAR) IS NOT NULL THEN s.{Dmt.dmt_stop.key}::DATE
        WHEN s.{Dmt.dmt_status.key} = 'yes' AND s.{Dmt.dmt_stop.key} IS NULL
            AND cast_date(s.{Dmt.date_visit.key}::VARCHAR) IS NOT NULL
            THEN s.{Dmt.date_visit.key}::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END)
    """,
    proc_source_value=f"'dmt_type_'||s.{Dmt.dmt_type.key}",
    source_table=Dmt,
    where=f"""WHERE s.{Dmt.dmt_type.key} = 'ahsct'""",
)

SQL_COUNT: Final[str] = (
    f"""SELECT COUNT(*) FROM {str(ProcedureOccurrence.__table__)};"""
)


SQL_ENTRIES: Final[Dict[str, str]] = {
    "MRI": MRI_SQL,
    "DMT": DMT_SQL,
    "PROCEDURE OCCURRENCE": SQL_COUNT,
}
