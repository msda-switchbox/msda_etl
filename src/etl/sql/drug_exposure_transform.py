"""SQL query string definition for the drug_exposure transformation"""

from typing import Final

from ..common import CONCEPT_ID_NOT_KNOWN, CONCEPT_ID_REGISTRY, DEFAULT_DATE
from ..models.lookupmodels import ConceptLookup
from ..models.omopcdm54.clinical import DrugExposure, Person, VisitOccurrence
from ..models.source import Dmt

SQL: Final[str] = f"""
INSERT INTO {str(DrugExposure.__table__)}
(
    {DrugExposure.person_id.key},
    {DrugExposure.drug_concept_id.key},
    {DrugExposure.drug_exposure_start_date.key},
    {DrugExposure.drug_exposure_start_datetime.key},
    {DrugExposure.drug_exposure_end_date.key},
    {DrugExposure.drug_exposure_end_datetime.key},
    {DrugExposure.verbatim_end_date.key},
    {DrugExposure.drug_type_concept_id.key},
    {DrugExposure.drug_source_value.key},
    {DrugExposure.stop_reason.key},
    {DrugExposure.refills.key},
    {DrugExposure.quantity.key},
    {DrugExposure.days_supply.key},
    {DrugExposure.sig.key},
    {DrugExposure.route_concept_id.key},
    {DrugExposure.lot_number.key},
    {DrugExposure.provider_id.key},
    {DrugExposure.visit_detail_id.key},
    {DrugExposure.visit_occurrence_id.key},
    {DrugExposure.drug_source_concept_id.key},
    {DrugExposure.route_source_value.key},
    {DrugExposure.dose_unit_source_value.key}
)
SELECT
    p.{Person.person_id.key},
    (CASE
        WHEN c_type.{ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN c_type.{ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END),
    (CASE
        WHEN cast_date(d.{Dmt.dmt_start.key}::VARCHAR) IS NOT NULL THEN d.{Dmt.dmt_start.key}::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END),
    NULL::DATE,
    (CASE
        WHEN d.{Dmt.dmt_stop.key} IS NOT NULL AND cast_date(d.{Dmt.dmt_stop.key}::VARCHAR)
            IS NOT NULL THEN d.{Dmt.dmt_stop.key}::DATE
        WHEN d.{Dmt.dmt_stop.key} IS NOT NULL AND cast_date(d.{Dmt.dmt_stop.key}::VARCHAR)
            IS NULL THEN '{DEFAULT_DATE}'::DATE
        WHEN lower(d.{Dmt.dmt_status.key}) = 'yes' AND d.{Dmt.dmt_stop.key} IS NULL
            AND cast_date(d.{Dmt.date_visit.key}::VARCHAR) IS NOT NULL THEN d.{Dmt.date_visit.key}::DATE
        WHEN lower(d.{Dmt.dmt_status.key}) = 'yes' AND d.{Dmt.dmt_stop.key} IS NULL
            AND cast_date(d.{Dmt.date_visit.key}::VARCHAR) IS NULL THEN '{DEFAULT_DATE}'::DATE
        WHEN lower(d.{Dmt.dmt_status.key}) = 'no' AND d.{Dmt.dmt_stop.key} IS NULL
            THEN '{DEFAULT_DATE}'::DATE
    END),
    NULL::DATE,
    NULL::DATE,
    '{CONCEPT_ID_REGISTRY}',
    'dmt_type_'||d.{Dmt.dmt_type.key},
    (CASE
        WHEN d.{Dmt.dmt_stop_reas.key} IS NOT NULL
            THEN c_stop.{ConceptLookup.standard_concept_id.key}::VARCHAR
        ELSE NULL::VARCHAR
    END),
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER,
    NULL::INTEGER,
    v.{VisitOccurrence.visit_occurrence_id.key},
    NULL::INTEGER,
    NULL::VARCHAR,
    NULL::INTEGER
FROM {str(Dmt.__table__)} d
INNER JOIN {str(Person.__table__)} p
    ON p.{Person.person_id.key} = d.{Dmt.patient_id.key}
LEFT JOIN {str(ConceptLookup.__table__)} c_type
    ON ('dmt_type_'||d.{Dmt.dmt_type.key}=LOWER(c_type.{ConceptLookup.concept_string.key})
        AND LOWER(c_type.{ConceptLookup.filter.key})='drug_exposure')
LEFT JOIN {str(ConceptLookup.__table__)} c_stop
    ON ('dmt_stop_reas_'||d.{Dmt.dmt_stop_reas.key}=LOWER(c_stop.{ConceptLookup.concept_string.key})
        AND LOWER(c_stop.{ConceptLookup.filter.key})='stop_reason')
INNER JOIN {str(VisitOccurrence.__table__)} v
    ON d.{Dmt.patient_id.key} = v.{VisitOccurrence.person_id.key}
        AND d.{Dmt.date_visit.key} = v.{VisitOccurrence.visit_start_date.key}
WHERE d.{Dmt.dmt_type.key} IS NOT NULL
    AND d.{Dmt.dmt_type.key} != 'ahsct'
    AND d.{Dmt.dmt_stop.key} IS NOT NULL
        OR (d.{Dmt.dmt_stop.key} IS NULL AND d.{Dmt.dmt_status.key} in ('yes', 'no'))
;

SELECT COUNT(*)
FROM {str(DrugExposure.__table__)};
""".strip().replace("\n", " ")
