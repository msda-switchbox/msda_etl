"""SQL query string definition for the person transformation"""

from typing import Final

from ..common import CONCEPT_ID_FEMALE, CONCEPT_ID_MALE, CONCEPT_ID_NOT_KNOWN
from ..models.omopcdm54.clinical import Person
from ..models.omopcdm54.health_systems import Location
from ..models.source import DiseaseHistory, Patient

SQL: Final[str] = f"""
WITH valid_disease_history AS (
    SELECT
        DISTINCT d.{DiseaseHistory.patient_id.key}
    FROM {str(DiseaseHistory.__table__)} d
    WHERE cast_date(d.{DiseaseHistory.date_diagnosis.key}::VARCHAR) IS NOT NULL
        OR cast_date(d.{DiseaseHistory.date_onset.key}::VARCHAR) IS NOT NULL
)
INSERT INTO {str(Person.__table__)}
(
    {Person.person_id.key},
    {Person.gender_concept_id.key},
    {Person.year_of_birth.key},
    {Person.month_of_birth.key},
    {Person.day_of_birth.key},
    {Person.birth_datetime.key},
    {Person.race_concept_id.key},
    {Person.ethnicity_concept_id.key},
    {Person.location_id.key},
    {Person.provider_id.key},
    {Person.care_site_id.key},
    {Person.person_source_value.key},
    {Person.gender_source_value.key},
    {Person.gender_source_concept_id.key},
    {Person.race_source_value.key},
    {Person.race_source_concept_id.key},
    {Person.ethnicity_source_value.key},
    {Person.ethnicity_source_concept_id.key}
)

SELECT
    p.{Patient.patient_id.key},
    (CASE
        WHEN p.{Patient.sex.key} = 'female' THEN {CONCEPT_ID_FEMALE}
        WHEN p.{Patient.sex.key} = 'male' THEN {CONCEPT_ID_MALE}
    END),
    EXTRACT(YEAR FROM p.{Patient.date_birth.key}::DATE),
    EXTRACT(MONTH FROM p.{Patient.date_birth.key}::DATE),
    EXTRACT(DAY FROM p.{Patient.date_birth.key}::DATE),
    NULL::DATE,
    {CONCEPT_ID_NOT_KNOWN},
    {CONCEPT_ID_NOT_KNOWN},
    l.{Location.location_id.key},
    NULL::INTEGER,
    NULL::INTEGER,
    'patient_id_'||p.{Patient.patient_id.key}::VARCHAR,
    'sex_'||p.{Patient.sex.key}::VARCHAR,
    NULL::INTEGER,
    (CASE
        WHEN p.{Patient.race_ethnicity.key} IS NULL THEN NULL::VARCHAR
        WHEN p.{Patient.race_ethnicity.key} IS NOT NULL THEN 'patient_race_ethnicity_'||p.{Patient.race_ethnicity.key}
    END),
    NULL::INTEGER,
    (CASE
        WHEN p.{Patient.race_ethnicity.key} IS NULL THEN NULL::VARCHAR
        WHEN p.{Patient.race_ethnicity.key} IS NOT NULL THEN 'patient_race_ethnicity_'||p.{Patient.race_ethnicity.key}
    END),
    NULL::INTEGER
FROM {str(Patient.__table__)} p
INNER JOIN (
    SELECT
        MAX({Patient.date_visit.key}) as date_visit,
        {Patient.patient_id.key}
        FROM {str(Patient.__table__)}
        GROUP BY {Patient.patient_id.key}
) b
    ON p.{Patient.patient_id.key} = b.{Patient.patient_id.key}
        AND p.{Patient.date_visit.key} = b.{Patient.date_visit.key}
INNER JOIN valid_disease_history vdh
    ON p.{Patient.patient_id.key} = vdh.{DiseaseHistory.patient_id.key}
LEFT JOIN {str(Location.__table__)} l
    ON p.{Patient.residence.key} = l.{Location.location_source_value.key}
WHERE cast_date(p.{Patient.date_birth.key}::VARCHAR) IS NOT NULL
    AND p.{Patient.sex.key} in ('female', 'male')
;

SELECT COUNT(*)
FROM {str(Person.__table__)};
""".strip().replace("\n", " ")
