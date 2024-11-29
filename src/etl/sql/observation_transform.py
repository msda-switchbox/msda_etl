"""SQL query string definition for the observation transformation"""

from typing import Dict, Final

from ..common import (
    CONCEPT_ID_CHRONIC_DISEASE,
    CONCEPT_ID_DATE_DIAGNOSIS,
    CONCEPT_ID_DATE_ONSET,
    CONCEPT_ID_EDUCATION,
    CONCEPT_ID_EMPLOYMENT_STATUS,
    CONCEPT_ID_EXACERBATION_MS,
    CONCEPT_ID_FH_MS,
    CONCEPT_ID_HISTORY_OF,
    CONCEPT_ID_LEFT_SIDE,
    CONCEPT_ID_MEDICAL_THERAPY,
    CONCEPT_ID_MS,
    CONCEPT_ID_NO,
    CONCEPT_ID_NO_EVIDENCE_OF,
    CONCEPT_ID_NO_TREATMENT_GIVEN,
    CONCEPT_ID_NOT_KNOWN,
    CONCEPT_ID_PROFESSIONAL_CARE,
    CONCEPT_ID_REGISTRY,
    CONCEPT_ID_RIGHT_SIDE,
    CONCEPT_ID_SIDE,
    CONCEPT_ID_SMOKING_COUNT,
    CONCEPT_ID_SMOKING_STATUS,
    CONCEPT_ID_SYMPTOM_SEVERITY,
    CONCEPT_ID_TREATMENT_GIVEN_NAACCR,
    CONCEPT_ID_TREATMENT_GIVEN_SNOMED,
    CONCEPT_ID_TREATMENT_NAIVE,
    CONCEPT_ID_VIB_SENSE,
    CONCEPT_ID_YES,
    DEFAULT_DATE,
)
from ..models.lookupmodels import ConceptLookup
from ..models.omopcdm54.clinical import Observation, Person, VisitOccurrence
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


# pylint: disable=too-many-arguments
def create_source_sql(
    obs_cid,
    obs_date,
    value_as_cid,
    obs_source_value,
    value_source_value,
    source_table,
    cte="",
    value_as_number="NULL::NUMERIC",
    visit_occ_id="NULL::INTEGER",
    join_clause="",
    where_clause="",
) -> str:
    return f"""
{cte}
INSERT INTO {str(Observation.__table__)}
(
    {Observation.person_id.key},
    {Observation.observation_concept_id.key},
    {Observation.observation_date.key},
    {Observation.observation_datetime.key},
    {Observation.observation_type_concept_id.key},
    {Observation.value_as_number.key},
    {Observation.value_as_string.key},
    {Observation.value_as_concept_id.key},
    {Observation.qualifier_concept_id.key},
    {Observation.unit_concept_id.key},
    {Observation.provider_id.key},
    {Observation.visit_occurrence_id.key},
    {Observation.visit_detail_id.key},
    {Observation.observation_source_value.key},
    {Observation.observation_source_concept_id.key},
    {Observation.unit_source_value.key},
    {Observation.qualifier_source_value.key},
    {Observation.value_source_value.key},
    {Observation.observation_event_id.key},
    {Observation.obs_event_field_concept_id.key}
)
SELECT
   p.{Person.person_id.key},
   {obs_cid},
   (CASE
        WHEN cast_date(s.{obs_date}::VARCHAR) IS NOT NULL THEN s.{obs_date}::DATE
        ELSE '{DEFAULT_DATE}'::DATE
    END),
   NULL::DATE,
   {CONCEPT_ID_REGISTRY},
   {value_as_number},
   NULL::VARCHAR,
   {value_as_cid},
   NULL::INTEGER,
   NULL::INTEGER,
   NULL::INTEGER,
   {visit_occ_id},
   NULL::INTEGER,
   {obs_source_value},
   NULL::INTEGER,
   NULL::VARCHAR,
   NULL::VARCHAR,
   {value_source_value},
   NULL::INTEGER,
   NULL::INTEGER
FROM {source_table} s
INNER JOIN {str(Person.__table__)} p
    ON s.patient_id = p.{Person.person_id.key}
{join_clause}
{where_clause}
;

SELECT COUNT(*) FROM {str(Observation.__table__)};
""".strip().replace("\n", " ")


SQL_COUNT: Final[str] = f"""SELECT COUNT(*) FROM {str(Observation.__table__)};"""

DISEASE_STATUS_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.table in ('ms_status_clin', 'ms_status_pat') THEN {CONCEPT_ID_CHRONIC_DISEASE}
        WHEN s.table in ('ninehpt_right', 'ninehpt_left') THEN {CONCEPT_ID_SIDE}
        WHEN s.table = 'vib_sense' THEN {CONCEPT_ID_VIB_SENSE}
    END)
    """,
    obs_date=DiseaseStatus.date_visit.key,
    value_as_cid=f"""(CASE
        WHEN s.table = 'ninehpt_right' THEN {CONCEPT_ID_RIGHT_SIDE}
        WHEN s.table = 'ninehpt_left' THEN {CONCEPT_ID_LEFT_SIDE}
        WHEN s.table in ('ms_status_clin', 'ms_status_pat', 'vib_sense')
            AND {ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN {ConceptLookup.standard_concept_id.key}
        WHEN s.table in ('ms_status_clin', 'ms_status_pat', 'vib_sense')
            AND {ConceptLookup.standard_concept_id.key} IS NULL
            THEN {CONCEPT_ID_NOT_KNOWN}
    END)""",
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="""(CASE
        WHEN s.table in ('ms_status_clin', 'ms_status_pat', 'vib_sense') THEN s.source_value
        WHEN s.table in ('ninehpt_right', 'ninehpt_left') THEN NULL::VARCHAR
    END)""",
    source_table="disease_status_cte",
    cte=f"""WITH disease_status_cte AS(
        SELECT
            {DiseaseStatus.patient_id.key},
            {DiseaseStatus.ms_status_clin.key} AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ms_status_clin' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ms_status_clin.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseStatus.patient_id.key},
            {DiseaseStatus.ms_status_pat.key} AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ms_status_pat' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ms_status_pat.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseStatus.patient_id.key},
            {DiseaseStatus.ninehpt_right.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ninehpt_right' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ninehpt_right.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseStatus.patient_id.key},
            {DiseaseStatus.ninehpt_left.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'ninehpt_left' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.ninehpt_left.key} IS NOT NULL
        UNION
        SELECT
            {DiseaseHistory.patient_id.key},
            {DiseaseStatus.vib_sense.key}::VARCHAR AS source_value,
            {DiseaseStatus.date_visit.key} AS date_visit,
            'vib_sense' AS table
        FROM {str(DiseaseStatus.__table__)}
        WHERE {DiseaseStatus.vib_sense.key} IS NOT NULL
    )
    """,
    join_clause=f"""LEFT JOIN {str(ConceptLookup.__table__)}
        ON LOWER(s.table||'_'||s.source_value ) = LOWER({ConceptLookup.concept_string.key})
    """,
)

SYMPTOM_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.table = 'current_symptom' THEN {CONCEPT_ID_HISTORY_OF}
        WHEN s.table = 'sever_symp' THEN {CONCEPT_ID_SYMPTOM_SEVERITY}
        WHEN s.table = 'treat_symp' THEN {CONCEPT_ID_TREATMENT_GIVEN_SNOMED}
    END)
    """,
    obs_date=Symptom.date_visit.key,
    value_as_cid=f"""
    (CASE
        WHEN s.table = 'current_symptom' AND {ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN {ConceptLookup.standard_concept_id.key}
        WHEN s.table = 'current_symptom' AND {ConceptLookup.standard_concept_id.key} IS NULL
            THEN {CONCEPT_ID_NOT_KNOWN}
        WHEN s.table = 'treat_symp' AND LOWER(s.source_value) = 'yes' THEN {CONCEPT_ID_YES}
        WHEN s.table = 'treat_symp' AND LOWER(s.source_value) = 'no' THEN {CONCEPT_ID_NO}
        ELSE NULL::INTEGER
    END)
    """,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value",
    source_table="symptom_cte",
    cte=f"""WITH symptom_cte AS(
        SELECT
            {Symptom.patient_id.key},
            {Symptom.current_symptom.key} AS source_value,
            {Symptom.date_visit.key} AS date_visit,
            'current_symptom' AS table
        FROM {str(Symptom.__table__)}
        WHERE {Symptom.current_symptom.key} IS NOT NULL
        UNION
        SELECT
            {Symptom.patient_id.key},
            {Symptom.sever_symp.key}::VARCHAR AS source_value,
            {Symptom.date_visit.key} AS date_visit,
            'sever_symp' AS table
        FROM {str(Symptom.__table__)}
        WHERE {Symptom.sever_symp.key} IS NOT NULL
        UNION
        SELECT
            {Symptom.patient_id.key},
            {Symptom.treat_symp.key} AS source_value,
            {Symptom.date_visit.key} AS date_visit,
            'treat_symp' AS table
        FROM {str(Symptom.__table__)}
        WHERE {Symptom.treat_symp.key} IS NOT NULL
    )
    """,
    value_as_number="""
    (CASE
        WHEN s.table = 'sever_symp' THEN s.source_value::NUMERIC
        ELSE NULL::NUMERIC
    END)
    """,
    join_clause=f"""LEFT JOIN {str(ConceptLookup.__table__)}
        ON LOWER(s.table||'_'||s.source_value )= lower({ConceptLookup.concept_string.key})
    """,
)

RELAPSES_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.table = 'relapse_recovery' THEN {CONCEPT_ID_NOT_KNOWN}
        WHEN s.table = 'relapse_treat' AND LOWER(s.source_value) = 'yes' THEN {CONCEPT_ID_TREATMENT_GIVEN_SNOMED}
        WHEN s.table = 'relapse_treat' AND LOWER(s.source_value) = 'no' THEN {CONCEPT_ID_NO_TREATMENT_GIVEN}
        WHEN s.table = 'relapse' THEN {CONCEPT_ID_NO_EVIDENCE_OF}
    END)
    """,
    obs_date=Relapses.date_visit.key,
    value_as_cid=CONCEPT_ID_EXACERBATION_MS,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value",
    source_table="relapse_cte",
    cte=f"""WITH relapse_cte AS(
        SELECT
            {Relapses.patient_id.key},
            {Relapses.relapse_recovery.key} AS source_value,
            {Relapses.date_visit.key} AS date_visit,
            'relapse_recovery' AS table
        FROM {str(Relapses.__table__)}
        WHERE {Relapses.relapse_recovery.key} IS NOT NULL
        UNION
        SELECT
            {Relapses.patient_id.key},
            {Relapses.relapse_treat.key} AS source_value,
            {Relapses.date_visit.key} AS date_visit,
            'relapse_treat' AS table
        FROM {str(Relapses.__table__)}
        WHERE LOWER({Relapses.relapse_treat.key}) IN ('yes', 'no')
        UNION
        SELECT
            {Relapses.patient_id.key},
            {Relapses.relapse.key} AS source_value,
            {Relapses.date_visit.key} AS date_visit,
            'relapse' AS table
        FROM {str(Relapses.__table__)}
        WHERE LOWER({Relapses.relapse.key}) = 'no'
    )
    """,
)

MRI_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.source_value = 0 THEN {CONCEPT_ID_NO_EVIDENCE_OF}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    obs_date=Mri.date_visit.key,
    value_as_cid="""
    (CASE
        WHEN s.source_value = 0 THEN s.source_value
        ELSE NULL::INTEGER
    END)
    """,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value::VARCHAR",
    source_table="mri_cte",
    cte=f"""WITH mri_cte AS(
        SELECT
            {Mri.patient_id.key},
            {Mri.mri_gd_les.key} AS source_value,
            {Mri.date_visit.key} AS date_visit,
            'mri_gd_les' AS table
        FROM {str(Mri.__table__)}
        WHERE {Mri.mri_gd_les.key} IS NOT NULL
        UNION
        SELECT
            {Mri.patient_id.key},
            {Mri.mri_new_les_t1.key} AS source_value,
            {Mri.date_visit.key} AS date_visit,
            'mri_new_les_t1' AS table
        FROM {str(Mri.__table__)}
        WHERE {Mri.mri_new_les_t1.key} IS NOT NULL
        UNION
        SELECT
            {Mri.patient_id.key},
            {Mri.mri_new_les_t2.key} AS source_value,
            {Mri.date_visit.key} AS date_visit,
            'mri_new_les_t2' AS table
        FROM {str(Mri.__table__)}
        WHERE {Mri.mri_new_les_t2.key} IS NOT NULL
    )
    """,
    value_as_number="""
    (CASE
        WHEN s.source_value = 0 THEN NULL::NUMERIC
        ELSE s.source_value::NUMERIC
    END)
    """,
)

PATIENT_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.table = 'education' THEN {CONCEPT_ID_EDUCATION}
        WHEN s.table = 'employment' THEN {CONCEPT_ID_EMPLOYMENT_STATUS}
        WHEN s.table = 'smoking' THEN {CONCEPT_ID_SMOKING_STATUS}
        WHEN s.table = 'ms_family' THEN {CONCEPT_ID_FH_MS}
        WHEN s.table = 'smoking_count' THEN {CONCEPT_ID_SMOKING_COUNT}
    END)
    """,
    obs_date=Patient.date_visit.key,
    value_as_cid=f"""
    (CASE
        WHEN s.table = 'smoking_count' THEN NULL::INTEGER
        WHEN {ConceptLookup.standard_concept_id.key} IS NOT NULL THEN {ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value",
    source_table="patient_cte",
    cte=f"""WITH patient_cte AS(
        SELECT
            {Patient.patient_id.key},
            {Patient.education.key} AS source_value,
            {Patient.date_visit.key} AS date_visit,
            'education' AS table
        FROM {str(Patient.__table__)}
        WHERE {Patient.education.key} IS NOT NULL
        UNION
        SELECT
            {Patient.patient_id.key},
            {Patient.employment.key} AS source_value,
            {Patient.date_visit.key} AS date_visit,
            'employment' AS table
        FROM {str(Patient.__table__)}
        WHERE {Patient.employment.key} IS NOT NULL
        UNION
        SELECT
            {Patient.patient_id.key},
            {Patient.smoking.key} AS source_value,
            {Patient.date_visit.key} AS date_visit,
            'smoking' AS table
        FROM {str(Patient.__table__)}
        WHERE {Patient.smoking.key} IS NOT NULL
        UNION
        SELECT
            {Patient.patient_id.key},
            {Patient.ms_family.key} AS source_value,
            {Patient.date_visit.key} AS date_visit,
            'ms_family' AS table
        FROM {str(Patient.__table__)}
        WHERE {Patient.ms_family.key} IS NOT NULL
        UNION
        SELECT
            {Patient.patient_id.key},
            {Patient.smoking_count.key}::VARCHAR AS source_value,
            {Patient.date_visit.key} AS date_visit,
            'smoking_count' AS table
        FROM {str(Patient.__table__)}
        WHERE {Patient.smoking.key} = 'current_smoker'
    )
    """,
    join_clause=f"""LEFT JOIN {str(ConceptLookup.__table__)}
        ON LOWER(s.table||'_'||s.source_value ) = LOWER({ConceptLookup.concept_string.key})
        AND LOWER({ConceptLookup.filter.key}) = 'vac'
    """,
    value_as_number="""
        (CASE
            WHEN s.table = 'smoking_count' THEN s.source_value::NUMERIC
            ELSE NULL::NUMERIC
        END)
    """,
)

DMT_SQL: Final[str] = create_source_sql(
    obs_cid=CONCEPT_ID_MEDICAL_THERAPY,
    obs_date=Dmt.date_visit.key,
    value_as_cid=f"""
    (CASE
        WHEN s.{Dmt.dmt_status.key} = 'yes' THEN {CONCEPT_ID_TREATMENT_GIVEN_NAACCR}
        WHEN s.{Dmt.dmt_status.key} = 'no' THEN {CONCEPT_ID_NO_TREATMENT_GIVEN}
        WHEN s.{Dmt.dmt_status.key} = 'dmt_naive' THEN {CONCEPT_ID_TREATMENT_NAIVE}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    obs_source_value=f"'dmt_status_'||s.{Dmt.dmt_status.key}",
    value_source_value=f"s.{Dmt.dmt_status.key}",
    source_table=str(Dmt.__table__),
    visit_occ_id=f"v.{VisitOccurrence.visit_occurrence_id.key}",
    join_clause=f"""LEFT JOIN {str(VisitOccurrence.__table__)} v
        ON s.{Dmt.patient_id.key} = v.{VisitOccurrence.person_id.key}
        AND s.{Dmt.date_visit.key} = v.{VisitOccurrence.visit_start_date.key}
    """,
    where_clause=f"WHERE s.{Dmt.dmt_status.key} IS NOT NULL",
)

DISEASE_HISTORY_SQL: Final[str] = create_source_sql(
    obs_cid=f"""
    (CASE
        WHEN s.table = 'date_onset' THEN {CONCEPT_ID_DATE_ONSET}
        WHEN s.table = 'date_diagnosis' THEN {CONCEPT_ID_DATE_DIAGNOSIS}
    END)
    """,
    obs_date="date_clinical_event",
    value_as_cid=CONCEPT_ID_MS,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value",
    source_table="disease_history_cte",
    cte=f"""WITH disease_history_cte AS(
        SELECT
            a.{DiseaseHistory.patient_id.key},
            a.{DiseaseHistory.date_onset.key} AS source_value,
            a.{DiseaseHistory.date_onset.key} AS date_clinical_event,
            'date_onset' AS table
        FROM {str(DiseaseHistory.__table__)} a
        INNER JOIN (
            SELECT
                MIN({DiseaseHistory.date_visit.key}) as date_visit,
                {DiseaseHistory.patient_id.key}
                FROM {str(DiseaseHistory.__table__)}
                GROUP BY {DiseaseHistory.patient_id.key}
        ) b
            ON a.{DiseaseHistory.patient_id.key} = b.{DiseaseHistory.patient_id.key}
                AND a.{DiseaseHistory.date_visit.key} = b.{DiseaseHistory.date_visit.key}
        WHERE {DiseaseHistory.date_onset.key} IS NOT NULL
        UNION
        SELECT
            a.{DiseaseHistory.patient_id.key},
            a.{DiseaseHistory.date_diagnosis.key} AS source_value,
            a.{DiseaseHistory.date_diagnosis.key} AS date_clinical_event,
            'date_diagnosis' AS table
        FROM {str(DiseaseHistory.__table__)} a
        INNER JOIN (
            SELECT
                MIN({DiseaseHistory.date_visit.key}) as date_visit,
                {DiseaseHistory.patient_id.key}
                FROM {str(DiseaseHistory.__table__)}
                GROUP BY {DiseaseHistory.patient_id.key}
        ) b
            ON a.{DiseaseHistory.patient_id.key} = b.{DiseaseHistory.patient_id.key}
                AND a.{DiseaseHistory.date_visit.key} = b.{DiseaseHistory.date_visit.key}
        WHERE {DiseaseHistory.date_diagnosis.key} IS NOT NULL
    )""",
)

NPT_SQL: Final[str] = create_source_sql(
    obs_cid=CONCEPT_ID_PROFESSIONAL_CARE,
    obs_date=Npt.date_visit.key,
    value_as_cid=f"""
    (CASE
        WHEN {ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN {ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    obs_source_value=f"'np_treat_type_'||{Npt.np_treat_type.key}",
    value_source_value=Npt.np_treat_type.key,
    source_table=str(Npt.__table__),
    join_clause=f"""LEFT JOIN {str(ConceptLookup.__table__)}
        ON 'np_treat_type_'||{Npt.np_treat_type.key} = LOWER({ConceptLookup.concept_string.key})
            AND LOWER({ConceptLookup.filter.key}) = 'vac'
    """,
    where_clause=f"WHERE {Npt.np_treat_type.key} IS NOT NULL",
)

COMORBIDITIES_SQL: Final[str] = create_source_sql(
    obs_cid=CONCEPT_ID_HISTORY_OF,
    obs_date=Comorbidities.date_visit.key,
    value_as_cid=f"""
    (CASE
        WHEN {ConceptLookup.standard_concept_id.key} IS NOT NULL
            THEN {ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END)
    """,
    obs_source_value="s.table||'_'||s.source_value",
    value_source_value="s.source_value",
    source_table="comorbidities_cte",
    cte=f"""WITH comorbidities_cte AS(
        SELECT
            {Comorbidities.patient_id.key},
            {Comorbidities.com_type.key} AS source_value,
            {Comorbidities.date_visit.key} AS date_visit,
            'com_type' AS table
        FROM {str(Comorbidities.__table__)}
        WHERE {Comorbidities.com_type.key} IS NOT NULL
        UNION
        SELECT
            {Comorbidities.patient_id.key},
            {Comorbidities.com_system.key} AS source_value,
            {Comorbidities.date_visit.key} AS date_visit,
            'com_system' AS table
        FROM {str(Comorbidities.__table__)}
        WHERE {Comorbidities.com_system.key} IS NOT NULL
            AND {Comorbidities.com_type.key} IS NULL
    )
    """,
    join_clause=f"""LEFT JOIN {str(ConceptLookup.__table__)}
        ON s.table||'_'||s.source_value = LOWER({ConceptLookup.concept_string.key})
            AND LOWER({ConceptLookup.filter.key}) = 'vac'
    """,
)

SQL_ENTRIES: Final[Dict[str, str]] = {
    "DISEASE_STATUS": DISEASE_STATUS_SQL,
    "SYMPTOM": SYMPTOM_SQL,
    "RELAPSES": RELAPSES_SQL,
    "MRI": MRI_SQL,
    "PATIENT": PATIENT_SQL,
    "DMT": DMT_SQL,
    "DISEASE_HISTORY": DISEASE_HISTORY_SQL,
    "NPT": NPT_SQL,
    "COMORBIDITIES": COMORBIDITIES_SQL,
    "OBSERVATION": SQL_COUNT,
}
