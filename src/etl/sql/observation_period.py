"""SQL query string definition for observation_period"""

# pylint: disable=no-member
from typing import Final, List

from ..common import CONCEPT_ID_REGISTRY, DEFAULT_DATE
from ..models.omopcdm54.clinical import (
    ConditionOccurrence,
    DrugExposure,
    Measurement,
    Observation,
    ObservationPeriod,
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)

TARGET_TABLENAME: Final[str] = f"{str(ObservationPeriod.__table__)}"
TARGET_COLUMNS: Final[List[str]] = [
    v.key for v in ObservationPeriod.__table__.columns.values()
]
DEFAULT_OBSERVATION_DATE: Final[str] = DEFAULT_DATE.isoformat()


def _obs_period_sql() -> str:
    return f"""
    INSERT INTO
        {TARGET_TABLENAME} (
            {ObservationPeriod.person_id.key},
            {ObservationPeriod.observation_period_start_date.key},
            {ObservationPeriod.observation_period_end_date.key},
            {ObservationPeriod.period_type_concept_id.key}
        )
    SELECT
        {ObservationPeriod.person_id.key},
        COALESCE(
            LEAST(
                minimum_measurement_date,
                minimum_condition_date,
                minimum_procedure_date,
                minimum_observation_date,
                minimum_visit_date,
                minimum_drug_exposure_date
            ),
            '{DEFAULT_OBSERVATION_DATE}'
        ) AS {ObservationPeriod.observation_period_start_date.key},
        COALESCE(
            GREATEST(
                maximum_visit_date
            ),
            '{DEFAULT_OBSERVATION_DATE}'
        ) AS {ObservationPeriod.observation_period_end_date.key},
        {CONCEPT_ID_REGISTRY} AS {ObservationPeriod.period_type_concept_id.key}
    FROM
    (
        SELECT
            *
        FROM

            /* measurement */
            (
                SELECT
                    {Measurement.person_id.key},
                    MIN({Measurement.measurement_date.key}) AS minimum_measurement_date
                FROM
                    {str(Measurement.__table__)}
                WHERE
                    {Measurement.measurement_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) measurement_date_range

            /* condition occurrence */
            FULL OUTER JOIN (
                SELECT
                    {ConditionOccurrence.person_id.key},
                    MIN({ConditionOccurrence.condition_start_date.key}) AS minimum_condition_date
                FROM
                    {str(ConditionOccurrence.__table__)}
                WHERE
                    {ConditionOccurrence.condition_start_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) condition_date_range USING ({ConditionOccurrence.person_id.key})

            /* visit occurrence */
            FULL OUTER JOIN (
                SELECT
                    {VisitOccurrence.person_id.key},
                    MIN(visit_date) AS minimum_visit_date,
                    MAX(visit_date) AS maximum_visit_date
                FROM
                    (
                        SELECT
                            {VisitOccurrence.person_id.key},
                            {VisitOccurrence.visit_start_date.key} AS visit_date
                        FROM
                            {str(VisitOccurrence.__table__)}
                        UNION
                        SELECT
                            {VisitOccurrence.person_id.key},
                            {VisitOccurrence.visit_end_date.key} AS visit_date
                        FROM
                            {str(VisitOccurrence.__table__)}
                    ) visit_dates
                WHERE
                    visit_date <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) visit_date_range USING ({VisitOccurrence.person_id.key})

            /* procedure occurrence */
            FULL OUTER JOIN (
                SELECT
                    {ProcedureOccurrence.person_id.key},
                    MIN({ProcedureOccurrence.procedure_date.key}) AS minimum_procedure_date
                FROM
                    {str(ProcedureOccurrence.__table__)}
                WHERE
                    {ProcedureOccurrence.procedure_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) procedure_date_range USING ({ProcedureOccurrence.person_id.key})

            /* observation */
            FULL OUTER JOIN (
                SELECT
                    {Observation.person_id.key},
                    MIN({Observation.observation_date.key}) AS minimum_observation_date
                FROM
                    {str(Observation.__table__)}
                GROUP BY
                    1
            ) observation_date_range USING ({Observation.person_id.key})

            /* drug_exposure */
            FULL OUTER JOIN (
                SELECT
                    {DrugExposure.person_id.key},
                    MIN({DrugExposure.drug_exposure_start_date.key}) AS minimum_drug_exposure_date
                FROM
                    {str(DrugExposure.__table__)}
                WHERE
                    {DrugExposure.drug_exposure_start_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) drug_exposure_date_range USING ({DrugExposure.person_id.key})
    ) all_ranges
    WHERE
        {Person.person_id.key} in (
            SELECT
                {Person.person_id.key}
            FROM
                {str(Person.__table__)}
        )
;
SELECT COUNT(*)
FROM {str(ObservationPeriod.__table__)};
""".strip().replace("\n", "")


SQL: Final[str] = _obs_period_sql()
