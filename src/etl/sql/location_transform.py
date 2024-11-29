"""SQL query string definition for the location transformation"""

from typing import Final

from ..common import CONCEPT_ID_NOT_KNOWN
from ..models.lookupmodels import ConceptLookup
from ..models.omopcdm54.health_systems import Location
from ..models.source import Patient

SQL: Final[str] = f"""
INSERT INTO {str(Location.__table__)}
(
    {Location.address_1.key},
    {Location.address_2.key},
    {Location.city.key},
    {Location.state.key},
    {Location.zip.key},
    {Location.county.key},
    {Location.location_source_value.key},
    {Location.country_concept_id.key},
    {Location.country_source_value.key}
)
SELECT DISTINCT
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    p.{Patient.residence.key},
    (CASE
        WHEN c.{ConceptLookup.standard_concept_id.key} IS NOT NULL THEN c.{ConceptLookup.standard_concept_id.key}
        ELSE {CONCEPT_ID_NOT_KNOWN}
    END),
    RIGHT({Patient.residence.key}, 2)
FROM {str(Patient.__table__)} p
LEFT JOIN {str(ConceptLookup.__table__)} c
    ON LOWER(RIGHT(p.{Patient.residence.key}, 2)) = LOWER(c.{ConceptLookup.concept_string.key})
         AND LOWER(c.{ConceptLookup.filter.key}) = 'location';

SELECT COUNT(*)
FROM {str(Location.__table__)};
""".strip().replace("\n", " ")
