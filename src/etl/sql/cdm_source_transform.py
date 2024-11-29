"""SQL query string definition for the cdm_source transformation"""

import datetime

from ..config import ETLConf
from ..models.omopcdm54.metadata import CDMSource
from ..models.omopcdm54.registry import TARGET_SCHEMA
from ..transform.transformutils import try_parsing_date
from ..util.etl_reference import get_cdm_etl_reference


def get_transform_sql(config: ETLConf) -> str:
    source_release_date_formatted = try_parsing_date(config.source_release_date)
    etl_reference = get_cdm_etl_reference(config.cdm_etl_ref)
    return f"""
INSERT INTO {str(CDMSource.__table__)}
(
    {CDMSource.cdm_source_name.key},
    {CDMSource.cdm_source_abbreviation.key},
    {CDMSource.cdm_holder.key},
    {CDMSource.source_description.key},
    {CDMSource.source_documentation_reference.key},
    {CDMSource.cdm_etl_reference.key},
    {CDMSource.source_release_date.key},
    {CDMSource.cdm_release_date.key},
    {CDMSource.cdm_version.key},
    {CDMSource.cdm_version_concept_id.key},
    {CDMSource.vocabulary_version.key}
)

SELECT
    '{config.cdm_source_name}',
    '{config.cdm_source_abbreviation}',
    '{config.cdm_holder}',
    '{config.source_description}',
    '{config.source_doc_reference}',
    '{etl_reference}',
    '{source_release_date_formatted}',
    '{str(datetime.date.today())}',
    '5.4.1',
    '798878',
    vocabulary_version from {TARGET_SCHEMA}.vocabulary where vocabulary_id = 'None'
;

SELECT COUNT(*), vocabulary_version
FROM {str(CDMSource.__table__)}
GROUP BY vocabulary_version;
""".strip().replace("\n", " ")
