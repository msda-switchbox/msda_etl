"""health economics models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final


# pylint: disable=duplicate-code
from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    DateTimeField,
    IntField,
    NumericField,
    PKIdMixin,
)
from .registry import OmopCdmModelBase as ModelBase, register_omop_model
from .vocabulary import Concept


@register_omop_model
class Metadata(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#METADATA
    """

    __tablename__: Final[str] = "metadata"
    __table_concept_id__ = 1147636

    metadata_id: Final[Column] = IntField(primary_key=True)
    metadata_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    metadata_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    name: Final[Column] = CharField(250, nullable=False)
    value_as_string: Final[Column] = CharField(250)
    value_as_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    value_as_number: Final[Column] = NumericField()
    metadata_date: Final[Column] = DateField()
    metadata_datetime: Final[Column] = DateTimeField()


@register_omop_model
class CDMSource(ModelBase, PKIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#CDM_SOURCE
    """

    __tablename__: Final[str] = "cdm_source"
    __table_concept_id__ = 1147325

    cdm_source_name: Final[Column] = CharField(255, nullable=False)
    cdm_source_abbreviation: Final[Column] = CharField(25, nullable=False)
    cdm_holder: Final[Column] = CharField(255, nullable=False)
    source_description: Final[Column] = CharField(None)
    source_documentation_reference: Final[Column] = CharField(255)
    cdm_etl_reference: Final[Column] = CharField(255)
    source_release_date: Final[Column] = DateField(nullable=False)
    cdm_release_date: Final[Column] = DateField(nullable=False)
    cdm_version: Final[Column] = CharField(10)
    cdm_version_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    vocabulary_version: Final[Column] = CharField(20, nullable=False)


__all__ = ["Metadata", "CDMSource"]
