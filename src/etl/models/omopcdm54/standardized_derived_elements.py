"""health economics models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

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
from .clinical import Person, PersonIdMixin
from .registry import OmopCdmModelBase as ModelBase, register_omop_model
from .vocabulary import Concept


@register_omop_model
class DrugEra(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#DRUG_ERA
    """

    __tablename__: Final[str] = "drug_era"
    __table_concept_id__ = 1147303

    drug_era_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id), nullable=False, index=True
    )
    drug_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False, index=True
    )
    drug_era_start_date: Final[Column] = DateField(nullable=False)
    drug_era_end_date: Final[Column] = DateField(nullable=False)
    drug_exposure_count: Final[Column] = IntField()
    gap_days: Final[Column] = IntField()


@register_omop_model
class DoseEra(ModelBase, PersonIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#DOSE_ERA
    """

    __tablename__: Final[str] = "dose_era"
    __table_concept_id__ = 1147324

    dose_era_id: Final[Column] = IntField(primary_key=True)
    drug_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    dose_value: Final[Column] = NumericField(nullable=False)
    dose_era_start_date: Final[Column] = DateField(nullable=False)
    dose_era_end_date: Final[Column] = DateField(nullable=False)


@register_omop_model
class ConditionEra(ModelBase, PersonIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONDITION_ERA
    """

    __tablename__: Final[str] = "condition_era"
    __table_concept_id__ = 1147329

    condition_era_id: Final[Column] = IntField(primary_key=True)
    condition_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    condition_era_start_date: Final[Column] = DateField(nullable=False)
    condition_era_end_date: Final[Column] = DateField(nullable=False)
    condition_occurrence_count: Final[Column] = IntField()


@register_omop_model
class Episode(ModelBase, PersonIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#EPISODE
    """

    __tablename__: Final[str] = "episode"
    __table_concept_id__ = 756347

    episode_id: Final[Column] = IntField(primary_key=True)
    episode_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    episode_start_date: Final[Column] = DateField(nullable=False)
    episode_start_datetime: Final[Column] = DateTimeField()
    episode_end_date: Final[Column] = DateField()
    episode_end_datetime: Final[Column] = DateTimeField()
    episode_parent_id: Final[Column] = IntField()
    episode_number: Final[Column] = IntField()
    episode_object_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    episode_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    episode_source_value: Final[Column] = CharField(50)
    episode_source_concept_id: Final[Column] = IntField()


@register_omop_model
class EpisodeEvent(ModelBase, PKIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#EPISODE_EVENT
    """

    __tablename__: Final[str] = "episode_event"
    __table_concept_id__ = 756348

    episode_id: Final[Column] = IntField(FK(Episode.episode_id), nullable=False)
    event_id: Final[Column] = IntField(nullable=False)
    episode_event_field_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


__all__ = ["DrugEra", "DoseEra", "ConditionEra", "Episode", "EpisodeEvent"]
