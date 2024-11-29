"""clinical models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

from sqlalchemy.orm import declarative_mixin, declared_attr

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
from ..omopcdm54.health_systems import CareSite, Location, Provider
from ..omopcdm54.registry import (
    OmopCdmModelBase as ModelBase,
    register_omop_model,
)
from ..omopcdm54.vocabulary import Concept


@register_omop_model
class Person(ModelBase):
    """
    Table Description
    ---
    This table serves as the central identity management for all Persons in the database.
    It contains records that uniquely identify each person or patient, and some demographic information.

    User Guide
    ---
    All records in this table are independent Persons.

    ETL Conventions
    ---
    All Persons in a database needs one record in this table, unless they fail data
    quality requirements specified in the ETL. Persons with no Events should have a
    record nonetheless. If more than one data source contributes Events to the database,
    Persons must be reconciled, if possible, across the sources to create one single record
    per Person. The BIRTH_DATETIME must be equivalent to the content of BIRTH_DAY, BIRTH_MONTH
    and BIRTH_YEAR. There is a helpful rule listed in table below for how to derive
    BIRTH_DATETIME if it is not available in the source. New to CDM v6.0 The person’s death date
    is now stored in this table instead of the separate DEATH table. In the case that multiple
    dates of death are given in the source data the ETL should make a choice as to which death
    date to put in the PERSON table. Any additional dates can be stored in the OBSERVATION table
    using the concept 4265167 which stands for ‘Date of death’ . Similarly, the cause of death
    is stored in the CONDITION_OCCURRENCE table using the CONDITION_STATUS_CONCEPT_ID 32891
    for ‘Cause of death’.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#PERSON
    """

    __tablename__: Final[str] = "person"
    __table_concept_id__ = 1147314

    person_id: Final[Column] = IntField(primary_key=True)
    gender_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    year_of_birth: Final[Column] = IntField(nullable=False)
    month_of_birth: Final[Column] = IntField()
    day_of_birth: Final[Column] = IntField()
    birth_datetime: Final[Column] = DateField()
    race_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    ethnicity_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    location_id: Final[Column] = IntField(FK(Location.location_id))
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    care_site_id: Final[Column] = IntField(FK(CareSite.care_site_id))
    person_source_value: Final[Column] = CharField(50)
    gender_source_value: Final[Column] = CharField(50)
    gender_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    race_source_value: Final[Column] = CharField(50)
    race_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    ethnicity_source_value: Final[Column] = CharField(50)
    ethnicity_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@declarative_mixin
class PersonIdMixin:
    """Mixin for person_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def person_id(cls) -> Column:
        return IntField(FK(Person.person_id), nullable=False)


@register_omop_model
class ObservationPeriod(ModelBase, PersonIdMixin):
    """
    Table Description
    ---
    This table contains records which define spans of time during which two conditions
    are expected to hold: (i) Clinical Events that happened to the Person are recorded
    in the Event tables, and (ii) absense of records indicate such Events did not occur
    during this span of time.

    User Guide
    ---
    For each Person, one or more OBSERVATION_PERIOD records may be present, but they will
    not overlap or be back to back to each other. Events may exist outside all of the time
    spans of the OBSERVATION_PERIOD records for a patient, however, absence of an Event
    outside these time spans cannot be construed as evidence of absence of an Event.
    Incidence or prevalence rates should only be calculated for the time of active OBSERVATION_PERIOD
    records. When constructing cohorts, outside Events can be used for inclusion criteria definition,
    but without any guarantee for the performance of these criteria. Also, OBSERVATION_PERIOD records
    can be as short as a single day, greatly disturbing the denominator of any rate calculation as
    part of cohort characterizations. To avoid that, apply minimal observation time as a requirement
    for any cohort definition.

    ETL Conventions
    ---
    Each Person needs to have at least one OBSERVATION_PERIOD record, which should represent
    time intervals with a high capture rate of Clinical Events. Some source data have very
    similar concepts, such as enrollment periods in insurance claims data. In other source
    data such as most EHR systems these time spans need to be inferred under a set of assumptions.
    It is the discretion of the ETL developer to define these assumptions. In many ETL solutions
    the start date of the first occurrence or the first high quality occurrence of a Clinical
    Event (Condition, Drug, Procedure, Device, Measurement, Visit) is defined as the start of
    the OBSERVATION_PERIOD record, and the end date of the last occurrence of last high quality
    occurrence of a Clinical Event, or the end of the database period becomes the end of the
    OBSERVATOIN_PERIOD for each Person. If a Person only has a single Clinical Event the
    OBSERVATION_PERIOD record can be as short as one day. Depending on these definitions
    it is possible that Clinical Events fall outside the time spans defined by OBSERVATION_PERIOD
    records. Family history or history of Clinical Events generally are not used to generate
    OBSERVATION_PERIOD records around the time they are referring to. Any two overlapping or
    adjacent OBSERVATION_PERIOD records have to be merged into one.
    https://ohdsi.github.io/CommonDataModel/cdm54.html#OBSERVATION_PERIOD
    """

    __tablename__: Final[str] = "observation_period"
    __table_concept_id__ = 1147321

    observation_period_id: Final[Column] = IntField(primary_key=True)
    observation_period_start_date: Final[Column] = DateField(nullable=False)
    observation_period_end_date: Final[Column] = DateField(nullable=False)
    period_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


@declarative_mixin
class CareSiteIdMixin:
    """Mixin for care_site_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def care_site_id(cls) -> Column:
        return IntField(FK(CareSite.care_site_id))


@declarative_mixin
class ProviderIdMixin:
    """Mixin for provider_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def provider_id(cls) -> Column:
        return IntField(FK(Provider.provider_id))


@register_omop_model
class VisitOccurrence(ModelBase, PersonIdMixin, CareSiteIdMixin, ProviderIdMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#VISIT_OCCURRENCE"""

    __tablename__: Final[str] = "visit_occurrence"
    __table_concept_id__ = 1147332

    visit_occurrence_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    visit_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    visit_start_date: Final[Column] = DateField(nullable=False)
    visit_start_datetime: Final[Column] = DateTimeField()
    visit_end_date: Final[Column] = DateField(nullable=False)
    visit_end_datetime: Final[Column] = DateTimeField()
    visit_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    care_site_id: Final[Column] = IntField(FK(CareSite.care_site_id))
    visit_source_value: Final[Column] = CharField(50)
    visit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_source_value: Final[Column] = CharField(50)
    discharged_to_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    discharged_to_source_value: Final[Column] = CharField(50)
    preceding_visit_occurrence_id: Final[Column] = IntField(
        FK("visit_occurrence.visit_occurrence_id")
    )


class VisitOccurrenceIdMixin:
    """Mixin for visit_occurrence_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def visit_occurrence_id(cls) -> Column:
        return IntField(FK(VisitOccurrence.visit_occurrence_id))


@register_omop_model
class VisitDetail(
    ModelBase,
    PersonIdMixin,
    CareSiteIdMixin,
    ProviderIdMixin,
    VisitOccurrenceIdMixin,
):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#VISIT_DETAIL"""

    __tablename__: Final[str] = "visit_detail"
    __table_concept_id__ = 1147637

    visit_detail_id: Final[Column] = IntField(primary_key=True)
    visit_detail_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_detail_start_date: Final[Column] = DateField(nullable=False)
    visit_detail_start_datetime: Final[Column] = DateTimeField()
    visit_detail_end_date: Final[Column] = DateField(nullable=False)
    visit_detail_end_datetime: Final[Column] = DateTimeField()
    visit_detail_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    visit_detail_source_value: Final[Column] = CharField(50)
    visit_detail_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    admitted_from_source_value: Final[Column] = CharField(50)
    discharged_to_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    discharged_to_source_value: Final[Column] = CharField(50)
    preceding_visit_detail_id: Final[Column] = IntField(
        FK("visit_detail.visit_detail_id")
    )
    parent_visit_detail_id: Final[Column] = IntField(FK("visit_detail.visit_detail_id"))


@declarative_mixin
class VisitAndProviderMixin(VisitOccurrenceIdMixin, ProviderIdMixin):
    """Mixin for provider_id, visit_occurrence_id, and visit_detail_id"""

    @declared_attr
    # pylint: disable=no-self-argument
    def visit_detail_id(cls) -> Column:
        return IntField(FK(VisitDetail.visit_detail_id))


@register_omop_model
class ConditionOccurrence(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#CONDITION_OCCURRENCE"""

    __tablename__: Final[str] = "condition_occurrence"
    __table_concept_id__ = 1147333

    condition_occurrence_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    condition_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    condition_start_date: Final[Column] = DateField(nullable=False)
    condition_start_datetime: Final[Column] = DateTimeField()
    condition_end_date: Final[Column] = DateField()
    condition_end_datetime: Final[Column] = DateTimeField()
    condition_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    condition_status_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    stop_reason: Final[Column] = CharField(20)
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id), index=True
    )
    visit_detail_id: Final[Column] = IntField(FK(VisitDetail.visit_detail_id))
    condition_source_value: Final[Column] = CharField(50)
    condition_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    condition_status_source_value: Final[Column] = CharField(50)


@register_omop_model
class DrugExposure(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DRUG_EXPOSURE"""

    __tablename__: Final[str] = "drug_exposure"
    __table_concept_id__ = 1147339

    drug_exposure_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    drug_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    drug_exposure_start_date: Final[Column] = DateField(nullable=False)
    drug_exposure_start_datetime: Final[Column] = DateTimeField()
    drug_exposure_end_date: Final[Column] = DateField(nullable=False)
    drug_exposure_end_datetime: Final[Column] = DateTimeField()
    verbatim_end_date: Final[Column] = DateField()
    drug_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    stop_reason: Final[Column] = CharField(20)
    refills: Final[Column] = IntField()
    quantity: Final[Column] = NumericField()
    days_supply: Final[Column] = IntField()
    sig: Final[Column] = CharField(None)
    route_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    lot_number: Final[Column] = CharField(50)
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    visit_detail_id: Final[Column] = IntField(FK(VisitDetail.visit_detail_id))
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id), index=True
    )
    drug_source_value: Final[Column] = CharField(50)
    drug_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    route_source_value: Final[Column] = CharField(50)
    dose_unit_source_value: Final[Column] = CharField(50)


@register_omop_model
class ProcedureOccurrence(ModelBase, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#PROCEDURE_OCCURRENCE"""

    __tablename__: Final[str] = "procedure_occurrence"
    __table_concept_id__ = 1147301

    procedure_occurrence_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    procedure_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    procedure_date: Final[Column] = DateField(nullable=False)
    procedure_datetime: Final[Column] = DateTimeField()
    procedure_end_date: Final[Column] = DateField()
    procedure_end_datetime: Final[Column] = DateTimeField()
    procedure_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    modifier_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    quantity: Final[Column] = IntField()
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    visit_detail_id: Final[Column] = IntField(FK(VisitDetail.visit_detail_id))
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id), index=True
    )
    procedure_source_value: Final[Column] = CharField(50)
    procedure_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    modifier_source_value: Final[Column] = CharField(50)


@register_omop_model
class DeviceExposure(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DEVICE_EXPOSURE"""

    __tablename__: Final[str] = "device_exposure"
    __table_concept_id__ = 1147305

    device_exposure_id: Final[Column] = IntField(primary_key=True)
    device_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    device_exposure_start_date: Final[Column] = DateField(nullable=False)
    device_exposure_start_datetime: Final[Column] = DateTimeField()
    device_exposure_end_date: Final[Column] = DateField()
    device_exposure_end_datetime: Final[Column] = DateTimeField()
    device_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    unique_device_id: Final[Column] = CharField(255)
    production_id: Final[Column] = CharField(255)
    quantity: Final[Column] = IntField()
    device_source_value: Final[Column] = CharField(50)
    device_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_source_value: Final[Column] = CharField(50)
    unit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class Measurement(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#MEASUREMENT"""

    __tablename__: Final[str] = "measurement"
    __table_concept_id__ = 1147330

    measurement_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    measurement_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    measurement_date: Final[Column] = DateField(nullable=False)
    measurement_datetime: Final[Column] = DateTimeField()
    measurement_time: Final[Column] = CharField(10)
    measurement_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    operator_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    value_as_number: Final[Column] = NumericField()
    value_as_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    range_low: Final[Column] = NumericField()
    range_high: Final[Column] = NumericField()
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id), index=True
    )
    visit_detail_id: Final[Column] = IntField(FK(VisitDetail.visit_detail_id))
    measurement_source_value: Final[Column] = CharField(50)
    measurement_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_source_value: Final[Column] = CharField(50)
    unit_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    value_source_value: Final[Column] = CharField(50)
    measurement_event_id: Final[Column] = IntField()
    meas_event_field_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class Observation(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """
    Table Description
    ---
    The OBSERVATION table captures clinical facts about a Person
    obtained in the context of examination, questioning or a procedure.
    Any data that cannot be represented by any other domains, such as
    social and lifestyle facts, medical history, family history, etc.
    are recorded here. New to CDM v6.0 An Observation can now be linked
    to other records in the CDM instance using the fields
    OBSERVATION_EVENT_ID and OBS_EVENT_FIELD_CONCEPT_ID.
    To link another record to an Observation, the primary key
    goes in OBSERVATION_EVENT_ID (CONDITION_OCCURRENCE_ID,
    DRUG_EXPOSURE_ID, etc.) and the Concept representing the
    field where the OBSERVATION_EVENT_ID was taken from go in
    the OBS_EVENT_FIELD_CONCEPT_ID. For example, a CONDITION_OCCURRENCE
    of Asthma might be linked to an Observation of a family history of
    Asthma. In this case the CONDITION_OCCURRENCE_ID of the Asthma
    record would go in OBSERVATION_EVENT_ID of the family history
    record and the CONCEPT_ID 1147127 would go in
    OBS_EVENT_FIELD_CONCEPT_ID to denote that the OBSERVATION_EVENT_ID
    represents a CONDITION_OCCURRENCE_ID.

    User Guide
    ---
    Observations differ from Measurements in that they do not
    require a standardized test or some other activity to
    generate clinical fact. Typical observations are medical history,
    family history, the stated need for certain treatment, social
    circumstances, lifestyle choices, healthcare utilization patterns,
    etc. If the generation clinical facts requires a standardized testing
    such as lab testing or imaging and leads to a standardized result,
    the data item is recorded in the MEASUREMENT table. If the clinical
    fact observed determines a sign, symptom, diagnosis of a disease
    or other medical condition, it is recorded in the CONDITION_OCCURRENCE
    table. Valid Observation Concepts are not enforced to be from any
    DOMAIN though they still should be Standard Concepts.

    ETL Conventions
    ---
    Records whose Source Values map to any domain besides Condition,
    Procedure, Drug, Measurement or Device should be stored in the
    Observation table. Observations can be stored as attribute value
    pairs, with the attribute as the Observation Concept and the value
    representing the clinical fact. This fact can be a Concept
    (stored in VALUE_AS_CONCEPT), a numerical value (VALUE_AS_NUMBER),
    a verbatim string (VALUE_AS_STRING), or a datetime (VALUE_AS_DATETIME).
    Even though Observations do not have an explicit result,
    the clinical fact can be stated separately from the type of
    Observation in the VALUE_AS_* fields. It is recommended for
    Observations that are suggestive statements of positive assertion
    should have a value of ‘Yes’ (concept_id=4188539), recorded,
    even though the null value is the equivalent.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#OBSERVATION
    """

    __tablename__: Final[str] = "observation"
    __table_concept_id__ = 1147304

    observation_id: Final[Column] = IntField(primary_key=True)
    person_id: Final[Column] = IntField(
        FK(Person.person_id, ondelete="CASCADE"), nullable=False, index=True
    )
    observation_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    observation_date: Final[Column] = DateField(nullable=False)
    observation_datetime: Final[Column] = DateTimeField()
    observation_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    value_as_number: Final[Column] = NumericField()
    value_as_string: Final[Column] = CharField(60)
    value_as_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    qualifier_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    provider_id: Final[Column] = IntField(FK(Provider.provider_id))
    visit_occurrence_id: Final[Column] = IntField(
        FK(VisitOccurrence.visit_occurrence_id)
    )
    visit_detail_id: Final[Column] = IntField(FK(VisitDetail.visit_detail_id))
    observation_source_value: Final[Column] = CharField(50)
    observation_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    unit_source_value: Final[Column] = CharField(50)
    qualifier_source_value: Final[Column] = CharField(50)
    value_source_value: Final[Column] = CharField(50)
    observation_event_id: Final[Column] = IntField()
    obs_event_field_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class Death(ModelBase, PersonIdMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#DEATH"""

    __tablename__: Final[str] = "death"
    __table_concept_id__ = 1147312

    person_id: Final[Column] = IntField(FK(Person.person_id), primary_key=True)
    death_date: Final[Column] = DateField(nullable=False)
    death_datetime: Final[Column] = DateTimeField()
    death_type_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    cause_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    cause_source_value: Final[Column] = CharField(50)
    cause_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class Note(ModelBase, PersonIdMixin, VisitAndProviderMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#NOTE"""

    __tablename__: Final[str] = "note"
    __table_concept_id__ = 1147317

    note_id: Final[Column] = IntField(primary_key=True)
    note_date: Final[Column] = DateField(nullable=False)
    note_datetime: Final[Column] = DateTimeField()
    note_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_class_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_title: Final[Column] = CharField(250)
    note_text: Final[Column] = CharField(None)
    encoding_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    language_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    note_source_value: Final[Column] = CharField(50)
    note_event_id: Final[Column] = IntField()
    note_event_field_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class NoteNlp(ModelBase):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#NOTE_NLP"""

    __tablename__: Final[str] = "note_nlp"
    __table_concept_id__ = 1147542

    note_nlp_id: Final[Column] = IntField(primary_key=True)
    note_id: Final[Column] = IntField(nullable=False)
    section_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    snippet: Final[Column] = CharField(250)
    offset: Final[Column] = CharField(250, key='"offset"')
    lexical_variant: Final[Column] = CharField(250, nullable=False)
    note_nlp_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    note_nlp_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    nlp_system: Final[Column] = CharField(250)
    nlp_date: Final[Column] = DateField(nullable=False)
    nlp_datetime: Final[Column] = DateTimeField()
    term_exists: Final[Column] = CharField(1)
    term_temporal: Final[Column] = CharField(50)
    term_modifiers: Final[Column] = CharField(2000)


@register_omop_model
class Specimen(ModelBase):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#SPECIMEN"""

    __tablename__: Final[str] = "specimen"
    __table_concept_id__ = 1147306

    specimen_id: Final[Column] = IntField(primary_key=True)
    specimen_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    specimen_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    specimen_date: Final[Column] = DateField(nullable=False)
    specimen_datetime: Final[Column] = DateTimeField()
    quantity: Final[Column] = NumericField()
    unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    anatomic_site_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    disease_status_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    specimen_source_id: Final[Column] = CharField(50)
    specimen_source_value: Final[Column] = CharField(50)
    unit_source_value: Final[Column] = CharField(50)
    anatomic_site_source_value: Final[Column] = CharField(50)
    disease_status_source_value: Final[Column] = CharField(50)


@register_omop_model
class FactRelationship(ModelBase, PKIdMixin):
    """https://ohdsi.github.io/CommonDataModel/cdm54.html#FACT_RELATIONSHIP"""

    __tablename__: Final[str] = "fact_relationship"
    __table_concept_id__ = 1147320

    domain_concept_id_1: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    fact_id_1: Final[Column] = IntField(nullable=False)
    domain_concept_id_2: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    fact_id_2: Final[Column] = IntField(nullable=False)
    relationship_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


__all__ = [
    "Person",
    "ObservationPeriod",
    "VisitOccurrence",
    "VisitDetail",
    "ConditionOccurrence",
    "DrugExposure",
    "ProcedureOccurrence",
    "DeviceExposure",
    "Measurement",
    "Observation",
    "Death",
    "Note",
    "NoteNlp",
    "Specimen",
    "FactRelationship",
]
