"""vocabulary models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    IntField,
    NumericField,
    PKIdMixin,
)
from .registry import OmopCdmModelBase as ModelBase


# do not register vocab models
# @register_analytical_model


class Concept(ModelBase):
    """
    The Standardized Vocabularies contains records, or Concepts, that uniquely
    identify each fundamental unit of meaning used to express clinical information
    in all domain tables of the CDM. Concepts are derived from vocabularies, which
    represent clinical information across a domain (e.g. conditions, drugs, procedures)
    through the use of codes and associated descriptions. Some Concepts are designated
    Standard Concepts, meaning these Concepts can be used as normative expressions of a
    clinical entity within the OMOP Common Data Model and within standardized analytics.
    Each Standard Concept belongs to one domain, which defines the location where the
    Concept would be expected to occur within data tables of the CDM.

    Concepts can represent broad categories (like ‘Cardiovascular disease’),
    detailed clinical elements (‘Myocardial infarction of the anterolateral wall’)
    or modifying characteristics and attributes that define Concepts at various levels
    of detail (severity of a disease, associated morphology, etc.).

    Records in the Standardized Vocabularies tables are derived from national or
    international vocabularies such as SNOMED-CT, RxNorm, and LOINC, or custom
    Concepts defined to cover various aspects of observational data analysis.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT
    """

    __tablename__: Final[str] = "concept"
    __table_concept_id__ = 1147311

    concept_id: Final[Column] = IntField(primary_key=True)
    concept_name: Final[Column] = CharField(255, nullable=False)
    domain_id: Final[Column] = CharField(20, FK("domain.domain_id"), nullable=False)
    vocabulary_id: Final[Column] = CharField(
        20, FK("vocabulary.vocabulary_id"), nullable=False
    )
    concept_class_id: Final[Column] = CharField(
        20, FK("concept_class.concept_class_id"), nullable=False
    )
    standard_concept: Final[Column] = CharField(1)
    concept_code: Final[Column] = CharField(50, nullable=False)
    valid_start_date: Final[Column] = DateField(nullable=False)
    valid_end_date: Final[Column] = DateField(nullable=False)
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model


class Vocabulary(ModelBase):
    """
    The VOCABULARY table includes a list of the Vocabularies collected from
    various sources or created de novo by the OMOP community. This reference
    table is populated with a single record for each Vocabulary source and
    includes a descriptive name and other associated attributes for the Vocabulary.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY
    """

    __tablename__: Final[str] = "vocabulary"
    __table_concept_id__ = 1147318

    vocabulary_id: Final[Column] = CharField(20, primary_key=True)
    vocabulary_name: Final[Column] = CharField(255, nullable=False)
    vocabulary_reference: Final[Column] = CharField(255)
    vocabulary_version: Final[Column] = CharField(255)
    vocabulary_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model


class Domain(ModelBase):
    """
    The DOMAIN table includes a list of OMOP-defined Domains the
    Concepts of the Standardized Vocabularies can belong to.
    A Domain defines the set of allowable Concepts for the standardized fields in the CDM tables.
    For example, the “Condition” Domain contains Concepts that describe a condition of a patient,
    and these Concepts can only be stored in the condition_concept_id field of the CONDITION_OCCURRENCE
    and CONDITION_ERA tables. This reference table is populated with a single record for each Domain
    and includes a descriptive name for the Domain.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#DOMAIN
    """

    __tablename__: Final[str] = "domain"
    __table_concept_id__ = 1147310

    domain_id: Final[Column] = CharField(20, primary_key=True)
    domain_name: Final[Column] = CharField(255, nullable=False)
    domain_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)


# do not register vocab models
# @register_analytical_model


class ConceptClass(ModelBase):
    """
    The CONCEPT_CLASS table is a reference table, which includes a list of the
    classifications used to differentiate Concepts within a given Vocabulary.
    This reference table is populated with a single record for each Concept Class.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_CLASS
    """

    __tablename__: Final[str] = "concept_class"
    __table_concept_id__ = 1147322

    concept_class_id: Final[Column] = CharField(20, primary_key=True)
    concept_class_name: Final[Column] = CharField(255, nullable=False)
    concept_class_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model


class ConceptRelationship(ModelBase):
    """
    The CONCEPT_RELATIONSHIP table contains records that define direct
    relationships between any two Concepts and the nature or type of the relationship.
    Each type of a relationship is defined in the RELATIONSHIP table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_RELATIONSHIP
    """

    __tablename__: Final[str] = "concept_relationship"
    __table_concept_id__ = 1147336

    concept_id_1: Final[Column] = IntField(
        FK(Concept.concept_id), primary_key=True, nullable=False
    )
    concept_id_2: Final[Column] = IntField(
        FK(Concept.concept_id), primary_key=True, nullable=False
    )
    relationship_id: Final[Column] = CharField(
        20, FK("relationship.relationship_id"), primary_key=True, nullable=False
    )
    valid_start_date: Final[Column] = DateField(nullable=False)
    valid_end_date: Final[Column] = DateField(nullable=False)
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model


class Relationship(ModelBase):
    """
    The RELATIONSHIP table provides a reference list of all types of
    relationships that can be used to associate any two concepts in
    the CONCEPT_RELATIONSHP table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#RELATIONSHIP
    """

    __tablename__: Final[str] = "relationship"
    __table_concept_id__ = 1147307

    relationship_id: Final[Column] = CharField(20, primary_key=True)
    relationship_name: Final[Column] = CharField(255, nullable=False)
    is_hierarchical: Final[Column] = CharField(1, nullable=False)
    defines_ancestry: Final[Column] = CharField(1, nullable=False)
    reverse_relationship_id: Final[Column] = CharField(20, nullable=False)
    relationship_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model


class ConceptSynonym(ModelBase, PKIdMixin):
    """
    The CONCEPT_SYNONYM table is used to store alternate names and descriptions for Concepts.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_SYNONYM
    """

    __tablename__: Final[str] = "concept_synonym"
    __table_concept_id__ = 1147308

    concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    concept_synonym_name: Final[Column] = CharField(1000, nullable=False)
    language_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model


class ConceptAncestor(ModelBase, PKIdMixin):
    """
    The CONCEPT_ANCESTOR table is designed to simplify observational analysis by
    providing the complete hierarchical relationships between Concepts.
    Only direct parent-child relationships between Concepts are stored
    in the CONCEPT_RELATIONSHIP table.
    To determine higher level ancestry connections, all individual direct
    relationships would have to be navigated at analysis time.
    The CONCEPT_ANCESTOR table includes records for all parent-child relationships,
    as well as grandparent-grandchild relationships and those of any other level of lineage.
    Using the CONCEPT_ANCESTOR table allows for querying for all descendants of a hierarchical concept.
    For example, drug ingredients and drug products are all descendants of a drug class ancestor.

    This table is entirely derived from the CONCEPT, CONCEPT_RELATIONSHIP and RELATIONSHIP tables.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_ANCESTOR
    """

    __tablename__: Final[str] = "concept_ancestor"
    __table_concept_id__ = 1147331

    ancestor_concept_id: Final[Column] = IntField(
        FK("concept.concept_id"), nullable=False
    )
    descendant_concept_id: Final[Column] = IntField(
        FK("concept.concept_id"), nullable=False
    )
    min_levels_of_separation: Final[Column] = IntField(nullable=False)
    max_levels_of_separation: Final[Column] = IntField(nullable=False)


# do not register vocab models
# @register_analytical_model


class SourceToConceptMap(ModelBase, PKIdMixin):
    """
    The source to concept map table is a legacy data structure within the OMOP Common Data Model,
    recommended for use in ETL processes to maintain local source codes which are not available
    as Concepts in the Standardized Vocabularies, and to establish mappings for each source code
    into a Standard Concept as target_concept_ids that can be used to populate the Common Data
    Model tables. The SOURCE_TO_CONCEPT_MAP table is no longer populated with content within the
    Standardized Vocabularies published to the OMOP community.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#SOURCE_TO_CONCEPT_MAP
    """

    __tablename__: Final[str] = "source_to_concept_map"
    __table_concept_id__ = 1147334

    source_code: Final[Column] = CharField(50, nullable=False)
    source_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    source_vocabulary_id: Final[Column] = CharField(20, nullable=False)
    source_code_description: Final[Column] = CharField(255)
    target_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    target_vocabulary_id: Final[Column] = CharField(20, nullable=False)
    valid_start_date: Final[Column] = DateField(nullable=False)
    valid_end_date: Final[Column] = DateField(nullable=False)
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model


class DrugStrength(ModelBase, PKIdMixin):
    """
    The DRUG_STRENGTH table contains structured content about the amount
    or concentration and associated units of a specific ingredient
    contained within a particular drug product. This table is supplemental
    information to support standardized analysis of drug utilization.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#DRUG_STRENGTH
    """

    __tablename__: Final[str] = "drug_strength"
    __table_concept_id__ = 1147337

    drug_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    ingredient_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    amount_value: Final[Column] = NumericField()
    amount_unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    numerator_value: Final[Column] = NumericField()
    numerator_unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    denominator_value: Final[Column] = NumericField()
    denominator_unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    box_size: Final[Column] = IntField()
    valid_start_date: Final[Column] = DateField(nullable=False)
    valid_end_date: Final[Column] = DateField(nullable=False)
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model


class Cohort(ModelBase, PKIdMixin):
    """
    Table Description
    ---
    The COHORT table contains records of subjects that satisfy a given set of criteria for a
    duration of time. The definition of the cohort is contained within the COHORT_DEFINITION
    table. It is listed as part of the RESULTS schema because it is a table that users of the
    database as well as tools such as ATLAS need to be able to write to. The CDM and Vocabulary
    tables are all read-only so it is suggested that the COHORT and COHORT_DEFINTION tables are
    kept in a separate schema to alleviate confusion.

    ETL Conventions
    ---
    Cohorts typically include patients diagnosed with a specific condition, patients exposed to
    a particular drug, but can also be Providers who have performed a specific Procedure. Cohort
    records must have a Start Date and an End Date, but the End Date may be set to Start Date or
    could have an applied censor date using the Observation Period Start Date. Cohort records must
    contain a Subject Id, which can refer to the Person, Provider, Visit record or Care Site though
    they are most often Person Ids. The Cohort Definition will define the type of subject through
    the subject concept id. A subject can belong (or not belong) to a cohort at any moment in time.
    A subject can only have one record in the cohort table for any moment of time, i.e. it is not
    possible for a person to contain multiple records indicating cohort membership that are
    overlapping in time.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#COHORT
    """

    __tablename__: Final[str] = "cohort"
    __table_concept_id__ = 756346

    cohort_definition_id: Final[Column] = IntField(nullable=False)
    subject_id: Final[Column] = IntField(nullable=False)

    # TO-DO: Implement the constraint that no subject_id in this table
    # can have overlapping periods in time
    cohort_start_date: Final[Column] = DateField(nullable=False)
    cohort_end_date: Final[Column] = DateField(nullable=False)


# do not register vocab models
# @register_analytical_model


class CohortDefinition(ModelBase, PKIdMixin):
    """
    Table Description
    ---
    The COHORT_DEFINITION table contains records defining a Cohort derived
    from the data through the associated description and syntax and upon
    instantiation (execution of the algorithm) placed into the COHORT table.
    Cohorts are a set of subjects that satisfy a given combination of inclusion
    criteria for a duration of time. The COHORT_DEFINITION table provides a
    standardized structure for maintaining the rules governing the inclusion of
    a subject into a cohort, and can store operational programming code to instantiate
    the cohort within the OMOP Common Data Model.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#COHORT_DEFINITION
    """

    __tablename__: Final[str] = "cohort_definition"
    __table_concept_id__ = 1147323

    cohort_definition_id: Final[Column] = IntField(nullable=False)
    cohort_definition_name: Final[Column] = CharField(255, nullable=False)
    cohort_definition_description: Final[Column] = CharField(None)
    definition_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    cohort_definition_syntax: Final[Column] = CharField(None)
    subject_concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)
    cohort_initiation_date: Final[Column] = DateField()


__all__ = [
    "Concept",
    "Vocabulary",
    "Domain",
    "ConceptClass",
    "ConceptRelationship",
    "Relationship",
    "ConceptSynonym",
    "ConceptAncestor",
    "SourceToConceptMap",
    "DrugStrength",
    "Cohort",
    "CohortDefinition",
]
