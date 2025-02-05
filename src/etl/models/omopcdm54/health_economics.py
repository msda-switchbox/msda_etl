"""health economics models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Final

from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    IntField,
    NumericField,
)
from ..omopcdm54.clinical import PersonIdMixin
from ..omopcdm54.registry import (
    OmopCdmModelBase as ModelBase,
    register_omop_model,
)
from ..omopcdm54.vocabulary import Concept, Domain


@register_omop_model
class PayerPlanPeriod(ModelBase, PersonIdMixin):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#PAYER_PLAN_PERIOD
    """

    __tablename__: Final[str] = "payer_plan_period"
    __table_concept_id__ = 1147327

    payer_plan_period_id: Final[Column] = IntField(primary_key=True)
    payer_plan_period_start_date: Final[Column] = DateField(nullable=False)
    payer_plan_period_end_date: Final[Column] = DateField(nullable=False)
    payer_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    payer_source_value: Final[Column] = CharField(50)
    payer_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    plan_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    plan_source_value: Final[Column] = CharField(50)
    plan_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    sponsor_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    sponsor_source_value: Final[Column] = CharField(50)
    sponsor_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    family_source_value: Final[Column] = CharField(50)
    stop_reason_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    stop_reason_source_value: Final[Column] = CharField(50)
    stop_reason_source_concept_id: Final[Column] = IntField(FK(Concept.concept_id))


@register_omop_model
class Cost(ModelBase):
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#COST
    """

    __tablename__: Final[str] = "cost"
    __table_concept_id__ = 1147366

    cost_id: Final[Column] = IntField(primary_key=True)
    cost_event_id: Final[Column] = IntField(nullable=False)
    cost_domain_id: Final[Column] = CharField(20, FK(Domain.domain_id), nullable=False)
    cost_type_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    currency_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    total_charge: Final[Column] = NumericField()
    total_cost: Final[Column] = NumericField()
    total_paid: Final[Column] = NumericField()
    paid_by_payer: Final[Column] = NumericField()
    paid_by_patient: Final[Column] = NumericField()
    paid_patient_copay: Final[Column] = NumericField()
    paid_patient_coinsurance: Final[Column] = NumericField()
    paid_patient_deductible: Final[Column] = NumericField()
    paid_by_primary: Final[Column] = NumericField()
    paid_ingredient_cost: Final[Column] = NumericField()
    paid_dispensing_fee: Final[Column] = NumericField()
    payer_plan_period_id: Final[Column] = IntField()
    amount_allowed: Final[Column] = NumericField()
    revenue_code_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    revenue_code_source_value: Final[Column] = CharField(50)
    drg_concept_id: Final[Column] = IntField(FK(Concept.concept_id))
    drg_source_value: Final[Column] = CharField(3)


__all__ = ["PayerPlanPeriod", "Cost"]
