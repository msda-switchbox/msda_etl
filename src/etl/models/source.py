"""source models module"""

# pylint: disable=too-many-lines
# pylint: disable=invalid-name
import os
from typing import Any, Dict, Final, List

from sqlalchemy import Column

from ..models.modelutils import (
    CharField,
    DateField,
    FloatField,
    IntField,
    PKIdMixin,
    make_model_base,
)

SOURCE_SCHEMA: Final[str] = os.environ.get("DB_SRC_SCHEMA", "source")
SourceModelBase: Final[Any] = make_model_base(schema=SOURCE_SCHEMA)


class SourceModelRegistry:
    """A simple global registry for source tables"""

    __shared_state: Dict[str, Dict[str, Any]] = {"registered": {}}

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state


def register_source_model(cls: Any) -> Any:
    """Class decorator to add model to registry"""
    borg = SourceModelRegistry()
    # pylint: disable=no-member
    borg.registered[cls.__name__] = cls
    return cls


@register_source_model
class DiseaseHistory(SourceModelBase, PKIdMixin):
    """
    The disease_history source database
    """

    __tablename__: Final[str] = "disease_history"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    date_diagnosis: Final[Column] = DateField(name="date_diagnosis")
    date_onset: Final[Column] = DateField(name="date_onset")
    csf_olib: Final[Column] = CharField(7, name="csf_olib")
    ms_course: Final[Column] = CharField(4, name="ms_course")


@register_source_model
class Npt(SourceModelBase, PKIdMixin):
    """
    The npt source database
    """

    __tablename__: Final[str] = "npt"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    np_treat_type: Final[Column] = CharField(20, name="np_treat_type")
    np_treat_start: Final[Column] = DateField(name="np_treat_start")
    np_treat_stop: Final[Column] = DateField(name="np_treat_stop")


@register_source_model
class Comorbidities(SourceModelBase, PKIdMixin):
    """
    The comorbidities source database
    """

    __tablename__: Final[str] = "comorbidities"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    comorbidity: Final[Column] = CharField(7, name="comorbidity")
    com_type: Final[Column] = CharField(20, name="com_type")
    com_system: Final[Column] = CharField(10, name="com_system")


@register_source_model
class DiseaseStatus(SourceModelBase, PKIdMixin):
    """
    The disease_status source database
    """

    __tablename__: Final[str] = "disease_status"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    ms_status_clin: Final[Column] = CharField(19, name="ms_status_clin")
    ms_status_pat: Final[Column] = CharField(15, name="ms_status_pat")
    edss_score: Final[Column] = FloatField(name="edss_score")
    pdds_score: Final[Column] = IntField(name="pdds_score")
    t25fw: Final[Column] = FloatField(name="t25fw")
    ninehpt_right: Final[Column] = FloatField(name="ninehpt_right")
    ninehpt_left: Final[Column] = FloatField(name="ninehpt_left")
    vib_sense: Final[Column] = CharField(19, name="vib_sense")
    sdmt: Final[Column] = IntField(name="sdmt")


@register_source_model
class Relapses(SourceModelBase, PKIdMixin):
    """
    The relapses source database
    """

    __tablename__: Final[str] = "relapses"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    relapse: Final[Column] = CharField(7, name="relapse")
    date_relapse: Final[Column] = DateField(name="date_relapse")
    relapse_treat: Final[Column] = CharField(3, name="relapse_treat")
    relapse_recovery: Final[Column] = CharField(16, name="relapse_recovery")


@register_source_model
class Mri(SourceModelBase, PKIdMixin):
    """
    The mri source database
    """

    __tablename__: Final[str] = "mri"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    mri: Final[Column] = CharField(3, name="mri")
    mri_region: Final[Column] = CharField(12, name="mri_region")
    mri_date: Final[Column] = DateField(name="mri_date")
    mri_gd_les: Final[Column] = IntField(name="mri_gd_les")
    mri_new_les_t1: Final[Column] = IntField(name="mri_new_les_t1")
    mri_new_les_t2: Final[Column] = IntField(name="mri_new_les_t2")


@register_source_model
class Dmt(SourceModelBase, PKIdMixin):
    """
    The dmt source database
    """

    __tablename__: Final[str] = "dmt"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    dmt_status: Final[Column] = CharField(9, name="dmt_status")
    dmt_type: Final[Column] = CharField(21, name="dmt_type")
    dmt_start: Final[Column] = DateField(name="dmt_start")
    dmt_stop: Final[Column] = DateField(name="dmt_stop")
    dmt_stop_reas: Final[Column] = CharField(21, name="dmt_stop_reas")


@register_source_model
class Patient(SourceModelBase, PKIdMixin):
    """
    The patient source database
    """

    __tablename__: Final[str] = "patient"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(primary_key=True, name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    date_birth: Final[Column] = DateField(name="date_birth")
    sex: Final[Column] = CharField(6, name="sex")
    residence: Final[Column] = CharField(12, name="residence")
    race_ethnicity: Final[Column] = CharField(5, name="race_ethnicity")
    education: Final[Column] = CharField(7, name="education")
    employment: Final[Column] = CharField(17, name="employment")
    smoking: Final[Column] = CharField(15, name="smoking")
    smoking_count: Final[Column] = IntField(name="smoking_count")
    ms_family: Final[Column] = CharField(7, name="ms_family")


@register_source_model
class Symptom(SourceModelBase, PKIdMixin):
    """
    The symptom source data
    """

    __tablename__: Final[str] = "symptom"
    __table_args__ = {"schema": SOURCE_SCHEMA}

    patient_id: Final[Column] = IntField(primary_key=True, name="patient_id")
    date_visit: Final[Column] = DateField(name="date_visit")
    current_symptom: Final[Column] = CharField(15, name="current_symptom")
    sever_symp: Final[Column] = IntField(name="sever_symp")
    treat_symp: Final[Column] = CharField(3, name="treat_symp")


# pylint: disable=no-member
SOURCE_MODELS: Final[Dict[str, SourceModelBase]] = (  # type: ignore
    SourceModelRegistry().registered
)

# pylint: disable=no-member
SOURCE_MODELS_FILENAME_KEY: Final[Dict] = dict(
    (v.__tablename__, v) for k, v in SOURCE_MODELS.items()
)

# pylint: disable=no-member
SOURCE_MODEL_NAMES: Final[List[str]] = [
    k for k, _ in SourceModelRegistry().registered.items()
]
