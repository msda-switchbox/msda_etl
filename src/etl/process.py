"""Run the ETL and supporting classes for transformations"""

# pylint: disable=unused-import
import importlib.resources
import logging
import time
from typing import Callable, Dict, TypeAlias

from sqlalchemy.engine import Connection

from .config import ETLConf
from .context import ETLContext
from .loader import CSVFileLoader
from .models.lookupmodels import LOOKUP_MODELS
from .models.omopcdm54.clinical import (
    ConditionOccurrence,
    DrugExposure,
    Measurement,
    Observation,
    ObservationPeriod,
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)
from .models.omopcdm54.health_systems import Location
from .models.omopcdm54.metadata import CDMSource
from .models.omopcdm54.standardized_derived_elements import (
    ConditionEra,
    DrugEra,
)
from .models.source import SOURCE_MODELS
from .transform import (
    cdm_source,
    condition,
    condition_era,
    create_logger_tables,
    create_lookup_tables,
    create_omopcdm_tables,
    create_source_tables,
    drug_era,
    drug_exposure,
    location,
    measurement,
    observation,
    observation_period,
    person,
    preprocessing,
    procedure,
    reload_vocab,
    visit_occurrence,
)
from .transform.etl_summary import ModelSummary, print_models_summary

logger = logging.getLogger(__name__)
CSV_DIR = importlib.resources.files("etl.csv")

StepsDict: TypeAlias = Dict[str, Callable[[ETLContext], None]]


def run_transformations(steps: StepsDict, ctxt: ETLContext):
    """Run the transformations"""
    for i, (stepname, func) in enumerate(steps.items()):
        logged_name = f"{stepname} ({func.__module__!r})"
        ctxt.log_big("step %s: %s start", i, logged_name)
        dur = time.time()
        func(ctxt)
        dur = time.time() - dur
        logger.info("step %s: %s done in %ss", i, logged_name, dur)


def run_etl(
    config: ETLConf,
    cnxn: Connection,
) -> None:
    """Run the full ETL and all transformations"""

    source_loader = CSVFileLoader(
        config.datadir,
        SOURCE_MODELS,
        delimiter=config.input_delimiter,
    )
    lookup_loader = CSVFileLoader(
        CSV_DIR,
        LOOKUP_MODELS,
        delimiter=config.lookup_delimiter,
    )

    lookup_loader.load()
    source_loader.load()
    ctxt = ETLContext(
        config,
        cnxn=cnxn,
        lookups=lookup_loader.data,
        sources=source_loader.data,
        logger=logger,
    )

    steps: StepsDict = {
        "reload_vocab": reload_vocab.transform,
        "preprocess_data": preprocessing.transform,
        "create_lookup": create_lookup_tables.transform,
        "create_logger": create_logger_tables.transform,
        "create_source": create_source_tables.transform,
        "create_omop": create_omopcdm_tables.transform,
        "cdm_source": cdm_source.transform,
        "location": location.transform,
        "person": person.transform,
        "visit_occurrence": visit_occurrence.transform,
        "drug_exposure": drug_exposure.transform,
        "condition": condition.transform,
        "procedure": procedure.transform,
        "measurement": measurement.transform,
        "observation": observation.transform,
        "observation_period": observation_period.transform,
        "condition_era": condition_era.transform,
        "drug_era": drug_era.transform,
    }

    etl_dur = time.time()
    run_transformations(steps, ctxt)

    summary = print_models_summary(
        ctxt,
        [
            ModelSummary(CDMSource),
            ModelSummary(Location),
            ModelSummary(Person),
            ModelSummary(VisitOccurrence),
            ModelSummary(DrugExposure),
            ModelSummary(ConditionOccurrence),
            ModelSummary(ProcedureOccurrence),
            ModelSummary(Measurement),
            ModelSummary(Observation),
            ModelSummary(ObservationPeriod),
            ModelSummary(ConditionEra),
            ModelSummary(DrugEra),
        ],
        title="OMOPCDM",
    )

    logger.info(summary)
    etl_dur = time.time() - etl_dur
    logger.info("ETL completed in %ss", etl_dur)
