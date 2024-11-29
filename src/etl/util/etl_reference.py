"""Helper to get the ETL_VERSION"""

import logging
import os
from typing import Tuple

logger = logging.getLogger(__name__)


def get_version_from_git_tag(
    env_var_name: str = "GITHUB_TAG",
) -> Tuple[bool, str]:
    """
    Attempts to get the version from the environment variable,
    default var name is "GITHUB_TAG", but this can be changed.

    Returns a tuple with the first indicating if it was defaulted or
    not and the second is the version as a string.

    If the environment variable cannot be found then it defaults
    to [True, "Unspecified"], since it was defaulted.

    If the environment variable is set then it should return
    to [False, "vX.X"].

    Does not do any checks on what is stored in that variable, assumes
    it is the version.
    """
    default_tag = "Unspecified"
    git_commit_tag = os.environ.get(env_var_name, default_tag)
    return git_commit_tag == default_tag, git_commit_tag


def get_cdm_etl_reference(cdm_etl_reference: str) -> str:
    git_commit_sha = "Unspecified"
    # Defaults to link to current master
    # cdm_etl_reference = os.environ.get("CDM_ETL_REFERENCE")
    try:
        git_commit_sha = os.environ["COMMIT_SHA"]
    except KeyError:
        logger.warning("No git version set, will default to Unspecified!")
    logger.info("Git version: %s", git_commit_sha)
    is_version_defaulted, git_commit_tag = get_version_from_git_tag(
        env_var_name="GITHUB_TAG"
    )
    if not is_version_defaulted:
        # Link to the exact revision
        cdm_etl_reference = cdm_etl_reference + f"releases/tag/{git_commit_tag}"
    else:
        logger.warning("No git tag set, will default to Unspecified!")
    logger.info("Git tag: %s", git_commit_tag)

    return cdm_etl_reference


# ETL_REFERENCE = get_cdm_etl_reference()
