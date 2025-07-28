import raven as rv

from typing import Set

from raven_features.utils.models import RavenQueryParameters
from raven_features.utils.logs import get_logger


############################
# Globals
############################
logger = get_logger(__name__)


############################
# Functions
############################
from typing import Set
import logging

logger = logging.getLogger(__name__)


def get_radiology_series_set(raven_query_config: RavenQueryParameters) -> Set[str]:
    """
    Queries Raven for organ and lesion segmentation masks based on the provided configuration,
    and returns the set of Series UIDs that have both types of masks available.

    Args:
        raven_query_config (RavenQueryParameters): A validated configuration object specifying
            the query parameters for retrieving segmentation masks.

    Returns:
        Set[str]: A set of Series UIDs for which both organ and lesion masks are available.

    Raises:
        ValueError: If no masks are defined or no common Series UIDs are found.
    """
    # Merge model-defined and extra fields, filter out excluded or null keys
    base_model_fields = raven_query_config.model_dump()
    base_extra_fields = raven_query_config.model_extra
    base_query = {
        k: v for k, v in {**base_model_fields, **base_extra_fields}.items()
        if k not in {"modality", "images", "masks"} and v is not None
    }

    # Validate masks
    if not raven_query_config.masks:
        msg = "No mask queries provided. Please define at least one mask under 'masks'."
        logger.error(f"❌ {msg}")
        raise ValueError(msg)

    # Execute each mask query and collect results
    all_series_sets = []
    for i, mask_query in enumerate(raven_query_config.masks):
        query = {**base_query, **mask_query.model_dump()}
        logger.debug(f"Mask query {i + 1}: {query}")
        results = rv.get_masks(**query)
        series_uids = {mask.series_uid for mask in results}
        logger.debug(f"Mask query {i + 1}: Found {len(series_uids)} matching series.")
        all_series_sets.append(series_uids)

    # Check for any empty result sets
    if not all_series_sets or any(len(s) == 0 for s in all_series_sets):
        msg = "One or more mask queries returned no results. Check your mask parameters."
        logger.error(f"❌ {msg}")
        raise ValueError(msg)

    # Compute intersection of all result sets
    series_intersection = set.intersection(*all_series_sets)

    # Final validation and log output
    if not series_intersection:
        msg = "No overlapping series found. Ensure your mask queries target the same series."
        logger.warning(f"⚠️ {msg}")
        raise ValueError(msg)

    logger.info('--------------------------------------------------')
    logger.info(f"✅ Found {len(series_intersection)} series at the intersection of the specified mask queries:")
    for uid in sorted(series_intersection):
        logger.info(f"  Series UID: {uid}")
    logger.info('--------------------------------------------------')

    return series_intersection



def get_pathology_slide_set(raven_query_config: RavenQueryParameters) -> Set[str]:

    return {'fake', 'path', 'set'}