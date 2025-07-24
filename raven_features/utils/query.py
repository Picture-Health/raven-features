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
def get_radiology_series_set(raven_query_config: RavenQueryParameters) -> Set[str]:
    """
    Queries Raven for organ and lesion segmentation masks based on the provided configuration,
    and returns the set of Series UIDs that have both types of masks available.

    Args:
        raven_query_config (RavenQueryParameters): A validated configuration object specifying
            the query parameters for retrieving segmentation masks.

    Returns:
        Set[str]: A set of Series UIDs for which both organ and lesion masks are available.
    """
    # Dump and filter query dict
    base_query = {
        k: v for k, v in raven_query_config.model_dump().items()
        if k not in {"modality", "images", "masks"} and v is not None
    }

    # Query and intersect all mask matches
    series_intersection = set.intersection(*[
        {mask.series_uid for mask in rv.get_masks(**{**base_query, **mask_query.model_dump()})}
        for mask_query in raven_query_config.masks
    ])

    # Log results
    logger.info('--------------------------------------------------')
    logger.info(f"Found ({len(series_intersection)} series at the intersection of the specified mask queries):")
    for uid in sorted(series_intersection):
        logger.info(f"  Series UID: {uid}")
    logger.info('--------------------------------------------------')

    return series_intersection



def get_pathology_slide_set(raven_query_config: RavenQueryParameters) -> Set[str]:

    return {'fake', 'path', 'set'}