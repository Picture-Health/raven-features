from __future__ import annotations

from typing import Set, Iterable

import raven as rv

from raven_features.utils.logs import get_logger
from raven_features.utils.models import (
    PipelineConfig,
    Featurization,
    ImageQuery,
    MaskSpec,
)

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Core helpers
# ──────────────────────────────────────────────────────────────────────────────

def _mask_intersection_series(
    images: ImageQuery,
    masks: Iterable[MaskSpec],
) -> Set[str]:
    """
    Intersect series sets returned by multiple mask queries, all constrained by the same base image query.
    """
    base = images.model_dump(exclude_none=True)
    all_sets: list[Set[str]] = []

    for i, m in enumerate(masks, 1):
        q = {**base, **m.model_dump(exclude_none=True)}
        logger.debug(f"[radiology] mask query {i}: {q}")
        results = rv.get_masks(**q)
        series = {res.series_uid for res in results}
        logger.debug(f"[radiology] mask query {i}: {len(series)} series")
        all_sets.append(series)

    if not all_sets or any(len(s) == 0 for s in all_sets):
        raise ValueError("One or more mask queries returned no results.")

    inter = set.intersection(*all_sets)
    if not inter:
        raise ValueError("No overlapping series found across mask queries.")
    return inter

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def get_radiology_series_set(images: ImageQuery, masks: Iterable[MaskSpec]) -> Set[str]:
    """
    Return the intersection of series UIDs for the given base image query and mask specs.
    """
    masks = list(masks)
    if not masks:
        raise ValueError("No mask queries provided. Define at least one mask under provisioning.masks.")
    series = _mask_intersection_series(images, masks)

    logger.info('--------------------------------------------------')
    logger.info(f"✅ Found {len(series)} radiology series at mask intersection")
    for uid in sorted(series):
        logger.info(f"  Series UID: {uid}")
    logger.info('--------------------------------------------------')

    return series


def get_pathology_slide_set(images: ImageQuery) -> Set[str]:
    """
    Example pathology trigger set. Adjust to your Raven API.
    """
    base = images.model_dump(exclude_none=True)
    # If your API is different, replace with the correct call/attribute:
    # e.g., results = rv.get_slides(**base); uids = {s.slide_uid for s in results}
    results = rv.get_slides(**base)  # <-- confirm this is the right function in your env
    uids = {getattr(s, "slide_uid", getattr(s, "uid", None)) for s in results}
    uids.discard(None)

    if not uids:
        raise ValueError("No pathology slides matched the base query.")
    logger.info(f"✅ Found {len(uids)} pathology slides")
    return set(uids)


def trigger_set_for_featurization(cfg: PipelineConfig, feat: Featurization) -> Set[str]:
    """
    Convenience: compute the trigger set for a single featurization, using
    the global base image query (`cfg.images`) plus this featurization's masks.
    """
    domain = (feat.domain or "radiology").lower()
    if domain == "radiology":
        return get_radiology_series_set(cfg.images, feat.provisioning.masks)
    if domain == "pathology":
        return get_pathology_slide_set(cfg.images)
    raise ValueError(f"Unsupported domain: {domain}")