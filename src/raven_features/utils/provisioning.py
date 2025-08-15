from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import raven as rv

from raven_features.utils.aws import (
    S3PathContext,
    S3Layout,
    upload_bytes,
    upload_json,
)
from raven_features.utils.logs import get_logger
from raven_features.utils.models import (
    PipelineConfig,
    Featurization,
    MaskSpec,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class BatchMeta:
    """
    Minimal context passed in from CLI/entrypoint.
    - bucket: target S3 bucket for all artifacts
    - config_yaml: the YAML text; we still let you upload it once per batch (config.yaml)
    - batch_id: timestamp-ish string used in the S3 path
    """
    bucket: str
    config_yaml: str
    batch_id: str


class Provisioner:
    """
    Centralized S3 writer for lightweight manifests (no large data copies).
    Writes per-series manifest under <...>/<series_uid>/manifest.json when the controller starts.
    """

    def __init__(self, cfg: PipelineConfig, batch: BatchMeta) -> None:
        self.cfg = cfg
        self.batch = batch

    # ----------------------------------------------------------------------
    # Optional: write the single config.yaml at the *batch* root once.
    # This keeps reproducibility handy but is not a "large" manifest.
    # Call this from the CLI (once per run) if you want it; otherwise skip.
    # ----------------------------------------------------------------------
    def ensure_batch_config(self) -> str:
        """
        Save the run's YAML at:
        s3://<bucket>/<project>/<first_feature_group_or_MULTI>/<user>/<batch_id>/config.yaml
        """
        ctx = S3PathContext(
            project_id=self.cfg.project.project_id,
            feature_group_id=self._batch_root_feature_group(),
            user=self.cfg.project.user,
            batch_id=self.batch.batch_id,
            series_uid="__BATCH__",
        )
        key = S3Layout.config_key(ctx)
        return upload_bytes(
            bucket=self.batch.bucket,
            key=key,
            data=self.batch.config_yaml.encode("utf-8"),
            content_type="text/yaml",
        )

    # ----------------------------------------------------------------------
    # REQUIRED: per-series manifest written inside <series_uid>/manifest.json
    # Called once when the PipelineController task starts (entrypoint).
    # ----------------------------------------------------------------------
    def ensure_series_manifest(self, *, feat: Featurization, series_uid: str) -> str:
        """
        Write a JSON manifest under <batch>/<series_uid>/manifest.json that includes:
          - the base image query used to resolve the series
          - a compact 'image' section with series/image details (no heavy blobs)
          - masks grouped by alias, each item with mask_index and useful pointers
        """
        # S3 context at the series level (note: no ext_id here)
        from raven_features.utils.aws import S3PathContext, S3Layout, upload_json
        ctx = S3PathContext(
            project_id=self.cfg.project.project_id,
            feature_group_id=feat.feature_group_id,
            user=self.cfg.project.user,
            batch_id=self.batch.batch_id,
            series_uid=series_uid,
        )

        # Base query = cfg.images + series_uid constraint
        base_q = self.cfg.images.model_dump(exclude_none=True, exclude_defaults=True)
        base_q["series_uid"] = series_uid

        # ---------------------------
        # IMAGE SECTION (compact)
        # ---------------------------
        # Try to get a representative image and some series-level details without heavy copying.
        image_info: dict[str, Any] = {
            "query": base_q,
            "series_uid": series_uid,
        }
        try:
            # Prefer a dedicated "series" lookup if your API has it; fallback to images
            series = None
            try:
                # If your API supports it
                series = rv.get_series(series_uid=series_uid)  # type: ignore[attr-defined]
            except Exception:
                series = None

            if series:
                # normalize single object vs list
                s = series[0] if isinstance(series, (list, tuple)) and series else series
                image_info.update({
                    "series_description": getattr(s, "series_description", None),
                    "series_number": getattr(s, "series_number", None),
                    "modality": getattr(s, "modality", None),
                    "orientation": getattr(s, "orientation", None),
                    "slice_thickness": getattr(s, "slice_thickness", None),
                    "pixel_spacing_value": getattr(s, "pixel_spacing_value", None),
                    "reconstruction_diameter": getattr(s, "reconstruction_diameter", None),
                    "manufacturer": getattr(s, "manufacturer", None),
                    "study_uid": getattr(s, "study_uid", None),
                })

            # Representative image (first match) for a few pointers
            imgs = rv.get_images(**base_q)
            if imgs:
                rep = imgs[0]
                image_info["representative_image"] = {
                    "image_index": getattr(rep, "image_index", None),
                    "image_uri": getattr(rep, "image_uri", None),
                    "image_type": getattr(rep, "image_type", None),
                    "creation_time": getattr(rep, "creation_time", None),
                }
                image_info["image_count"] = len(imgs)
        except Exception as e:
            # Safe failure; manifest still useful
            logger.debug(f"[manifest:{series_uid}] Image/series details lookup failed: {e!r}")

        # ---------------------------
        # MASKS SECTION (with mask_index)
        # ---------------------------
        masks_by_alias: dict[str, list[dict[str, Any]]] = {}

        # Resolve mask list (ext override happens in step; here we use featurization masks)
        masks = list(feat.provisioning.masks or [])
        for m in masks:
            alias = getattr(m, "alias", None) or f"{m.provenance}:{m.mask_type}"
            q = {**base_q, **m.model_dump(exclude_none=True, exclude_defaults=True)}
            try:
                results = rv.get_masks(**q)
            except Exception as e:
                logger.warning(f"[manifest:{series_uid}] mask query failed for {alias}: {e!r}")
                results = []

            entries: list[dict[str, Any]] = []
            for item in results:
                entries.append({
                    "alias": alias,
                    "mask_type": m.mask_type,
                    "provenance": m.provenance,
                    "qa_code": getattr(m, "qa_code", None),
                    "series_uid": getattr(item, "series_uid", None),
                    "mask_index": getattr(item, "mask_index", None),  # ← requested
                    "mask_uri": getattr(item, "mask_uri", None),
                    "mask_metadata": getattr(item, "mask_metadata", None),
                })
            masks_by_alias[alias] = entries

        # ---------------------------
        # FINAL MANIFEST
        # ---------------------------
        manifest = {
            "schema_version": 1,
            "project_id": self.cfg.project.project_id,
            "feature_group_id": feat.feature_group_id,
            "featurization_id": feat.id,
            "series_uid": series_uid,
            "image": image_info,
            "masks": masks_by_alias,
            "sources": {"raven": True},
        }

        key = S3Layout.key_for(ctx, S3Layout.series_root(ctx), "manifest.json")
        return upload_json(bucket=self.batch.bucket, key=key, obj=manifest)

    # ----------------------------------------------------------------------
    # helpers
    # ----------------------------------------------------------------------
    def _batch_root_feature_group(self) -> str:
        """
        Pick a stable feature_group_id for the batch root (for config.yaml).
        If multiple featurizations exist, just pick the first; otherwise "MULTI".
        """
        if self.cfg.featurizations:
            return self.cfg.featurizations[0].feature_group_id
        return "MULTI"


def _mask_spec_public_dict(m: MaskSpec) -> Dict[str, Any]:
    """
    Convert a MaskSpec to a JSON-safe dict for 'masks_requested', including optional alias if present.
    """
    d = m.model_dump(exclude_none=True, exclude_defaults=True)
    # keep only user-meaningful fields; if your MaskSpec contains internals, drop here.
    # If your model has 'alias', it will be present; otherwise no-op.
    return d