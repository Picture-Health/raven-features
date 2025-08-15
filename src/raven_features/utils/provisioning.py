from __future__ import annotations

from typing import Iterable, Any, Optional, Dict, List, Set
from dataclasses import dataclass

import raven as rv  # your API
from raven_features.utils.aws import (
    S3PathContext, S3Layout,
    upload_bytes, upload_json, upload_stream,
)
from raven_features.utils.logs import get_logger
from raven_features.utils.models import (
    PipelineConfig, Featurization, Extraction, ImageQuery, MaskSpec
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class BatchMeta:
    bucket: str
    config_yaml: str
    batch_id: str


class Provisioner:
    """
    Centralized provisioning API.
    All S3 key conventions go through S3Layout.
    Only small manifests are written by default.
    """

    def __init__(self, cfg: PipelineConfig, batch: BatchMeta):
        self.cfg = cfg
        self.batch = batch

    # ------------------------
    # Batch-level (CLI)
    # ------------------------
    def ensure_batch_config(self) -> str:
        """
        Write the single config.yaml at the batch root (overwrites OK / versioned).
        Call from the CLI after loading YAML.
        """
        ctx = S3PathContext(
            project_id=self.cfg.project.project_id,
            feature_group_id=self._feature_group_for_logs(),
            user=self.cfg.project.user,
            batch_id=self.batch.batch_id,
            series_uid="__BATCH__",
        )
        key = S3Layout.config_key(ctx)
        return upload_bytes(bucket=self.batch.bucket, key=key,
                            data=self.batch.config_yaml.encode("utf-8"),
                            content_type="text/yaml")

    # ------------------------
    # Series-level (Entrypoint)
    # ------------------------
    def ensure_series_base_manifest(self, *, series_uid: str, images: ImageQuery) -> str:
        """
        Write a tiny JSON manifest of the series base inputs (pointers only).
        Call once per series in the entrypoint.
        """
        ctx = S3PathContext(
            project_id=self.cfg.project.project_id,
            feature_group_id=self._feature_group_for_logs(),
            user=self.cfg.project.user,
            batch_id=self.batch.batch_id,
            series_uid=series_uid,
        )
        manifest = {
            "query": images.model_dump(exclude_none=True, exclude_defaults=True),
            "sources": {
                "dataset_id": images.dataset_id,
                "series_uid": images.series_uid,
            },
        }
        key = S3Layout.key_for(ctx, S3Layout.base_inputs_prefix(ctx), "manifest.json")
        return upload_json(bucket=self.batch.bucket, key=key, obj=manifest)

    # ------------------------
    # Extraction-level (Step)
    # ------------------------
    def provision_masks_for_extraction(
        self,
        *,
        series_uid: str,
        feat: Featurization,
        ext: Extraction,
        images: ImageQuery,
    ) -> Dict[str, List[str]]:
        """
        Fetch masks per the config (ext override -> feat default) and write files
        under the extraction's masks prefix. Returns a small index of what was written.
        """
        masks: List[MaskSpec] = []
        if ext.provisioning and ext.provisioning.masks:
            masks = list(ext.provisioning.masks)
        elif feat.provisioning and feat.provisioning.masks:
            masks = list(feat.provisioning.masks)

        if not masks:
            logger.info(f"[{feat.id}:{ext.id}] No masks requested; skipping mask provisioning.")
            return {"written": []}

        ctx = S3PathContext(
            project_id=self.cfg.project.project_id,
            feature_group_id=feat.feature_group_id,
            user=self.cfg.project.user,
            batch_id=self.batch.batch_id,
            series_uid=series_uid,
            feat_id=feat.id,
            ext_id=ext.id,
        )

        base = images.model_dump(exclude_none=True, exclude_defaults=True)

        written: Dict[str, List[str]] = {}
        for m in masks:
            q = {**base, **m.model_dump(exclude_none=True, exclude_defaults=True)}
            logger.debug(f"[{feat.id}:{ext.id}] mask query: {q}")
            results = rv.get_masks(**q)

            # Here, decide what you actually persist; often you only need a
            # small derivative (e.g., converted mask files) or pointers.
            # For example, if results carry URIs, write a manifest instead of copying blobs:
            written_keys: List[str] = []

            # Example strategy: write a small manifest of mask objects we intend to use
            manifest_obj = [
                {
                    "mask_type": m.mask_type,
                    "provenance": m.provenance,
                    "series_uid": getattr(item, "series_uid", None),
                    "mask_uri": getattr(item, "mask_uri", None),
                    "metadata": getattr(item, "mask_metadata", None),
                }
                for item in results
            ]

            manifest_name = f"{m.provenance}_{m.mask_type}.manifest.json"
            key = S3Layout.key_for(ctx, S3Layout.masks_prefix(ctx), manifest_name)
            upload_json(bucket=self.batch.bucket, key=key, obj=manifest_obj)
            written_keys.append(f"s3://{self.batch.bucket}/{key}")

            written[f"{m.provenance}:{m.mask_type}"] = written_keys

        return written

    # ------------------------
    # Helpers
    # ------------------------
    def _feature_group_for_logs(self) -> str:
        """
        For batch root, you can choose any of the run's feature_group_id values.
        If multiple featurizations exist, the batch is still a single run –
        pick the first (or a stable synthetic like 'MULTI').
        """
        if self.cfg.featurizations:
            return self.cfg.featurizations[0].feature_group_id
        return "MULTI"