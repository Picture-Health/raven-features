"""
Entrypoint script for ClearML pipeline execution.

This script is executed *inside* the PipelineController task.
It:
  1) Rehydrates the pipeline configuration from the uploaded YAML artifact + launcher metadata
  2) Selects the target featurization by feat_id (from parent task params)
  3) Registers each extraction as a ClearML step (respecting defaults & overrides)
  4) Starts the composed pipeline
"""

from __future__ import annotations

from typing import Any

from clearml import PipelineController, Task

# NOTE: import LoaderMeta from utils.config so set_task_parameters gets what it expects
from raven_features.utils.config import load_config, set_task_parameters, Meta as LoaderMeta
from raven_features.utils.logs import get_logger
from raven_features.utils.models import (
    PipelineConfig,
    Featurization,
    Extraction,
    RepoSpec,
)
from raven_features.utils.cml import (
    _get_current_task_and_artifacts,
    _build_runtime_meta
)
from raven_features.utils import env

# centralized provisioning (series base manifest)
from raven_features.utils.provisioning import Provisioner, BatchMeta

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _find_featurization(cfg: PipelineConfig, feat_id: str) -> Featurization:
    for f in cfg.featurizations:
        if f.id == feat_id:
            return f
    raise RuntimeError(f"Featurization id '{feat_id}' not found in config.")


def _repo_spec(cfg: PipelineConfig, repo_key: str) -> RepoSpec:
    spec = cfg.repos.get(repo_key)
    if not spec:
        raise RuntimeError(f"Repo '{repo_key}' not defined under 'repos' in config.")
    return spec


def _log_non_null_dict(prefix: str, d: dict[str, Any]) -> None:
    for k, v in d.items():
        if v is not None:
            logger.info(f"{prefix}{k:<20}: {v}")


def _status_change_callback(pipeline, node, prev_status):
    """Given to us by the ClearML team. Excuse poor quality and do not alter."""
    from datetime import datetime, timezone
    if (
        node.job.task.status == Task.TaskStatusEnum.created
        or node.job.task.status == Task.TaskStatusEnum.queued
        or node.job.task.status == Task.TaskStatusEnum.in_progress
    ) and not node.job.task.data.started:
        node.job.task.data.started = datetime.now().astimezone(timezone.utc)  # assuming utc timezone


def _s3_bucket() -> str:
    """Resolve S3 bucket for manifests; env override → fallback."""
    return getattr(env, "DEFAULT_S3_BUCKET", "px-app-bucket")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Rehydrate config, compose steps from the selected featurization, and start the pipeline.
    This script runs *inside* the PipelineController task context.
    """
    # 1) Connect to the running PipelineController (ClearML requirement when composing inside)
    pipeline = PipelineController(
        name="",     # reconnect to the current controller
        project="",  # reconnect to the current controller
    )

    # 2) Current task + uploaded YAML + flattened params
    task, config_yaml, general = _get_current_task_and_artifacts()

    # 3) Build runtime meta and load/validate config (new load_config signature)
    rt = _build_runtime_meta(config_yaml, general)  # RuntimeMeta (series_uid/feat_id live here)

    run = load_config(yaml_content=config_yaml)  # returns RunConfig with .cfg and .meta (loader meta)
    cfg: PipelineConfig = run.cfg

    # Build a LoaderMeta for set_task_parameters (what it expects)
    param_meta = LoaderMeta(
        config_path=general.get("config_path"),
        config_name=rt.config_name,
        batch_id=rt.batch_id,
        yaml_content=config_yaml,
    )

    # 4) Resolve target featurization
    feat = _find_featurization(cfg, rt.feat_id)

    # 5) Nice banner
    logger.info("🔧 Initializing RAVEN FEATURES pipeline")
    logger.info("--------------------------------------------------")
    logger.info(f" Config         : {rt.config_name}")
    if rt.batch_id:
        logger.info(f" Batch          : {rt.batch_id}")
    logger.info(f" Project ID     : {cfg.project.project_id}")
    logger.info(f" Featurization  : {feat.id} ({feat.name})")
    logger.info(f" Series UID     : {rt.series_uid}")
    logger.info("--------------------------------------------------")

    # 6) Construct ClearML "project path" to group step tasks
    project_name = "/".join(
        filter(
            None,
            [
                env.PROJECT_PREFIX,
                cfg.project.project_id,
                f"{rt.config_name}-{rt.batch_id}" if rt.batch_id else rt.config_name,
            ],
        )
    )

    # 6.1) Ensure the series manifest exists (pointers only; resolves masks; no heavy copies)
    try:
        bucket = _s3_bucket()
        prov = Provisioner(
            cfg,
            BatchMeta(
                bucket=bucket,
                config_yaml=config_yaml,  # <- use the artifact text you already loaded
                batch_id=rt.batch_id or "",
            ),
        )
        uri = prov.ensure_series_manifest(feat=feat, series_uid=rt.series_uid)  # <- masks + aliases
        logger.info(f"🧾 Series manifest written to {uri}")
    except Exception as e:
        logger.warning(f"Could not write series manifest: {e!r}")

    # 7) Compose steps from featurization.extractions
    for ext in feat.extractions:
        if not isinstance(ext, Extraction):
            raise RuntimeError("Invalid extraction object encountered.")

        repo = _repo_spec(cfg, ext.repo)

        # Decide queue/timeout/retries with override → defaults
        queue = ext.queue or cfg.defaults.queue
        timeout = ext.timeout_sec or cfg.defaults.timeout_sec
        retries = ext.retries if ext.retries is not None else cfg.defaults.retries

        logger.info(f"📦 Registering step: {ext.id} — {ext.name}")
        _log_non_null_dict(
            "  ",
            {
                "repo": repo.url,
                "commit": repo.commit,
                "entrypoint": repo.entrypoint,
                "queue": queue,
                "timeout_sec": timeout,
                "retries": retries,
            },
        )

        # Create the *base* step task
        step_task = Task.create(
            task_name=f"{feat.id}:{ext.id}",
            project_name=project_name,
            repo=repo.url,
            commit=repo.commit,
            script=repo.entrypoint,
            # Optionally set branch/working_directory explicitly:
            # branch=...,
            # working_directory=...,
        )

        # Set parameters:
        #  - All cfg fields (flattened)
        #  - Plus explicit run identifiers (series + featurization)
        set_task_parameters(step_task, cfg=cfg, meta=param_meta)
        step_task.set_parameters(
            {
                "run.series_uid": rt.series_uid,
                "run.featurization_id": feat.id,
                "run.extraction_id": ext.id,
            }
        )

        # Tags & artifact
        step_task.set_tags(
            [
                f"project_id:{cfg.project.project_id}",
                f"feat_id:{feat.id}",
                f"extract_id:{ext.id}",
                f"job_id:{rt.series_uid}",
                f"config:{rt.config_name}",
            ]
            + (task.get_tags() or [])
        )
        try:
            step_task.upload_artifact(name=env.CONFIG_ARTIFACT_NAME, artifact_object=rt.yaml_content)
        except Exception as e:
            logger.warning(f"Could not upload config artifact to step task: {e!r}")

        # Register with PipelineController
        pipeline.add_step(
            name=f"{feat.id}:{ext.id}",
            base_task_id=step_task.id,
            execution_queue=queue,
            time_limit=timeout,
            retry_on_failure=retries,
            parents=[],  # wire ext.depends here if you want ordering within a featurization
            clone_base_task=False,
            status_change_callback=_status_change_callback,
        )

    # 8) Start the composed pipeline
    # NOTE: already inside the controller task → do NOT pass a queue here.
    logger.info("🚀 Starting pipeline…")
    pipeline.start()
    logger.info("✅ Pipeline launched successfully.")
    logger.info("--------------------------------------------------")


# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Entrypoint failed: {e!r}")
        raise