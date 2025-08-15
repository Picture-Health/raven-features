from __future__ import annotations

from pathlib import Path
from typing import Optional, Any

import typer
from clearml import PipelineController, Task

from raven_features.utils.config import (
    load_config,                 # returns RunConfig with .cfg (PipelineConfig) and .meta
    set_task_parameters,
    log_pipeline_config_to_console,
    PipelineConfig,
)
from raven_features.utils.query import trigger_set_for_featurization
from raven_features.utils.logs import get_logger
from raven_features.utils.time import formatted_date
from raven_features.utils import env

# NEW: centralized provisioning (batch config write happens here)
from raven_features.utils.provisioning import Provisioner, BatchMeta

# NEW: batch auditing (enqueue logs)
from raven_features.utils.audit import BatchAudit

# NEW: summarize command
from raven_features.utils.summarize import summarize_batch as summarize_batch_fn

# NEW: S3 context for audit paths
from raven_features.utils.aws import S3PathContext


# ──────────────────────────────────────────────────────────────────────────────
# Globals
# ──────────────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
app = typer.Typer(add_completion=False, help="Featurization pipeline CLI")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _require_exactly_one(config_uri: Optional[str], config_local_path: Optional[Path]) -> None:
    """Validate that exactly one config source is provided."""
    if bool(config_uri) == bool(config_local_path):
        raise typer.BadParameter("Provide exactly one of --config-uri OR --config-local-path.")

def _launcher_queue(cfg: PipelineConfig) -> str:
    """
    Decide which ClearML queue receives the *pipeline controller* task.
    For now we take it from ENV to keep concerns separated from extraction queues.
    """
    q = getattr(env, "CLEARML_LAUNCHER_QUEUE", None)
    if not q:
        raise typer.BadParameter("env.CLEARML_LAUNCHER_QUEUE is not set")
    return q

def _s3_bucket() -> str:
    """
    Resolve default S3 bucket to write batch config & audit files.
    Prefer an env var if you have one; fall back to px-app-bucket.
    """
    return getattr(env, "DEFAULT_S3_BUCKET", "px-app-bucket")


# ──────────────────────────────────────────────────────────────────────────────
# ClearML: Pipeline launcher
# ──────────────────────────────────────────────────────────────────────────────

def launch_pipeline(
    cfg: PipelineConfig,
    series_uid: str,
    *,
    feat_id: str,
    meta: Any,
) -> dict:
    """
    Create and enqueue a ClearML PipelineController task for a given series/slide,
    tagged by featurization. Uses `meta` for config_name + YAML artifact.
    """
    logger.info(f"   🚀 Creating pipeline task for {series_uid} [{feat_id}]")

    task_name = "-".join([
        (getattr(meta, "config_name", None) or "config"),
        feat_id,
        series_uid,
    ])

    pipeline = PipelineController.create(
        project_name="/".join([env.PROJECT_PREFIX, cfg.project.project_id]),
        task_name=task_name,
        repo=env.ENTRYPOINT_REPO,
        branch=env.ENTRYPOINT_BRANCH,
        script=env.ENTRYPOINT_SCRIPT,
        working_directory=env.WORKING_DIR,
        packages=env.PACKAGES,
        add_run_number=True,
    )

    # Attach parameters from the validated config
    set_task_parameters(pipeline.task, cfg=cfg, meta=meta)

    # Make the featurization & series explicit params for easy filtering
    pipeline.task.set_parameters({
        "run.series_uid": series_uid,
        "run.featurization_id": feat_id,
    })

    # Tags + system tags
    pipeline.task.set_tags([
        f"project_id:{cfg.project.project_id}",
        f"feat_id:{feat_id}",
        f"job_id:{series_uid}",
        f"config:{getattr(meta, 'config_name', None) or 'config'}",
    ])
    pipeline.task.set_system_tags((pipeline.task.get_system_tags() or []) + ["pipeline"])

    # Upload the raw YAML config as an artifact (for reproducibility)
    try:
        pipeline.task.upload_artifact(
            name=env.CONFIG_ARTIFACT_NAME,
            artifact_object=(getattr(meta, "yaml_content", None) or "<missing yaml_content>"),
        )
    except Exception as e:
        logger.warning(f"Could not upload config artifact: {e!r}")

    # Enqueue the controller on the launcher queue
    Task.enqueue(
        queue_name=_launcher_queue(cfg),
        task=pipeline.task,
    )

    # Helpful URL if available
    try:
        url = pipeline.task.get_output_log_web_page()
    except Exception:
        url = None

    return {
        "message": "Pipeline task created",
        "series_uid": series_uid,
        "feat_id": feat_id,
        "task_id": pipeline.task.id,
        "clearml_url": url,
    }


# ──────────────────────────────────────────────────────────────────────────────
# CLI: featurize
# ──────────────────────────────────────────────────────────────────────────────

@app.command("featurize")
def featurize(
    config_uri: Optional[str] = typer.Option(
        None, "--config-uri",
        help="S3 URI to the pipeline configuration YAML (e.g., s3://bucket/path/config.yaml)"
    ),
    config_local_path: Optional[Path] = typer.Option(
        None, "--config-local-path", exists=True, dir_okay=False, readable=True,
        help="Local path to the pipeline configuration YAML"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", min=1, help="Optionally cap how many items from the trigger set to launch"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Parse config and show trigger summary; do not enqueue anything"
    ),
) -> None:
    """
    Load a pipeline config (S3 or local), initialize a ClearML run record,
    resolve Raven trigger sets per featurization, and enqueue pipelines.
    """
    # 1) Validate options
    _require_exactly_one(config_uri, config_local_path)

    # 2) Load & validate config
    run = load_config(
        config_uri=config_uri if config_uri else None,
        config_path=str(config_local_path) if config_local_path else None,
    )
    cfg: PipelineConfig = run.cfg
    meta = run.meta

    # 3) Create a parent ClearML *tracking* task (for the launcher session)
    launcher_task = Task.init(
        project_name=env.PROJECT_PREFIX,
        task_name=f"Featurization - {meta.config_name or 'config'} @ {formatted_date()}",
        task_type=Task.TaskTypes.data_processing,
    )
    set_task_parameters(launcher_task, cfg=cfg, meta=meta)
    try:
        launcher_task.upload_artifact(
            name=env.CONFIG_ARTIFACT_NAME,
            artifact_object=meta.yaml_content,
        )
    except Exception as e:
        logger.warning(f"Could not upload launcher config artifact: {e!r}")

    # 4) Log the fully loaded config
    log_pipeline_config_to_console(cfg, meta=meta)

    # 4.1) Write batch config to S3 (once per run), unless dry-run
    try:
        if dry_run:
            logger.info("⏭️  Dry-run: skipping batch config upload to S3.")
        else:
            bucket = _s3_bucket()
            prov = Provisioner(
                cfg,
                BatchMeta(bucket=bucket, config_yaml=meta.yaml_content, batch_id=meta.batch_id),
            )
            uri = prov.ensure_batch_config()
            logger.info(f"📝 Batch config written to {uri}")
    except Exception as e:
        logger.warning(f"Could not write batch config to S3: {e!r}")

    # 5) Resolve trigger sets PER FEATURIZATION and launch
    logger.info("🔎 Performing Raven queries per featurization…")
    total_planned = 0
    total_launched = 0

    for feat in cfg.featurizations:
        dom = (feat.domain or "radiology").lower()
        logger.info(f"— featurization: {feat.id} (domain={dom})")

        # Per-featurization audit (so _batch/ sits under the correct feature_group_id)
        ctx = S3PathContext(
            project_id=cfg.project.project_id,
            feature_group_id=feat.feature_group_id,
            user=cfg.project.user,
            batch_id=meta.batch_id or "unknown",
            series_uid="",  # not needed at batch level
        )
        audit = BatchAudit(
            bucket=_s3_bucket(),
            ctx=ctx,
            config_name=meta.config_name or "config",
            dry_run=dry_run,
        )

        try:
            series_set = list(trigger_set_for_featurization(cfg, feat))
        except Exception as e:
            logger.exception(f"Failed to resolve trigger set for {feat.id}: {e!r}")
            try:
                audit.flush()  # still persist empty audit so the folder exists
            except Exception:
                pass
            continue

        if not series_set:
            logger.info(f"⚠️ No items for {feat.id}; skipping.")
            try:
                audit.flush()
            except Exception:
                pass
            continue

        if limit is not None:
            series_set = series_set[:limit]

        # Record planned items
        for sid in series_set:
            audit.record_planned(feat.id, sid)

        total_planned += len(series_set)
        logger.info(f"✨ {feat.id}: {len(series_set)} item(s) to process")

        # 6) Dry-run support
        if dry_run:
            for i, sid in enumerate(series_set, 1):
                logger.info(f"[dry-run] {feat.id} ({i}/{len(series_set)}): would launch series_uid={sid}")
            try:
                audit.flush()
            except Exception as e:
                logger.warning(f"audit flush failed: {e!r}")
            continue

        # 7) Launch pipelines
        launched = 0
        for sid in series_set:
            try:
                res = launch_pipeline(cfg, sid, feat_id=feat.id, meta=meta)
                launched += 1
                audit.record_launched(
                    feat.id,
                    sid,
                    task_id=res.get("task_id", ""),
                    clearml_url=res.get("clearml_url"),
                )
            except Exception as e:
                logger.exception(f"{feat.id}: launch failed for series_uid={sid}: {e!r}")
                audit.record_error(
                    feat.id,
                    message=str(e),
                    series_uid=sid,
                )

        total_launched += launched
        logger.info(f"✅ {feat.id}: enqueued {launched}/{len(series_set)}")

        # Flush per-featurization audit
        try:
            audit.flush()
        except Exception as e:
            logger.warning(f"audit flush failed: {e!r}")

    # 8) Summary
    if dry_run:
        logger.info(f"🏁 Dry-run complete. Total planned: {total_planned}")
    else:
        logger.info(f"🏁 Done. Total enqueued: {total_launched}/{total_planned}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI: summarize-batch
# ──────────────────────────────────────────────────────────────────────────────

@app.command("summarize-batch")
def summarize_batch(
    enqueue_uri: str = typer.Option(
        ...,
        "--enqueue-uri",
        help="s3://.../_batch/enqueue_<feat_id>.jsonl to summarize.",
    ),
):
    """
    Read enqueue.jsonl, pull ClearML statuses, and write final_summary.json (+ CSV) next to it.
    """
    summary, csv_uri = summarize_batch_fn(enqueue_uri=enqueue_uri, write_csv=True)
    logger.info(f"Summary: {summary}")
    if csv_uri:
        logger.info(f"CSV written to: {csv_uri}")


# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        app()
    except typer.Exit as te:
        raise SystemExit(te.exit_code)
    except Exception as e:
        logger.exception(f"Unhandled error: {e!r}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()