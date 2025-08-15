from __future__ import annotations

import yaml
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional, Dict
from urllib.parse import urlparse

from clearml import Task
from pydantic import BaseModel

from raven_features.utils.aws import retrieve_s3_file
from raven_features.utils.filesystem import retrieve_local_file
from raven_features.utils.models import PipelineConfig
from raven_features.utils.logs import get_logger

# ──────────────────────────────────────────────────────────────────────────────
# Globals
# ──────────────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Meta wrapper (keeps strict config clean)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Meta:
    config_path: Optional[str]
    config_name: Optional[str]
    batch_id: str
    yaml_content: str

# Convenience container used by CLI
@dataclass(frozen=True)
class RunConfig:
    cfg: PipelineConfig
    meta: Meta

# ──────────────────────────────────────────────────────────────────────────────
# Config Processing
# ──────────────────────────────────────────────────────────────────────────────

def process_config_yaml(yaml_content: str) -> dict:
    return yaml.safe_load(StringIO(yaml_content))

def get_config_name(source: Optional[str]) -> Optional[str]:
    if source is None:
        return None
    if source.startswith("s3://"):
        return Path(urlparse(source).path).stem
    return Path(source).stem

def read_config_content(
    *,
    config_path: Optional[str] = None,
    config_uri: Optional[str] = None,
    yaml_content: Optional[str] = None,
) -> tuple[str, Optional[str]]:
    """
    Resolve the config content from exactly one source.
    Returns: (yaml_str, source_reference)
    """
    if sum(x is not None for x in [config_path, config_uri, yaml_content]) != 1:
        raise ValueError("Must provide exactly one of config_path, config_uri, or yaml_content")
    if config_path:
        return retrieve_local_file(config_path), config_path
    if config_uri:
        return retrieve_s3_file(config_uri), config_uri
    return yaml_content or "", None

def load_config(
    *,
    config_path: Optional[str] = None,
    config_uri: Optional[str] = None,
    yaml_content: Optional[str] = None,
) -> RunConfig:
    """
    Load & validate the new PipelineConfig. Meta is kept separate from the strict model.
    """
    content, source = read_config_content(
        config_path=config_path, config_uri=config_uri, yaml_content=yaml_content
    )
    cfg_dict = process_config_yaml(content)
    cfg = PipelineConfig(**cfg_dict)

    meta = Meta(
        config_path=source,
        config_name=get_config_name(source),
        batch_id=datetime.now().strftime("%Y-%m-%d__%H-%M-%S"),
        yaml_content=content,
    )
    return RunConfig(cfg=cfg, meta=meta)

# ──────────────────────────────────────────────────────────────────────────────
# ClearML Parameter Handling (new-schema aware)
# ──────────────────────────────────────────────────────────────────────────────

def _collect_parameters_from_model(model: BaseModel, prefix: Optional[str] = None) -> Dict[str, str]:
    """
    Recursively flatten a pydantic model, skipping None/unset values.
    """
    params: Dict[str, str] = {}
    data = model.model_dump(exclude_none=True, exclude_unset=True)  # <— only real values
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, BaseModel):
            params.update(_collect_parameters_from_model(value, full_key))
        elif isinstance(value, list) and value and isinstance(value[0], BaseModel):
            for i, item in enumerate(value):
                params.update(_collect_parameters_from_model(item, f"{full_key}[{i}]"))
        else:
            params[full_key] = str(value)
    return params

def set_task_parameters(
    task: Task,
    *,
    cfg: PipelineConfig,
    meta: Optional[Meta] = None,
) -> None:
    """
    Set ClearML params from the new config plus optional meta.
    """
    params = _collect_parameters_from_model(cfg)
    if meta:
        params.update({
            "meta.config_path": str(meta.config_path),
            "meta.config_name": str(meta.config_name),
            "meta.batch_id": meta.batch_id,
        })
    task.set_parameters(params)

# ──────────────────────────────────────────────────────────────────────────────
# Config Logging (new schema)
# ──────────────────────────────────────────────────────────────────────────────

def _log_kv_block(title: str, rows: Dict[str, object]) -> None:
    logger.info(title)
    logger.info('--------------------------------------------------')
    for k, v in rows.items():
        logger.info(f"  {k:<22}: {v}")
    logger.info('--------------------------------------------------\n')

def log_pipeline_config_to_console(cfg: PipelineConfig, meta: Optional[Meta] = None) -> None:
    """
    Pretty log the new PipelineConfig shape for CLI visibility, skipping None/unset values.
    """
    logger.info('--------------------------------------------------')
    logger.info(" PxPipeline Featurization            ,_")
    logger.info("                                    >' )")
    logger.info("                                    ( ( \\")
    logger.info("                                   rn''|\\")
    logger.info('--------------------------------------------------')
    if meta:
        logger.info(f" Config: {meta.config_name}  |  Batch: {meta.batch_id}")
    logger.info(f" Project ID: {cfg.project.project_id}")
    logger.info('--------------------------------------------------\n')

    # project
    _log_kv_block("Project:", cfg.project.model_dump(exclude_none=True, exclude_unset=True))

    # images (base query)
    _log_kv_block("Image Query:", cfg.images.model_dump(exclude_none=True, exclude_unset=True))

    # defaults
    _log_kv_block("Defaults:", cfg.defaults.model_dump(exclude_none=True, exclude_unset=True))

    # repos (each one individually)
    logger.info("Repos:")
    logger.info('--------------------------------------------------')
    for name, spec in cfg.repos.items():
        logger.info(f"  {name}:")
        for k, v in spec.model_dump(exclude_none=True, exclude_unset=True).items():
            logger.info(f"    {k:<20}: {v}")
    logger.info('--------------------------------------------------\n')

    # featurizations
    logger.info("Featurizations:")
    logger.info('--------------------------------------------------')
    for f in cfg.featurizations:
        logger.info(f"  - id: {f.id}  |  feature_group_id: {f.feature_group_id}  |  domain: {f.domain}")
        # masks (clean dicts without None)
        if f.provisioning.masks:
            logger.info("    masks:")
            for m in f.provisioning.masks:
                clean = m.model_dump(exclude_none=True, exclude_unset=True)
                logger.info(f"      - {clean}")
        if f.provisioning.custom:
            logger.info(f"    custom: {f.provisioning.custom}")
        logger.info("    extractions:")
        for e in f.extractions:
            ed = e.model_dump(exclude_none=True, exclude_unset=True)
            # show a compact one-liner w/ queue fallback to defaults
            queue = ed.get("queue") or cfg.defaults.queue
            logger.info(
                f"      - id: {e.id}  name: {e.name}  repo: {e.repo}  queue: {queue}"
            )
    logger.info('--------------------------------------------------\n')