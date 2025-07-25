import yaml

from clearml import Task
from datetime import datetime
from io import StringIO
from pathlib import Path
from pydantic import BaseModel
from typing import Optional
from urllib.parse import urlparse

from raven_features.utils.aws import retrieve_s3_file
from raven_features.utils.filesystem import retrieve_local_file
from raven_features.utils.models import PipelineConfig, PipelineStep
from raven_features.utils.logs import get_logger


# ----------------------------
# Globals
# ----------------------------
logger = get_logger(__name__)


# ----------------------------
# Config Processing
# ----------------------------
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
    yaml_content: Optional[str] = None
) -> tuple[str, Optional[str]]:
    """
    Resolves the config content from one of three sources.
    Returns: (yaml_content_str, source_reference)
    """
    if sum(x is not None for x in [config_path, config_uri, yaml_content]) != 1:
        raise ValueError("Must provide exactly one of config_path, config_uri, or yaml_content")

    if config_path:
        return retrieve_local_file(config_path), config_path
    elif config_uri:
        return retrieve_s3_file(config_uri), config_uri
    else:
        return yaml_content, None


def load_config(
    *,
    config_path: Optional[str] = None,
    config_uri: Optional[str] = None,
    yaml_content: Optional[str] = None,
    featurization_metadata: Optional[dict] = None,
) -> PipelineConfig:
    """
    Loads a config from a local path, S3 URI, or raw YAML string.

    Args:
        config_path (str, optional): Local file path to the YAML config.
        config_uri (str, optional): S3 URI to the YAML config.
        yaml_content (str, optional): Raw YAML string.
        featurization_metadata (dict, optional): Optional metadata to inject into
            the config before validation. If not provided, the following defaults
            will be added:
                - 'config_path': inferred from path or URI
                - 'config_name': derived from filename
                - 'batch_id': current timestamp
                - 'yaml_content': full raw YAML string

    Returns:
        PipelineConfig: Validated config model with metadata injected.
    """
    content, source = read_config_content(
        config_path=config_path,
        config_uri=config_uri,
        yaml_content=yaml_content,
    )

    config_dict = process_config_yaml(content)

    if featurization_metadata is None:
        featurization_metadata = {
            "config_path": source,
            "config_name": get_config_name(source),
            "batch_id": datetime.now().strftime("%Y-%m-%d__%H-%M-%S"),
            "yaml_content": content,
        }

    config_dict.update(featurization_metadata)

    return PipelineConfig(**config_dict)


# ----------------------------
# ClearML Parameter Handling
# ----------------------------
def collect_output_paths_from_ancestors(
    steps: list[PipelineStep],
    current_step_name: str
) -> list[str]:
    """
    Recursively collects `output_path`s from the current step and all its ancestors.

    Args:
        steps: List of all PipelineStep instances from the PipelineConfig.
        current_step_name: Name of the current step to start traversal.

    Returns:
        A list of unique S3 output paths from all ancestor steps.
    """
    name_to_step = {step.name: step for step in steps}
    visited = set()
    output_paths = []

    def dfs(step_name: str):
        if step_name in visited:
            return
        visited.add(step_name)

        step = name_to_step.get(step_name)
        if not step:
            return

        if step.output_path:
            output_paths.append(step.output_path)

        for parent_name in step.parent_steps or []:
            dfs(parent_name)

    dfs(current_step_name)
    return sorted(set(output_paths))


def collect_parameters_from_model(
    model: BaseModel, prefix: Optional[str] = None
) -> dict[str, str]:
    """
    Recursively collects parameters from a nested Pydantic model.
    """
    params = {}
    for field_name, field in model.model_fields.items():
        value = getattr(model, field_name)
        if value is None:
            continue

        full_key = f"{prefix}.{field_name}" if prefix else field_name

        if isinstance(value, BaseModel):
            params.update(collect_parameters_from_model(value, prefix=full_key))
        elif isinstance(value, list) and value and isinstance(value[0], BaseModel):
            for i, item in enumerate(value):
                params.update(collect_parameters_from_model(item, prefix=f"{full_key}[{i}]"))
        else:
            params[full_key] = str(value)
    return params


def collect_flat_non_model_fields(model: BaseModel) -> dict[str, str]:
    """
    Collects only the top-level scalar (non-model) fields from a Pydantic model.
    """
    params = {}
    for field_name, field in model.model_fields.items():
        value = getattr(model, field_name)
        if (
            value is not None
            and not isinstance(value, BaseModel)
            and not (isinstance(value, list) and value and isinstance(value[0], BaseModel))
        ):
            params[field_name] = str(value)
    return params


def set_task_parameters(
    task: Task,
    *,
    config: Optional[PipelineConfig] = None,
    step: Optional[PipelineStep] = None,
    base: Optional[PipelineConfig] = None,
) -> None:
    """
    Sets ClearML parameters from a PipelineConfig or a single PipelineStep.
    """
    if (config is None) == (step is None):
        raise ValueError("You must provide exactly one of `config` or `step`.")

    all_params = {}

    if config:
        all_params.update(collect_parameters_from_model(config))
        for idx, step in enumerate(config.pipeline_steps or []):
            all_params.update(collect_parameters_from_model(step, prefix=f"pipeline_steps[{idx}]"))
    else:
        if base:
            # Grab top-level scalar fields only
            all_params.update(collect_flat_non_model_fields(base))
        # Add the actual step fields
        all_params.update(collect_parameters_from_model(step))

    task.set_parameters(all_params)


# ----------------------------
# Config Logging Functions
# ----------------------------
def log_pipeline_config_to_console(config: PipelineConfig) -> None:
    """
    Pretty-logs PipelineConfig details for CLI visibility.
    """
    logger.info('--------------------------------------------------')
    logger.info(" PxPipeline Featurization            ,_")
    logger.info("                                    >' )")
    logger.info("                                    ( ( \\")
    logger.info("                                   rn''|\\")
    logger.info('--------------------------------------------------')
    logger.info(f' Project ID: {config.project_parameters.project_id} ')
    logger.info('--------------------------------------------------\n')

    for section_name in ["project_parameters", "autoscaler_parameters", "raven_query_parameters"]:
        section = getattr(config, section_name)
        logger.info(f"{section_name.replace('_', ' ').title()}:")
        logger.info('--------------------------------------------------')
        for k, v in section.model_dump().items():
            logger.info(f"  {k:<25}: {v}")
        logger.info('--------------------------------------------------\n')

    logger.info("Pipeline Steps:")
    logger.info('--------------------------------------------------')
    for idx, step in enumerate(config.pipeline_steps):
        logger.info(f"  Step {idx + 1}: {step.name}")
        for k, v in step.model_dump().items():
            if k != "name":
                logger.info(f"    {k:<20}: {v}")
        logger.info("")
    logger.info('--------------------------------------------------\n\n')
