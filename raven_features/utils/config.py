import yaml

from clearml import Task
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from raven_features.utils.aws import retrieve_s3_file
from raven_features.utils.filesystem import retrieve_local_file
from raven_features.utils.models import PipelineConfig
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
    yaml_content: Optional[str] = None
) -> PipelineConfig:
    """
    Loads a config from local path, S3 URI, or raw YAML.
    Returns a validated PipelineConfig model.
    """
    content, source = read_config_content(
        config_path=config_path,
        config_uri=config_uri,
        yaml_content=yaml_content,
    )

    config_dict = process_config_yaml(content)

    # Add metadata before model validation
    config_dict.update({
        "config_path": source,
        "config_name": get_config_name(source),
        "batch_id": datetime.now().strftime("%Y-%m-%d__%H-%M-%S"),
        "yaml_content": content,
    })

    return PipelineConfig(**config_dict)


# ----------------------------
# Config Logging Functions
# ----------------------------
def set_task_parameters_from_config(task: Task, config: PipelineConfig) -> None:
    """
    Sets ClearML parameters using model_dump() of each submodel in the PipelineConfig.
    """
    # Set flat sections
    for section_name in ["project_parameters", "autoscaler_parameters", "raven_query_parameters"]:
        section = getattr(config, section_name)
        task.set_parameters_as_dict({f"{section_name}.{k}": str(v) for k, v in section.model_dump().items()})

    # Set pipeline steps
    for idx, step in enumerate(config.pipeline_steps):
        step_params = {f"pipeline_steps[{idx}].{k}": str(v) for k, v in step.model_dump().items()}
        task.set_parameters_as_dict(step_params)


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
