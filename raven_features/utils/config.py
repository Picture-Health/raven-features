import ast
import yaml

from clearml import Task
from io import StringIO
from pydantic import BaseModel, EmailStr, field_validator, model_validator
from typing import Any, List, Optional, Literal, Union

from raven_features.utils.aws import retrieve_s3_file
from raven_features.utils.filesystem import retrieve_local_file
from raven_features.utils.logs import get_logger


# ----------------------------
# Globals
# ----------------------------
logger = get_logger(__name__)


# ----------------------------
# PROJECT PARAMETERS MODEL
# ----------------------------
class ProjectParameters(BaseModel):
    email: EmailStr
    project_id: str
    feature_group_id: str


# ----------------------------
# AUTOSCALER PARAMETERS MODEL
# ----------------------------
class AutoscalerParameters(BaseModel):
    launcher_queue: str


# ----------------------------
# RAVEN QUERY PARAMETERS MODEL
# ----------------------------

class ImageParameters(BaseModel):
    provenance: str


class MaskParameters(BaseModel):
    mask_type: str
    provenance: str


class RavenQueryParameters(BaseModel):
    modality: Literal["radiology", "pathology"]

    # Universal query parameters
    dataset_id: Optional[Union[str, List[str]]] = None
    clinical_id: Optional[str] = None
    ingestion_id: Optional[str] = None

    # New flexible image & mask parameters
    images: Optional[ImageParameters] = None
    masks: Optional[List[MaskParameters]] = None

    @model_validator(mode="after")
    def at_least_one_id(cls, model: "RavenQueryParameters"):
        if not (model.dataset_id or model.clinical_id or model.ingestion_id):
            raise ValueError("At least one of dataset_id, clinical_id, or ingestion_id must be provided.")
        return model


# ----------------------------
# PIPELINE STEPS MODEL
# ----------------------------
class PipelineStep(BaseModel):
    name: str
    repo: str
    branch: str
    working_directory: str
    commit: str
    script: str
    output_path: Optional[str] = None
    execution_queue: str
    parent_steps: List[str] = []
    time_limit: int

    @field_validator("time_limit")
    @classmethod
    def positive_time_limit(cls, v):
        if v <= 0:
            raise ValueError("time_limit must be positive")
        return v


# ----------------------------
# ROOT PIPELINE CONFIG MODEL
# ----------------------------
class PipelineConfig(BaseModel):
    config_name: Optional[str] = None
    batch_id: Optional[str] = None
    series_uid: Optional[str] = None

    project_parameters: ProjectParameters
    autoscaler_parameters: AutoscalerParameters
    raven_query_parameters: RavenQueryParameters
    pipeline_steps: List[PipelineStep]

    @field_validator("pipeline_steps")
    @classmethod
    def validate_pipeline_steps_exist(cls, v):
        if not v:
            raise ValueError("pipeline_steps must contain at least one step")
        return v


# ----------------------------
# Config Retrieval Functions
# ----------------------------
def process_config_yaml(yaml_content: str) -> dict:
    """
    Parses a YAML string and returns the corresponding dictionary.
    """
    return yaml.safe_load(StringIO(yaml_content))

def load_config(config_path_or_uri: str) -> tuple[dict, str]:
    """
    General-purpose loader that handles both S3 and local config files.

    Returns:
        Tuple of (parsed_dict, raw_yaml_string)
    """
    if config_path_or_uri.startswith("s3://"):
        yaml_content = retrieve_s3_file(config_path_or_uri)
    else:
        yaml_content = retrieve_local_file(config_path_or_uri)

    config_dict = process_config_yaml(yaml_content)
    return config_dict, yaml_content

# ----------------------------
# Model Manipulation Functions
# ----------------------------
def flatten_config(model: BaseModel, prefix: str = "") -> dict[str, Any]:
    """
    Recursively flattens nested Pydantic models into a flat dictionary with dot-notated keys.
    Lists of models are flattened with index-based keys (e.g., steps.0.name).
    """
    flat = {}

    for name, value in model.model_dump(exclude_none=True).items():
        key = f"{prefix}.{name}" if prefix else name

        if isinstance(value, BaseModel):
            flat.update(flatten_config(value, prefix=key))

        elif isinstance(value, list):
            if all(isinstance(i, BaseModel) for i in value):
                for idx, item in enumerate(value):
                    flat.update(flatten_config(item, prefix=f"{key}.{idx}"))
            else:
                flat[key] = value  # Let non-BaseModel lists pass through

        elif isinstance(value, dict):
            for sub_key, sub_val in value.items():
                flat[f"{key}.{sub_key}"] = sub_val

        else:
            flat[key] = value

    return flat


def unflatten_config(
    params: dict[str, Any],
    model_class: type[BaseModel],
    prefix: str = "General/"
) -> BaseModel:
    """
    Reconstructs a nested Pydantic model from flattened ClearML parameters.
    """
    nested = {}

    for full_key, value in params.items():
        if prefix and not full_key.startswith(prefix):
            continue

        key = full_key[len(prefix):] if prefix else full_key

        # Parse stringified lists/dicts if needed
        if isinstance(value, str) and value.strip().startswith(("{", "[")):
            try:
                value = ast.literal_eval(value)
            except Exception:
                pass

        # Convert dotted key into nested dict structure
        parts = key.split(".")
        d = nested
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = value

    return model_class.model_validate(nested)


# ----------------------------
# Config Logging Functions
# ----------------------------
def log_model_parameters(task, model: BaseModel, section_name: str = None):
    """
    Logs all fields from a Pydantic model to the ClearML task parameters.

    Args:
        task: A ClearML Task instance.
        model: A Pydantic BaseModel instance.
        section_name: Optional section prefix to group parameters.
    """
    flat_params = flatten_config(model)
    param_prefix = f"{section_name or model.__class__.__name__}."

    for k, v in flat_params.items():
        if isinstance(v, (dict, list)):
            v = str(v)
        task.set_parameter(f"{param_prefix}{k}", v)


def log_pipeline_config_to_clearml(task: Task, config: PipelineConfig):
    """
    Pretty-logs and sets ClearML parameters from a PipelineConfig model.
    """
    logger.info('--------------------------------------------------')
    logger.info(" PxPipeline Featurization            ,_")
    logger.info("                                    >' )")
    logger.info("                                    ( ( \\")
    logger.info("                                   rn''|\\")
    logger.info('--------------------------------------------------')
    logger.info(f' Project ID: {config.project_parameters.project_id} ')
    logger.info('--------------------------------------------------\n')

    # Log project, autoscaler, and query params
    for section_name in ["project_parameters", "autoscaler_parameters", "raven_query_parameters"]:
        section = getattr(config, section_name)
        logger.info(f"{section_name.replace('_', ' ').title()}:")
        logger.info('--------------------------------------------------')
        for k, v in section.model_dump().items():
            logger.info(f"  {k:<25}: {v}")
            task.set_parameter(f"{section_name}.{k}", str(v))
        logger.info('--------------------------------------------------\n')

    # Log pipeline steps with clearer formatting
    logger.info("Pipeline Steps:")
    logger.info('--------------------------------------------------')
    for idx, step in enumerate(config.pipeline_steps):
        logger.info(f"  Step {idx + 1}: {step.name}")
        for k, v in step.model_dump().items():
            if k == "name":
                continue  # Already printed above
            logger.info(f"    {k:<20}: {v}")
            task.set_parameter(f"pipeline_steps[{idx}].{k}", str(v))
        logger.info("")

    logger.info('--------------------------------------------------\n\n')
