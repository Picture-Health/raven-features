from pydantic import BaseModel, EmailStr, field_validator, model_validator
from typing import List, Optional, Literal, Union


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
    config_path: Optional[str] = None
    config_name: Optional[str] = None
    batch_id: Optional[str] = None
    series_uid: Optional[str] = None
    yaml_content: Optional[str] = None

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