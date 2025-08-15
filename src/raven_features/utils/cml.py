from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from clearml import Task

from raven_features.utils.logs import get_logger
from raven_features.utils import env

logger = get_logger(__name__)


@dataclass(frozen=True)
class RuntimeMeta:
    """Entry-point local runtime identifiers propagated by the launcher."""
    config_name: Optional[str]
    batch_id: Optional[str]
    yaml_content: str
    series_uid: str
    feat_id: str


def _get_current_task_and_artifacts() -> tuple[Task, str, dict[str, str]]:
    """
    Returns: (current_task, config_yaml, general_params)
    - config_yaml is fetched from the CONFIG_ARTIFACT_NAME artifact
    - general_params is the 'General' parameter group (flattened dict)
    """
    task = Task.current_task()
    if task is None:
        raise RuntimeError("No current ClearML task found (Task.current_task() is None).")

    if env.CONFIG_ARTIFACT_NAME not in task.artifacts:
        raise RuntimeError(
            f"Expected artifact '{env.CONFIG_ARTIFACT_NAME}' not found on task {task.id}."
        )

    config_yaml = task.artifacts[env.CONFIG_ARTIFACT_NAME].get()
    params = task.get_parameters_as_dict() or {}
    general = params.get("General") or {}
    return task, config_yaml, general


def _build_runtime_meta(config_yaml: str, general: dict[str, str]) -> RuntimeMeta:
    def g(key: str, required: bool = False, default: Optional[str] = None) -> Optional[str]:
        if required and not general.get(key):
            raise RuntimeError(f"Missing required parent param: General.{key}")
        return general.get(key, default)

    return RuntimeMeta(
        config_name=g("config_name", required=False, default="config"),
        batch_id=g("batch_id", required=False),
        yaml_content=config_yaml,
        series_uid=g("run.series_uid", required=True),      # set by launcher
        feat_id=g("run.featurization_id", required=True),   # set by launcher
    )