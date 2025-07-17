import json
from typing import Dict, Any
from utils.config import flatten_model, unflatten_config, PipelineConfig


def build_clearml_params(
    config: PipelineConfig,
    config_name: str,
    batch_id: str,
    series_uid: str,
) -> Dict[str, Any]:
    """
    Build a ClearML-compatible parameter dict with flattened, intuitive keys.
    Removes nested 'config.' prefix and avoids namespacing like 'Pipeline/'.
    """
    flat_config = flatten_model(config)  # No prefix here

    params = {
        "config_name": config_name,
        "batch_id": batch_id,
        "series_uid": series_uid,
        "job_id": series_uid,
        **{
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in flat_config.items()
        }
    }
    return params


def parse_clearml_params(params: Dict[str, str], section: str = None) -> PipelineConfig:
    prefix = f"{section}/" if section else ""
    return unflatten_config(params, PipelineConfig, prefix=prefix)
