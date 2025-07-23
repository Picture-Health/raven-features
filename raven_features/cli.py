import click
import raven as rv

from clearml import PipelineController, Task
from datetime import datetime
from pathlib import Path
from typing import Set, Optional

from raven_features.utils.config import (
    load_config,
    log_pipeline_config_to_clearml,
    PipelineConfig,
    RavenQueryParameters,
)
from raven_features.utils.clearml import build_clearml_params
from raven_features.utils.logs import get_logger
from raven_features.utils.time import formatted_date
from raven_features.utils import env


############################
# Globals
############################
logger = get_logger(__name__)


############################
# Pipeline Launcher
############################
def launch_pipeline(
    config_name: str,
    batch_id: str,
    series_uid: str,
    config: PipelineConfig,
) -> dict:
    """
    Launch a ClearML pipeline task for a given series_uid.
    """
    logger.info(f"   🚀 Creating pipeline task for {series_uid}")

    # Create pipeline task
    pipeline = PipelineController.create(
        project_name="/".join([env.PROJECT_PREFIX, config.project_parameters.project_id]),
        task_name="-".join([config_name, batch_id]),
        repo=env.ENTRYPOINT_REPO,
        branch=env.ENTRYPOINT_BRANCH,
        script=env.ENTRYPOINT_SCRIPT,
        working_directory=env.WORKING_DIR,
        packages=env.PACKAGES,
        add_run_number=True,
    )

    # Connect task parameters to pipeline
    params = build_clearml_params(
        config=config,
        config_name=config_name,
        batch_id=batch_id,
        series_uid=series_uid
    )

    pipeline.task.connect(params)

    # Set user and system tags
    pipeline.task.set_tags([
        f"project_id:{config.project_parameters.project_id}",
        f"job_id:{series_uid}",
    ])
    pipeline.task.set_system_tags((pipeline.task.get_system_tags() or []) + ["pipeline"])

    # Enqueue the task
    Task.enqueue(
        queue_name=config.autoscaler_parameters.launcher_queue,
        task=pipeline.task
    )

    return {"message": "Pipeline task created successfully"}


def get_radiology_series_set(raven_query_config: RavenQueryParameters) -> Set[str]:
    """
    Queries Raven for organ and lesion segmentation masks based on the provided configuration,
    and returns the set of Series UIDs that have both types of masks available.

    Args:
        raven_query_config (RavenQueryParameters): A validated configuration object specifying
            the query parameters for retrieving segmentation masks.

    Returns:
        Set[str]: A set of Series UIDs for which both organ and lesion masks are available.
    """
    # Dump and filter query dict
    base_query = {
        k: v for k, v in raven_query_config.model_dump().items()
        if k not in {"modality", "images", "masks"} and v is not None
    }

    # Query and intersect all mask matches
    series_intersection = set.intersection(*[
        {mask.series_uid for mask in rv.get_masks(**{**base_query, **mask_query.model_dump()})}
        for mask_query in raven_query_config.masks
    ])

    # Log results
    logger.info('--------------------------------------------------')
    logger.info(f"Found ({len(series_intersection)} series at the intersection of the specified mask queries):")
    for uid in sorted(series_intersection):
        logger.info(f"  Series UID: {uid}")
    logger.info('--------------------------------------------------')

    return list(series_intersection)[:5]



def get_pathology_slide_set(raven_query_config: RavenQueryParameters) -> Set[str]:

    return {'fake', 'path', 'set'}



############################
# Featurization CLI
############################
@click.command()
@click.option('--config-uri', default=None, help='S3 URI to a pipeline configuration YAML file')
@click.option('--config-local-path', default=None, type=click.Path(exists=True), help='Local path to a pipeline configuration YAML file')
def featurize(config_uri, config_local_path):
    """
    Loads the pipeline config from either S3 or local path and runs the featurization pipeline.
    """
    # Step 1: Validate mutually exclusive CLI options
    if bool(config_uri) == bool(config_local_path):
        raise click.UsageError("You must provide exactly one of --config-uri or --config-local-path.")

    # Step 2: Load config
    config_path = config_uri or config_local_path
    config_dict, yaml_content = load_config(config_path)
    config_title = Path(config_path).stem
    batch_id = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")

    # Step 3: Initialize ClearML
    task = Task.init(
        project_name=env.PROJECT_PREFIX,
        task_name=f"Featurization - {config_title} @ {formatted_date()}",
        task_type=Task.TaskTypes.data_processing
    )
    task.set_parameter("config_path", config_path)

    config_obj = PipelineConfig.model_validate(config_dict)
    log_pipeline_config_to_clearml(task, config_obj)
    task.upload_artifact(name="config_yaml", artifact_object=yaml_content)

    # Step 4: Query Raven
    logger.info("🔎 Performing Raven query...")

    modality_dispatch = {
        'radiology': get_radiology_series_set,
        'pathology': get_pathology_slide_set,
    }

    try:
        trigger_set = modality_dispatch[config_obj.raven_query_parameters.modality](
            config_obj.raven_query_parameters
        )
    except KeyError:
        logger.info(f"⚠️ Modality '{config_obj.raven_query_parameters.modality}' is not supported. Aborting.")
        return

    if not trigger_set:
        logger.info("⚠️ No pipelines to launch. Aborting procedure.")
        return

    # Step 5: Trigger pipelines
    logger.info("🚀 Launching featurization pipelines...")

    for set_item in trigger_set:
        launch_pipeline(
            config_name=config_title,
            batch_id=batch_id,
            series_uid=set_item,
            config=config_obj
        )

    logger.info("✅ Featurization pipelines successfully triggered.")



# if __name__ == "__main__":
#     featurize(
#         config_uri=None, #'s3://px-app-bucket/config/eng-test.yaml',
#         config_local_path='../config/eng-test-lung.yaml'
#     )
