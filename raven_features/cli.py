import click
import raven as rv

from clearml import PipelineController, Task
from datetime import datetime
from typing import Set

from utils.aws import load_config_yaml_from_s3
from utils.config import (
    PipelineConfig,
    RavenQueryParameters,
    log_pipeline_config_to_clearml
)
from utils.params import build_clearml_params
from utils.logs import get_logger
from utils.time import formatted_date
from utils import env


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
    query_dict = raven_query_config.model_dump()

    # Unpack query values that do not directly translate to Raven
    _ = query_dict.pop('modality')
    organ_mask_type = query_dict.pop('organ_mask_type')
    organ_mask_provenance = query_dict.pop('organ_mask_provenance')
    lesion_mask_type = query_dict.pop('lesion_mask_type')
    lesion_mask_provenance = query_dict.pop('lesion_mask_provenance')

    # Remove any remaining NoneTypes
    query_dict = {k: v for k, v in query_dict.items() if v is not None}

    # Construct valid Raven query for organ masks
    query_dict['mask_type'] = organ_mask_type
    query_dict['provenance'] = organ_mask_provenance
    organ_mask_list = rv.get_masks(**query_dict)
    organ_mask_series_uids = {mask.series_uid for mask in organ_mask_list}

    # Construct valid Raven query for lesion masks
    query_dict['mask_type'] = lesion_mask_type
    query_dict['provenance'] = lesion_mask_provenance
    lesion_mask_list = rv.get_masks(**query_dict)
    lesion_mask_series_uids = {mask.series_uid for mask in lesion_mask_list}

    # Compute intersection
    series_intersection = organ_mask_series_uids & lesion_mask_series_uids

    # Log results
    logger.info('--------------------------------------------------')
    logger.info(f"Found ({len(series_intersection)} series with requested organ and lesion masks):")
    for uid in sorted(series_intersection):
        logger.info(f"  Series UID: {uid}")
    logger.info('--------------------------------------------------')

    return series_intersection



def get_pathology_slide_set(raven_query_config: RavenQueryParameters) -> Set[str]:

    return {'fake', 'path', 'set'}



############################
# Featurization CLI
############################
@click.command()
@click.option('--config-uri', required=True, help='S3 URI to a pipeline configuration YAML file')
def featurize(config_uri: str):
    """
    Loads the pipeline config YAML from S3 and runs the featurization pipeline.
    """
    batch_id = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
    config_filename = config_uri.split('/')[-1]
    config_title = config_filename.split('.')[0]

    # Step 1: Initiate ClearML task
    task = Task.init(
        project_name=env.PROJECT_PREFIX,
        task_name=f"Featurization - {config_title} @ {formatted_date()}",
        task_type=Task.TaskTypes.data_processing
    )

    task.set_parameter("config_uri", config_uri)

    # Step 2: Load and validate pipeline config
    logger.info(f"📥 Loading pipeline config from: {config_uri}")
    config_dict, yaml_content = load_config_yaml_from_s3(config_uri)

    config_obj = PipelineConfig.model_validate(config_dict)

    # Log structured config parameters to ClearML and upload raw YAML as an artifact
    log_pipeline_config_to_clearml(task, config_obj)
    task.upload_artifact(name="config_yaml", artifact_object=yaml_content)


    # Step 3: Query Raven
    logger.info("🔎 Performing Raven query...")

    modality = config_obj.raven_query_parameters.modality
    if modality == 'radiology':
        trigger_set = get_radiology_series_set(config_obj.raven_query_parameters)
    elif modality == 'pathology':
        trigger_set = get_pathology_slide_set(config_obj.raven_query_parameters)
    else:
        logger.info(f"⚠️ Modality {modality} is not currently supported in Raven. Aborting procedure.")
        return

    if not trigger_set:
        logger.info("⚠️ No pipelines to launch. Aborting procedure.")
        return


    # Step 4: Trigger pipelines
    logger.info("🚀 Launching featurization pipelines...")

    # TODO: REMOVE TRIGGER SET TRUNCATION AFTER TESTING
    trigger_set = set(list(trigger_set)[:5])
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
#         config_uri='s3://px-app-bucket/config/eng-test.yaml'
#     )
