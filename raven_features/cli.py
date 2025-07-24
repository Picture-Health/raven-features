import click

from clearml import PipelineController, Task

from raven_features.utils.config import (
    load_config,
    set_task_parameters_from_config,
    log_pipeline_config_to_console,
    PipelineConfig,
)
from raven_features.utils.query import get_radiology_series_set, get_pathology_slide_set
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
def launch_pipeline(config: PipelineConfig) -> dict:
    """
    Launch a ClearML pipeline task for a given series_uid.
    """
    logger.info(f"   🚀 Creating pipeline task for {config.series_uid}")

    # Create pipeline controller task
    pipeline = PipelineController.create(
        project_name="/".join([env.PROJECT_PREFIX, config.project_parameters.project_id]),
        task_name="-".join([config.config_name, config.batch_id]),
        repo=env.ENTRYPOINT_REPO,
        branch=env.ENTRYPOINT_BRANCH,
        script=env.ENTRYPOINT_SCRIPT,
        working_directory=env.WORKING_DIR,
        packages=env.PACKAGES,
        add_run_number=True,
    )

    # Set parameters, tags and config artifact to pipeline controller task
    set_task_parameters_from_config(pipeline.task, config)
    pipeline.task.set_tags([
        f"project_id:{config.project_parameters.project_id}",
        f"job_id:{config.series_uid}",
    ])
    pipeline.task.set_system_tags((pipeline.task.get_system_tags() or []) + ["pipeline"])
    pipeline.task.upload_artifact(
        name=env.CONFIG_ARTIFACT_NAME,
        artifact_object=config.yaml_content
    )

    # Enqueue the task
    Task.enqueue(
        queue_name=config.autoscaler_parameters.launcher_queue,
        task=pipeline.task
    )

    return {"message": "Pipeline task created successfully"}


############################
# Featurization CLI
############################
# @click.command()
# @click.option('--config-uri', default=None, help='S3 URI to a pipeline configuration YAML file')
# @click.option('--config-local-path', default=None, type=click.Path(exists=True), help='Local path to a pipeline configuration YAML file')
def featurize(config_uri, config_local_path):
    """
    Loads the pipeline config from either S3 or local path and runs the featurization pipeline.
    """
    # Step 1: Validate mutually exclusive CLI options
    if bool(config_uri) == bool(config_local_path):
        raise click.UsageError("You must provide exactly one of --config-uri or --config-local-path.")

    # Step 2: Call load_config with correct keyword argument
    config = load_config(
        config_uri=config_uri if config_uri else None,
        config_path=config_local_path if config_local_path else None
    )

    # Step 3: Initialize ClearML
    task = Task.init(
        project_name=env.PROJECT_PREFIX,
        task_name=f"Featurization - {config.config_name} @ {formatted_date()}",
        task_type=Task.TaskTypes.data_processing
    )
    set_task_parameters_from_config(task, config)
    task.upload_artifact(
        name=env.CONFIG_ARTIFACT_NAME,
        artifact_object=config.yaml_content
    )

    # Step 4: Log the fully loaded task
    log_pipeline_config_to_console(config)

    # Step 5: Query Raven
    logger.info("🔎 Performing Raven query...")
    modality_dispatch = {
        'radiology': get_radiology_series_set,
        'pathology': get_pathology_slide_set,
    }

    try:
        trigger_set = modality_dispatch[config.raven_query_parameters.modality](
            config.raven_query_parameters
        )
    except KeyError:
        logger.info(f"⚠️ Modality '{config.raven_query_parameters.modality}' is not supported. Aborting.")
        return

    if not trigger_set:
        logger.info("⚠️ No pipelines to launch. Aborting procedure.")
        return

    # Step 5: Trigger pipelines
    logger.info("🚀 Launching featurization pipelines...")
    debug_counter = 0
    for series_uid in trigger_set:
        if debug_counter < 3:
            config.series_uid = series_uid
            launch_pipeline(config=config)
            debug_counter += 1
    logger.info("✅ Featurization pipelines successfully triggered.")



if __name__ == "__main__":
    featurize(
        config_uri=None, #'s3://px-app-bucket/config/eng-test.yaml',
        config_local_path='../config/eng-test-lung.yaml'
    )
