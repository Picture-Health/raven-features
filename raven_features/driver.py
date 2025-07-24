"""
Entrypoint script for ClearML pipeline execution.

This script is triggered by a parent job and is responsible for:
1. Rehydrating the pipeline configuration from flattened parameters.
2. Registering each pipeline step as a ClearML task.
3. Launching the full pipeline on the ClearML orchestration queue.
"""

from clearml import PipelineController, Task

from raven_features.utils.config import load_config, set_task_parameters
from raven_features.utils.models import  PipelineStep
from raven_features.utils.logs import get_logger
from raven_features.utils import env


############################
# Globals
############################
logger = get_logger(__name__)


############################
# Functions
############################
def status_change_callback(pipeline, node, prev_status):
    from datetime import datetime, timezone
    if (
        node.job.task.status == Task.TaskStatusEnum.created
        or node.job.task.status == Task.TaskStatusEnum.queued
        or node.job.task.status == Task.TaskStatusEnum.in_progress
    ) and not node.job.task.data.started:
        node.job.task.data.started = datetime.now().astimezone(timezone.utc)  # assuming utc timezone


def main():
    """
    Main function to rehydrate config, register pipeline steps, and launch execution.
    """
    # Create pipeline controller
    pipeline = PipelineController(
        name="",    # required to reconnect to PipelineController
        project="", # required to reconnect to PipelineController
    )

    logger.info("🔎 Initializing pipeline from upstream trigger...")
    logger.info('--------------------------------------------------')
    logger.info(" RAVEN FEATURES                      ,_")
    logger.info("                                    >' )")
    logger.info("                                    ( ( \\")
    logger.info("                                   rn''|\\")
    logger.info('--------------------------------------------------')

    # Current ClearML task (i.e. the pipeline job)
    task = Task.current_task()
    tags = task.get_tags()
    config_artifact = task.artifacts[env.CONFIG_ARTIFACT_NAME].get()
    controller_params = task.get_parameters_as_dict()['General']

    config = load_config(
        yaml_content=config_artifact,
        featurization_metadata={
            "config_path": controller_params['config_path'],
            "config_name": controller_params['config_name'],
            "batch_id": controller_params['batch_id'],
            "series_uid": controller_params['series_uid'],
            "yaml_content": config_artifact
        }
    )

    logger.info(f' Project ID: {config.project_parameters.project_id} ')
    logger.info('--------------------------------------------------\n')
    logger.info("📌 Pipeline Parameters:")
    logger.info('--------------------------------------------------')
    logger.info(f"       - Config Name: {config.config_name}")
    logger.info(f"       - Batch ID: {config.batch_id}")
    logger.info(f"       - Series UID: {config.series_uid}")
    logger.info(f"       - Tags: {tags}")
    logger.info("")

    # Construct ClearML project path
    project_name = "/".join([
        env.PROJECT_PREFIX,
        config.project_parameters.project_id,
        f"{config.config_name}-{config.batch_id}"
    ])

    # Register each step as a ClearML task
    for step_cfg in config.pipeline_steps:
        step = step_cfg if isinstance(step_cfg, PipelineStep) else PipelineStep(**step_cfg)

        logger.info(f"📦 Registering Step: {step.name}")
        logger.info('--------------------------------------------------')
        logger.info(f"       - Repo: {step.repo}")
        logger.info(f"       - Branch: {step.branch}")
        logger.info(f"       - Script: {step.script}")
        logger.info(f"       - Queue: {step.execution_queue}")

        step_task = Task.create(
            task_name=step.name,
            project_name=project_name,
            repo=step.repo,
            branch=step.branch,
            working_directory=step.working_directory,
            commit=step.commit,
            script=step.script,
        )
        set_task_parameters(step_task, step=step, base=config)
        step_task.add_tags(tags)
        step_task.upload_artifact(
            name=env.CONFIG_ARTIFACT_NAME,
            artifact_object=config.yaml_content
        )

        pipeline.add_step(
            name=step.name,
            base_task_id=step_task.id,
            execution_queue=step.execution_queue,
            time_limit=step.time_limit,
            retry_on_failure=3,
            parents=step.parent_steps,
            clone_base_task=False,
            status_change_callback=status_change_callback,
        )

    logger.info(f"🚀 Launching pipeline from {config.autoscaler_parameters.launcher_queue}...")
    pipeline.start(queue=config.autoscaler_parameters.launcher_queue)
    logger.info("✅ Pipeline launched successfully.")



############################
# Entrypoint
############################
if __name__ == "__main__":
    main()
