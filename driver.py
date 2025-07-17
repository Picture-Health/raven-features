"""
Entrypoint script for ClearML pipeline execution.

This script is triggered by a parent job and is responsible for:
1. Rehydrating the pipeline configuration from flattened parameters.
2. Registering each pipeline step as a ClearML task.
3. Launching the full pipeline on the ClearML orchestration queue.
"""

from clearml import PipelineController, Task
from datetime import datetime, timezone

from utils.config import PipelineStep
from utils.params import parse_clearml_params, build_clearml_params
from utils.logs import get_logger
from utils import env


############################
# Globals
############################
logger = get_logger(__name__)


############################
# Functions
############################
def status_change_callback(pipeline, node, prev_status):
    """
    Custom callback to update task start time metadata
    once ClearML begins execution of the task.
    """
    if node.job.task.status in (
            Task.TaskStatusEnum.created,
            Task.TaskStatusEnum.queued,
            Task.TaskStatusEnum.in_progress
    ) and not node.job.task.data.started:
        node.job.task.data.started = datetime.now().astimezone(timezone.utc)


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
    params = task.get_parameters()
    env_tags = task.get_tags()

    # Restore nested config from flattened params
    config = parse_clearml_params(params, section="General")
    config_name = config.config_name
    batch_id = config.batch_id
    series_uid = config.series_uid

    logger.info(f' Project ID: {config.project_parameters.project_id} ')
    logger.info('--------------------------------------------------\n')
    logger.info("📌 Pipeline Parameters:")
    logger.info('--------------------------------------------------')
    logger.info(f"       - Config Name: {config_name}")
    logger.info(f"       - Batch ID: {batch_id}")
    logger.info(f"       - Series UID: {series_uid}")
    logger.info(f"       - Tags: {env_tags}")
    logger.info("")

    # Construct ClearML project path
    project_name = "/".join([
        env.PROJECT_PREFIX,
        config.project_parameters.project_id,
        f"{config_name}-{batch_id}"
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
        step_params = build_clearml_params(config, config_name, batch_id, series_uid)
        step_task.connect(step_params)
        step_task.add_tags(env_tags)

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
