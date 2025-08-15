############################
# ClearML Paramaters
############################
PROJECT_PREFIX = "RAVEN-FEATURES"
CONFIG_ARTIFACT_NAME = 'config_yaml'
CLEARML_LAUNCHER_QUEUE = 'PxPipeline_Launcher_gcp'


############################
# Driver Entrypoint
############################
ENTRYPOINT_REPO = "https://github.com/Picture-Health/raven-features.git"
ENTRYPOINT_BRANCH = "main"
ENTRYPOINT_SCRIPT = "src/raven_features/entrypoint.py"
WORKING_DIR = "."
PACKAGES = [
    "numpy==1.26.4",
    "boto3==1.35.62",
    "python-dotenv==1.0.1",
    "hydra-core",
    "omegaconf",
    "email-validator==2.2.0",
    "pydantic==2.11.7"
]
