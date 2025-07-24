############################
# ClearML Paramaters
############################
PROJECT_PREFIX = "RAVEN-FEATURES"
CONFIG_ARTIFACT_NAME = 'config_yaml'


############################
# Driver Entrypoint
############################
ENTRYPOINT_REPO = "https://github.com/Picture-Health/raven-features.git"
ENTRYPOINT_BRANCH = "main"
ENTRYPOINT_SCRIPT = "raven_features/driver.py"
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