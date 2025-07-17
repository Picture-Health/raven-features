PROJECT_PREFIX = "RAVEN-FEATURES"
ENTRYPOINT_REPO = "https://github.com/Picture-Health/px-pipeline.git"
ENTRYPOINT_BRANCH = "ref/config-standardization"
ENTRYPOINT_SCRIPT = "driver.py"
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
LESIONS_SUFFIX = "-LESIONS"