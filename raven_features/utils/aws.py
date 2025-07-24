import boto3

from io import BytesIO
from urllib.parse import urlparse

from raven_features.utils import env
from raven_features.utils.models import PipelineConfig
from raven_features.utils.logs import get_logger

############################
# Globals
############################
logger = get_logger(__name__)

############################
# Functions
############################
def retrieve_s3_file(s3_uri: str) -> str:
    """
    Retrieves file contents from an S3 URI as a UTF-8 string.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    s3 = boto3.client("s3")
    _, _, bucket_and_key = s3_uri.partition("s3://")
    bucket, _, key = bucket_and_key.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def list_existing_files_in_s3(s3_paths: list[str]) -> list[str]:
    """
    For each S3 prefix in `s3_paths`, checks if any files exist under that prefix.

    Returns:
        A list of full S3 URIs to all discovered files.
    """
    s3 = boto3.client("s3")
    existing_files = []

    for s3_uri in s3_paths:
        bucket, prefix = parse_s3_uri(s3_uri)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            existing_files.append(f"s3://{bucket}/{obj['Key']}")

    return existing_files


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """
    Parses s3://bucket/key URI into (bucket, key)
    """
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def construct_s3_key(filename: str, config: PipelineConfig) -> str:
    """Constructs an S3 key using PipelineConfig metadata."""
    key = '/'.join([
        config.project_parameters.email,
        config.batch_id,
        config.series_uid
        filename
    ])
    return key


def upload_s3_file(
    file_content: BytesIO,
    filename: str,
    config: PipelineConfig,
    content_type: str = "application/gzip",
    bucket: str = "px-app-bucket"
) -> str:
    """
    Uploads a file-like object (e.g., BytesIO) to S3 using metadata from a PipelineConfig.

    Args:
        file_content: File-like object supporting .seek() and .read().
        filename: Name of the file to store in S3.
        config: PipelineConfig object with project/user metadata.
        content_type: MIME type of the file. Default is gzip.
        bucket: S3 bucket to upload to.

    Returns:
        The full S3 URI of the uploaded file.
    """
    s3 = boto3.client("s3")
    key = construct_s3_key(filename, config=config)

    try:
        file_content.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_content,
            ContentType=content_type
        )
        uri = f"s3://{bucket}/{key}"
        logger.info(f"✅ Uploaded {filename} to {uri}")
        return uri
    except Exception as e:
        logger.exception(f"❌ Failed to upload {filename} to S3")
        raise