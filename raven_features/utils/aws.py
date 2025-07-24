import boto3

from urllib.parse import urlparse


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