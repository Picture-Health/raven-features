import boto3
import yaml

from io import StringIO


def load_config_yaml_from_s3(s3_uri: str) -> tuple[dict, str]:
    """
    Loads and parses a YAML file from the given S3 URI.

    Returns:
        A tuple of (parsed_dict, raw_yaml_string) for both programmatic use and ClearML artifact logging.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("config_uri must be an S3 URI")

    s3 = boto3.client("s3")
    _, _, bucket_and_key = s3_uri.partition("s3://")
    bucket, _, key = bucket_and_key.partition("/")

    obj = s3.get_object(Bucket=bucket, Key=key)
    yaml_content = obj["Body"].read().decode("utf-8")
    config_dict = yaml.safe_load(StringIO(yaml_content))
    return config_dict, yaml_content