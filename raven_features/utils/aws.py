import boto3


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
