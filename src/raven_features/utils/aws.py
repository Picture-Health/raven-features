# ──────────────────────────────────────────────────────────────────────────────
# utils/aws.py  — S3 helpers & layout
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Optional, Iterable, Dict, List, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from raven_features.utils.logs import get_logger

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ──────────────────────────────────────────────────────────────────────────────

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parses s3://bucket/key → (bucket, key)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    p = urlparse(s3_uri)
    return p.netloc, p.path.lstrip("/")


def retrieve_s3_file(s3_uri: str) -> str:
    """Read object as UTF-8 text."""
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def download_s3_file(s3_uri: str, local_path: str) -> None:
    """Download object to local path."""
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    s3.download_file(Bucket=bucket, Key=key, Filename=local_path)


def list_existing_files_in_s3(s3_prefixes: list[str]) -> list[str]:
    """
    List all objects under the provided prefixes. Paginates safely.
    Returns full s3:// URIs.
    """
    s3 = boto3.client("s3")
    out: list[str] = []
    for uri in s3_prefixes:
        bucket, prefix = parse_s3_uri(uri)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                out.append(f"s3://{bucket}/{obj['Key']}")
    return out


def upload_bytes(
    *,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> str:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    uri = f"s3://{bucket}/{key}"
    logger.info(f"✅ Uploaded {uri}")
    return uri


def upload_json(
    *,
    bucket: str,
    key: str,
    obj: Any,
    indent: int = 2,
) -> str:
    payload = json.dumps(obj, indent=indent, sort_keys=True).encode("utf-8")
    return upload_bytes(bucket=bucket, key=key, data=payload, content_type="application/json")


def upload_stream(
    *,
    bucket: str,
    key: str,
    stream: BytesIO,
    content_type: str = "application/octet-stream",
) -> str:
    stream.seek(0)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=stream, ContentType=content_type)
    uri = f"s3://{bucket}/{key}"
    logger.info(f"✅ Uploaded {uri}")
    return uri


# ──────────────────────────────────────────────────────────────────────────────
# Layout builder for predictable keys
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class S3PathContext:
    """
    Minimal context needed to form deterministic keys.
    batch_id is your timestamp; feat_id/ext_id are optional until you’re inside a step.
    """
    project_id: str
    feature_group_id: str
    user: str
    batch_id: str
    series_uid: str
    feat_id: Optional[str] = None
    ext_id: Optional[str] = None


class S3Layout:
    """
    Pure functions returning key prefixes to keep structure consistent.
    You can safely use these in launcher, entrypoint, or steps.
    """
    @staticmethod
    def batch_root(ctx: S3PathContext) -> str:
        return "/".join([ctx.project_id, ctx.feature_group_id, ctx.user, ctx.batch_id])

    @staticmethod
    def config_key(ctx: S3PathContext) -> str:
        # one copy per batch
        return f"{S3Layout.batch_root(ctx)}/config.yaml"

    @staticmethod
    def batch_manifest_key(ctx: S3PathContext) -> str:
        return f"{S3Layout.batch_root(ctx)}/MANIFEST.json"

    @staticmethod
    def series_root(ctx: S3PathContext) -> str:
        return f"{S3Layout.batch_root(ctx)}/{ctx.series_uid}"

    @staticmethod
    def series_manifest_key(ctx: S3PathContext) -> str:
        return f"{S3Layout.series_root(ctx)}/manifest.json"

    @staticmethod
    def base_inputs_prefix(ctx: S3PathContext) -> str:
        return f"{S3Layout.series_root(ctx)}/inputs/base/"

    @staticmethod
    def extraction_root(ctx: S3PathContext) -> str:
        if not ctx.feat_id or not ctx.ext_id:
            raise ValueError("feat_id and ext_id required for extraction paths")
        return f"{S3Layout.series_root(ctx)}/extractions/{ctx.feat_id}/{ctx.ext_id}"

    @staticmethod
    def masks_prefix(ctx: S3PathContext) -> str:
        return f"{S3Layout.extraction_root(ctx)}/inputs/masks/"

    @staticmethod
    def outputs_prefix(ctx: S3PathContext) -> str:
        return f"{S3Layout.extraction_root(ctx)}/outputs/"

    @staticmethod
    def logs_prefix(ctx: S3PathContext) -> str:
        return f"{S3Layout.extraction_root(ctx)}/logs/"

    @staticmethod
    def key_for(ctx: S3PathContext, prefix: str, filename: str) -> str:
        prefix = prefix if prefix.endswith("/") else prefix + "/"
        return f"{prefix}{filename}"


# ──────────────────────────────────────────────────────────────────────────────
# Public high-level helpers you’ll actually call
# ──────────────────────────────────────────────────────────────────────────────

def upload_batch_config(*, bucket: str, ctx: S3PathContext, yaml_text: str) -> str:
    """
    Save one copy of the YAML at the batch root.
    Call from the launcher (once per batch) or the first entrypoint that notices it missing.
    Idempotent: will overwrite; S3 is versioned in most setups.
    """
    key = S3Layout.config_key(ctx)
    return upload_bytes(bucket=bucket, key=key, data=yaml_text.encode("utf-8"), content_type="text/yaml")


def ensure_base_image_manifest(
    *,
    bucket: str,
    ctx: S3PathContext,
    image_pointers: dict[str, Any],
) -> str:
    """
    Store a tiny manifest describing the series-level base inputs (e.g., source URIs).
    Keep the actual large blobs wherever they already live; fetch on-demand in steps.
    """
    key = S3Layout.key_for(ctx, S3Layout.base_inputs_prefix(ctx), "manifest.json")
    return upload_json(bucket=bucket, key=key, obj=image_pointers)


def upload_mask_file(
    *,
    bucket: str,
    ctx: S3PathContext,
    provenance: str,
    mask_type: str,
    filename: str,
    data: bytes | BytesIO,
    content_type: str = "application/octet-stream",
) -> str:
    """
    Put mask data under the extraction-specific masks prefix, namespaced by provenance/mask_type.
    """
    base = f"{S3Layout.masks_prefix(ctx)}{provenance}/{mask_type}/"
    key = f"{base}{filename}"
    if isinstance(data, BytesIO):
        return upload_stream(bucket=bucket, key=key, stream=data, content_type=content_type)
    return upload_bytes(bucket=bucket, key=key, data=data, content_type=content_type)


def upload_extraction_output(
    *,
    bucket: str,
    ctx: S3PathContext,
    filename: str,
    data: bytes | BytesIO,
    content_type: str = "application/octet-stream",
) -> str:
    """Save an output artifact under the extraction outputs prefix."""
    key = S3Layout.key_for(ctx, S3Layout.outputs_prefix(ctx), filename)
    if isinstance(data, BytesIO):
        return upload_stream(bucket=bucket, key=key, stream=data, content_type=content_type)
    return upload_bytes(bucket=bucket, key=key, data=data, content_type=content_type)