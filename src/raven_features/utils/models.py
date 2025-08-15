from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Type, Iterable
from pydantic import BaseModel, Field, ConfigDict, create_model, field_validator, constr

# ──────────────────────────────────────────────────────────────────────────────
# Utilities: build dynamic models from ORM classes
# ──────────────────────────────────────────────────────────────────────────────

EXCLUDE_ATTRS = {
    "metadata", "registry", "query", "query_class",
    "prepare", "from_statement", "select", "autoload_with",
}

# NEW: env/flag to fail fast in prod when required fields missing
STRICT_REQUIRED: bool = True  # flip to False for local dev if desired

# NEW: map common SQLAlchemy types to Python
def _sa_pytype(col) -> type | Any:
    try:
        from sqlalchemy import (
            Integer, Float, String, Boolean, Date, DateTime, BigInteger, Numeric
        )  # type: ignore
        t = getattr(col, "type", None)
        if isinstance(t, (Integer, BigInteger)): return int
        if isinstance(t, (Float, Numeric)): return float
        if isinstance(t, String): return str
        if isinstance(t, Boolean): return bool
        if isinstance(t, Date): return str   # or date, but configs usually pass strings
        if isinstance(t, DateTime): return str  # ISO string in configs
    except Exception:
        pass
    return Any

@lru_cache(maxsize=64)
def _sqlalchemy_public_fields(orm_cls: type, include_relationships: bool = False) -> List[str]:
    try:
        from sqlalchemy import inspect as sa_inspect  # type: ignore
        mapper = sa_inspect(orm_cls)
        cols = list(getattr(mapper, "columns").keys())
        names = cols
        if include_relationships:
            names = list(sorted(set(cols) | set(getattr(mapper, "relationships").keys())))
        return sorted(names)
    except Exception:
        return []

def _public_attrs(orm_cls: type) -> List[str]:
    names: List[str] = []
    for n in dir(orm_cls):
        if n.startswith("_") or n in EXCLUDE_ATTRS:
            continue
        try:
            v = getattr(orm_cls, n)
        except Exception:
            continue
        if callable(v):
            continue
        names.append(n)
    return sorted(set(names))

def allowed_fields_from_orm(orm_cls: type) -> List[str]:
    sa = _sqlalchemy_public_fields(orm_cls, include_relationships=False)
    return sa if sa else _public_attrs(orm_cls)

def allowed_fields_from_orms(orm_classes: Iterable[type]) -> List[str]:
    """Union of allowed fields across multiple ORM classes."""
    allow: set[str] = set()
    for cls in orm_classes:
        allow.update(allowed_fields_from_orm(cls))
    return sorted(allow)

# NEW: capture SQLAlchemy column objects to infer types (optional)
def _sqlalchemy_columns(orm_cls: type) -> Dict[str, Any]:
    try:
        from sqlalchemy import inspect as sa_inspect  # type: ignore
        mapper = sa_inspect(orm_cls)
        return {c.key: c for c in mapper.columns}
    except Exception:
        return {}

def _sqlalchemy_columns_map(orm_classes: Iterable[type]) -> Dict[str, Any]:
    """
    Merge SQLAlchemy column objects from multiple ORM classes.
    First class wins on duplicates to keep behavior deterministic.
    """
    merged: Dict[str, Any] = {}
    for cls in orm_classes:
        cols = _sqlalchemy_columns(cls)
        for k, v in cols.items():
            merged.setdefault(k, v)
    return merged

def build_dynamic_model(
    orm_classes: Type[Any] | Iterable[Type[Any]],
    *,
    required: Dict[str, type],
    name: str,
    extra_fields: Iterable[str] = (),
) -> Type[BaseModel]:
    """
    Create a Pydantic model with required fields (as given) and every other
    non-private ORM attribute from one or more ORM classes allowed as Optional[<inferred>]
    (fallback Any). Unknown keys are forbidden.
    """
    # normalize to list
    if isinstance(orm_classes, type):
        orm_list: List[Type[Any]] = [orm_classes]
    else:
        orm_list = list(orm_classes)

    # union of allowed attributes from all ORM classes + explicit allowlist
    allow = set(allowed_fields_from_orms(orm_list))
    allow.update(extra_fields)

    # strict check that required keys exist somewhere
    missing_required = [k for k in required.keys() if k not in allow]
    if missing_required:
        msg = f"{name}: required fields not present on any ORM {[c.__name__ for c in orm_list]}: {missing_required}"
        if STRICT_REQUIRED:
            raise RuntimeError(msg)
        else:
            import warnings; warnings.warn(msg)

    # type inference from SQLAlchemy columns where available
    sa_cols = _sqlalchemy_columns_map(orm_list)

    fields: Dict[str, Tuple[type, Any]] = {}
    # REQUIRED: if str, enforce non-empty via constr()
    for k, t in required.items():
        t_eff = constr(strip_whitespace=True, min_length=1) if t is str else t
        fields[k] = (t_eff, ...)

    # OPTIONAL: all allowed minus required, with inferred types when possible
    for k in sorted(allow - set(required.keys())):
        inferred = _sa_pytype(sa_cols.get(k)) if k in sa_cols else Any
        fields[k] = (Optional[inferred], None)  # type: ignore[index]

    # v2 config
    M = create_model(
        name,
        __config__=ConfigDict(extra="forbid", str_strip_whitespace=True),
        **fields,  # type: ignore[arg-type]
    )
    return M

# ──────────────────────────────────────────────────────────────────────────────
# Build your two dynamic models from Raven ORM
# ──────────────────────────────────────────────────────────────────────────────

try:
    import raven as rv  # type: ignore
    SeriesORM = rv.__api__.Series
    ImageORM  = rv.__api__.Image
    MaskORM   = rv.__api__.Mask
except Exception:
    class _SeriesStub: ...
    class _ImageStub: ...
    class _MaskStub: ...
    SeriesORM, ImageORM, MaskORM = _SeriesStub, _ImageStub, _MaskStub

ImageQuery = build_dynamic_model(
    orm_classes=[ImageORM, SeriesORM],                 # ← accept both
    required={"dataset_id": str, "qa_code": str},
    name="ImageQuery",
    extra_fields=("modality", "orientation"),          # ← allowlist for known non-column filters
)

MaskSpec = build_dynamic_model(
    orm_classes=MaskORM,
    required={"mask_type": str, "provenance": str, "qa_code": str},
    name="MaskSpec",
)

# ──────────────────────────────────────────────────────────────────────────────
# Core blocks
# ──────────────────────────────────────────────────────────────────────────────

class Project(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)
    project_id: str
    user: str
    tags: List[str] = Field(default_factory=list)

class Provisioning(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    masks: List[MaskSpec] = Field(default_factory=list)
    custom: List[str] = Field(default_factory=list)

class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    queue: Optional[str] = None
    timeout_sec: int = 3600
    retries: int = 1

    @field_validator("timeout_sec")
    @classmethod
    def _positive_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("defaults.timeout_sec must be > 0")
        return v

    @field_validator("retries")
    @classmethod
    def _non_negative_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("defaults.retries must be >= 0")
        return v

class RepoSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)
    url: str
    commit: str
    entrypoint: str

# ──────────────────────────────────────────────────────────────────────────────
# Featurization subtree
# ──────────────────────────────────────────────────────────────────────────────

class Extraction(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    name: str
    repo: str
    queue: Optional[str] = None
    timeout_sec: Optional[int] = None
    retries: Optional[int] = None
    provisioning: Optional[Provisioning] = None

class Featurization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    name: str
    feature_group_id: str
    domain: Optional[str] = None
    load_features: bool = True
    provisioning: Provisioning = Field(default_factory=Provisioning)
    extractions: List[Extraction]

# ──────────────────────────────────────────────────────────────────────────────
# Root config
# ──────────────────────────────────────────────────────────────────────────────

class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: int = 1
    project: Project
    images: ImageQuery
    defaults: Defaults = Field(default_factory=Defaults)
    repos: Dict[str, RepoSpec]
    featurizations: List[Featurization]

# Optional: exported helpers for tests/observability
def snapshot_schema() -> Dict[str, List[str]]:
    """Return allowed fields for Image and Mask as discovered from the ORM."""
    return {
        "SeriesQuery_allowed": allowed_fields_from_orm(SeriesORM),
        "ImageQuery_allowed": allowed_fields_from_orm(ImageORM),
        "MaskSpec_allowed": allowed_fields_from_orm(MaskORM),
    }

