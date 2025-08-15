from __future__ import annotations
import json
import time
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

from raven_features.utils.aws import upload_bytes, S3PathContext, S3Layout
from raven_features.utils.logs import get_logger

logger = get_logger(__name__)


@dataclass
class EnqueueEvent:
    ts: float
    feat_id: str
    series_uid: Optional[str] = None
    planned: bool = False
    launched: bool = False
    task_id: Optional[str] = None
    clearml_url: Optional[str] = None
    error: Optional[str] = None
    extras: Optional[Dict] = None


class BatchAudit:
    """
    Collect enqueue events for a *single featurization* and flush once.
    Writes to:
      s3://.../<batch_root>/_batch/enqueue_<feat>.jsonl
      s3://.../<batch_root>/_batch/summary_<feat>.json
    """
    def __init__(self, *, bucket: str, ctx: S3PathContext, config_name: str, dry_run: bool) -> None:
        self.bucket = bucket
        self.ctx = ctx
        self.config_name = config_name
        self.dry_run = dry_run
        self._events: List[EnqueueEvent] = []

    # ---- recording helpers ----------------------------------------------------

    def record_planned(self, feat_id: str, series_uid: str, *, extras: Optional[Dict] = None) -> None:
        self._events.append(EnqueueEvent(
            ts=time.time(), feat_id=feat_id, series_uid=series_uid, planned=True, launched=False, extras=extras
        ))

    def record_launched(
        self, feat_id: str, series_uid: str, *, task_id: str, clearml_url: Optional[str] = None,
        extras: Optional[Dict] = None
    ) -> None:
        # IMPORTANT: planned=False here so summaries don’t double count
        self._events.append(EnqueueEvent(
            ts=time.time(), feat_id=feat_id, series_uid=series_uid,
            planned=False, launched=True, task_id=task_id, clearml_url=clearml_url, extras=extras
        ))

    def record_error(self, feat_id: str, *, message: str, series_uid: Optional[str] = None,
                     extras: Optional[Dict] = None) -> None:
        self._events.append(EnqueueEvent(
            ts=time.time(), feat_id=feat_id, series_uid=series_uid,
            planned=False, launched=False, error=message, extras=extras
        ))

    # ---- flush ----------------------------------------------------------------

    def flush(self) -> tuple[str, str] | None:
        """
        Persist NDJSON of events and a small per-featurization summary.
        Returns (enqueue_uri, summary_uri) or None if there were no events.
        """
        if not self._events:
            logger.info("BatchAudit: no events to flush")
            return None

        # All events should be from one featurization for this instance; guard anyway.
        feat_ids = {e.feat_id for e in self._events}
        if len(feat_ids) != 1:
            # This shouldn’t happen if you create one BatchAudit per feat_id
            logger.warning(f"BatchAudit.flush: mixed feat_ids detected: {sorted(feat_ids)}")
        feat_id = next(iter(feat_ids))

        batch_root = S3Layout.batch_root(self.ctx)

        # 1) Write NDJSON (per-feat file to avoid overwrites across featurizations)
        jsonl_payload = "\n".join(json.dumps(asdict(e)) for e in self._events).encode("utf-8")
        jsonl_key = f"{batch_root}/_batch/enqueue_{feat_id}.jsonl"
        enqueue_uri = upload_bytes(
            bucket=self.bucket, key=jsonl_key, data=jsonl_payload,
            content_type="application/x-ndjson"  # more accurate than application/jsonl
        )

        # 2) Compact summary (counts only; the summarize command builds a richer view)
        planned = sum(1 for e in self._events if e.planned)
        launched = sum(1 for e in self._events if e.launched)
        errors = sum(1 for e in self._events if e.error)

        summary_obj = {
            "generated_at": time.time(),
            "config_name": self.config_name,
            "batch_id": self.ctx.batch_id,
            "feature_group_id": self.ctx.feature_group_id,
            "feat_id": feat_id,
            "dry_run": self.dry_run,
            "totals": {"planned": planned, "launched": launched, "errors": errors},
            "sources": {"enqueue": enqueue_uri},
        }
        summary_key = f"{batch_root}/_batch/summary_{feat_id}.json"
        summary_uri = upload_bytes(
            bucket=self.bucket,
            key=summary_key,
            data=json.dumps(summary_obj, indent=2).encode("utf-8"),
            content_type="application/json",
        )

        logger.info(f"BatchAudit: uploaded {enqueue_uri} and {summary_uri}")
        return enqueue_uri, summary_uri