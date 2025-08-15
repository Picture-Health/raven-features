from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from clearml import Task

from raven_features.utils.aws import retrieve_s3_file, parse_s3_uri, upload_bytes
from raven_features.utils.logs import get_logger

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Event model (aligned with BatchAudit)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EnqueueEvent:
    ts: float
    feat_id: str
    series_uid: Optional[str] = None
    planned: bool = False
    launched: bool = False
    task_id: Optional[str] = None
    clearml_url: Optional[str] = None
    error: Optional[str] = None
    extras: Optional[dict] = None


# ──────────────────────────────────────────────────────────────────────────────
# IO helpers
# ──────────────────────────────────────────────────────────────────────────────

def _load_enqueue_events(s3_uri: str) -> List[EnqueueEvent]:
    """Read JSONL enqueue log and parse into EnqueueEvent items."""
    ndjson = retrieve_s3_file(s3_uri)
    events: List[EnqueueEvent] = []
    for line in ndjson.splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        # be forgiving if older lines didn't include all fields
        events.append(EnqueueEvent(
            ts=d.get("ts", time.time()),
            feat_id=d["feat_id"],
            series_uid=d.get("series_uid"),
            planned=bool(d.get("planned", False)),
            launched=bool(d.get("launched", False)),
            task_id=d.get("task_id"),
            clearml_url=d.get("clearml_url"),
            error=d.get("error"),
            extras=d.get("extras"),
        ))
    return events


def _task_status(task_id: str) -> str:
    """Fetch status for a ClearML task id."""
    try:
        t = Task.get_task(task_id=task_id)
        return str(t.get_status()) if t else "unknown"
    except Exception as e:
        logger.warning(f"Status lookup failed for {task_id}: {e!r}")
        return "lookup_error"


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def summarize_batch(
    *,
    enqueue_uri: str,          # s3://.../_batch/enqueue.jsonl
    write_csv: bool = True,
) -> Tuple[dict, Optional[str]]:
    """
    Read enqueue.jsonl, pull ClearML statuses, and write final_summary.json (+ CSV) next to it.
    Returns (summary_dict, csv_s3_uri_or_None).
    """
    bucket, enqueue_key = parse_s3_uri(enqueue_uri)
    batch_prefix = enqueue_key.rsplit("/", 1)[0]  # .../_batch
    final_json_key = f"{batch_prefix}/final_summary.json"

    events = _load_enqueue_events(enqueue_uri)

    # Group by featurization
    by_feat: Dict[str, Dict[str, int]] = {}
    # Status breakdown only counts launched tasks
    by_feat_status: Dict[str, Dict[str, int]] = {}

    # CSV rows (launched tasks + errors for visibility)
    rows: List[List[str]] = [["feat_id", "series_uid", "task_id", "status", "url", "error"]]

    # First pass: count planned/errored; collect launched for status queries
    launched_events: List[EnqueueEvent] = []
    for e in events:
        f = e.feat_id
        agg = by_feat.setdefault(f, {"planned": 0, "launched": 0, "errors": 0})
        if e.planned:
            agg["planned"] += 1
        if e.error:
            agg["errors"] += 1
            rows.append([f, e.series_uid or "", "", "", "", e.error])
        if e.launched and e.task_id:
            launched_events.append(e)

    # Second pass: fetch statuses for launched
    for e in launched_events:
        status = _task_status(e.task_id)
        by_feat[e.feat_id]["launched"] += 1
        status_map = by_feat_status.setdefault(e.feat_id, {"completed": 0, "failed": 0, "aborted": 0, "other": 0})
        sl = (status or "").lower()
        if sl == "completed":
            status_map["completed"] += 1
        elif sl == "failed":
            status_map["failed"] += 1
        elif sl == "aborted":
            status_map["aborted"] += 1
        else:
            status_map["other"] += 1

        rows.append([e.feat_id, e.series_uid or "", e.task_id or "", status, e.clearml_url or "", ""])

    # Build overall totals
    totals = {
        "planned": sum(v["planned"] for v in by_feat.values()),
        "launched": sum(v["launched"] for v in by_feat.values()),
        "errors": sum(v["errors"] for v in by_feat.values()),
    }
    overall_status = {
        "completed": sum(by_feat_status.get(f, {}).get("completed", 0) for f in by_feat),
        "failed":    sum(by_feat_status.get(f, {}).get("failed", 0) for f in by_feat),
        "aborted":   sum(by_feat_status.get(f, {}).get("aborted", 0) for f in by_feat),
        "other":     sum(by_feat_status.get(f, {}).get("other", 0) for f in by_feat),
    }

    # Merge per-feat structures for JSON
    per_feat_json: Dict[str, dict] = {}
    for f, counts in by_feat.items():
        per_feat_json[f] = {
            **counts,
            "status": by_feat_status.get(f, {"completed": 0, "failed": 0, "aborted": 0, "other": 0}),
            "not_launched": max(0, counts["planned"] - counts["launched"]),
        }

    summary = {
        "generated_at": time.time(),
        "sources": {"enqueue": enqueue_uri},
        "totals": {**totals, "not_launched": max(0, totals["planned"] - totals["launched"])},
        "overall_status": overall_status,
        "by_featurization": per_feat_json,
    }

    # Write final_summary.json
    upload_bytes(
        bucket=bucket,
        key=final_json_key,
        data=json.dumps(summary, indent=2).encode("utf-8"),
        content_type="application/json",
    )
    logger.info(f"Uploaded final summary to s3://{bucket}/{final_json_key}")

    csv_uri = None
    if write_csv:
        import csv
        from io import StringIO
        sio = StringIO()
        w = csv.writer(sio)
        w.writerows(rows)
        final_csv_key = f"{batch_prefix}/final_summary.csv"
        upload_bytes(
            bucket=bucket,
            key=final_csv_key,
            data=sio.getvalue().encode("utf-8"),
            content_type="text/csv",
        )
        csv_uri = f"s3://{bucket}/{final_csv_key}"
        logger.info(f"Uploaded CSV to {csv_uri}")

    return summary, csv_uri