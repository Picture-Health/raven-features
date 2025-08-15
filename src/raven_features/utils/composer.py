from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class TaskSpec:
    id: str
    name: str
    repo_url: Optional[str] = None
    repo_commit: Optional[str] = None
    entrypoint: Optional[str] = None
    queue: Optional[str] = None
    timeout_sec: Optional[int] = None
    retries: Optional[int] = None
    parents: List[str] = None
    params: Dict = None
    artifacts_in: Dict[str, str] = None
    artifacts_out: Dict[str, str] = None
    kind: str = "custom"  # "provision" | "extract" | "publish" | "custom"

def compose_clearml_plan(cfg: PipelineConfig) -> List[TaskSpec]:
    plan: List[TaskSpec] = []
    def_path = lambda *parts: "/".join(parts)

    for f in cfg.featurizations:
        # implicit provision
        prov_id = f"prov:{f.id}"
        prov_out = {
            "images": def_path("provision", f.id, "images"),
            "masks": def_path("provision", f.id, "masks"),
            "custom": def_path("provision", f.id, "custom"),
        }
        plan.append(TaskSpec(
            id=prov_id,
            name=f"[{f.id}] Provision",
            queue=cfg.defaults.queue,
            timeout_sec=cfg.defaults.timeout_sec,
            retries=cfg.defaults.retries,
            parents=[],
            params={
                "project": cfg.project.model_dump(),
                "images": cfg.images.model_dump(),
                "provisioning": f.provisioning.model_dump(),
            },
            artifacts_out=prov_out,
            kind="provision",
        ))

        # extractions
        extract_outputs = []
        for e in f.extractions:
            repo = cfg.repos[e.repo]
            ex_id = f"ext:{f.id}:{e.id}"
            out_path = def_path("extract", f.id, e.id, "features")
            extract_outputs.append(out_path)
            plan.append(TaskSpec(
                id=ex_id,
                name=f"[{f.id}] {e.name}",
                repo_url=repo.url,
                repo_commit=repo.commit,
                entrypoint=repo.entrypoint,
                queue=e.queue or cfg.defaults.queue,
                timeout_sec=e.timeout_sec or cfg.defaults.timeout_sec,
                retries=e.retries if e.retries is not None else cfg.defaults.retries,
                parents=[prov_id],
                params={
                    "project": cfg.project.model_dump(),
                    "inputs": {"provision": prov_out},
                    "provisioning_override": (e.provisioning or Provisioning()).model_dump(),
                },
                artifacts_in=prov_out,
                artifacts_out={"features": out_path},
                kind="extract",
            ))

        # implicit publish
        if f.load_features:
            pub_id = f"pub:{f.id}"
            plan.append(TaskSpec(
                id=pub_id,
                name=f"[{f.id}] Publish to Feature Group",
                queue=cfg.defaults.queue,
                timeout_sec=cfg.defaults.timeout_sec,
                retries=cfg.defaults.retries,
                parents=[f"ext:{f.id}:{e.id}" for e in f.extractions],
                params={
                    "feature_group_id": f.feature_group_id,
                    "project": cfg.project.model_dump(),
                    "inputs": [{"features": p} for p in extract_outputs],
                },
                artifacts_in={f"features_{i}": p for i, p in enumerate(extract_outputs)},
                kind="publish",
            ))
    return plan