import os
from functools import lru_cache


def _resolve_ready_ref_path() -> str:
    rel_path = "docs/ready_ref.md"
    if os.path.exists(rel_path):
        return rel_path

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    return os.path.join(base_dir, "docs", "ready_ref.md")


@lru_cache(maxsize=1)
def load_ready_ref() -> str:
    path = _resolve_ready_ref_path()
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
