"""Find the git commit of the running code, for the ``code_version`` field of manifests.

Works without the ``git`` binary (the slim Python image has none): it reads ``.git`` directly.
Order: ``CODE_VERSION`` env var, ``GITHUB_SHA`` env var, ``.git`` folder, ``"unknown"``.
"""

from __future__ import annotations

import os
from pathlib import Path

from common.config import REPO_ROOT

SHORT_LENGTH = 7


def _read_git_head(repo_root: Path) -> str | None:
    git_dir = repo_root / ".git"
    if git_dir.is_file():  # worktree or submodule: "gitdir: <path>"
        text = git_dir.read_text(encoding="utf-8").strip()
        if not text.startswith("gitdir:"):
            return None
        git_dir = (repo_root / text.split(":", 1)[1].strip()).resolve()
    head_file = git_dir / "HEAD"
    if not head_file.is_file():
        return None
    head = head_file.read_text(encoding="utf-8").strip()
    if not head.startswith("ref:"):
        return head  # detached HEAD: already a commit id
    ref = head.split(":", 1)[1].strip()
    ref_file = git_dir / ref
    if ref_file.is_file():
        return ref_file.read_text(encoding="utf-8").strip()
    packed = git_dir / "packed-refs"
    if packed.is_file():
        for line in packed.read_text(encoding="utf-8").splitlines():
            if line.endswith(f" {ref}") and not line.startswith(("#", "^")):
                return line.split(" ", 1)[0]
    return None


def code_version(repo_root: Path = REPO_ROOT) -> str:
    """Short commit id of the collector code (``"unknown"`` if it cannot be determined)."""
    for name in ("CODE_VERSION", "GITHUB_SHA"):
        value = os.environ.get(name, "").strip()
        if value:
            return value[:SHORT_LENGTH] if len(value) >= 40 else value
    try:
        commit = _read_git_head(repo_root)
    except OSError:
        commit = None
    return commit[:SHORT_LENGTH] if commit else "unknown"
