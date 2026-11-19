"""Controls for the public, immutable Research data preview."""
from __future__ import annotations

import os
from pathlib import Path


class ReadOnlyPreviewError(RuntimeError):
    pass


def is_read_only() -> bool:
    return os.environ.get("TARASHA_READ_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}


def require_writable() -> None:
    if is_read_only():
        raise ReadOnlyPreviewError("This Research preview is read-only. Changes cannot be saved.")


def snapshot_path() -> Path:
    configured = os.environ.get("TARASHA_RESEARCH_SNAPSHOT", "").strip()
    if not configured:
        raise ReadOnlyPreviewError("The Research preview requires a published data snapshot.")
    path = Path(configured).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ReadOnlyPreviewError("The Research preview snapshot is unavailable.")
    return path


def sqlite_snapshot_uri() -> str:
    # Both SQLite and SQLAlchemy must interpret mode=ro as a URI parameter.
    return f"{snapshot_path().as_uri()}?mode=ro&immutable=1"
