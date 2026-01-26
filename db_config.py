import os
from pathlib import Path


def _is_azure_app_service() -> bool:
    return bool(os.environ.get("WEBSITE_SITE_NAME") or os.environ.get("WEBSITE_INSTANCE_ID"))


def _repo_dir() -> Path:
    return Path(__file__).resolve().parent


def get_sqlite_path() -> Path:
    env_path = (os.environ.get("TARASHA_DB_PATH") or "").strip()
    if env_path:
        p = Path(env_path)
        return p if p.is_absolute() else (_repo_dir() / p)

    if _is_azure_app_service():
        home = os.environ.get("HOME") or "/home"
        return Path(home) / "app.db"

    return _repo_dir() / "app.db"


def get_db_url() -> str:
    url = (os.environ.get("TARASHA_DB_URL") or "").strip()
    if url:
        return url

    sqlite_path = get_sqlite_path().resolve()
    return f"sqlite:///{sqlite_path.as_posix()}"


def is_sqlite_url(url: str) -> bool:
    return url.lower().startswith("sqlite:")
