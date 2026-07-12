import os
import subprocess
import sys
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

    # On the owner's Mac, use the shared PostgreSQL URL from Keychain when it
    # has been provisioned. Other machines and Azure retain the documented
    # environment-variable/SQLite fallback behavior.
    if sys.platform == "darwin" and not _is_azure_app_service():
        try:
            keychain = subprocess.run(
                [
                    "security",
                    "find-generic-password",
                    "-a",
                    "TaRaShaResearch",
                    "-s",
                    "TaRaSha Shared Database URL",
                    "-w",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
            keychain_url = keychain.stdout.strip()
            if keychain.returncode == 0 and keychain_url:
                return keychain_url
        except (OSError, subprocess.SubprocessError):
            pass

    sqlite_path = get_sqlite_path().resolve()
    return f"sqlite:///{sqlite_path.as_posix()}"


def is_sqlite_url(url: str) -> bool:
    return url.lower().startswith("sqlite:")
