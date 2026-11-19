"""Build a source/data ZIP for normal Azure Python/Oryx build automation.

Release mode requires a clean checkout of an approved full main commit SHA.
Draft mode is explicitly nondeployable and records every packaged source hash.
No cloud API is called. Linux dependencies are installed by Oryx from the
deployment-only lock copied to artifact-root requirements.txt.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import zipfile


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(repo, *arguments):
    return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True).strip()


def package(repo, snapshot, snapshot_sha, source_commit, output, draft=False):
    repo = repo.resolve()
    snapshot = snapshot.resolve(strict=True)
    output = output.resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("A full immutable source commit SHA is required.")
    if not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha):
        raise ValueError("A full approved snapshot SHA-256 is required.")
    if git(repo, "rev-parse", "HEAD") != source_commit:
        raise ValueError("The checkout must match the selected source commit.")
    dirty = bool(git(repo, "status", "--porcelain", "--untracked-files=normal"))
    main_verified = False
    if not draft:
        if dirty:
            raise ValueError("Release packaging requires a clean, committed checkout.")
        subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", source_commit, "refs/remotes/origin/main"], check=True, capture_output=True)
        main_verified = True
    manifest = json.loads(snapshot.with_suffix(".manifest.json").read_text())
    if manifest.get("verification") != "passed" or manifest.get("snapshot_sha256") != snapshot_sha or sha256(snapshot) != snapshot_sha:
        raise ValueError("The reviewed snapshot and verified manifest must match.")
    if output.is_relative_to(repo):
        raise ValueError("The private release package must remain outside Git.")
    lock = repo / "deployment/requirements-research.lock"
    lock_bytes = lock.read_bytes()
    pins = [line for line in lock_bytes.decode().splitlines() if line and not line.startswith("#")]
    if not pins or any(not re.fullmatch(r"[A-Za-z0-9_.-]+==[A-Za-z0-9_.+!-]+", line) for line in pins):
        raise ValueError("The deployment lock must contain only exact package versions.")
    tracked = git(repo, "ls-files", "-z").split("\0")
    paths = {path for path in tracked if path and (("/" not in path and path.endswith(".py")) or path.startswith("assets/"))}
    paths.update({"preview_mode.py", "deployment/start-research-preview.sh", "deployment/requirements-research.lock"})
    if not draft:
        for path in paths:
            subprocess.run(["git", "-C", str(repo), "ls-files", "--error-unmatch", path], check=True, capture_output=True)
    entries = {}
    for relative in sorted(paths):
        path = repo / relative
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(repo):
            raise ValueError("Unexpected source path: " + relative)
        entries[relative] = path
    entries["requirements.txt"] = lock
    entries["data/research-preview.db"] = snapshot
    entries["data/research-preview.manifest.json"] = snapshot.with_suffix(".manifest.json")
    members = {name: {"sha256": sha256(path), "bytes": path.stat().st_size} for name, path in sorted(entries.items())}
    source_identity = hashlib.sha256(json.dumps({name: value for name, value in members.items() if not name.startswith("data/")}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    release = {
        "format": 1,
        "mode": "review_draft" if draft else "immutable_release",
        "deployable": not draft,
        "source_commit": source_commit,
        "source_commit_verified_on_main": main_verified,
        "source_worktree_dirty": dirty,
        "packaged_source_sha256": source_identity,
        "snapshot_sha256": snapshot_sha,
        "snapshot_rows": sum(table["rows"] for table in manifest["tables"].values()),
        "dependency_lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "dependency_versions": pins,
        "startup_command": "bash deployment/start-research-preview.sh",
        "app_settings": {"TARASHA_READ_ONLY": "1", "TARASHA_RESEARCH_SNAPSHOT": "data/research-preview.db", "PYTHONDONTWRITEBYTECODE": "1", "SCM_DO_BUILD_DURING_DEPLOYMENT": "true"},
        "deployment_method": "normal Azure Python Oryx build automation; compressed output under wwwroot, runtime extraction under APP_PATH in /tmp",
        "members": members,
    }
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    zip_path = output / "research-preview-source-data.zip"
    descriptor = os.open(zip_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream, zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, path in sorted(entries.items()):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100600 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            with path.open("rb") as source, archive.open(info, "w", force_zip64=True) as target:
                while block := source.read(1024 * 1024):
                    target.write(block)
        info = zipfile.ZipInfo("research-release-manifest.json", date_time=(2020, 1, 1, 0, 0, 0))
        info.external_attr = 0o100600 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, json.dumps(release, sort_keys=True, indent=2) + "\n")
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.read("requirements.txt") == lock_bytes
        for name, expected in members.items():
            with archive.open(name) as stream:
                assert hashlib.file_digest(stream, "sha256").hexdigest() == expected["sha256"], name
    release["source_data_zip_sha256"] = sha256(zip_path)
    release["source_data_zip_bytes"] = zip_path.stat().st_size
    descriptor = os.open(output / "research-release-review.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(release, stream, indent=2)
        stream.write("\n")
    return {key: release[key] for key in ["mode", "deployable", "source_commit", "snapshot_sha256", "dependency_lock_sha256", "source_data_zip_sha256", "source_data_zip_bytes"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--approved-snapshot-sha256", required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--draft", action="store_true")
    args = parser.parse_args()
    print(json.dumps(package(Path(__file__).resolve().parents[1], args.snapshot, args.approved_snapshot_sha256, args.source_commit, args.output, args.draft)))
