import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts.package_research_preview import package


class ResearchReleasePackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.repo = self.base / "repo"
        for name, content in {
            "app.py": "print('fixture')\n", "preview_mode.py": "READ_ONLY = True\n",
            "assets/logo.svg": "<svg/>", "consumer/.env": "must-not-package",
            ".env": "must-not-package", "requirements.txt": "unmodified-local-requirements\n",
            "deployment/requirements-research.lock": "streamlit==1.50.0\n",
            "deployment/start-research-preview.sh": "#!/bin/bash\ntrue\n",
        }.items():
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        self.snapshot = self.base / "snapshot.db"
        with sqlite3.connect(self.snapshot) as conn:
            conn.execute("CREATE TABLE companies(id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO companies VALUES (1)")
        self.digest = hashlib.sha256(self.snapshot.read_bytes()).hexdigest()
        self.snapshot.with_suffix(".manifest.json").write_text(json.dumps({"verification": "passed", "snapshot_sha256": self.digest, "tables": {"companies": {"rows": 1}}}))

    def tearDown(self):
        self.temp.cleanup()

    def fake_git(self, repo, *args):
        if args[0] == "rev-parse":
            return "a" * 40
        if args[0] == "status":
            return " M app.py"
        if args[0] == "ls-files":
            return "app.py\0assets/logo.svg\0consumer/.env\0requirements.txt\0"
        raise AssertionError(args)

    def test_draft_is_bound_filtered_locked_and_reproducible(self):
        with patch("scripts.package_research_preview.git", side_effect=self.fake_git):
            first = package(self.repo, self.snapshot, self.digest, "a" * 40, self.base / "first", draft=True)
            second = package(self.repo, self.snapshot, self.digest, "a" * 40, self.base / "second", draft=True)
        self.assertFalse(first["deployable"])
        self.assertEqual(first["source_data_zip_sha256"], second["source_data_zip_sha256"])
        with zipfile.ZipFile(self.base / "first/research-preview-source-data.zip") as archive:
            self.assertNotIn(".env", archive.namelist())
            self.assertFalse(any(path.startswith("consumer/") for path in archive.namelist()))
            self.assertEqual(archive.read("requirements.txt"), b"streamlit==1.50.0\n")
            review = json.loads(archive.read("research-release-manifest.json"))
            self.assertEqual(review["source_commit"], "a" * 40)
            self.assertEqual(review["snapshot_sha256"], self.digest)
            self.assertEqual(review["app_settings"]["TARASHA_RESEARCH_SNAPSHOT"], "data/research-preview.db")
        self.assertEqual((self.repo / "requirements.txt").read_text(), "unmodified-local-requirements\n")

    def test_release_refuses_uncommitted_source(self):
        with patch("scripts.package_research_preview.git", side_effect=self.fake_git):
            with self.assertRaisesRegex(ValueError, "clean, committed"):
                package(self.repo, self.snapshot, self.digest, "a" * 40, self.base / "rejected")
        self.assertFalse((self.base / "rejected").exists())

    def test_wrong_source_or_snapshot_hash_fails_before_packaging(self):
        with patch("scripts.package_research_preview.git", side_effect=self.fake_git):
            with self.assertRaisesRegex(ValueError, "selected source commit"):
                package(self.repo, self.snapshot, self.digest, "b" * 40, self.base / "wrong-source", draft=True)
            with self.assertRaisesRegex(ValueError, "reviewed snapshot"):
                package(self.repo, self.snapshot, "0" * 64, "a" * 40, self.base / "wrong-data", draft=True)
        self.assertFalse((self.base / "wrong-source").exists())
        self.assertFalse((self.base / "wrong-data").exists())
