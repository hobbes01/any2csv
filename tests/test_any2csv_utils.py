import os
import tempfile
import pytest
from unittest.mock import MagicMock

import any2csv_utils

class MockSnapshotWithType:
    """
    Mock for the SnapshotWithType protobuf class for isolated testing.
    """
    def __init__(self):
        self.snapshot = MagicMock()
        self.snapshot.data.details.fields = {}
        self.snapshot.data.relationLinks = []
    
    def ParseFromString(self, s):
        return MockSnapshotWithType()

@pytest.fixture
def pbdir(tmp_path):
    """
    Fixture to create a temporary protobuf directory for testing.
    """
    return tmp_path / "pbdir"

def test_extract_archive(tmp_path):
    """
    Test extracting a simple zip archive to a directory.
    """
    zip_path = tmp_path / "dummy.zip"
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    (archive_dir / "test.txt").write_text("hello")
    import zipfile
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(archive_dir / "test.txt", arcname="test.txt")

    extract_dir = tmp_path / "extract"
    any2csv_utils.extract_archive(str(zip_path), str(extract_dir))
    assert (extract_dir / "test.txt").exists()

def test_ensure_directories(tmp_path):
    """
    Test that ensure_directories creates csv and data subdirectories.
    """
    pbdir = tmp_path / "myproto"
    pbdir.mkdir()
    csvdir, datadir = any2csv_utils.ensure_directories(str(pbdir))
    assert os.path.isdir(csvdir)
    assert os.path.isdir(datadir)

def test_load_single_message_from_file(monkeypatch, tmp_path):
    """
    Test loading a protobuf message from a file, using a mock class.
    """
    test_file = tmp_path / "file.pb"
    test_file.write_bytes(b"fake content")

    monkeypatch.setattr(any2csv_utils, "SnapshotWithType", MockSnapshotWithType)
    result = any2csv_utils.load_single_message_from_file(str(test_file))
    assert isinstance(result, MockSnapshotWithType)

def test_read_rel_option_handles_missing(monkeypatch, tmp_path):
    """
    Test that read_rel_option handles missing options and updates the unknown_options dict.
    """
    pbdir = tmp_path / "pbdir"
    pbdir.mkdir()
    unknown_options = {}

    # Patch load_single_message_from_file to always return None
    monkeypatch.setattr(any2csv_utils, "load_single_message_from_file", lambda x: None)
    result = any2csv_utils.read_rel_option("missing", str(pbdir), unknown_options, any2csv_utils.load_single_message_from_file)
    assert result == ""
    assert unknown_options["missing"] == 1

def test_build_cache_empty(tmp_path, monkeypatch):
    """
    Test that build_cache returns empty dicts when no pb files are present.
    """
    pbdir = tmp_path / "pbdir"
    for sub in ["types", "relations", "objects"]:
        (pbdir / sub).mkdir(parents=True, exist_ok=True)
    regex = MagicMock()
    regex.match.return_value = False
    monkeypatch.setattr(any2csv_utils, "load_single_message_from_file", lambda x: None)
    cache = any2csv_utils.build_cache(str(pbdir), regex, any2csv_utils.load_single_message_from_file)
    assert set(cache.keys()) == {"types", "relations", "objects", "revrel"}
