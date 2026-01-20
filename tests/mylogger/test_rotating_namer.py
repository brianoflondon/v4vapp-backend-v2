from pathlib import Path
from types import SimpleNamespace

from v4vapp_backend_v2.config.setup import make_rotation_namer


def test_namer_pads_and_places_before_extension(tmp_path):
    handler = SimpleNamespace(backupCount=10)
    namer = make_rotation_namer(handler, rotation_folder=False, min_width=3)

    orig = str(tmp_path / "hive_monitor_v2.jsonl.1")
    new = namer(orig)
    assert new.endswith("hive_monitor_v2.001.jsonl")
    assert "rotation" not in new

    orig2 = str(tmp_path / "hive_monitor_v2.jsonl.10")
    new2 = namer(orig2)
    assert new2.endswith("hive_monitor_v2.010.jsonl")


def test_namer_rotation_folder_creates_directory(tmp_path):
    handler = SimpleNamespace(backupCount=5)
    namer = make_rotation_namer(handler, rotation_folder=True, min_width=3)

    orig = str(tmp_path / "hive_monitor_v2.jsonl.2")
    new = namer(orig)

    rotation_dir = Path(orig).parent / "rotation"
    assert rotation_dir.exists(), "rotation directory should be created by namer"
    assert str(rotation_dir / "hive_monitor_v2.002.jsonl") == new


def test_namer_non_numeric_suffix_returns_same(tmp_path):
    handler = SimpleNamespace(backupCount=5)
    namer = make_rotation_namer(handler)

    orig = str(tmp_path / "hive_monitor_v2.jsonl.bak")
    assert namer(orig) == orig
