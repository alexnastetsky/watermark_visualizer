"""End-to-end test: load a complete sample checkpoint via LocalFileReader."""
from pathlib import Path

from wmviz.checkpoint import load_all_commits, load_all_offsets, list_source_indexes
from wmviz.fs import LocalFileReader

CHECKPOINT = str(Path(__file__).parent / "fixtures" / "sample_checkpoint")


def test_load_all_offsets():
    reader = LocalFileReader()
    offsets = load_all_offsets(reader, CHECKPOINT)
    assert [b.batch_id for b in offsets] == [0, 1]
    # batch 0
    assert offsets[0].batch_watermark_ms == 0
    assert offsets[0].batch_timestamp_ms == 1714478000000
    assert len(offsets[0].sources) == 2
    assert offsets[0].sources[0].reservoir_id == "impressions-reservoir"
    assert offsets[0].sources[0].reservoir_version == 1
    assert offsets[0].sources[1].reservoir_id == "clicks-reservoir"
    # batch 1
    assert offsets[1].batch_watermark_ms == 1714478100000
    assert offsets[1].sources[0].reservoir_version == 2
    assert offsets[1].sources[1].reservoir_version == 2


def test_load_all_commits():
    reader = LocalFileReader()
    commits = load_all_commits(reader, CHECKPOINT)
    assert [c.batch_id for c in commits] == [0, 1]
    assert commits[0].next_batch_watermark_ms == 1714478100000
    assert commits[1].next_batch_watermark_ms == 1714478400000


def test_list_source_indexes_empty_when_dir_missing():
    reader = LocalFileReader()
    # Sample checkpoint has no sources/ dir, so this returns []
    assert list_source_indexes(reader, CHECKPOINT) == []
