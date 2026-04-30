from pathlib import Path

import pytest

from wmviz.checkpoint import parse_commit_file, parse_offsets_file

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_offsets_file_basic():
    text = (FIXTURES / "offsets_42").read_text()
    bo = parse_offsets_file(text, batch_id=42)
    assert bo.batch_id == 42
    assert bo.batch_watermark_ms == 1714478400000
    assert bo.batch_timestamp_ms == 1714478700000
    assert bo.conf == {"spark.sql.shuffle.partitions": "200"}
    assert len(bo.sources) == 2
    assert bo.sources[0].is_delta
    assert bo.sources[0].reservoir_version == 17
    assert bo.sources[0].reservoir_id == "impressions-reservoir"
    assert bo.sources[1].reservoir_version == 11
    assert bo.sources[1].reservoir_id == "clicks-reservoir"


def test_parse_offsets_skips_dash_placeholders():
    """A `-` placeholder means that source slot had no offset change in this batch.
    The slot's positional index should be preserved in the parsed output."""
    text = "v1\n" + '{"batchWatermarkMs":0,"batchTimestampMs":0}\n-\n{"reservoirId":"x","reservoirVersion":1}\n'
    bo = parse_offsets_file(text, batch_id=0)
    assert len(bo.sources) == 1
    assert bo.sources[0].source_index == 1
    assert bo.sources[0].reservoir_version == 1


def test_parse_offsets_rejects_empty():
    with pytest.raises(ValueError):
        parse_offsets_file("", batch_id=99)


def test_parse_commit_file_basic():
    text = (FIXTURES / "commits_42").read_text()
    bc = parse_commit_file(text, batch_id=42)
    assert bc.batch_id == 42
    assert bc.next_batch_watermark_ms == 1714478700000
    assert bc.state_unique_keys is None


def test_parse_commit_file_with_state_keys():
    text = (FIXTURES / "commits_with_state").read_text()
    bc = parse_commit_file(text, batch_id=42)
    assert bc.next_batch_watermark_ms == 1714478700000
    assert bc.state_unique_keys == 12345 + 6789


def test_parse_commit_handles_missing_next_watermark():
    bc = parse_commit_file("v1\n{}\n", batch_id=7)
    assert bc.next_batch_watermark_ms is None


def test_parse_commit_empty_file():
    bc = parse_commit_file("", batch_id=7)
    assert bc.batch_id == 7
    assert bc.next_batch_watermark_ms is None
