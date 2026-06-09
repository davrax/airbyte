#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from datetime import date
from unittest.mock import MagicMock

import freezegun
import pytest

from source_netsuite.streams import IncrementalNetsuiteStream


def make_incremental_stream(start_datetime="2000-01-01T00:00:00Z", window_in_days=1):
    return IncrementalNetsuiteStream(
        auth=MagicMock(),
        object_name="journalentry",
        base_url="https://example.suitetalk.api.netsuite.com/services/rest/record/v1/",
        start_datetime=start_datetime,
        window_in_days=window_in_days,
    )


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_emit_iso_datetime_bounds():
    """stream_slices must emit ISO datetime strings, not locale date-only strings."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T00:00:00Z"}))
    assert len(slices) == 1
    assert slices[0]["start"] == "2024-01-15T00:00:00Z"
    assert slices[0]["end"] == "2024-01-16T00:00:00Z"


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_truncates_cursor_to_utc_midnight():
    """A cursor with a non-midnight time must produce a slice starting at UTC midnight
    of that date, not at the next calendar date."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T14:30:00Z"}))
    assert len(slices) == 1
    assert slices[0]["start"] == "2024-01-15T00:00:00Z"


@freezegun.freeze_time("2024-01-17")
def test_stream_slices_produces_contiguous_daily_windows():
    """Slices must cover every UTC day from cursor to today with no gaps."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T00:00:00Z"}))
    assert len(slices) == 3
    assert [s["start"] for s in slices] == [
        "2024-01-15T00:00:00Z",
        "2024-01-16T00:00:00Z",
        "2024-01-17T00:00:00Z",
    ]
    for i in range(len(slices) - 1):
        assert slices[i]["end"] == slices[i + 1]["start"]


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_returns_empty_for_future_cursor():
    """A cursor ahead of today must yield no slices."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-20T00:00:00Z"}))
    assert slices == []
