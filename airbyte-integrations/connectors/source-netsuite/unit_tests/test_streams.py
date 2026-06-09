#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from datetime import date
from unittest.mock import MagicMock

import freezegun
import pytest

from source_netsuite.streams import IncrementalNetsuiteStream


def make_incremental_stream(start_datetime="2000-01-01T00:00:00Z", window_in_days=1, lookback_window_in_days=0):
    return IncrementalNetsuiteStream(
        auth=MagicMock(),
        object_name="journalentry",
        base_url="https://example.suitetalk.api.netsuite.com/services/rest/record/v1/",
        start_datetime=start_datetime,
        window_in_days=window_in_days,
        lookback_window_in_days=lookback_window_in_days,
    )


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_emit_locale_date_bounds():
    """stream_slices must emit locale date strings accepted by NetSuite's N/query module."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T00:00:00Z"}))
    assert len(slices) == 1
    assert slices[0]["start"] == "01/15/2024"
    assert slices[0]["end"] == "01/16/2024"


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_cursor_time_component_is_stripped():
    """A cursor with a non-midnight time must produce a slice starting at the cursor's
    calendar date, not a shifted date."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T14:30:00Z"}))
    assert len(slices) == 1
    assert slices[0]["start"] == "01/15/2024"


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_lookback_steps_behind_cursor():
    """With lookback_window_in_days=1 (default), the first slice must start one day
    before the cursor date so records in the timezone dead zone are re-queried."""
    stream = make_incremental_stream(lookback_window_in_days=1)
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T00:00:00Z"}))
    assert slices[0]["start"] == "01/14/2024"


@freezegun.freeze_time("2024-01-17")
def test_stream_slices_produces_contiguous_daily_windows():
    """Slices must cover every calendar day from cursor to today with no gaps."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-15T00:00:00Z"}))
    assert len(slices) == 3
    assert [s["start"] for s in slices] == [
        "01/15/2024",
        "01/16/2024",
        "01/17/2024",
    ]
    for i in range(len(slices) - 1):
        assert slices[i]["end"] == slices[i + 1]["start"]


@freezegun.freeze_time("2024-01-15")
def test_stream_slices_returns_empty_for_future_cursor():
    """A cursor ahead of today must yield no slices."""
    stream = make_incremental_stream()
    slices = list(stream.stream_slices(stream_state={"lastModifiedDate": "2024-01-20T00:00:00Z"}))
    assert slices == []
