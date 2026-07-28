import pandas as pd
import pytest

from atmosphere_data_cl.utils.time import (
    as_interval,
    chunk_period,
    manage_time_interval,
)


class TestResolutionDetection:
    """A bare date should span its own bucket, not collapse to an instant."""

    def test_month_expands_to_whole_month(self):
        start, end = manage_time_interval("2022-09")
        assert start == pd.Timestamp("2022-09-01")
        assert end == pd.Timestamp("2022-09-30 23:59:59.999999999")

    def test_year_expands_to_whole_year(self):
        start, end = manage_time_interval("2022")
        assert start == pd.Timestamp("2022-01-01")
        assert end == pd.Timestamp("2022-12-31 23:59:59.999999999")

    def test_day_expands_to_whole_day(self):
        start, end = manage_time_interval("2022-09-09")
        assert start == pd.Timestamp("2022-09-09")
        assert end == pd.Timestamp("2022-09-09 23:59:59.999999999")

    def test_hour_is_an_exact_point(self):
        start, end = manage_time_interval("2022-09-09 14:00")
        assert start == end == pd.Timestamp("2022-09-09 14:00")

    def test_descending_granularity_not_dayfirst(self):
        """Dates read year → month → day, so 2026-07-01 is July 1st, not Jan 7th."""
        for token in ("2026-07-01", "2026/07/01", "2026-7-1"):
            start, _ = manage_time_interval(token)
            assert (start.year, start.month, start.day) == (2026, 7, 1), token

    def test_slash_and_dash_separators_are_equivalent(self):
        assert manage_time_interval("2022/09/09") == manage_time_interval("2022-09-09")


class TestRanges:
    def test_spaced_dash_and_to_are_equivalent(self):
        assert manage_time_interval("2022-09-01 - 2022-09-30") == manage_time_interval(
            "2022-09-01 to 2022-09-30"
        )

    def test_bare_hyphen_range_with_slash_dates(self):
        # Slash dates keep the hyphen unambiguous, so it still reads as a range.
        assert manage_time_interval("2022/09/01-2022/09/30") == manage_time_interval(
            "2022-09-01 to 2022-09-30"
        )

    def test_range_covers_both_endpoints_fully(self):
        start, end = manage_time_interval("2022-09-01 to 2022-09-30")
        assert start == pd.Timestamp("2022-09-01")
        assert end == pd.Timestamp("2022-09-30 23:59:59.999999999")

    def test_mixed_resolutions_warn_but_still_resolve(self, caplog):
        start, end = manage_time_interval("2022 to 2022-09-30")
        assert start == pd.Timestamp("2022-01-01")
        assert end == pd.Timestamp("2022-09-30 23:59:59.999999999")
        assert "different temporal resolutions" in caplog.text


class TestAsInterval:
    def test_accepts_a_string(self):
        assert as_interval("2022-09")[0] == pd.Timestamp("2022-09-01")

    def test_accepts_a_pair(self):
        start, end = as_interval(("2022-09-01", "2022-09-02"))
        assert (start, end) == (pd.Timestamp("2022-09-01"), pd.Timestamp("2022-09-02"))

    def test_rejects_unparseable(self):
        with pytest.raises(ValueError):
            as_interval("not a date")


class TestChunking:
    """Sources differ in what one request can cover; chunking encodes that."""

    def test_none_grain_is_a_single_bucket(self):
        start, end = manage_time_interval("2022-09")
        assert chunk_period(start, end, None) == [(start, end)]

    def test_hourly_grain_gives_24_buckets_for_a_day(self):
        start, end = manage_time_interval("2022-09-09")
        buckets = chunk_period(start, end, "hour")
        assert len(buckets) == 24
        assert buckets[0][0] == pd.Timestamp("2022-09-09 00:00")
        assert buckets[-1][0] == pd.Timestamp("2022-09-09 23:00")

    def test_monthly_grain_over_a_year(self):
        start, end = manage_time_interval("2022")
        assert len(chunk_period(start, end, "month")) == 12

    def test_buckets_are_clipped_to_the_request(self):
        start, end = manage_time_interval("2022-09-15 to 2022-09-20")
        buckets = chunk_period(start, end, "day")
        assert buckets[0][0] == start
        assert buckets[-1][1] == end

    def test_unknown_grain_rejected(self):
        start, end = manage_time_interval("2022-09")
        with pytest.raises(ValueError, match="Unknown chunk grain"):
            chunk_period(start, end, "fortnight")

    def test_iso_date_is_not_split_on_its_hyphens(self):
        """Regression: '2026-07-20 12:00' was split on the first '-' into
        ('2026', '07-20 12:00'). An ISO date is one instant, not a range."""
        start, end = manage_time_interval("2026-07-20 12:00")
        assert start == pd.Timestamp("2026-07-20 12:00")
        assert end == pd.Timestamp("2026-07-20 12:00")

    def test_iso_range_with_to(self):
        start, end = manage_time_interval("2026-07-20 to 2026-07-25")
        assert start == pd.Timestamp("2026-07-20")
        assert end == pd.Timestamp("2026-07-25 23:59:59.999999999")

    def test_single_instant_is_one_bucket(self):
        """A mid-bucket instant must yield exactly one bucket, not one per hour
        since midnight. Regression: day-flooring used to emit inverted buckets."""
        start, end = manage_time_interval("2022-09-09 14:00")
        buckets = chunk_period(start, end, "hour")
        assert len(buckets) == 1
        assert buckets[0] == (pd.Timestamp("2022-09-09 14:00"), pd.Timestamp("2022-09-09 14:00"))

    def test_mid_bucket_start_has_no_inverted_buckets(self):
        start, end = manage_time_interval("2022-09-09 14:30 to 2022-09-09 17:00")
        buckets = chunk_period(start, end, "hour")
        assert all(lo <= hi for lo, hi in buckets)  # never start-after-end
        assert buckets[0][0] == start  # first bucket clipped to the request
