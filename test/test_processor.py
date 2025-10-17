from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, Optional

import pytest

from iotanalyzer.filters import (
    device_filter,
    end_time_filter,
    metric_filter,
    site_filter,
    start_time_filter,
)
from iotanalyzer.models import Metric, Unit, Recording
from iotanalyzer.processor import process_recordings
from iotanalyzer.statistics import Statistic


def _write_sample_csv(tmp_path: Path) -> Path:
    rows = [
        "2025-01-01 00:00:00 +0000 UTC,site_1,device_a,temperature,C,19.0",
        "2025-01-01 00:05:00 +0000 UTC,site_1,device_b,temperature,C,21.0",
        "2025-01-01 00:10:00 +0000 UTC,site_2,device_a,humidity,%RH,45.0",
        "2025-01-01 00:15:00 +0000 UTC,site_2,device_c,pressure,kPa,100.5",
    ]
    content = "\n".join(["time,site,device,metric,unit,value", *rows])
    path = tmp_path / "records.csv"
    path.write_text(content)
    return path


class RecordingCollector(Statistic):
    def __init__(self, name: str = "Collector", requires_second_pass: bool = False) -> None:
        super().__init__(name=name)
        self.requires_second_pass = requires_second_pass
        self.begin_pass_flags: list[bool] = []
        self.consumed: list[tuple[int, Recording]] = []
        self._current_pass = 0

    def begin_pass(self, is_second_pass: bool) -> None:
        self.begin_pass_flags.append(is_second_pass)
        self._current_pass = 2 if is_second_pass else 1

    def consume(self, record: Recording) -> None:
        self.consumed.append((self._current_pass, record))

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        total = len(self.consumed)
        return f"{total} records processed"

    def consumed_count(self, *, pass_number: Optional[int] = None) -> int:
        if pass_number is None:
            return len(self.consumed)
        return sum(1 for p, _ in self.consumed if p == pass_number)


def test_process_recordings_without_statistics_produces_no_output(tmp_path, capsys):
    csv_path = _write_sample_csv(tmp_path)

    process_recordings(input_file=str(csv_path))

    captured = capsys.readouterr()
    assert captured.out == ""


def test_process_recordings_with_statistic_collects_all_records(tmp_path, capsys):
    csv_path = _write_sample_csv(tmp_path)
    collector = RecordingCollector()

    process_recordings(input_file=str(csv_path), statistics=[collector])

    assert collector.begin_pass_flags == [False]
    assert collector.consumed_count() == 4
    captured = capsys.readouterr().out
    assert "Results for Collector" in captured
    assert "4 records processed" in captured


def test_process_recordings_applies_site_and_metric_filters(tmp_path, capsys):
    csv_path = _write_sample_csv(tmp_path)
    collector = RecordingCollector()
    filters = [
        site_filter(["site_1"]),
        metric_filter([Metric.TEMPERATURE]),
    ]

    process_recordings(str(csv_path), filters=filters, statistics=[collector])

    assert collector.consumed_count() == 2  # both site_1 temperature records
    captured = capsys.readouterr().out
    assert "2 records processed" in captured


def test_process_recordings_applies_all_filters(tmp_path, capsys):
    csv_path = _write_sample_csv(tmp_path)
    collector = RecordingCollector()
    start = datetime(2025, 1, 1, 0, 5, tzinfo=timezone.utc)
    end = start
    filters = [
        start_time_filter(start),
        end_time_filter(end),
        site_filter(["site_1"]),
        metric_filter([Metric.TEMPERATURE]),
        device_filter(["device_b"]),
    ]

    process_recordings(str(csv_path), filters=filters, statistics=[collector])

    assert collector.consumed_count() == 1
    captured = capsys.readouterr().out
    assert "1 records processed" in captured
