import io
from datetime import datetime, timezone

import pytest

from iotanalyzer.models import Metric, Recording, Unit
from iotanalyzer.reader import recordings_from_csv


def _csv(*rows: str) -> str:
    header = "time,site,device,metric,unit,value"
    return "\n".join((header, *rows))


def test_recordings_from_text_stream_parses_rows():
    csv_data = _csv(
        "2025-01-01 00:00:00 +0000 UTC,site_1,device_a_001,humidity,%RH,42.5",
        "2025-01-01 00:05:00 +0000 UTC,site_1,device_a_001,temperature,C,19.8",
    )
    stream = io.StringIO(csv_data)

    records = list(recordings_from_csv(stream))

    assert len(records) == 2
    assert records[0] == Recording(
        time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        site="site_1",
        device="device_a_001",
        metric=Metric.HUMIDITY,
        unit=Unit.RELATIVE_HUMIDITY,
        value=42.5,
    )
    assert records[1].metric is Metric.TEMPERATURE
    assert records[1].unit is Unit.CELSIUS
    assert records[1].value == 19.8


def test_recordings_from_path_reads_lazy(tmp_path):
    csv_text = _csv(
        "2025-01-01 00:00:00 +0000 UTC,site_1,device_b_002,temp,Cel,21.0",
    )
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text)

    iterator = recordings_from_csv(csv_path)

    first_record = next(iterator)
    assert isinstance(first_record, Recording)
    assert first_record.metric is Metric.TEMPERATURE
    assert first_record.unit is Unit.CELSIUS
    with pytest.raises(StopIteration):
        next(iterator)
