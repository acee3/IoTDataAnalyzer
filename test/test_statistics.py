from datetime import datetime
import pytest

from iotanalyzer.models import Metric, Recording, Unit
from iotanalyzer.statistics import AverageStatistic


def make_record(time: datetime, site: str, device: str, metric: Metric, unit: Unit, value: float) -> Recording:
    return Recording(
        time=time,
        site=site,
        device=device,
        metric=metric,
        unit=unit,
        value=value,
    )


def test_average_statistic_groups_by_site_device_metric():
    stats = AverageStatistic()
    stats.begin_pass(is_second_pass=False)

    stats.consume(make_record(datetime.now(), "site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 40.0))
    stats.consume(make_record(datetime.now(), "site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 50.0))
    stats.consume(make_record(datetime.now(), "site2", "dev2", Metric.TEMPERATURE, Unit.CELSIUS, 20.0))
    stats.consume(make_record(datetime.now(), "site2", "dev2", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 30.0))

    result = stats.get_result()

    expected_lines = {
        "site1/dev1 humidity = 45.00%RH",
        "site2/dev2 humidity = 30.00%RH",
        "site2/dev2 temperature = 20.00°C",
    }
    assert set(result.splitlines()) == expected_lines
    
def test_average_statistic_handles_no_data():
    stats = AverageStatistic()
    stats.begin_pass(is_second_pass=False)

    result = stats.get_result()

    assert result == "N/A"
