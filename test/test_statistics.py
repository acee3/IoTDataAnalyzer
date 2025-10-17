from datetime import datetime, timezone
import statistics

import pytest

from iotanalyzer.models import Metric, Recording, Unit
from iotanalyzer.statistics import (
    AverageStatistic,
    CountStatistic,
    MaxStatistic,
    MinStatistic,
    PopulationStandardDeviationStatistic,
)

SAMPLE_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)


def make_record(site: str, device: str, metric: Metric, unit: Unit, value: float) -> Recording:
    return Recording(
        time=SAMPLE_TIME,
        site=site,
        device=device,
        metric=metric,
        unit=unit,
        value=value,
    )


def test_average_statistic_groups_by_site_device_metric():
    stats = AverageStatistic()
    stats.begin_pass(is_second_pass=False)

    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 40.0))
    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 50.0))
    stats.consume(make_record("site2", "dev2", Metric.TEMPERATURE, Unit.CELSIUS, 20.0))

    result = stats.get_result()

    expected_lines = {
        "dev1/site1 Humidity\t=\t45.00%RH",
        "dev2/site2 Temperature\t=\t20.00°C",
    }
    assert set(result.splitlines()) == expected_lines


def test_average_statistic_handles_no_data():
    stats = AverageStatistic()
    stats.begin_pass(is_second_pass=False)
    assert stats.get_result() == "N/A"


def test_average_statistic_raises_on_unit_mismatch():
    stats = AverageStatistic()
    stats.begin_pass(is_second_pass=False)
    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 40.0))

    with pytest.raises(ValueError):
        stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.CELSIUS, 42.0))


def test_min_statistic_reports_group_minima():
    stats = MinStatistic()
    stats.begin_pass(is_second_pass=False)
    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 55.0))
    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 48.0))

    assert stats.get_result() == "dev1/site1 Humidity\t=\t48.00%RH"


def test_max_statistic_reports_group_maxima():
    stats = MaxStatistic()
    stats.begin_pass(is_second_pass=False)
    stats.consume(make_record("site1", "dev1", Metric.TEMPERATURE, Unit.CELSIUS, 18.0))
    stats.consume(make_record("site1", "dev1", Metric.TEMPERATURE, Unit.CELSIUS, 24.0))

    assert stats.get_result() == "dev1/site1 Temperature\t=\t24.00°C"


def test_count_statistic_counts_records_per_group():
    stats = CountStatistic()
    stats.begin_pass(is_second_pass=False)
    stats.consume(make_record("site1", "dev1", Metric.PRESSURE, Unit.KILO_PASCAL, 101.0))
    stats.consume(make_record("site1", "dev1", Metric.PRESSURE, Unit.KILO_PASCAL, 102.0))
    stats.consume(make_record("site2", "dev2", Metric.PRESSURE, Unit.KILO_PASCAL, 103.0))

    lines = set(stats.get_result().splitlines())
    assert lines == {
        "dev1/site1 Pressure\t=\t2.00",
        "dev2/site2 Pressure\t=\t1.00",
    }


def test_standard_deviation_statistic_computes_per_group():
    stats = PopulationStandardDeviationStatistic()
    stats.begin_pass(is_second_pass=False)
    values = [10.0, 12.0, 14.0]
    for value in values:
        stats.consume(make_record("site1", "dev1", Metric.TEMPERATURE, Unit.CELSIUS, value))

    expected_stddev = statistics.pstdev(values)
    expected_line = f"dev1/site1 Temperature\t=\t{expected_stddev:.2f}°C"

    assert stats.get_result() == expected_line


def test_standard_deviation_statistic_unit_mismatch():
    stats = PopulationStandardDeviationStatistic()
    stats.begin_pass(is_second_pass=False)
    stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.RELATIVE_HUMIDITY, 40.0))

    with pytest.raises(ValueError):
        stats.consume(make_record("site1", "dev1", Metric.HUMIDITY, Unit.CELSIUS, 45.0))
