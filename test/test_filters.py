from datetime import datetime, timezone, timedelta

from iotanalyzer.filters import (
    device_filter,
    end_time_filter,
    metric_filter,
    site_filter,
    start_time_filter,
)
from iotanalyzer.models import Metric, Recording, Unit


def make_record(
    *,
    time: datetime,
    site: str = "site1",
    device: str = "device1",
    metric: Metric = Metric.TEMPERATURE,
    unit: Unit = Unit.CELSIUS,
    value: float = 0.0,
) -> Recording:
    return Recording(
        time=time,
        site=site,
        device=device,
        metric=metric,
        unit=unit,
        value=value,
    )


def test_start_time_filter_includes_only_records_after_start():
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    start = start_time_filter(base + timedelta(minutes=1))
    early = make_record(time=base)
    later = make_record(time=base + timedelta(minutes=2))

    assert not start(early)
    assert start(later)


def test_end_time_filter_includes_only_records_before_end():
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = end_time_filter(base + timedelta(minutes=1))
    early = make_record(time=base)
    later = make_record(time=base + timedelta(minutes=2))

    assert end(early)
    assert not end(later)


def test_site_filter_matches_case_insensitively():
    filter_fn = site_filter(["Site_A", "site_b"])
    assert filter_fn(make_record(time=datetime.now(tz=timezone.utc), site="site_a"))
    assert filter_fn(make_record(time=datetime.now(tz=timezone.utc), site="SITE_B"))
    assert not filter_fn(make_record(time=datetime.now(tz=timezone.utc), site="site_c"))


def test_device_filter_matches_case_insensitively():
    filter_fn = device_filter(["device_1"])
    assert filter_fn(make_record(time=datetime.now(tz=timezone.utc), device="DEVICE_1"))
    assert not filter_fn(make_record(time=datetime.now(tz=timezone.utc), device="device_2"))


def test_metric_filter_matches_exact_metric():
    filter_fn = metric_filter([Metric.HUMIDITY, Metric.PRESSURE])
    assert filter_fn(make_record(time=datetime.now(tz=timezone.utc), metric=Metric.HUMIDITY))
    assert not filter_fn(make_record(time=datetime.now(tz=timezone.utc), metric=Metric.TEMPERATURE))
