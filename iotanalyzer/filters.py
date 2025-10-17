from __future__ import annotations

from datetime import datetime
from typing import Callable, Iterable, Set

from iotanalyzer.models import Metric, Recording


Filter = Callable[[Recording], bool]


def start_time_filter(start: datetime) -> Filter:
    """Allow only recordings whose timestamps are at or after the given start."""
    def _filter(record: Recording) -> bool:
        return record.time >= start
    return _filter


def end_time_filter(end: datetime) -> Filter:
    """Allow only recordings whose timestamps are at or before the given end."""
    def _filter(record: Recording) -> bool:
        return record.time <= end
    return _filter


def site_filter(allowed_sites: Iterable[str]) -> Filter:
    sites: Set[str] = {site.lower() for site in allowed_sites}

    def _filter(record: Recording) -> bool:
        return record.site.lower() in sites

    return _filter


def device_filter(allowed_devices: Iterable[str]) -> Filter:
    devices: Set[str] = {device.lower() for device in allowed_devices}

    def _filter(record: Recording) -> bool:
        return record.device.lower() in devices

    return _filter


def metric_filter(allowed_metrics: Iterable[Metric]) -> Filter:
    metrics = set(allowed_metrics)

    def _filter(record: Recording) -> bool:
        return record.metric in metrics

    return _filter
