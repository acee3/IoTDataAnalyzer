from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Dict, Optional, Tuple

from iotanalyzer.models import Metric, Recording, Unit


class Statistic(ABC):
    """
    Base contract for streaming statistics that may require multiple passes.

    Implementations override consume/finalize. If two passes are required,
    set requires_second_pass = True and adjust internal state in begin_pass.
    """

    UNKNOWN_VALUE = "N/A"
    requires_second_pass: bool = False

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name or self.__class__.__name__

    def begin_pass(self, is_second_pass: bool) -> None:
        """Hook to reset per-pass state. Called before each pass."""

    @abstractmethod
    def consume(self, record: Recording) -> None:
        """Ingest a single recording into the statistic."""

    @abstractmethod
    def get_result(self) -> str:
        """Returns the statistic result as a string."""


class SingleNumberStatistic(Statistic):
    @dataclass(frozen=True)
    class SingleNumberStatisticEntry:
        site: str
        device: str
        metric: Metric
        value: float
        unit: Optional[Unit] = None
    
    @classmethod
    def sorted_entries(cls, entries: list[SingleNumberStatisticEntry]) -> list[SingleNumberStatisticEntry]:
        return sorted(entries, key=lambda e: (e.device, e.site, e.metric.name))

    @classmethod
    def format_line(
        cls,
        statistic_entry: SingleNumberStatisticEntry
    ) -> str:
        value_str = f"{statistic_entry.value:.2f}"
        if statistic_entry.unit is not None:
            value_str = f"{value_str}{statistic_entry.unit.display_name}"
        return (
            f"{statistic_entry.device}/{statistic_entry.site} "
            f"{statistic_entry.metric.display_name}\t=\t{value_str}"
        )

    @classmethod
    def format_entries(
        cls,
        entries: list[SingleNumberStatisticEntry]
    ) -> str:
        lines = []
        for entry in cls.sorted_entries(entries):
            lines.append(cls.format_line(entry))
        return "\n".join(lines)


class AverageStatistic(SingleNumberStatistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._groups: Dict[Tuple[str, str, Metric], Tuple[Unit, float, int]] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        if key not in self._groups:
            self._groups[key] = (record.unit, 0.0, 0)
        unit, total, count = self._groups[key]
        if unit != record.unit:
            raise ValueError("Inconsistent units in AverageStatistic")
        self._groups[key] = (unit, total + record.value, count + 1)

    def get_result(self) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        statistic_entries = [SingleNumberStatistic.SingleNumberStatisticEntry(
            site=site,
            device=device,
            metric=metric,
            unit=unit,
            value=total / count,
        ) for (site, device, metric), (unit, total, count) in self._groups.items()]
        return self.format_entries(statistic_entries)


class MinStatistic(SingleNumberStatistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._groups: Dict[Tuple[str, str, Metric], Tuple[Unit, float]] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        if key not in self._groups:
            self._groups[key] = (record.unit, record.value)
            return

        unit, current_min = self._groups[key]
        if unit != record.unit:
            raise ValueError("Inconsistent units in MinStatistic")
        if record.value < current_min:
            self._groups[key] = (unit, record.value)

    def get_result(self) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        statistic_entries = [SingleNumberStatistic.SingleNumberStatisticEntry(
            site=site,
            device=device,
            metric=metric,
            unit=unit,
            value=min_value,
        ) for (site, device, metric), (unit, min_value) in self._groups.items()]
        return self.format_entries(statistic_entries)


class MaxStatistic(SingleNumberStatistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._groups: Dict[Tuple[str, str, Metric], Tuple[Unit, float]] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        if key not in self._groups:
            self._groups[key] = (record.unit, record.value)
            return

        unit, current_max = self._groups[key]
        if unit != record.unit:
            raise ValueError("Inconsistent units in MaxStatistic")
        if record.value > current_max:
            self._groups[key] = (unit, record.value)

    def get_result(self) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        statistic_entries = [
            SingleNumberStatistic.SingleNumberStatisticEntry(
                site=site,
                device=device,
                metric=metric,
                unit=unit,
                value=max_value,
            )
            for (site, device, metric), (unit, max_value) in self._groups.items()
        ]
        return self.format_entries(statistic_entries)


class CountStatistic(SingleNumberStatistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._counts: Dict[Tuple[str, str, Metric], int] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        self._counts[key] = self._counts.get(key, 0) + 1

    def get_result(self) -> str:
        if not self._counts:
            return self.UNKNOWN_VALUE

        statistic_entries = [
            SingleNumberStatistic.SingleNumberStatisticEntry(
                site=site,
                device=device,
                metric=metric,
                value=float(count),
                unit=None,
            )
            for (site, device, metric), count in self._counts.items()
        ]
        return self.format_entries(statistic_entries)


class PopulationStandardDeviationStatistic(SingleNumberStatistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._groups: Dict[Tuple[str, str, Metric], Tuple[Unit, int, float, float]] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        unit = record.unit
        if key not in self._groups:
            # (unit, count, mean, M2)
            self._groups[key] = (unit, 0, 0.0, 0.0)

        current_unit, count, mean, m2 = self._groups[key]
        if current_unit != unit:
            raise ValueError("Inconsistent units in PopulationStandardDeviationStatistic")

        count += 1
        delta = record.value - mean
        mean += delta / count
        delta2 = record.value - mean
        m2 += delta * delta2
        self._groups[key] = (unit, count, mean, m2)

    def get_result(self) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        statistic_entries: list[SingleNumberStatistic.SingleNumberStatisticEntry] = []
        for (site, device, metric), (unit, count, _mean, m2) in self._groups.items():
            if count == 0:
                stddev = float("nan")
            else:
                variance = m2 / count
                stddev = math.sqrt(variance)
            statistic_entries.append(
                SingleNumberStatistic.SingleNumberStatisticEntry(
                    site=site,
                    device=device,
                    metric=metric,
                    unit=unit,
                    value=stddev,
                )
            )
        return self.format_entries(statistic_entries)


def statistic_from_string(name: str) -> Statistic:
    mapping: dict[str, type[Statistic]] = {
        "average": AverageStatistic,
        "min": MinStatistic,
        "max": MaxStatistic,
        "count": CountStatistic,
        "population_stddev": PopulationStandardDeviationStatistic,
    }
    if name not in mapping:
        raise ValueError(f"Unknown statistic: {name}")
    return mapping[name]()
