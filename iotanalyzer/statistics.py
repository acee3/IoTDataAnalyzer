from __future__ import annotations

from abc import ABC, abstractmethod
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


class AverageStatistic(Statistic):
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

        lines = []
        sorted_items = sorted(self._groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].name))
        for (site, device, metric), (unit, total, count) in sorted_items:
            avg = total / count
            lines.append(f"{site}/{device} {metric.name.lower()} = {avg:.2f}{unit.display_name}")
        return "\n".join(lines)


class MinStatistic(Statistic):
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

        lines = []
        sorted_items = sorted(self._groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].name))
        for (site, device, metric), (unit, min_value) in sorted_items:
            lines.append(f"{site}/{device} {metric.name.lower()} min = {min_value:.2f}{unit.display_name}")
        return "\n".join(lines)


class MaxStatistic(Statistic):
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

        lines = []
        sorted_items = sorted(self._groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].name))
        for (site, device, metric), (unit, max_value) in sorted_items:
            lines.append(f"{site}/{device} {metric.name.lower()} max = {max_value:.2f}{unit.display_name}")
        return "\n".join(lines)


class CountStatistic(Statistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._counts: Dict[Tuple[str, str, Metric], int] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        self._counts[key] = self._counts.get(key, 0) + 1

    def get_result(self) -> str:
        if not self._counts:
            return self.UNKNOWN_VALUE

        lines = []
        for (site, device, metric), count in sorted(self._counts.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].name)):
            lines.append(f"{site}/{device} {metric.name.lower()} count = {count}")
        return "\n".join(lines)


class PopulationStandardDeviationStatistic(Statistic):
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

        lines = []
        sorted_items = sorted(self._groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2].name))
        for (site, device, metric), (unit, count, _mean, m2) in sorted_items:
            if count == 0:
                stddev = float("nan")
            else:
                variance = m2 / count
                stddev = math.sqrt(variance)
            lines.append(f"{site}/{device} {metric.name.lower()} stddev = {stddev:.2f}{unit.display_name}")
        return "\n".join(lines)


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
