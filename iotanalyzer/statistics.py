from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
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


def statistic_from_string(name: str) -> Statistic:
    mapping: dict[str, type[Statistic]] = {
        "average": AverageStatistic,
    }
    if name not in mapping:
        raise ValueError(f"Unknown statistic: {name}")
    return mapping[name]()
