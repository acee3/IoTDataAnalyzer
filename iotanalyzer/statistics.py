from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Callable, ClassVar, Dict, Literal, Optional, Tuple, cast

from iotanalyzer.models import Metric, Recording, Unit


class Statistic(ABC):
    """
    Base contract for streaming statistics that may require multiple passes.

    Implementations override consume/finalize. If two passes are required,
    set requires_second_pass = True and adjust internal state in begin_pass.
    """

    UNKNOWN_VALUE = "N/A"
    
    requires_second_pass: bool = False
    
    SortKeyName = Literal["value_asc", "value_desc", "device_site_metric"]
    SortKeyFunc = Callable[["Statistic.StatisticEntry"], Tuple[float, str, str, str]]
    DEFAULT_SORT_KEY: ClassVar[SortKeyName] = "value_desc"
    DEFAULT_K: ClassVar[Optional[int]] = 10

    _SORT_KEY_FUNCTIONS: ClassVar[Dict[SortKeyName, SortKeyFunc]] = {
        "value_asc": lambda e: (e.value, e.metric.name, e.device, e.site),
        "value_desc": lambda e: (-e.value, e.metric.name, e.device, e.site),
        "device_site_metric": lambda e: (0.0, e.device, e.site, e.metric.name),
    }

    @dataclass(frozen=True)
    class StatisticEntry:
        site: str
        device: str
        metric: Metric
        value: float
        unit: Optional[Unit] = None

    def __init__(
        self,
        name: Optional[str] = None,
        default_sort_key: SortKeyName = DEFAULT_SORT_KEY,
        default_k: Optional[int] = DEFAULT_K,
    ) -> None:
        self.name = name or self.__class__.__name__
        if default_sort_key not in self._SORT_KEY_FUNCTIONS:
            raise ValueError(f"Unsupported default sort key: {default_sort_key}")
        if default_k is not None and default_k < 0:
            raise ValueError("default_k must be non-negative or None")
        self._default_sort_key: Statistic.SortKeyName = default_sort_key
        self._default_k: Optional[int] = default_k

    def begin_pass(self, is_second_pass: bool) -> None:
        """Hook to reset per-pass state. Called before each pass."""

    @abstractmethod
    def consume(self, record: Recording) -> None:
        """Ingest a single recording into the statistic."""

    @abstractmethod
    def get_result(
        self,
        sort_key: Optional[SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        """Returns the statistic result as a string."""

    @property
    def default_sort_key(self) -> SortKeyName:
        return self._default_sort_key

    @property
    def default_k(self) -> Optional[int]:
        return self._default_k

    @classmethod
    def format_line(
        cls,
        statistic_entry: StatisticEntry
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
        entries: list[StatisticEntry],
        sort_key: SortKeyName = DEFAULT_SORT_KEY,
        k: Optional[int] = DEFAULT_K,
    ) -> str:
        try:
            key_func = cls._SORT_KEY_FUNCTIONS[sort_key]
        except KeyError as exc:
            raise ValueError(f"Unsupported sort key: {sort_key}") from exc
        sorted_entries = sorted(entries, key=key_func)
        if k is not None:
            if k <= 0:
                sorted_entries = []
            else:
                sorted_entries = sorted_entries[:k]
        lines = []
        for entry in sorted_entries:
            lines.append(cls.format_line(entry))
        return "\n".join(lines)


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

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        effective_sort = sort_key or self.default_sort_key
        effective_k = self.default_k if k is None else k

        statistic_entries = [Statistic.StatisticEntry(
            site=site,
            device=device,
            metric=metric,
            unit=unit,
            value=total / count,
        ) for (site, device, metric), (unit, total, count) in self._groups.items()]
        return self.format_entries(statistic_entries, sort_key=effective_sort, k=effective_k)


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

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        effective_sort = sort_key or self.default_sort_key
        effective_k = self.default_k if k is None else k

        statistic_entries = [Statistic.StatisticEntry(
            site=site,
            device=device,
            metric=metric,
            unit=unit,
            value=min_value,
        ) for (site, device, metric), (unit, min_value) in self._groups.items()]
        return self.format_entries(statistic_entries, sort_key=effective_sort, k=effective_k)


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

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        effective_sort = sort_key or self.default_sort_key
        effective_k = self.default_k if k is None else k

        statistic_entries = [
            Statistic.StatisticEntry(
                site=site,
                device=device,
                metric=metric,
                unit=unit,
                value=max_value,
            )
            for (site, device, metric), (unit, max_value) in self._groups.items()
        ]
        return self.format_entries(statistic_entries, sort_key=effective_sort, k=effective_k)


class CountStatistic(Statistic):
    def begin_pass(self, is_second_pass: bool) -> None:
        self._counts: Dict[Tuple[str, str, Metric], int] = {}

    def consume(self, record: Recording) -> None:
        key = (record.site, record.device, record.metric)
        self._counts[key] = self._counts.get(key, 0) + 1

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        if not self._counts:
            return self.UNKNOWN_VALUE

        effective_sort = sort_key or self.default_sort_key
        effective_k = self.default_k if k is None else k

        statistic_entries = [
            Statistic.StatisticEntry(
                site=site,
                device=device,
                metric=metric,
                value=float(count),
                unit=None,
            )
            for (site, device, metric), count in self._counts.items()
        ]
        return self.format_entries(statistic_entries, sort_key=effective_sort, k=effective_k)


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

    def get_result(
        self,
        sort_key: Optional[Statistic.SortKeyName] = None,
        k: Optional[int] = None,
    ) -> str:
        if not self._groups:
            return self.UNKNOWN_VALUE

        effective_sort = sort_key or self.default_sort_key
        effective_k = self.default_k if k is None else k

        statistic_entries: list[Statistic.StatisticEntry] = []
        for (site, device, metric), (unit, count, _mean, m2) in self._groups.items():
            if count == 0:
                stddev = float("nan")
            else:
                variance = m2 / count
                stddev = math.sqrt(variance)
            statistic_entries.append(
                Statistic.StatisticEntry(
                    site=site,
                    device=device,
                    metric=metric,
                    unit=unit,
                    value=stddev,
                )
            )
        return self.format_entries(statistic_entries, sort_key=effective_sort, k=effective_k)


def statistic_from_string(name: str) -> Statistic:
    """
    Parse a statistic specification string.

    The basic form is "<statistic_name>".
    Options can be provided after a colon, separated by commas, e.g.:
        "average:sort=value_desc,k=5"
    Supported options:
        - sort: one of Statistic.SortKeyName values
        - k: positive integer (top-k results, default 10). Use "all" (or empty) for all rows.
        - name: optional custom display name for the statistic
    """
    mapping: dict[str, type[Statistic]] = {
        "average": AverageStatistic,
        "min": MinStatistic,
        "max": MaxStatistic,
        "count": CountStatistic,
        "population_stddev": PopulationStandardDeviationStatistic,
    }
    spec = name.strip()
    if not spec:
        raise ValueError("Statistic specification cannot be empty")

    stat_name, _, options = spec.partition(":")
    stat_key = stat_name.strip().lower()
    if stat_key not in mapping:
        raise ValueError(f"Unknown statistic: {stat_name}")

    sort_key: Statistic.SortKeyName = Statistic.DEFAULT_SORT_KEY
    default_k: Optional[int] = Statistic.DEFAULT_K
    custom_name: Optional[str] = None

    if options:
        for raw_option in options.split(","):
            option = raw_option.strip()
            if not option:
                continue
            key, sep, value = option.partition("=")
            if not sep:
                raise ValueError(f"Invalid statistic option '{option}'. Expected key=value.")
            key = key.strip().lower()
            value = value.strip()

            if key == "sort":
                sort_candidate = value.lower()
                if sort_candidate not in Statistic._SORT_KEY_FUNCTIONS:
                    allowed = ", ".join(Statistic._SORT_KEY_FUNCTIONS.keys())
                    raise ValueError(f"Unsupported sort key '{value}'. Allowed values: {allowed}")
                sort_key = cast(Statistic.SortKeyName, sort_candidate)
            elif key == "k":
                if value.lower() in {"", "all", "none"}:
                    default_k = None
                else:
                    try:
                        parsed_k = int(value)
                    except ValueError as exc:
                        raise ValueError(f"Invalid integer for k: {value}") from exc
                    if parsed_k < 0:
                        raise ValueError("k must be non-negative")
                    default_k = parsed_k
            elif key == "name":
                custom_name = value
            else:
                raise ValueError(f"Unknown statistic option '{key}'")

    stat_cls = mapping[stat_key]
    return stat_cls(name=custom_name, default_sort_key=sort_key, default_k=default_k)
