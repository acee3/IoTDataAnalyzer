from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Metric(Enum):
    TEMPERATURE = {"temp", "temperature"}
    HUMIDITY = {"hum", "humidity"}
    PRESSURE = {"press", "pressure"}
    
    @property
    def alias_set(self) -> set[str]:
        return self.value

    @classmethod
    def from_string(cls, metric_name: str) -> "Metric":
        """Convert a string to a Metruc enum, handling aliases and casing
        Args:
            metric_name: The name of the metric to convert
        Returns:
            The Metric enum
        Raises:
            ValueError: If the metric name is not found
        """
        for metric in cls:
            if metric_name.strip().lower() in metric.alias_set:
                return metric
        raise ValueError(f"Unknown metric: {metric_name}")


class Unit(Enum):
    RELATIVE_HUMIDITY = ({"%rh"}, Metric.HUMIDITY)
    KILO_PASCAL = ({"kpa"}, Metric.PRESSURE)
    CELSIUS = ({"c"}, Metric.TEMPERATURE)

    @property
    def alias_set(self) -> set[str]:
        return self.value[0]

    @property
    def metric(self) -> Metric:
        return self.value[1]
    
    @classmethod
    def from_string(cls, unit_name: str) -> "Unit":
        """Convert a string to a Unit enum, handling aliases and casing
        Args:
            unit_name: The name of the unit to convert
        Returns:
            The Unit enum
        Raises:
            ValueError: If the unit name is not found
        """
        for unit in cls:
            if unit_name.strip().lower() in unit.alias_set:
                return unit
        raise ValueError(f"Unknown unit: {unit_name}")


@dataclass(frozen=True)
class Recording:
    time: datetime
    site: str
    device: str
    metric: Metric
    unit: Unit
    value: float
