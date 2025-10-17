from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Metric(Enum):
    TEMPERATURE = {"temp", "temperature"}
    HUMIDITY = {"hum", "humidity"}
    PRESSURE = {"press", "pressure"}
    
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
            if metric_name.lower() in metric.value:
                return metric
        raise ValueError(f"Unknown metric: {metric_name}")



class Unit(Enum):
    RELATIVE_HUMIDITY = ("%RH", Metric.HUMIDITY)
    KILO_PASCAL = ("kPa", Metric.PRESSURE)
    CELSIUS = ("C", Metric.TEMPERATURE)
    
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
            if unit_name.lower() in unit.value:
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
