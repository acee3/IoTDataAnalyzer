import csv
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union, TextIO

from .models import Recording, Metric, Unit


def _parse_timestamp(raw_timestamp: str) -> datetime:
    """Parse timestamps in the format used by the IoT data exports."""
    return datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S %z %Z")


def _parse_unit(raw_unit: str) -> Unit:
    """Handle the few known CSV aliases before consulting Unit.from_string."""
    try:
        return Unit.from_string(raw_unit)
    except ValueError:
        normalized = raw_unit.strip().lower()
        alias_map = {
            "c": Unit.CELSIUS,
            "cel": Unit.CELSIUS,
            "celsius": Unit.CELSIUS,
            "%rh": Unit.RELATIVE_HUMIDITY,
            "relative_humidity": Unit.RELATIVE_HUMIDITY,
            "kpa": Unit.KILO_PASCAL,
        }
        if normalized in alias_map:
            return alias_map[normalized]
        raise


def _row_to_recording(row: dict) -> Recording:
    metric = Metric.from_string(row["metric"])
    unit = _parse_unit(row["unit"])

    return Recording(
        time=_parse_timestamp(row["time"]),
        site=row["site"],
        device=row["device"],
        metric=metric,
        unit=unit,
        value=float(row["value"]),
    )


def recordings_from_csv(source: Union[str, Path, TextIO]) -> Iterator[Recording]:
    """
    Lazily read recordings from a CSV file.

    Args:
        source: Path to the CSV file or an open text file handle positioned at the start.

    Yields:
        Recording instances, one for each row in the CSV.
    """
    if isinstance(source, (str, Path)):
        with open(source, newline="") as csv_file:
            yield from recordings_from_csv(csv_file)
        return

    csv_reader = csv.DictReader(source)
    for row in csv_reader:
        yield _row_to_recording(row)
