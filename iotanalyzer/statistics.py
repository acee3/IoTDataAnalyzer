from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from .models import Recording


class Statistic(ABC):
    """
    Base contract for streaming statistics that may require multiple passes.

    Implementations override consume/finalize. If two passes are required,
    set requires_second_pass = True and adjust internal state in begin_pass.
    """

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
