from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class RetryAttempt:
    number: int
    total: int
    delay: float

    @property
    def is_last(self) -> bool:
        return self.number == self.total


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    base_delay: float = 1.0
    exponential: bool = False

    def __iter__(self) -> Iterator[RetryAttempt]:
        for number in range(1, self.attempts + 1):
            exponent = number - 1 if self.exponential else 0
            yield RetryAttempt(
                number=number,
                total=self.attempts,
                delay=self.base_delay * (2**exponent),
            )
