from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class BackoffPolicy:
    initial_seconds: float
    maximum_seconds: float
    jitter_ratio: float = 0.0

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0:
            raise ValueError("initial_seconds must be positive")
        if self.maximum_seconds < self.initial_seconds:
            raise ValueError("maximum_seconds must be greater than or equal to initial_seconds")
        if not 0.0 <= self.jitter_ratio <= 1.0:
            raise ValueError("jitter_ratio must be between 0 and 1")

    def delay(self, consecutive_failure: int, random_fn: Callable[[], float] = random.random) -> float:
        if consecutive_failure <= 0:
            raise ValueError("consecutive_failure must be positive")
        exponent = min(consecutive_failure - 1, 60)
        base = min(self.maximum_seconds, self.initial_seconds * (2**exponent))
        if self.jitter_ratio == 0:
            return base
        spread = base * self.jitter_ratio
        jittered = base - spread + (2 * spread * random_fn())
        return min(self.maximum_seconds, max(0.0, jittered))
