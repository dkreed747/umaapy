from dataclasses import dataclass
import time
from functools import total_ordering
from typing import Union

from umaapy.types import UMAA_Common_Measurement_DateTime


@total_ordering
@dataclass(frozen=False)
class Timestamp:
    seconds: int
    nanoseconds: int

    def __post_init__(self):
        self._normalize()

    def _normalize(self):
        extra_sec, self.nanoseconds = divmod(self.nanoseconds, 1_000_000_000)
        self.seconds += extra_sec
        if self.nanoseconds < 0:
            self.nanoseconds += 1_000_000_000
            self.seconds -= 1

    @staticmethod
    def now():
        t = time.time()
        sec = int(t)
        nsec = int((t - sec) * 1_000_000_000)
        return Timestamp(sec, nsec)

    @staticmethod
    def from_umaa(ts: UMAA_Common_Measurement_DateTime):
        return Timestamp(ts.seconds, ts.nanoseconds)

    def __eq__(self, other: "Timestamp") -> bool:
        return (self.seconds, self.nanoseconds) == (other.seconds, other.nanoseconds)

    def __lt__(self, other: "Timestamp") -> bool:
        return (self.seconds, self.nanoseconds) < (other.seconds, other.nanoseconds)

    def __add__(self, other: Union["Timestamp", float]) -> "Timestamp":
        if isinstance(other, Timestamp):
            return Timestamp(self.seconds + other.seconds, self.nanoseconds + other.nanoseconds)
        elif isinstance(other, (int, float)):
            sec = int(other)
            nsec = int((other - sec) * 1_000_000_000)
            return Timestamp(self.seconds + sec, self.nanoseconds + nsec)
        else:
            raise TypeError("Unsupported type for addition")

    def __sub__(self, other: Union["Timestamp", float]) -> Union["Timestamp", float]:
        if isinstance(other, Timestamp):
            sec_diff = self.seconds - other.seconds
            nsec_diff = self.nanoseconds - other.nanoseconds
            return sec_diff + nsec_diff / 1_000_000_000
        elif isinstance(other, (int, float)):
            return self + (-other)
        else:
            raise TypeError("Unsupported type for subtraction")

    def to_float(self) -> float:
        return self.seconds + self.nanoseconds / 1_000_000_000

    def to_umaa(self) -> UMAA_Common_Measurement_DateTime:
        return UMAA_Common_Measurement_DateTime(self.seconds, self.nanoseconds)

    def __repr__(self):
        return f"Timestamp(seconds={self.seconds}, nanoseconds={self.nanoseconds})"
