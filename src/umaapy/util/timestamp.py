from dataclasses import dataclass
import time
from functools import total_ordering
from typing import Union

from umaapy.umaa_types import UMAA_Common_Measurement_DateTime


@total_ordering
@dataclass(frozen=False)
class Timestamp:
    """
    Represents a point in time with second and nanosecond precision.

    Supports normalization, arithmetic, comparison, and conversion to/from UMAA types.

    :param seconds: Integer seconds since epoch.
    :param nanoseconds: Additional nanoseconds component (0 <= nanoseconds < 1e9).
    """

    seconds: int
    nanoseconds: int

    def __post_init__(self) -> None:
        """
        Post-initialization hook to normalize the timestamp fields.
        """
        self._normalize()

    def _normalize(self) -> None:
        """
        Normalize the nanoseconds to be within [0, 1e9), adjusting seconds accordingly.
        """
        # Divide nanoseconds into whole seconds and remainder
        extra_sec, self.nanoseconds = divmod(self.nanoseconds, 1_000_000_000)
        self.seconds += extra_sec
        # If nanoseconds is negative, borrow from seconds
        if self.nanoseconds < 0:
            self.nanoseconds += 1_000_000_000
            self.seconds -= 1

    @staticmethod
    def now() -> "Timestamp":
        """
        Create a Timestamp representing the current system time.

        :return: Timestamp for now (time.time()).
        :rtype: Timestamp
        """
        t = time.time()
        sec = int(t)
        nsec = int((t - sec) * 1_000_000_000)
        return Timestamp(sec, nsec)

    @staticmethod
    def from_umaa(ts: UMAA_Common_Measurement_DateTime) -> "Timestamp":
        """
        Construct a Timestamp from a UMAA Common Measurement DateTime.

        :param ts: UMAA_Common_Measurement_DateTime instance.
        :type ts: UMAA_Common_Measurement_DateTime
        :return: Equivalent Timestamp.
        :rtype: Timestamp
        """
        return Timestamp(ts.seconds, ts.nanoseconds)

    def __eq__(self, other: object) -> bool:
        """
        Equality comparison based on seconds and nanoseconds.
        """
        if not isinstance(other, Timestamp):
            return NotImplemented
        return (self.seconds, self.nanoseconds) == (other.seconds, other.nanoseconds)

    def __hash__(self):
        return hash((self.seconds, self.nanoseconds))

    def __lt__(self, other: "Timestamp") -> bool:
        """
        Less-than comparison for ordering timestamps.
        """
        return (self.seconds, self.nanoseconds) < (other.seconds, other.nanoseconds)

    def __add__(self, other: Union["Timestamp", float]) -> "Timestamp":
        """
        Add another Timestamp or float seconds to this timestamp.

        :param other: Timestamp or float seconds to add.
        :type other: Union[Timestamp, float]
        :return: New normalized Timestamp.
        :rtype: Timestamp
        :raises TypeError: If other type is unsupported.
        """
        if isinstance(other, Timestamp):
            # Add fields and normalize
            return Timestamp(self.seconds + other.seconds, self.nanoseconds + other.nanoseconds)
        elif isinstance(other, (int, float)):
            sec = int(other)
            nsec = int((other - sec) * 1_000_000_000)
            return Timestamp(self.seconds + sec, self.nanoseconds + nsec)
        else:
            raise TypeError(f"Unsupported type for addition: {type(other)}")

    def __sub__(self, other: Union["Timestamp", float]) -> Union[float, "Timestamp"]:
        """
        Subtract another Timestamp or float seconds from this timestamp.

        :param other: Timestamp or float seconds to subtract.
        :type other: Union[Timestamp, float]
        :return: Difference in seconds if other is Timestamp, or new Timestamp.
        :rtype: Union[float, Timestamp]
        :raises TypeError: If other type is unsupported.
        """
        if isinstance(other, Timestamp):
            # Compute difference as float seconds
            sec_diff = self.seconds - other.seconds
            nsec_diff = self.nanoseconds - other.nanoseconds
            return sec_diff + nsec_diff / 1_000_000_000
        elif isinstance(other, (int, float)):
            # Subtract float seconds by adding negative
            return self + (-other)
        else:
            raise TypeError(f"Unsupported type for subtraction: {type(other)}")

    def to_float(self) -> float:
        """
        Convert this Timestamp to floating-point seconds.

        :return: Total seconds as float.
        :rtype: float
        """
        return self.seconds + self.nanoseconds / 1_000_000_000

    def to_umaa(self) -> UMAA_Common_Measurement_DateTime:
        """
        Convert this Timestamp to a UMAA Common Measurement DateTime.

        :return: UMAA_Common_Measurement_DateTime equivalent.
        :rtype: UMAA_Common_Measurement_DateTime
        """
        return UMAA_Common_Measurement_DateTime(self.seconds, self.nanoseconds)

    def __repr__(self) -> str:
        """
        Developer-friendly representation of the Timestamp.

        :return: String repr with seconds and nanoseconds.
        :rtype: str
        """
        return f"Timestamp(seconds={self.seconds}, nanoseconds={self.nanoseconds})"
