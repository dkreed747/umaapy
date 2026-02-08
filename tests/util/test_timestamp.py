import time
import pytest
from umaapy.util.timestamp import Timestamp
from umaapy.umaa_types import UMAA_Common_Measurement_DateTime

pytestmark = pytest.mark.unit


def test_now():
    t = Timestamp.now()
    assert isinstance(t.seconds, int)
    assert 0 <= t.nanoseconds < 1_000_000_000


def test_normalization():
    ts = Timestamp(1000, 1_500_000_000)
    assert ts.seconds == 1001
    assert ts.nanoseconds == 500_000_000

    ts = Timestamp(1000, -500_000_000)
    assert ts.seconds == 999
    assert ts.nanoseconds == 500_000_000


def test_addition_timestamp():
    t1 = Timestamp(100, 600_000_000)
    t2 = Timestamp(2, 500_000_000)
    result = t1 + t2
    assert result.seconds == 103
    assert result.nanoseconds == 100_000_000


def test_addition_float():
    t1 = Timestamp(5, 500_000_000)
    result = t1 + 2.6
    assert result.seconds == 8
    assert abs(result.nanoseconds - 100_000_000) < 5  # Allow rounding difference


def test_subtraction_timestamp():
    t1 = Timestamp(10, 500_000_000)
    t2 = Timestamp(8, 100_000_000)
    diff = t1 - t2
    assert abs(diff - 2.4) < 1e-9


def test_subtraction_float():
    t = Timestamp(10, 200_000_000)
    result = t - 1.5
    assert result.seconds == 8
    assert abs(result.nanoseconds - 700_000_000) < 5


def test_comparison():
    t1 = Timestamp(10, 0)
    t2 = Timestamp(10, 500)
    t3 = Timestamp(11, 0)

    assert t1 < t2
    assert t2 < t3
    assert t1 != t2
    assert t3 > t1


def test_to_float():
    t = Timestamp(3, 250_000_000)
    f = t.to_float()
    assert abs(f - 3.25) < 1e-9


def test_from_and_to_umaa():
    umaa = UMAA_Common_Measurement_DateTime(123, 456_789_000)
    ts = Timestamp.from_umaa(umaa)
    assert ts.seconds == 123
    assert ts.nanoseconds == 456_789_000

    umaa_roundtrip = ts.to_umaa()
    assert umaa_roundtrip.seconds == 123
    assert umaa_roundtrip.nanoseconds == 456_789_000
