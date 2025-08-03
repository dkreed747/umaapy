from umaapy.util.specializations import *

from umaapy.umaa_types import (
    UMAA_MM_Conditional_ConditionalType as ConditionalType,
)


def test_temp():
    matches = get_specializations_from_generalization(ConditionalType)
    print(len(matches))
