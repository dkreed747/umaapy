from typing import override
from time import sleep

from umaapy.util.specializations import *
from umaapy.util.umaa_utils import topic_from_type
from umaapy.util.dds_configurator import UmaaQosProfileCategory

from umaapy import get_configurator

from umaapy.umaa_types import (
    UMAA_MM_Conditional_ConditionalType as ConditionalType,
    UMAA_MM_BaseType_ObjectiveType as ObjectiveType,
)


def test_temp():
    matches = get_specializations_from_generalization(ObjectiveType)
    globals().update(matches)
    for name, cls in matches.items():
        print(name)
