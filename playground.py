from umaapy.util.specializations import *
import inspect

from umaapy.umaa_types import (
    UMAA_MM_Conditional_ConditionalType as ConditionalType,
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportType as GlobalPoseReportType,
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_ConditionalReport_ConditionalReportType as ConditionalReportType,
)

from umaapy.util.umaa_utils import *


def what_is_that_type_doin(umaa_type):
    message = f"{umaa_type.__name__} is a "
    types = []
    for concept, paths in umaa_concepts_on_type(umaa_type, False).items():
        if len(paths) == 0:
            continue
        types.append(f"{concept.name}@{paths.keys()}")

    print(message + ", ".join(types))


# what_is_that_type_doin(ObjectiveExecutorCommandType)


x = classify_obj_by_umaa(ObjectiveExecutorCommandType())
print(x)
