import time
import pytest

from umaapy.dds_backend import dds

from umaapy.util.umaa_utils import NumericGUID
from umaapy.util.uuid_factory import generate_guid
from umaapy.util.multi_topic_support import UmaaWriterAdapter, UmaaReaderAdapter

from umaapy.umaa_types import (
    UMAA_MM_ConditionalReport_ConditionalReportType as ConditionalReportType,
    UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElement as ConditionalReportTypeConditionalsSetElement,
    UMAA_MM_Conditional_DepthConditionalType as DepthConditionalType,
    UMAA_MM_Conditional_ExpConditionalType as ExpConditionalType,
)

from umaapy import get_configurator, reset_dds_participant


@pytest.mark.timeout(30)
def test_roundtrip_conditional_report():
    reset_dds_participant()
    w: UmaaWriterAdapter = get_configurator().get_umaa_writer(ConditionalReportType)
    r: UmaaReaderAdapter = get_configurator().get_umaa_reader(ConditionalReportType)

    time.sleep(0.5)  # allow discovery

    conditional_report = w.new_combined()
    conditionals = w.editor_for_set(
        conditional_report,
        path=(),
        set_name="conditionals",
        element_type=ConditionalReportTypeConditionalsSetElement,
    )

    c1 = conditionals.add_new()
    c2 = conditionals.add_new()
    c1 = c1.use_specialization(DepthConditionalType, at=("element",))
    c1.depth = 42.0
    c2 = c2.use_specialization(ExpConditionalType, at=("element",))
    c2.expConditionalName = "I am a custom conditional!"

    w.write(conditional_report)

    # Read back one combined sample
    deadline = time.time() + 8.0
    cs = None

    while time.time() < deadline:
        xs = r.take_data()
        if xs:
            cs = xs[-1]
            break
        time.sleep(0.05)

    assert cs is not None
    v = cs.view

    conditionals = v.collections.get("conditionals", [])
    assert len(conditionals) == 2
    assert conditionals[0].element.depth == 42.0
    assert conditionals[1].element.expConditionalName == "I am a custom conditional!"
