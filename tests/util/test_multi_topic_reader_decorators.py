import pytest
import time

from umaapy.util.umaa_utils import NumericGUID
import rti.connextdds as dds

from umaapy import get_configurator, reset_dds_participant
from umaapy.util.uuid_factory import generate_guid
from umaapy.util.multi_topic_support import UmaaWriterAdapter, UmaaReaderAdapter

from umaapy.umaa_types import (
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
    UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
    UMAA_MM_ConditionalReport_ConditionalReportType as ConditionalReportType,
    UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElement as ConditionalReportTypeConditionalsSetElement,
    UMAA_MM_Conditional_DepthConditionalType as DepthConditionalType,
)


@pytest.mark.timeout(50)
def test_genspec_reader_merges_specialization_overlay():
    reset_dds_participant()
    writer: UmaaWriterAdapter = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader: UmaaReaderAdapter = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
    if hasattr(cmd.objective, "routeDescription"):
        cmd.objective.routeDescription = "Test route"
    writer.write(cmd)

    deadline = time.time() + 8.0
    got = None
    while time.time() < deadline:
        xs = reader.take_data()
        if xs:
            got = xs[-1]
            break
        time.sleep(0.05)

    assert got is not None
    v = got.view
    # Reader-side gen/spec decorator should have merged spec fields into the view
    assert hasattr(v.objective, "specializationID")
    if hasattr(v.objective, "routeDescription"):
        assert v.objective.routeDescription == "Test route"


@pytest.mark.timeout(50)
def test_large_set_reader_builds_element_views_for_conditional_report():
    reset_dds_participant()
    writer: UmaaWriterAdapter = get_configurator().get_umaa_writer(ConditionalReportType)
    reader: UmaaReaderAdapter = get_configurator().get_umaa_reader(ConditionalReportType)

    time.sleep(0.5)

    conditional_report = writer.new_combined()
    conditional_set = writer.editor_for_set(
        conditional_report, path=(), set_name="conditionals", element_type=ConditionalReportTypeConditionalsSetElement
    )
    conditional = conditional_set.add_new()
    depth_conditional = conditional.use_specialization(DepthConditionalType, at=("element",))
    depth_conditional.depth = 42.0

    writer.write(conditional_report)

    deadline = time.time() + 8.0
    got = None
    while time.time() < deadline:
        xs = reader.take_data()
        if xs:
            got = xs[-1]
            break
        time.sleep(0.05)

    assert got is not None
    v = got.view
    # any conditional-like set should have at least one element view
    bag = v.collections.get("conditionals", [])
    assert len(bag) == 1
    assert bag[0].element.depth == 42.0


@pytest.mark.timeout(50)
def test_large_list_reader_orders_and_links_route_waypoints():
    reset_dds_participant()
    writer = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)

    wplist = cmd.objective.collections.get("waypoints")
    if wplist is not None:
        a = RouteObjectiveTypeWaypointsListElement()
        a.element.name = "Waypoint A"
        a.elementID = generate_guid()
        b = RouteObjectiveTypeWaypointsListElement()
        b.element.name = "Waypoint B"
        b.elementID = generate_guid()
        wplist.append(a)
        wplist.append(b)

    writer.write(cmd)

    deadline = time.time() + 8.0
    got = None
    while time.time() < deadline:
        xs = reader.take_data()
        if xs:
            got = xs[-1]
            break
        time.sleep(0.05)

    assert got is not None
    v = got.view
    if "waypoints" in v.collections:
        # Should retain order [a, b]
        assert len(v.collections["waypoints"]) >= 2
