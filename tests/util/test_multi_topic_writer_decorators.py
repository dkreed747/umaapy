import pytest
import time
import rti.connextdds as dds

from umaapy import get_configurator
from umaapy.util.uuid_factory import generate_guid
from umaapy.util.multi_topic_support import UmaaWriterAdapter, UmaaReaderAdapter, OverlayView

from umaapy.umaa_types import (
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
    UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
)


@pytest.mark.timeout(50)
def test_genspec_writer_sets_ids_and_base_writes_after_children():
    writer = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
    writer.write(cmd)

    deadline = time.time() + 8.0
    xs = None
    while time.time() < deadline:
        xs = reader.take_data()
        if xs:
            break
        time.sleep(0.05)

    assert xs, "No samples received"
    cs = xs[-1]
    v = cs.view
    # specialization linkage present (writer filled it before base write)
    assert hasattr(v.objective, "specializationID")


@pytest.mark.timeout(50)
def test_large_list_writer_sets_metadata_visible_on_base_after_roundtrip():
    writer: UmaaWriterAdapter = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader: UmaaReaderAdapter = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    route_objective_executor_cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
    route_waypoints_list = writer.editor_for_list(
        route_objective_executor_cmd,
        path=("objective",),
        list_name="waypoints",
        element_type=RouteObjectiveTypeWaypointsListElement,
    )
    wp1 = route_waypoints_list.append_new()
    wp1.element.name = "Waypoint 1"
    wp2 = route_waypoints_list.append_new()
    wp2.element.name = "Waypoint 2"

    writer.write(route_objective_executor_cmd)

    # Read back one combined sample
    deadline = time.time() + 10.0
    cs = None
    while time.time() < deadline:
        vs = reader.take_data()
        if vs:
            cs = vs[-1]
            break
        time.sleep(0.05)

    assert cs is not None
    v: OverlayView = cs.view
    # Check that the waypoint list is present and has the correct elements
    print(f"Dump view: {v._collections.items()}")
    waypoints = v.objective.collections.get("waypoints", [])
    assert len(waypoints) == 2
    assert waypoints[0].element.name == "Waypoint 1"
    assert waypoints[1].element.name == "Waypoint 2"
