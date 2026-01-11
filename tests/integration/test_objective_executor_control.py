import time
import pytest

from umaapy.dds_backend import dds

from umaapy.util.umaa_utils import NumericGUID
from umaapy.util.uuid_factory import generate_guid

from umaapy.umaa_types import (
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
    UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
)

from umaapy import get_configurator, reset_dds_participant


@pytest.mark.timeout(30)
def test_roundtrip_objective_executor_command():
    reset_dds_participant()
    w = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    r = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)  # allow discovery

    route_objective_executor_cmd = w.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
    # Set fields on base command/generalization at objective level
    route_objective_executor_cmd.objective.name = "My objective inside a command"
    # Set fields on the specialization
    route_objective_executor_cmd.objective.routeDescription = "Test route description"
    # Setup a list editor for route waypoints
    route_waypoints_list = w.editor_for_list(
        route_objective_executor_cmd,
        path=("objective",),
        list_name="waypoints",
        element_type=RouteObjectiveTypeWaypointsListElement,
    )
    wp1 = route_waypoints_list.append_new()
    wp2 = route_waypoints_list.append_new()
    wp3 = route_waypoints_list.append_new()
    wp1.element.name = "Waypoint 1"
    wp2.element.name = "Waypoint 2"
    wp3.element.name = "Waypoint 3"

    # Sanity check: ensure the list is built correctly
    current_waypoints = route_objective_executor_cmd.objective.collections.get("waypoints", [])
    assert len(current_waypoints) == 3

    w.write(route_objective_executor_cmd)

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

    # Check that the command was written correctly
    assert hasattr(v.objective, "name")
    assert v.objective.name == "My objective inside a command"
    assert hasattr(v.objective, "routeDescription")
    assert v.objective.routeDescription == "Test route description"

    # Check that the waypoint list is present and has the correct elements
    waypoints = v.objective.collections.get("waypoints", [])
    assert len(waypoints) == 3
    assert waypoints[0].element.name == "Waypoint 1"
    assert waypoints[1].element.name == "Waypoint 2"
    assert waypoints[2].element.name == "Waypoint 3"
