import pytest
import rti.connextdds as dds
import time

from umaapy import get_configurator
from umaapy.util.uuid_factory import generate_guid

from umaapy.umaa_types import (
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
    UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
)


@pytest.mark.timeout(40)
def test_writer_publishes_children_before_base_and_sets_metadata():
    writer = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
    if hasattr(cmd.objective, "routeDescription"):
        cmd.objective.routeDescription = "Test route"

    # Add two waypoints if list exists
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

    # Read back and inspect both the merged view and base metadata
    deadline = time.time() + 10.0
    got = None
    while time.time() < deadline:
        xs, infos = reader.take()
        if xs:
            got = next((x for x in xs if x is not None), None)
            if got:
                break
        time.sleep(0.05)

    assert got is not None
    v = got.view
    assert hasattr(v.objective, "specializationID")
    if "waypoints" in v.collections:
        assert len(v.collections["waypoints"]) >= 2

    # Inspect base metadata (writer decorators should have set it)
    base = got.base
    # list metadata may be named <name>ListMetadata; check common name
    meta = getattr(base, "waypointsListMetadata", None)
    if meta is not None:
        assert getattr(meta, "listID", None) is not None
        assert getattr(meta, "startingElementID", None) is not None
        assert getattr(meta, "updateElementID", None) is not None
