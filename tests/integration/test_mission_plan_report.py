import time
import pytest

import rti.connextdds as dds

from umaapy.util.umaa_utils import NumericGUID
from umaapy.util.uuid_factory import generate_guid
from umaapy.util.multi_topic_support import UmaaWriterAdapter, UmaaReaderAdapter

from umaapy.umaa_types import (
    UMAA_MM_MissionPlanReport_MissionPlanReportType as MissionPlanReportType,
    UMAA_MM_MissionPlanReport_MissionPlanReportTypeMissionPlanSetElement as MissionPlanReportTypeMissionPlanSetElement,
    UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElement as MissionPlanReportTypeConstraintsSetElement,
    UMAA_MM_BaseType_MissionPlanType as MissionPlanType,
    UMAA_MM_BaseType_MissionPlanTypeTaskPlansSetElement as MissionPlanTypeTaskPlansSetElement,
    UMAA_MM_BaseType_TaskPlanTypeObjectivesSetElement as TaskPlanTypeObjectivesSetElement,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
    UMAA_MM_BaseType_CircleObjectiveType as CircleObjectiveType,
    UMAA_MM_BaseType_DriftObjectiveType as DriftObjectiveType,
    UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
)

from umaapy import get_configurator, reset_dds_participant

pytestmark = pytest.mark.integration_vendor


@pytest.mark.timeout(30)
def test_roundtrip_mission_plan_report():
    reset_dds_participant()
    w: UmaaWriterAdapter = get_configurator().get_umaa_writer(MissionPlanReportType)
    r: UmaaReaderAdapter = get_configurator().get_umaa_reader(MissionPlanReportType)

    time.sleep(0.5)  # allow discovery

    mission_plan_report = w.new_combined()
    mission_plan = w.editor_for_set(
        mission_plan_report,
        path=(),
        set_name="missionPlan",
        element_type=MissionPlanReportTypeMissionPlanSetElement,
    )
    constraints = w.editor_for_set(
        mission_plan_report,
        path=(),
        set_name="constraints",
        element_type=MissionPlanReportTypeConstraintsSetElement,
    )

    constraints.add_new().element.name = "Keep in safe zone"
    constraints.add_new().element.name = "No fly below 50m depth"

    mission = mission_plan.add_new()
    mission.element.name = "My Mission"
    task_plan = w.editor_for_set(
        mission,
        path=(),
        set_name="taskPlans",
        element_type=MissionPlanTypeTaskPlansSetElement,
    )

    task = task_plan.add_new()
    task.element.name = "My Task"
    objectives = w.editor_for_set(
        task,
        path=("element",),
        set_name="objectives",
        element_type=TaskPlanTypeObjectivesSetElement,
    )

    ingress_eh = objectives.add_new()
    ingress = ingress_eh.use_specialization(RouteObjectiveType, at=("element",))
    circle = objectives.add_new().use_specialization(CircleObjectiveType, at=("element",))
    recovery = objectives.add_new().use_specialization(DriftObjectiveType, at=("element",))

    ingress.name = "Ingress Route"
    ingress_waypoints = w.editor_for_list(
        ingress_eh,
        path=("element",),
        list_name="waypoints",
        element_type=RouteObjectiveTypeWaypointsListElement,
    )

    for i in range(3):
        wp = ingress_waypoints.append_new()
        wp.name = f"Waypoint {i+1}"

    circle.name = "Circle an area"
    circle.radius = 50.0

    recovery.name = "Recovery Drift"
    recovery.driftRadius.distance = 25.0

    w.write(mission_plan_report)

    # Read back one combined sample
    deadline = time.time() + 8.0
    cs = None

    while time.time() < deadline:
        for cand in r.take_data():
            v = cand.view
            # Gate on the data you actually assert on:
            have_missions = len(v.collections.get("missionPlan", [])) >= 1
            have_constraints = len(v.collections.get("constraints", [])) >= 2
            if have_missions and have_constraints:
                cs = cand
        if cs:
            break
        time.sleep(0.05)

    assert cs is not None
    v = cs.view

    recv_missions = v.collections.get("missionPlan", [])
    recv_constraints = v.collections.get("constraints", [])
    assert len(recv_missions) == 1
    assert len(recv_constraints) == 2

    # Check missions
    recv_mission = recv_missions[0]
    assert recv_mission.name == "My Mission"

    # Check constraints
    recv_c1 = recv_constraints[0]
    recv_c2 = recv_constraints[1]
    assert recv_c1.name == "Keep in safe zone"
    assert recv_c2.name == "No fly below 50m depth"

    # Check tasks
    recv_tasks = recv_mission.collections.get("taskPlans", [])
    assert len(recv_tasks) == 1
    recv_task = recv_tasks[0]
    assert recv_task.name == "My Task"

    # Check objectives
    recv_objectives = recv_task.collections.get("objectives", [])
    assert len(recv_objectives) == 3
    recv_route = recv_objectives[0]
    recv_circle = recv_objectives[1]
    recv_drift = recv_objectives[2]
    assert recv_route.name == "Ingress Route"
    assert recv_circle.name == "Circle an area"
    assert recv_circle.radius == 50.0
    assert recv_drift.name == "Recovery Drift"
    assert recv_drift.driftRadius.distance == 25.0

    # Check route waypoints
    recv_route_waypoints = recv_route.collections.get("waypoints", [])
    assert len(recv_route_waypoints) == 3
    recv_wp1 = recv_route_waypoints[0]
    recv_wp2 = recv_route_waypoints[1]
    recv_wp3 = recv_route_waypoints[2]
    assert recv_wp1.name == "Waypoint 1"
    assert recv_wp2.name == "Waypoint 2"
    assert recv_wp3.name == "Waypoint 3"
