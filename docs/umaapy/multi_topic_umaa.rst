Multi-Topic UMAA
================

This guide explains UMAA multi-topic composition (generalization/specialization, Large Sets, and Large Lists) and shows how UMAAPy assembles and publishes these graphs.

Overview
--------

- **Generalization/Specialization**: A generalization field (e.g., ``objective``) points to a specialization topic (e.g., ``RouteObjectiveType``). UMAAPy automatically binds the specialization to the generalization when reading and writing.
- **Large Sets**: Unordered sets of elements with metadata. UMAAPy assembles elements into views and updates metadata when publishing.
- **Large Lists**: Ordered lists with linked elements. UMAAPy links ``nextElementID``, sets ``startingElementID``/``updateElementID``, and updates size; readers reconstruct the ordered chain.

Quick Start (Readers)
---------------------

.. code-block:: python

   from umaapy import get_configurator, reset_dds_participant
   from umaapy.umaa_types import (
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
   )

   reset_dds_participant()
   reader = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

   # Wait and take an assembled sample
   combined_samples = reader.take_data()
   for cs in combined_samples:
       v = cs.view
       # Access generalization and specialization fields transparently
       print(v.objective.name)
       # Access list elements
       for wp in v.objective.collections.get("waypoints", []):
           print(wp.element.name)

Quick Start (Writers)
---------------------

.. code-block:: python

   from umaapy import get_configurator, reset_dds_participant
   from umaapy.umaa_types import (
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
       UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
       UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement as RouteObjectiveTypeWaypointsListElement,
   )

   reset_dds_participant()
   w = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)

   cmd = w.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
   cmd.objective.name = "My objective inside a command"
   cmd.objective.routeDescription = "Test route description"

   route_waypoints = w.editor_for_list(
       cmd,
       path=("objective",),
       list_name="waypoints",
       element_type=RouteObjectiveTypeWaypointsListElement,
   )
   route_waypoints.append_new().element.name = "Waypoint 1"
   route_waypoints.append_new().element.name = "Waypoint 2"
   route_waypoints.append_new().element.name = "Waypoint 3"

   w.write(cmd)

Integration Test Use Cases
--------------------------

The following scenarios are covered by the integration tests and demonstrate end-to-end read/write of UMAA graphs:

- **Objective Executor Control** (lists + specialization): see `tests/integration/test_objective_executor_control.py`.
- **Mission Plan Report** (nested sets + lists): see `tests/integration/test_mission_plan_report.py`.
- **Conditional Report** (sets + specialization): see `tests/integration/test_conditional_report.py`.

API Reference Highlights
------------------------

- `umaapy.util.multi_topic_reader` — Readers, decorators (GenSpecReader, LargeSetReader, LargeListReader)
- `umaapy.util.multi_topic_writer` — Writers, decorators (GenSpecWriter, LargeSetWriter, LargeListWriter)
- `umaapy.util.multi_topic_support` — CombinedSample/CombinedBuilder, editors, and helpers
