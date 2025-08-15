multi_topic_support
===================

.. automodule:: umaapy.util.multi_topic_support
   :members:
   :undoc-members:
   :show-inheritance:
   :autosummary:

Usage Examples
--------------

.. code-block:: python

   from umaapy import get_configurator, reset_dds_participant
   from umaapy.umaa_types import (
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
       UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
   )

   reset_dds_participant()
   w = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
   r = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

   cmd = w.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)
   cmd.objective.name = "My objective"
   w.write(cmd)

   for cs in r.take_data():
       print(cs.view.objective.name)
