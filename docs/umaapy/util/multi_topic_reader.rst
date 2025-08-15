multi_topic_reader
==================

.. automodule:: umaapy.util.multi_topic_reader
   :members:
   :undoc-members:
   :show-inheritance:
   :autosummary:

Example
-------

.. code-block:: python

   from umaapy import get_configurator
   from umaapy.umaa_types import UMAA_MM_MissionPlanReport_MissionPlanReportType as MissionPlanReportType

   r = get_configurator().get_umaa_reader(MissionPlanReportType)
   samples = r.take_data()
   for cs in samples:
       v = cs.view
       for mp in v.collections.get("missionPlan", []):
           for tp in mp.element.collections.get("taskPlans", []):
               for obj in tp.element.collections.get("objectives", []):
                   print(getattr(obj, "name", "<no name>"))
