multi_topic_writer
==================

.. automodule:: umaapy.util.multi_topic_writer
   :members:
   :undoc-members:
   :show-inheritance:
   :autosummary:

Example
-------

.. code-block:: python

   from umaapy import get_configurator
   from umaapy.umaa_types import (
       UMAA_MM_MissionPlanReport_MissionPlanReportType as MissionPlanReportType,
       UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElement as ConstraintsElem,
   )

   w = get_configurator().get_umaa_writer(MissionPlanReportType)
   builder = w.new()
   constraints = builder.ensure_collection_at("constraints", "set")
   elem = ConstraintsElem()
   constraints.add(elem)
   w.write(builder)
