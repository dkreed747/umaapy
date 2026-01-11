Getting Started
===============

Minimal Reader/Writer
---------------------

.. code-block:: python

   from umaapy import get_configurator, reset_dds_participant
   from umaapy.umaa_types import UMAA_SA_GlobalPoseStatus_GlobalPoseReportType as GlobalPoseReport

   reset_dds_participant()
   cfg = get_configurator()
   reader = cfg.get_reader(GlobalPoseReport)
   writer = cfg.get_writer(GlobalPoseReport)

   writer.write(GlobalPoseReport())
   data = list(reader.read_data())
   print(f"Received {len(data)} samples")

Minimal Report Provider/Consumer
--------------------------------

.. code-block:: python

   from umaapy import get_configurator, reset_dds_participant
   from umaapy.core.report_provider import ReportProvider
   from umaapy.core.report_consumer import ReportConsumer
   from umaapy.umaa_types import UMAA_SA_GlobalPoseStatus_GlobalPoseReportType as GlobalPoseReport
   from umaapy.util.uuid_factory import build_identifier_type

   reset_dds_participant()
   cfg = get_configurator()
   # Provider publishes reports with a source ID
   source_id = build_identifier_type("cec418f0-32de-4aee-961d-9530e79869bd", "8ca7d105-5832-4a4b-bec2-a405ebd33e33")
   provider = ReportProvider(source_id, GlobalPoseReport)
   # Consumer filters by source (only receives matching reports)
   consumer = ReportConsumer([source_id], GlobalPoseReport)
   # Option A: Event callbacks (recommended for streaming)
   def on_report(report):
       print("new report", type(report).__name__ if report else None)
   consumer.add_report_callback(on_report)
   # Option B: Poll the latest report when needed
   latest = consumer.get_latest_report()
   print("latest:", type(latest).__name__ if latest else None)

   # provider publishes a report
   report = GlobalPoseReport()
   provider.publish(report)

   # consumer reads back (callback triggers asynchronously). If polling:
   latest = consumer.get_latest_report()
   print("latest:", type(latest).__name__ if latest else None)

Minimal Command Provider/Consumer
---------------------------------

.. code-block:: python

   import time
   from umaapy import get_configurator, reset_dds_participant
   from umaapy.dds_backend import dds
   # Example command types below act as placeholders; replace with actual UMAA types
   from umaapy.umaa_types import (
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as CommandType,
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorAckType as AckType,
       UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStatusType as StatusType,
   )
   from umaapy.umaa_types import UMAA_Common_IdentifierType as IdentifierType
   from umaapy.core.command_consumer import CommandConsumer

   reset_dds_participant()
   cfg = get_configurator()

   # Minimal provider using the factory pattern
   from umaapy.util.umaa_command import UmaaCommandFactory, UmaaCommand
   class MyCommand(UmaaCommand):
       def on_executing(self):
           # Block until updated or canceled, or your predicate is met
           # If you return from on_executing, the framework marks the command COMPLETED
           # To fail the command, raise UmaaCommandException with a UMAA reason
           done, updated = self.wait_for(lambda: False, timeout=1.0)
           if updated:
               # handle update case (e.g., re-read self.command and continue)
               return
           # You could fail the command like this:
           # raise UmaaCommandException(CmdReason.SERVICE_FAILED, "bad input")

   class MyCommandFactory(UmaaCommandFactory):
       def build(self, command: CommandType):
           return MyCommand(self.source_id, command, self.logger, self._ack_writer, self._status_writer, self._execution_status_writer)

   from umaapy.core.command_provider import CommandProvider
   provider = CommandProvider(IdentifierType(), MyCommandFactory(AckType, StatusType, None), CommandType)

   # Consumer that can send commands and track status
   consumer = CommandConsumer(
       source_id=IdentifierType(),
       command_type=CommandType,
       ack_type=AckType,
       status_type=StatusType,
   )

   # Add a provider stub (in practice discovered automatically)
   provider_id = IdentifierType()
   consumer.add_provider(provider_id, name="Provider")

   # Send a command synchronously
   cmd = CommandType()
   ok, status, reason, msg = consumer.execute(cmd, provider_id, timeout=1.0, wait_for_terminal=False)
   print("sent:", ok)

   # Or asynchronously
   session = consumer.create_command_session(CommandType(), provider_id)
   session.execute_async()
   # ... later
   session.cancel(block=True)
