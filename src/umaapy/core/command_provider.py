from typing import Any, Type, Optional
from uuid import UUID
import logging
from concurrent.futures import Future
import rti.connextdds as dds

from umaapy import get_configurator, get_event_processor
from umaapy.util.umaa_command import UmaaCommand, UmaaCommandFactory
from umaapy.util.event_processor import EventProcessor, LOW, MEDIUM, HIGH
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy.util.umaa_utils import validate_command
from umaapy.util.uuid_factory import guid_to_hex

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
)


class CommandProvider(dds.DataReaderListener):
    """
    A DDS DataReaderListener that listens for UMAA command topics, builds and submits
    UMAA commands to the event processor, and handles command updates and cancellations.

    :param source: Identifier of this command consumer's parent and instance IDs.
    :type source: UMAA_Common_IdentifierType
    :param cmd_factory: Factory to build UmaaCommand instances from raw DDS data.
    :type cmd_factory: UmaaCommandFactory
    :param cmd_type: The DDS command type class (e.g., SomeCommandType).
    :type cmd_type: Type
    :param cmd_priority: Priority at which commands are submitted to the event processor.
    :type cmd_priority: int
    :raises RuntimeError: If the provided cmd_type is not a valid UMAA command.
    """

    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        cmd_factory: UmaaCommandFactory,
        cmd_type: Type,
        cmd_priority: int = LOW,
    ):
        super().__init__()
        # Store the UMAA source identifier for filtering commands
        self._source_id: UMAA_Common_IdentifierType = source
        self._cmd_factory = cmd_factory
        self._cmd_priority = cmd_priority

        # Validate command type by instantiating and checking UMAA compliance
        if not validate_command(cmd_type()):
            raise RuntimeError(f"'{cmd_type.__name__.split('_')[-1]}' is not a valid UMAA command.")
        self._cmd_type: Type = cmd_type

        # Create a filtered DDS DataReader for this command type and source
        filter_expr = (
            f"destination.parentID = &hex({guid_to_hex(source.parentID)})"
            f" AND destination.id = &hex({guid_to_hex(source.id)})"
        )
        self._cmd_reader: dds.DataReader = get_configurator().get_filtered_reader(
            cmd_type,
            filter_expr,
            profile_category=UmaaQosProfileCategory.COMMAND,
        )

        # Active command tracking: the Future from the event processor and the command object
        self._active_command_future: Future = None
        self._active_command: Optional[UmaaCommand] = None

        # Setup logger and name for this provider
        self.name = self._cmd_type.__name__.split("CommandType")[0].split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")

        # Configure factory context and attach listener
        self._cmd_factory.source_id = self._source_id
        self._cmd_factory.logger = self._logger
        self._cmd_reader.set_listener(self, dds.StatusMask.ALL)
        self._logger.info(f"Initialized {self.name}...")

    def on_data_available(self, reader: dds.DataReader):
        """
        Callback when new data is available on the DDS reader. Builds new commands,
        updates existing ones, or handles disposals/cancellations.

        :param reader: The DDS DataReader that triggered this callback.
        :type reader: dds.DataReader
        """
        for data, info in reader.take():
            if info.valid:
                # New or updated command sample
                if self._active_command_future is None or self._active_command_future.done():
                    # Build and submit a fresh command
                    self._active_command = self._cmd_factory.build(data)
                    self._active_command_future = get_event_processor().submit(self._active_command, self._cmd_priority)
                else:
                    # If same session, update the running command
                    if self._active_command.command.sessionID == data.sessionID:
                        self._active_command.update(data)
                    else:
                        # Drop overlapping commands
                        self._logger.warning("New command received while busy executing another - dropping.")
            else:
                # Handle disposed or no-writer samples
                if self._active_command_future is None:
                    continue
                try:
                    keyed_sample = reader.key_value(info.instance_handle)
                    if self._active_command.command.sessionID != keyed_sample.sessionID:
                        continue
                except dds.InvalidArgumentError:
                    # Instance already reclaimed
                    self._logger.debug("Instance already reclaimed by DDS")
                # Cancel and cleanup based on instance state
                match info.state.instance_state:
                    case dds.InstanceState.NOT_ALIVE_DISPOSED:
                        self._active_command_future.cancel()
                        self._active_command.cancel()
                        self._active_command_future = None
                    case dds.InstanceState.NOT_ALIVE_NO_WRITERS:
                        self._active_command_future.cancel()
                        self._active_command.cancel()
                        self._active_command_future = None
                    case _:
                        self._logger.warning(f"Unhandled instance state received - {info.instance_state}")

    def on_liveliness_changed(self, reader: dds.DataReader, status: dds.LivelinessChangedStatus):
        """
        Debug hook for liveliness changes of the DataReader.
        """
        self._logger.debug("On liveliness changed triggered")
        self._logger.debug(f"{self.name} consumer liveliness count: {status.alive_count}")

    def on_requested_deadline_missed(self, reader: dds.DataReader, status: dds.RequestedDeadlineMissedStatus):
        """
        Hook for missed deadline events on the DataReader.
        """
        self._logger.debug("On requested deadline missed triggered")

    def on_requested_incompatible_qos(self, reader: dds.DataReader, status: dds.RequestedIncompatibleQosStatus):
        """
        Hook for incompatible QoS events on the DataReader.
        """
        self._logger.debug("On requested incompatible qos triggered")

    def on_sample_lost(self, reader: dds.DataReader, status: dds.SampleLostStatus):  # noqa
        """
        Hook for sample lost events on the DataReader.
        """
        self._logger.debug("On sample lost triggered")

    def on_sample_rejected(self, reader: dds.DataReader, status: dds.SampleRejectedStatus):
        """
        Hook for sample rejected events on the DataReader.
        """
        self._logger.debug("On sample rejected triggered")

    def on_subscription_matched(self, reader: dds.DataReader, status: dds.SubscriptionMatchedStatus):
        """
        Hook for subscription matched events on the DataReader.
        """
        self._logger.debug("On subscription matched triggered")
