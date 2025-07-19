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
    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        cmd_factory: UmaaCommandFactory,
        cmd_type: Type,
        cmd_priority: int = LOW,
    ):
        super().__init__()
        self._source_id: UMAA_Common_IdentifierType = source
        self._cmd_factory = cmd_factory
        self._cmd_priority = cmd_priority

        if not validate_command(cmd_type()):
            raise RuntimeError(f"'{cmd_type.__name__.split("_")[-1]}' is not a valid UMAA command.")
        self._cmd_type: Type = cmd_type

        self._cmd_reader: dds.DataReader = get_configurator().get_filtered_reader(
            cmd_type,
            f"destination.parentID = &hex({guid_to_hex(source.parentID)}) AND destination.id = &hex({guid_to_hex(source.id)})",
            profile_category=UmaaQosProfileCategory.COMMAND,
        )

        self._active_command_future: Future = None
        self._active_command: Optional[UmaaCommand] = None

        self.name = self._cmd_type.__name__.split("CommandType")[0].split("_")[-1] + self.__class__.__name__
        self._logger = logging.getLogger(f"{self.name}")
        self._cmd_factory.source_id = self._source_id
        self._cmd_factory.logger = self._logger
        self._cmd_reader.set_listener(self, dds.StatusMask.ALL)
        self._logger.info(f"Initialized {self.name}...")

    def on_data_available(self, reader: dds.DataReader):
        for data, info in reader.take():
            if info.valid:
                if self._active_command_future is None or self._active_command_future.done():
                    self._active_command = self._cmd_factory.build(data)
                    self._active_command_future = get_event_processor().submit(self._active_command, self._cmd_priority)
                else:
                    if self._active_command.command.sessionID == data.sessionID:
                        self._active_command.update(data)
                    else:
                        self._logger.warning("New command received while busy executing another - dropping.")
            else:
                if self._active_command_future is None:
                    continue

                try:
                    keyed_sample = reader.key_value(info.instance_handle)
                    if self._active_command.command.sessionID != keyed_sample.sessionID:
                        continue
                except dds.InvalidArgumentError:
                    self._logger.debug("Instance already reclaimed by")

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
        self._logger.debug("On liveliness changed triggered")
        self._logger.debug(f"{self.name} consumer liveliness count: {status.alive_count}")

    def on_requested_deadline_missed(self, reader: dds.DataReader, status: dds.RequestedDeadlineMissedStatus):
        self._logger.debug("On requested deadline missed triggered")

    def on_requested_incompatible_qos(self, reader: dds.DataReader, status: dds.RequestedIncompatibleQosStatus):
        self._logger.debug("On requested incompatible qos triggered")

    def on_sample_lost(self, reader: dds.DataReader, status: dds.SampleLostStatus):
        self._logger.debug("On sample lost triggered")

    def on_sample_rejected(self, reader: dds.DataReader, status: dds.SampleRejectedStatus):
        self._logger.debug("On sample rejected triggered")

    def on_subscription_matched(self, reader: dds.DataReader, status: dds.SubscriptionMatchedStatus):
        self._logger.debug("On subscription matched triggered")
