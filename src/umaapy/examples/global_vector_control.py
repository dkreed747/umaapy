import logging
import rti.connextdds as dds
from typing import Any, override

from umaapy.core.command_provider import CommandProvider
from umaapy.util.umaa_command import UmaaCommand, UmaaCommandFactory
from umaapy.util.uuid_factory import build_identifier_type, guid_to_hex

from umaapy.types import (
    UMAA_Common_IdentifierType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportTypeTopic,
)


class GlobalVectorControlCommand(UmaaCommand):
    def __init__(
        self,
        source: UMAA_Common_IdentifierType,
        command: UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        logger: logging.Logger,
        ack_writer: dds.DataWriter,
        status_writer: dds.DataWriter,
        execution_status_writer: dds.DataWriter,
    ):
        super().__init__(source, command, logger, ack_writer, status_writer, execution_status_writer)

    @override
    def on_commanded(self):
        self._logger.info(f"Overloaded commanded!")

    @override
    def on_executing(self):
        self._logger.info(f"Executing command")
        (pred, updated) = self.wait_for(lambda: False)
        if updated:
            self._logger.info("Vector command updated!")
        else:
            self._logger.info("Vector command canceled :(")

    @override
    def on_terminal(self):
        self._logger.info("Vector command is terminal")


class GlobalVectorControlCommandFactory(UmaaCommandFactory):
    @override
    def build(self, command: UMAA_MO_GlobalVectorControl_GlobalVectorCommandType):
        return GlobalVectorControlCommand(
            self.source_id, command, self.logger, self._ack_writer, self._status_writer, self._execution_status_writer
        )


_global_vector_control_source_id = build_identifier_type(
    "476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"
)

_global_vector_control_command_factory = GlobalVectorControlCommandFactory(
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportTypeTopic,
)
