import pytest
import logging
from typing import override
from time import sleep
import logging

import rti.connextdds as dds

from umaapy.core.command_consumer import CommandConsumer
from umaapy.core.command_provider import CommandProvider

import umaapy.util.umaa_utils as utils
from umaapy.util.uuid_factory import build_identifier_type
from umaapy.util.umaa_command import UmaaCommand, UmaaCommandFactory

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType as IdentifierType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType as GlobalVectorCommandType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType as GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType as GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType as GlobalVectorExecutionStatusReportType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CommandStatusEnumType,
)


_logger = logging.getLogger(__name__)


class GlobalVectorControlCommand(UmaaCommand):
    def __init__(
        self,
        source: IdentifierType,
        command: GlobalVectorCommandType,
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
    def build(self, command: GlobalVectorCommandType):
        return GlobalVectorControlCommand(
            self.source_id, command, self.logger, self._ack_writer, self._status_writer, self._execution_status_writer
        )


def test_test():
    global_vector_provider = CommandProvider(
        build_identifier_type("476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorControlCommandFactory(
            GlobalVectorCommandAckReportType, GlobalVectorCommandStatusType, GlobalVectorExecutionStatusReportType
        ),
        GlobalVectorCommandType,
    )

    global_vector_consumer = CommandConsumer(
        build_identifier_type("9c069109-2a86-4f5d-aca3-b22faf8695b5", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorCommandType,
        GlobalVectorCommandAckReportType,
        GlobalVectorCommandStatusType,
        GlobalVectorExecutionStatusReportType,
    )

    sleep(2)

    test_provider = global_vector_consumer.get_providers()[0]
    test_provider.name = "UnitTestVectorControlProvider"
    # _logger.info(f"Provider ID: {test_provider.source.id}")

    cmd = GlobalVectorCommandType()
    command_session = global_vector_consumer.create_command_session(cmd, test_provider.source)

    def status_cb(status: CommandStatusEnumType):
        _logger.info(f"We got status! {status.commandStatus}")

    command_session.add_status_callback(CommandStatusEnumType.ISSUED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.COMMANDED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.EXECUTING, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.COMPLETED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.FAILED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.CANCELED, status_cb)

    sleep(2)

    command_session.execute_async()

    sleep(3)

    command_session.cancel(True, 5.0)

    _logger.info(global_vector_consumer._ack_cft.filter_expression)
