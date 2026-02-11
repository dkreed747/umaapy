import pytest
import logging
from typing import override
from time import sleep
import logging
from threading import Condition

import rti.connextdds as dds

from umaapy.core.command_consumer import CommandConsumer
from umaapy.core.command_provider import CommandProvider

import umaapy.util.umaa_utils as utils
from umaapy.util.uuid_factory import build_identifier_type
from umaapy.util.umaa_command import UmaaCommand, UmaaCommandFactory
from umaapy import reset_dds_participant


from umaapy.umaa_types import (
    UMAA_Common_IdentifierType as IdentifierType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType as GlobalVectorCommandType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType as GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType as GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType as GlobalVectorExecutionStatusReportType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CommandStatusEnumType,
    UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType as CommandStatusReasonEnumType,
)

_logger = logging.getLogger(__name__)
pytestmark = pytest.mark.integration_vendor


class GlobalVectorControlCommand(UmaaCommand):
    def __init__(
        self,
        source: IdentifierType,
        command: GlobalVectorCommandType,
        logger: logging.Logger,
        ack_writer: dds.DataWriter,
        status_writer: dds.DataWriter,
        execution_status_writer: dds.DataWriter,
        complete_on_executing: bool,
    ):
        super().__init__(source, command, logger, ack_writer, status_writer, execution_status_writer)
        self._complete_on_executing: bool = complete_on_executing

    @override
    def on_executing(self):
        self._logger.info(f"Executing command")
        (pred, updated) = self.wait_for(lambda: self._complete_on_executing)


class GlobalVectorControlCommandFactory(UmaaCommandFactory):
    def __init__(self, ack_type, status_type, execution_status_type, complete_on_executing: bool) -> None:
        super().__init__(ack_type, status_type, execution_status_type)
        self._complete_on_executing: bool = complete_on_executing

    @override
    def build(self, command: GlobalVectorCommandType):
        return GlobalVectorControlCommand(
            self.source_id,
            command,
            self.logger,
            self._ack_writer,
            self._status_writer,
            self._execution_status_writer,
            self._complete_on_executing,
        )


def test_63_synchronous_command_execution():
    global_vector_provider = CommandProvider(
        build_identifier_type("476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorControlCommandFactory(
            GlobalVectorCommandAckReportType, GlobalVectorCommandStatusType, GlobalVectorExecutionStatusReportType, True
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

    sleep(1)
    test_provider = global_vector_consumer.get_providers()[0]
    cmd = GlobalVectorCommandType()
    command_session = global_vector_consumer.create_command_session(cmd, test_provider.source)

    (ok, status, reason, msg) = command_session.execute()
    assert ok
    assert status == CommandStatusEnumType.COMPLETED
    assert reason == CommandStatusReasonEnumType.SUCCEEDED

    reset_dds_participant()


def test_64_asynchronous_command_execution():
    global_vector_provider = CommandProvider(
        build_identifier_type("476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorControlCommandFactory(
            GlobalVectorCommandAckReportType,
            GlobalVectorCommandStatusType,
            GlobalVectorExecutionStatusReportType,
            False,
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

    def status_cb(status: GlobalVectorCommandStatusType):
        _logger.info(f"{status_dict[status.commandStatus]}")

    sleep(1)
    test_provider = global_vector_consumer.get_providers()[0]
    cmd = GlobalVectorCommandType()
    command_session = global_vector_consumer.create_command_session(cmd, test_provider.source)

    command_session.execute_async()
    sleep(1)
    assert command_session.state() == CommandStatusEnumType.EXECUTING

    command_session.cancel(block=True, timeout=10.0)

    assert command_session.state() is None

    reset_dds_participant()


def test_65_test_command_timeout():
    global_vector_provider = CommandProvider(
        build_identifier_type("476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorControlCommandFactory(
            GlobalVectorCommandAckReportType,
            GlobalVectorCommandStatusType,
            GlobalVectorExecutionStatusReportType,
            False,
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

    sleep(1)
    test_provider = global_vector_consumer.get_providers()[0]
    cmd = GlobalVectorCommandType()
    command_session = global_vector_consumer.create_command_session(cmd, test_provider.source)

    (ok, status, reason, msg) = command_session.execute(timeout=1.0)
    assert not ok
    assert status is None
    assert reason is None

    reset_dds_participant()


def test_66_consumer_callbacks():
    global_vector_provider = CommandProvider(
        build_identifier_type("476c6f62-616c-5665-6374-6f724374726c", "00000000-0000-0000-0000-000000000000"),
        GlobalVectorControlCommandFactory(
            GlobalVectorCommandAckReportType,
            GlobalVectorCommandStatusType,
            GlobalVectorExecutionStatusReportType,
            True,
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

    status_lock = Condition()
    times_called = 0
    umaa_command_flow = [
        CommandStatusEnumType.ISSUED,
        CommandStatusEnumType.COMMANDED,
        CommandStatusEnumType.EXECUTING,
        CommandStatusEnumType.COMPLETED,
    ]

    def status_cb(status: GlobalVectorCommandStatusType):
        nonlocal times_called, status_lock
        with status_lock:
            assert umaa_command_flow[times_called] == status.commandStatus
            times_called += 1
            if status.commandStatus == CommandStatusEnumType.COMPLETED:
                status_lock.notify_all()

    sleep(1)
    test_provider = global_vector_consumer.get_providers()[0]
    cmd = GlobalVectorCommandType()
    command_session = global_vector_consumer.create_command_session(cmd, test_provider.source)
    command_session.add_status_callback(CommandStatusEnumType.ISSUED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.COMMANDED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.EXECUTING, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.COMPLETED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.FAILED, status_cb)
    command_session.add_status_callback(CommandStatusEnumType.CANCELED, status_cb)

    with status_lock:
        command_session.execute_async()
        assert status_lock.wait(timeout=10.0)

    reset_dds_participant()
