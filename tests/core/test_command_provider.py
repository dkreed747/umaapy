import pytest
from typing import override
from time import sleep
import logging
import rti.connextdds as dds

from umaapy import configurator
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy.core.command_provider import CommandProvider
from umaapy.util.uuid_factory import *
from umaapy import event_processor

from umaapy.examples.global_vector_control import (
    _global_vector_control_source_id,
    _global_vector_control_command_factory,
)

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CmdStatus,
)

_logger = logging.getLogger(__name__)


class TestStatusListener(dds.DataReaderListener):
    def __init__(self):
        super().__init__()
        self.sample_list: List[str] = []

    @override
    def on_data_available(self, reader: dds.DataReader):
        for sample in reader.take_data():
            self.sample_list.append(sample.commandStatus)


global_vector_control_service_provider = CommandProvider(
    _global_vector_control_source_id,
    _global_vector_control_command_factory,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
)


def test_49_umaa_command_flow():
    test_status_flow = [
        CmdStatus.ISSUED,
        CmdStatus.COMMANDED,
        CmdStatus.EXECUTING,
        CmdStatus.ISSUED,
        CmdStatus.COMMANDED,
        CmdStatus.EXECUTING,
        CmdStatus.CANCELED,
    ]
    status_listener = TestStatusListener()

    test_cmd_writer = configurator.get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    test_ack_reader = configurator.get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader = configurator.get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader.set_listener(status_listener, dds.StatusMask.DATA_AVAILABLE)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    gv_cmd.destination = _global_vector_control_source_id

    sleep(0.5)
    test_cmd_writer.write(gv_cmd)
    sleep(0.5)
    test_cmd_writer.write(gv_cmd)
    sleep(0.5)
    ih = test_cmd_writer.lookup_instance(gv_cmd)
    test_cmd_writer.dispose_instance(ih)
    sleep(0.5)

    assert len(test_ack_reader.read_data()) > 0

    for status, test_status in zip(status_listener.sample_list, test_status_flow):
        assert status == test_status


def test_50_destination_content_filter():
    status_listener = TestStatusListener()

    test_cmd_writer = configurator.get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader = configurator.get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader.set_listener(status_listener, dds.StatusMask.DATA_AVAILABLE)

    sleep(0.5)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    test_cmd_writer.write(gv_cmd)

    sleep(1)

    assert len(status_listener.sample_list) == 0


def test_51_new_commands_added_to_thread_pool():
    test_cmd_writer = configurator.get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
        UmaaQosProfileCategory.COMMAND,
    )

    sleep(0.5)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    gv_cmd.destination = _global_vector_control_source_id
    test_cmd_writer.write(gv_cmd)

    assert event_processor.get_pending_task_count() == 0
