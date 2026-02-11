import pytest
from typing import override
from time import sleep
import logging
import rti.connextdds as dds

from umaapy import get_configurator, get_event_processor, reset_dds_participant
from umaapy.util.dds_configurator import UmaaQosProfileCategory
from umaapy.core.command_provider import CommandProvider
from umaapy.util.uuid_factory import *

from umaapy.examples.global_vector_control import GlobalVectorControlCommandFactory, _global_vector_control_source_id

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
    UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
    UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType as CmdStatus,
)

_logger = logging.getLogger(__name__)
pytestmark = pytest.mark.integration_vendor


class TestStatusListener(dds.DataReaderListener):
    def __init__(self):
        super().__init__()
        self.sample_list: List[str] = []

    @override
    def on_data_available(self, reader: dds.DataReader):
        for sample in reader.take_data():
            self.sample_list.append(sample.commandStatus)


def test_49_umaa_command_flow():
    reset_dds_participant()

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

    GlobalVectorControlCommandFactory(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    )

    global_vector_control_service_provider = CommandProvider(
        _global_vector_control_source_id,
        GlobalVectorControlCommandFactory(
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
            UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
        ),
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    )

    test_cmd_writer = get_configurator().get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    test_ack_reader = get_configurator().get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader = get_configurator().get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader.set_listener(status_listener, dds.StatusMask.DATA_AVAILABLE)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    gv_cmd.destination = _global_vector_control_source_id
    sleep(2.0)

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
    reset_dds_participant()
    sleep(1)
    status_listener = TestStatusListener()

    GlobalVectorControlCommandFactory(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    )

    global_vector_control_service_provider = CommandProvider(
        _global_vector_control_source_id,
        GlobalVectorControlCommandFactory(
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
            UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
        ),
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    )

    test_cmd_writer = get_configurator().get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader = get_configurator().get_reader(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    test_status_reader.set_listener(status_listener, dds.StatusMask.DATA_AVAILABLE)

    sleep(0.5)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    test_cmd_writer.write(gv_cmd)

    sleep(1)

    assert len(status_listener.sample_list) == 0


def test_51_new_commands_added_to_thread_pool():
    reset_dds_participant()
    sleep(1)

    GlobalVectorControlCommandFactory(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
    )

    global_vector_control_service_provider = CommandProvider(
        _global_vector_control_source_id,
        GlobalVectorControlCommandFactory(
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
            UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
            UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
        ),
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
    )

    test_cmd_writer = get_configurator().get_writer(
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        profile_category=UmaaQosProfileCategory.COMMAND,
    )

    sleep(0.5)

    gv_cmd = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType()
    gv_cmd.destination = _global_vector_control_source_id
    test_cmd_writer.write(gv_cmd)

    assert get_event_processor().get_pending_task_count() == 0
