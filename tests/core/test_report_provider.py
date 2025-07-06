import pytest
from typing import override
from time import sleep
import logging
import rti.connextdds as dds

from umaapy import configurator
from umaapy.core.report_provider import ReportProvider, WriterListenerEventType
from umaapy.util.event_processor import Command
from umaapy.util.uuid_factory import *
from umaapy.util.timestamp import Timestamp

from umaapy.umaa_types import (
    UMAA_Common_IdentifierType,
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
)

test_logger = logging.getLogger(f"{__file__.split("/")[-2]}")


class TestCommand(Command):
    executed = False

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    @override
    def execute(self):
        for k, val in self.kwargs.items():
            test_logger.info(f"{k}: {val}")
        TestCommand.executed = True


def test_46_provider_accepts_source_id():
    source_id = build_identifier_type("cec418f0-32de-4aee-961d-9530e79869bd", "8ca7d105-5832-4a4b-bec2-a405ebd33e33")

    gpr = ReportProvider(
        source_id,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
    )

    assert gpr._source_id == source_id


def test_47_send_report():
    source_id = build_identifier_type("cec418f0-32de-4aee-961d-9530e79869bd", "8ca7d105-5832-4a4b-bec2-a405ebd33e33")

    gpr = ReportProvider(
        source_id,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
    )

    now = Timestamp.now()
    sleep(0.5)

    test_reader = configurator.get_reader(
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportType, UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic
    )

    send_sample = UMAA_SA_GlobalPoseStatus_GlobalPoseReportType()
    send_sample.position.geodeticLatitude = 47.654
    send_sample.position.geodeticLongitude = -122.6079

    gpr.publish(send_sample)
    sleep(0.5)

    samples: List[UMAA_SA_GlobalPoseStatus_GlobalPoseReportType] = test_reader.take_data()
    assert len(samples) > 0

    sample = samples[0]
    assert sample.source == source_id
    assert Timestamp.from_umaa(sample.timeStamp) > now
    assert sample.position == send_sample.position


def test_48_writer_callbacks():
    source_id = build_identifier_type("cec418f0-32de-4aee-961d-9530e79869bd", "8ca7d105-5832-4a4b-bec2-a405ebd33e33")

    gpr = ReportProvider(
        source_id,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
    )

    gpr.add_event_callback(WriterListenerEventType.ON_PUBLICATION_MATCHED, TestCommand)
    assert not TestCommand.executed
    gpr.on_publication_matched(None, dds.PublicationMatchedStatus)
    sleep(1)
    assert TestCommand.executed
