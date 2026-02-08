import pytest
from typing import Optional
from time import sleep
import logging
import rti.connextdds as dds

from umaapy import get_configurator, reset_dds_participant
from umaapy.core.report_consumer import ReportConsumer, ReaderListenerEventType
from umaapy.util.event_processor import Command, HIGH
from umaapy.util.uuid_factory import *
from umaapy.util.timestamp import Timestamp

from umaapy.umaa_types import UMAA_SA_GlobalPoseStatus_GlobalPoseReportType as GlobalPoseReportType

pytestmark = pytest.mark.integration_vendor


def test_55_consumer_reads_filtered_reports_only():
    reset_dds_participant()
    test_report_source_id = build_identifier_type(
        "336062c9-54da-424f-9c67-2dd83c4e3e28", "4d388a62-91d0-4ceb-a386-c142178c97db"
    )

    test_different_source_id = build_identifier_type(
        "6f1ad659-e729-47af-8b50-d59b8811d901", "96db3ddd-e0f0-44b0-ac63-dc3128bf0eed"
    )

    test_nil_source_id = build_identifier_type(
        "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000000"
    )

    good_report, different_report, nil_report = GlobalPoseReportType(), GlobalPoseReportType(), GlobalPoseReportType()
    good_report.source = test_report_source_id
    different_report.source = test_different_source_id
    nil_report.source = test_nil_source_id

    report_count = 0

    def on_new_report(report: GlobalPoseReportType) -> None:
        nonlocal report_count
        report_count += 1

    global_pose_status_consumer = ReportConsumer([test_report_source_id], GlobalPoseReportType, HIGH)
    global_pose_status_consumer.add_report_callback(on_new_report)

    pose_writer = get_configurator().get_writer(GlobalPoseReportType)
    sleep(1)

    pose_writer.write(good_report)
    sleep(0.5)
    pose_writer.write(different_report)
    sleep(0.5)
    pose_writer.write(nil_report)
    sleep(0.5)

    global_pose_status_consumer.remove_report_callback(on_new_report)

    assert report_count == 1


def test_56_user_registers_report_callback():
    reset_dds_participant()
    test_report_source_id = build_identifier_type(
        "ad44c213-077d-47b6-9dd7-8c7a631bc3fe", "ed66f244-e680-47c6-88d1-13bb9252ec6c"
    )

    report = GlobalPoseReportType()
    report.source = test_report_source_id
    report.depth = 5
    report.altitudeASF = 15

    test_report: Optional[GlobalPoseReportType] = None

    def on_new_report(report: GlobalPoseReportType) -> None:
        nonlocal test_report
        test_report = report

    global_pose_status_consumer = ReportConsumer([test_report_source_id], GlobalPoseReportType, HIGH)
    global_pose_status_consumer.add_report_callback(on_new_report)

    pose_writer = get_configurator().get_writer(GlobalPoseReportType)
    sleep(1)

    pose_writer.write(report)
    sleep(1)

    assert test_report is not None
    assert test_report.depth == report.depth
    assert test_report.altitudeASF == report.altitudeASF


def test_57_user_registers_event_callback():
    reset_dds_participant()
    test_report_source_id = build_identifier_type(
        "f511c492-b143-442f-9a06-e245d1598173", "4119a344-c207-4221-a93a-b19094bdb8f1"
    )

    call_count = 0

    def on_sub_status(reader: dds.DataReader, status: dds.SubscriptionMatchedStatus) -> None:
        nonlocal call_count
        call_count += 1
        print(f"Current publishers: {status.current_count}")

    global_pose_status_consumer = ReportConsumer([test_report_source_id], GlobalPoseReportType, HIGH)
    global_pose_status_consumer.add_event_callback(ReaderListenerEventType.ON_SUBSCRIPTION_MATCHED, on_sub_status)

    pose_writer = get_configurator().get_writer(GlobalPoseReportType)
    sleep(1)
    pose_writer.close()
    sleep(1)

    assert call_count == 2
