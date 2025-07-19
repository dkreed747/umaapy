import pytest
from time import sleep
from umaapy.util.dds_configurator import DDSConfigurator
from importlib.resources import files


from umaapy.umaa_types import (
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
)


def test_get_topic():
    qos_file = str(files("umaapy.resource") / "umaapy_qos_lib.xml")
    config_boy = DDSConfigurator(0, qos_file)

    topic = config_boy.get_topic(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType)

    config_boy = DDSConfigurator(0, qos_file)
    assert topic is not None


def test_load_reader_writer():
    qos_file = str(files("umaapy.resource") / "umaapy_qos_lib.xml")
    config_boy = DDSConfigurator(0, qos_file)
    gpr_reader = config_boy.get_reader(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType)
    gpr_writer = config_boy.get_writer(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType)
    gpr_writer.write(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType())
    sleep(1)
    assert len(gpr_reader.read()) > 0


def test_filtered_reader():
    qos_file = str(files("umaapy.resource") / "umaapy_qos_lib.xml")
    config_boy = DDSConfigurator(0, qos_file)
    gpr_filtered_reader = config_boy.get_filtered_reader(
        UMAA_SA_GlobalPoseStatus_GlobalPoseReportType, "depth = %0", ["42.0"]
    )
    gpr_writer = config_boy.get_writer(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType)

    test_report = UMAA_SA_GlobalPoseStatus_GlobalPoseReportType()
    gpr_writer.write(test_report)
    sleep(1)
    assert len(gpr_filtered_reader.take_data()) == 0
    test_report.depth = 42.0
    gpr_writer.write(test_report)
    sleep(1)
    assert len(gpr_filtered_reader.take_data()) > 0
