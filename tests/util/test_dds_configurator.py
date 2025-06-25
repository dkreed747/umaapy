import pytest
from time import sleep
from umaapy.util.dds_configurator import DDSConfigurator
from importlib.resources import files


from umaapy.types import (
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportType,
    UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic,
)


class TestDDSConfiguratior:
    def test_load_singleton(self):
        qos_file = str(files("umaapy.resource") / "umaapy_qos_lib.xml")
        config_boy = DDSConfigurator(0, qos_file)
        gpr = config_boy.get_reader(
            UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic, UMAA_SA_GlobalPoseStatus_GlobalPoseReportType
        )
        assert len(config_boy.topics) > 0
        # Calling again will already know the topic
        config_boy = DDSConfigurator(0, "/workspace/umaapy/src/umaapy/resource/umaapy_qos_lib.xml")
        assert len(config_boy.topics) > 0

    def test_load_reader_writer(self):
        qos_file = str(files("umaapy.resource") / "umaapy_qos_lib.xml")
        config_boy = DDSConfigurator(0, qos_file)
        gpr_reader = config_boy.get_reader(
            UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic, UMAA_SA_GlobalPoseStatus_GlobalPoseReportType
        )
        gpr_writer = config_boy.get_writer(
            UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic, UMAA_SA_GlobalPoseStatus_GlobalPoseReportType
        )
        gpr_writer.write(UMAA_SA_GlobalPoseStatus_GlobalPoseReportType())
        sleep(1)
        assert len(gpr_reader.read()) > 0
