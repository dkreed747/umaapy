import pytest
import time
import rti.connextdds as dds

from umaapy import get_configurator, reset_dds_participant
from umaapy.util.uuid_factory import generate_guid, generate_identifier_type
from umaapy.util.multi_topic_support import UmaaWriterAdapter, UmaaReaderAdapter

from umaapy.umaa_types import (
    UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType as ObjectiveExecutorCommandType,
    UMAA_MM_BaseType_RouteObjectiveType as RouteObjectiveType,
)

pytestmark = pytest.mark.integration_vendor


@pytest.mark.timeout(40)
def test_reader_builds_combined_sample_and_listener_fires():
    reset_dds_participant()
    writer: UmaaWriterAdapter = get_configurator().get_umaa_writer(ObjectiveExecutorCommandType)
    reader: UmaaReaderAdapter = get_configurator().get_umaa_reader(ObjectiveExecutorCommandType)

    time.sleep(0.5)

    cmd = writer.new_combined(spec_at=("objective",), spec_type=RouteObjectiveType)

    if hasattr(cmd, "sessionID"):
        cmd.sessionID = generate_guid()
    if hasattr(cmd, "destination"):
        cmd.destination = generate_identifier_type()
    if hasattr(cmd, "source"):
        cmd.source = generate_identifier_type()

    # specialization fields
    if hasattr(cmd.objective, "routeDescription"):
        cmd.objective.routeDescription = "Test route"

    # listener counter
    calls = {"data": 0}

    class MyListener(dds.NoOpDataReaderListener):
        def on_data_available(self, r):
            calls["data"] += 1

    reader.set_listener(MyListener(), dds.StatusMask.DATA_AVAILABLE)

    writer.write(cmd)

    # Wait for callback and data
    deadline = time.time() + 8.0
    got = None
    while time.time() < deadline:
        vs = reader.take_data()
        if vs:
            got = vs[-1]
            break
        time.sleep(0.05)

    assert calls["data"] >= 1, "on_data_available was not fired"
    assert got is not None

    v = got.view
    assert hasattr(v.objective, "specializationID")
    if hasattr(v.objective, "routeDescription"):
        assert v.objective.routeDescription == "Test route"
