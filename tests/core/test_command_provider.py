import pytest
from typing import override
from time import sleep
import logging
import rti.connextdds as dds

from umaapy import configurator
from umaapy.core.command_provider import CommandProvider
from umaapy.util.event_processor import Command
from umaapy.util.uuid_factory import *
from umaapy.util.timestamp import Timestamp

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


def test_temp():
    source_id = build_identifier_type("cec418f0-32de-4aee-961d-9530e79869bd", "8ca7d105-5832-4a4b-bec2-a405ebd33e33")
    gvcsp = CommandProvider(
        source_id,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType,
        UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic,
        UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType,
        UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportTypeTopic,
    )
