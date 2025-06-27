from xml.dom.minidom import Identified
import pytest

from umaapy.util.uuid_factory import *


def test_generate_guid():
    rand_id: UMAA_Common_Measurement_NumericGUID = generate_guid()
    assert NIL_GUID != rand_id


def test_guid_from_string():
    guid_str: str = "54455354-2047-5549-4420-202020202020"
    test_result: UMAA_Common_Measurement_NumericGUID = UMAA_Common_Measurement_NumericGUID(
        [84, 69, 83, 84, 32, 71, 85, 73, 68, 32, 32, 32, 32, 32, 32, 32]
    )
    guid: UMAA_Common_Measurement_NumericGUID = guid_from_string(guid_str)
    assert guid == test_result


def test_build_identifier_type():
    source_str: str = "54455354-2047-5549-4420-202020202020"
    parent_str: str = "00000000-0000-0000-0000-000000000000"

    identifier: UMAA_Common_IdentifierType = build_identifier_type(source_str, parent_str)

    assert guid_from_string(source_str) == identifier.id
    assert guid_from_string(parent_str) == identifier.parentID
