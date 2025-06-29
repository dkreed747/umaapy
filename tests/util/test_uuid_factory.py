from xml.dom.minidom import Identified
import pytest

from umaapy.util.uuid_factory import *


def test_guid_to_hex():
    guid_str: str = "00112233-4455-6677-8888-99AABBCCDDEE"
    hex_string = guid_to_hex(guid_from_string(guid_str))
    assert hex_string == "00 11 22 33 44 55 66 77 88 88 99 AA BB CC DD EE".lower()


def test_generate_guid():
    rand_id: UMAA_Common_Measurement_NumericGUID = generate_guid()
    assert NIL_GUID != rand_id


def test_guid_from_string():
    guid_str: str = "54455354-2047-5549-4420-202020202020"
    test_result: UMAA_Common_Measurement_NumericGUID = UMAA_Common_Measurement_NumericGUID(
        [0x54, 0x45, 0x53, 0x54, 0x20, 0x47, 0x55, 0x49, 0x44, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20]
    )
    guid: UMAA_Common_Measurement_NumericGUID = guid_from_string(guid_str)
    assert guid == test_result


def test_build_identifier_type():
    source_str: str = "54455354-2047-5549-4420-202020202020"
    parent_str: str = "00000000-0000-0000-0000-000000000000"

    identifier: UMAA_Common_IdentifierType = build_identifier_type(source_str, parent_str)

    assert guid_from_string(source_str) == identifier.id
    assert guid_from_string(parent_str) == identifier.parentID
