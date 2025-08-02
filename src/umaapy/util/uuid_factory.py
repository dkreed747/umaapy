"""
Utility functions for GUID generation and conversion between UUIDs and UMAA NumericGUID types.

This module provides:
- A NIL_GUID constant representing an all-zero GUID.
- Converters between UMAA_Common_Measurement_NumericGUID and hex/string UUIDs.
- Builders for UMAA identifier types.
"""

import uuid
from typing import List, Optional, Tuple
from itertools import chain

import rti.connextdds as dds
from umaapy.umaa_types import UMAA_Common_IdentifierType, UMAA_Common_Measurement_NumericGUID


# Global constant for a nil (all zeros) GUID in UMAA NumericGUID format
NIL_GUID: UMAA_Common_Measurement_NumericGUID = UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq([0] * 16))


def guid_to_hex(guid: UMAA_Common_Measurement_NumericGUID) -> str:
    """
    Convert a UMAA NumericGUID to a lowercase hex string without separators.

    :param guid: UMAA NumericGUID instance containing 16 bytes.
    :type guid: UMAA_Common_Measurement_NumericGUID
    :return: Hexadecimal representation (e.g., 'aabbcc...').
    :rtype: str
    """
    # Join each byte formatted as two-digit hex
    return " ".join(f"{b:02x}" for b in guid.value)


def guid_pretty_print(guid: UMAA_Common_Measurement_NumericGUID) -> str:
    return str(uuid.UUID(bytes=bytes(guid.value)))


def guid_to_filter_params(*guids: UMAA_Common_Measurement_NumericGUID) -> List[int]:
    if not guids:
        raise ValueError("Must pass at least one GUID")

    return [str(b) for b in chain(*map(lambda guid: guid.value, guids))]


def make_filter_for_guid(*names: str) -> str:
    """
    Build a DDS filter expression for one or more GUID-like arrays.

    Each `name` will get clauses:
      name[0] = %N  …  name[15] = %M

    where the array index (inside the brackets) always restarts at 0,
    but the placeholder index (%) increments across all names.

    :param names: one or more field-names (e.g. "source", "dest", …)
    :param octet_count: how many octets per GUID-array (default 16)
    :returns: a single AND-joined filter string
    """
    if not names:
        raise ValueError("Must pass at least one GUID name")
    clauses: List[str] = []
    placeholder = 0
    for name in names:
        for idx in range(16):
            clauses.append(f"{name}[{idx}] = %{placeholder}")
            placeholder += 1
    return " AND ".join(clauses)


def generate_guid() -> UMAA_Common_Measurement_NumericGUID:
    """
    Generate a new random UUID4 and wrap it in a UMAA NumericGUID.

    :return: A UMAA NumericGUID representing a new UUID4.
    :rtype: UMAA_Common_Measurement_NumericGUID
    """
    # Generate Python UUID4, take raw bytes, and construct DDS sequence
    return UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq(uuid.uuid4().bytes))


def guid_from_string(guid_str: str) -> UMAA_Common_Measurement_NumericGUID:
    """
    Parse a standard UUID string into a UMAA NumericGUID.

    :param guid_str: UUID string (e.g., '550e8400-e29b-41d4-a716-446655440000').
    :type guid_str: str
    :return: A UMAA NumericGUID representing the parsed UUID.
    :rtype: UMAA_Common_Measurement_NumericGUID
    """
    # Use Python's UUID parsing and wrap bytes in DDS Uint8Seq
    py_uuid = uuid.UUID(guid_str)
    return UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq(py_uuid.bytes))


def guid_from_uuid(py_uuid: uuid.UUID) -> UMAA_Common_Measurement_NumericGUID:
    return UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq(py_uuid.bytes))


def build_identifier_type(source_id: str, parent_id: str) -> UMAA_Common_IdentifierType:
    """
    Construct a UMAA_Common_IdentifierType given string GUIDs for source and parent.

    :param source_id: Hex UUID string for this identifier's ID.
    :type source_id: str
    :param parent_id: Hex UUID string for this identifier's parent ID.
    :type parent_id: str
    :return: A populated UMAA_Common_IdentifierType.
    :rtype: UMAA_Common_IdentifierType
    """
    # Initialize empty identifier object
    identifier = UMAA_Common_IdentifierType()
    # Parse and assign each GUID
    identifier.id = guid_from_string(source_id)
    identifier.parentID = guid_from_string(parent_id)
    return identifier
