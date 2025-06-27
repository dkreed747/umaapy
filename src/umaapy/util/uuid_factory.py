import uuid
from typing import List

import rti.connextdds as dds
from umaapy.types import UMAA_Common_IdentifierType, UMAA_Common_Measurement_NumericGUID

NIL_GUID = UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq([0 for _ in range(16)]))


def generate_guid() -> UMAA_Common_Measurement_NumericGUID:
    return UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq(uuid.uuid4().bytes))


def guid_from_string(guid: str) -> UMAA_Common_Measurement_NumericGUID:
    return UMAA_Common_Measurement_NumericGUID(dds.Uint8Seq(uuid.UUID(guid).bytes))


def build_identifier_type(source_id: str, parent_id: str) -> UMAA_Common_IdentifierType:
    identifier = UMAA_Common_IdentifierType()
    identifier.id = guid_from_string(source_id)
    identifier.parentID = guid_from_string(parent_id)
    return identifier
