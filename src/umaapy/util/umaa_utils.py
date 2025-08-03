from typing import Any, Type, Sequence, List, Set, Dict, Tuple
import logging
import inspect
from enum import Enum, auto

_logger = logging.getLogger(__name__)


from umaapy.umaa_types import (
    UMAA_Common_Measurement_NumericGUID as NumericGUID,
    UMAA_Common_IdentifierType as IdentifierType,
)


class UMAAConcept(Enum):
    COMMAND = (auto(), ["timeStamp", "source", "destination", "sessionID"])
    ACKNOWLEDGEMENT = (auto(), ["timeStamp", "source", "sessionID", "command"])
    STATUS = (
        auto(),
        [
            "timeStamp",
            "source",
            "sessionID",
            "commandStatus",
            "commandStatusReason",
            "logMessage",
        ],
    )
    EXECUTION_STATUS = (auto(), ["timeStamp", "source", "sessionID"])
    REPORT = (auto(), ["timeStamp", "source"])
    GENERALIZATION = (auto(), ["specializationTopic", "specializationID", "specializationTimestamp"])
    SPECIALIZATION = (auto(), ["specializationReferenceID", "specializationReferenceTimestamp"])
    LARGE_SET = (auto(), ["setID", "updateElementID", "updateElementTimestamp", "size"])
    LARGE_SET_ELEMENT = (auto(), ["element", "setID", "elementID", "elementTimestamp"])
    LARGE_LIST = (auto(), ["listID", "updateElementID", "updateElementTimestamp", "startingElementID", "size"])
    LARGE_LIST_ELEMENT = (
        auto(),
        [
            "element",
            "listID",
            "elementID",
            "elementTimestamp",
            "nextElementID",
        ],
    )

    def __init__(self, numeric_value: int, attrs: List[str]):
        self.attrs = attrs


class HashableNumericGUID(NumericGUID):
    """
    A hashable wrapper for NumericGUID that allows instances to be used
    in hashed collections (e.g. as dict keys or set members).

    Inherits from NumericGUID and implements equality and hashing based
    on the GUID's raw value.
    """

    __slots__ = ()

    def __init__(self, base: NumericGUID):
        """
        Initialize a HashableNumericGUID from an existing NumericGUID.

        :param base: The NumericGUID instance to wrap.
        :type base: NumericGUID
        """
        super().__init__(value=base.value)

    def __eq__(self, other: Any) -> bool:
        """
        Compare two GUIDs for equality based on their tuple value.

        :param other: The object to compare against.
        :type other: Any
        :return: True if other is a NumericGUID with the same value;
                 NotImplemented if other isn't a NumericGUID.
        :rtype: bool
        """
        if not isinstance(other, NumericGUID):
            return NotImplemented
        return tuple(self.value) == tuple(other.value)

    def __hash__(self) -> int:
        """
        Compute a hash from the GUID's tuple value.

        :return: The hash of the underlying GUID tuple.
        :rtype: int
        """
        return hash(tuple(self.value))

    def to_umaa(self) -> NumericGUID:
        """
        Convert back to a standard (non-hashable) NumericGUID.

        :return: A new NumericGUID instance with the same value.
        :rtype: NumericGUID
        """
        return NumericGUID(value=self.value)


class HashableIdentifierType(IdentifierType):
    """
    A hashable wrapper for IdentifierType, making it usable in hashed
    collections by delegating to HashableNumericGUID for its IDs.

    Inherits from IdentifierType and implements equality and hashing.
    """

    __slots__ = ()

    def __init__(self, base: IdentifierType):
        """
        Initialize a HashableIdentifierType from an existing IdentifierType.

        :param base: The IdentifierType instance to wrap.
        :type base: IdentifierType
        """
        super().__init__(
            id=HashableNumericGUID(base.id),
            parentID=HashableNumericGUID(base.parentID),
        )

    def __eq__(self, other: Any) -> bool:
        """
        Compare two IdentifierTypes for equality based on their IDs.

        :param other: The object to compare against.
        :type other: Any
        :return: True if other is an IdentifierType with the same id
                 and parentID; NotImplemented if other isn't IdentifierType.
        :rtype: bool
        """
        if not isinstance(other, IdentifierType):
            return NotImplemented
        return self.id == other.id and self.parentID == other.parentID

    def __hash__(self) -> int:
        """
        Compute a hash from the tuple of this IdentifierType's id and parentID.

        :return: The hash of (id, parentID).
        :rtype: int
        """
        return hash((self.id, self.parentID))

    def to_umaa(self) -> IdentifierType:
        """
        Convert back to a standard (non-hashable) IdentifierType.

        :return: A new IdentifierType with the same id and parentID.
        :rtype: IdentifierType
        """
        return IdentifierType(
            id=self.id.to_umaa(),
            parentID=self.parentID.to_umaa(),
        )


def find_fields(
    obj: Any, fields: Sequence[str], verbose: bool = False, *, context: str = None, _visited: Set[int] = None
) -> Dict[str, Type]:
    """
    Recursively find all attributes whose names are in `fields`.
    Returns a list of attribute-access paths (dot-separated), e.g.
      ["MyType.field1.nestedField2", "MyType.other.nestedField2"]
    If nothing matches, returns [].

    :param obj:     The object or class to inspect.
    :param fields:  A sequence of attribute names to look for.
    :param verbose: If True, log a debug each time we find one.
    :param context: Internal—dot-path to this object (rooted at its type name).
    :param _visited: Internal—set of object ids to avoid infinite loops.
    """
    if _visited is None:
        _visited = set()
    oid = id(obj)
    if oid in _visited:
        return []
    _visited.add(oid)

    # initialize context to the root object's type name
    if context is None:
        context = "self"

    matches: Dict[str, Type] = {}
    if all(hasattr(obj, f) for f in fields):
        if verbose:
            _logger.debug(f"{context} has all required fields {fields}")
        matches[context] = type(obj)

    for name, val in inspect.getmembers(obj):
        if (
            name.startswith("_")
            or name.startswith("type_support")
            or isinstance(val, (str, bytes, int, float, bool, type(None)))
        ):
            continue
        full_path = f"{context}.{name}"
        matches.update(find_fields(val, fields, verbose, context=full_path, _visited=_visited))
    return matches


def validate_umaa_type(umaa_type: Any, concept: UMAAConcept, verbose: bool = False) -> bool:
    """
    Validate that the given object has the required fields for a UMAA special concept.

    :param umaa_type: An instance of a DDS UMAA data type.
    :type umaa_type: Any
    :param concept: UMAA Concept to validate against
    :type concept: UMAAConcept
    :param verbose: If True, log errors and debug info.
    :type verbose: bool
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    name = type(umaa_type).__name__

    for attr in concept.attrs:
        if not hasattr(umaa_type, attr):
            if verbose:
                _logger.error(f"'{name}' missing required '{attr}' field for a UMAA {concept.name}")
            return False

    if verbose:
        _logger.debug(f"'{name}' has all required fields for a UMAA {concept.name}")
    return True


def umaa_concepts_on_type(umaa_type: Type, verbose: bool = False) -> Dict[UMAAConcept, Dict[str, Type]]:
    results: Dict[UMAAConcept, Dict[str, Type]] = {
        concept: find_fields(umaa_type(), concept.attrs, verbose) for concept in UMAAConcept
    }

    for path in results[UMAAConcept.COMMAND]:
        results[UMAAConcept.EXECUTION_STATUS].pop(path)
        results[UMAAConcept.REPORT].pop(path)

    for path in results[UMAAConcept.EXECUTION_STATUS]:
        results[UMAAConcept.REPORT].pop(path)

    return results


def topic_from_type(umaa_type: Type) -> str:
    """
    Derive a DDS topic name from a UMAA type class by replacing underscores with '::'.

    :param umaa_type: The UMAA DDS type class.
    :type umaa_type: Type
    :return: Topic name string used in DDS filters.
    :rtype: str
    """
    # Convert C++-style nested names to :: separators
    return umaa_type.__name__.replace("_", "::")
