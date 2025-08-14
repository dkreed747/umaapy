from typing import Any, Type, Sequence, List, Set, Dict, Tuple
import logging
import inspect
import importlib
import inspect
import re
from enum import Enum, auto
from dataclasses import dataclass, field
from collections import deque

_logger = logging.getLogger(__name__)


from umaapy.umaa_types import (
    UMAA_Common_Measurement_NumericGUID as NumericGUID,
    UMAA_Common_IdentifierType as IdentifierType,
)


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


class UMAAConcept(Enum):
    COMMAND = (auto(), {"timeStamp", "source", "destination", "sessionID"}, {"source", "destination", "sessionID"})
    ACKNOWLEDGEMENT = (auto(), {"timeStamp", "source", "sessionID", "command"}, {"source", "sessionID"})
    STATUS = (
        auto(),
        {
            "timeStamp",
            "source",
            "sessionID",
            "commandStatus",
            "commandStatusReason",
            "logMessage",
        },
        {"source", "sessionID"},
    )
    EXECUTION_STATUS = (auto(), {"timeStamp", "source", "sessionID"}, {"source", "sessionID"})
    REPORT = (auto(), {"timeStamp", "source"}, {"source"})
    GENERALIZATION = (auto(), {"specializationTopic", "specializationID", "specializationTimestamp"}, set())
    SPECIALIZATION = (
        auto(),
        {"specializationReferenceID", "specializationReferenceTimestamp"},
        {"specializationReferenceID"},
    )
    LARGE_SET = (auto(), {"setID", "updateElementID", "updateElementTimestamp", "size"}, set())
    LARGE_SET_ELEMENT = (auto(), {"element", "setID", "elementID", "elementTimestamp"}, {"setID", "elementID"})
    LARGE_LIST = (auto(), {"listID", "updateElementID", "updateElementTimestamp", "startingElementID", "size"}, set())
    LARGE_LIST_ELEMENT = (
        auto(),
        {
            "element",
            "listID",
            "elementID",
            "elementTimestamp",
            "nextElementID",
        },
        {"listID", "elementID"},
    )

    def __init__(self, _, attrs: Set[str], keys: Set[str]) -> None:
        self.attrs: Set[str] = attrs
        self.keys: Set[str] = keys


@dataclass
class UMAAFieldInfo:
    classifications: Set[UMAAConcept] = field(default_factory=set)
    python_type: Type[Any] = None


def validate_umaa_obj(obj: Any, concept: UMAAConcept) -> bool:
    """
    Validate that the given object has the required fields for a UMAA special concept.

    :param obj: An instance of a DDS UMAA data type.
    :type obj: Any
    :param concept: UMAA Concept to validate against
    :type concept: UMAAConcept
    :return: True if the object has all required fields, False otherwise.
    :rtype: bool
    """
    return concept.attrs.issubset(set(vars(obj).keys()))


def classify_obj_by_umaa(obj: Any) -> Dict[Tuple[str, ...], UMAAFieldInfo]:
    """
    Traverse `obj`’s instance‐attributes and at each path pick only the
    *most restrictive* UMAAConcept(s)—i.e. those whose attr_set is not
    a strict subset of any other matching concept’s attr_set.
    """
    cmap: Dict[Tuple[str, ...], UMAAFieldInfo] = {}
    seen_ids = set()
    queue = deque([((), obj)])

    while queue:
        path, current = queue.popleft()
        oid = id(current)
        if oid in seen_ids:
            continue
        seen_ids.add(oid)

        if not hasattr(current, "__dict__"):
            continue

        matched = [c for c in UMAAConcept if c.attrs.issubset(set(vars(current).keys()))]
        if matched:
            winners = {c for c in matched if not any(c.attrs.issubset(other.attrs) and c != other for other in matched)}
            cmap[path] = UMAAFieldInfo(classifications=winners, python_type=type(current))

        for name, val in vars(current).items():
            if hasattr(val, "__dict__"):
                queue.append((path + (name,), val))

    return cmap


def get_specializations_from_generalization(
    generalization: Type, module_name: str = "umaapy.umaa_types"
) -> Dict[str, Type]:
    """
    Scans `module_name` for all classes matching *generalization's* base-type
    (using your regex), then returns a dict mapping the short name (after the
    last underscore) to the actual class object.
    """
    if not validate_umaa_obj(generalization(), UMAAConcept.GENERALIZATION):
        raise RuntimeError(f"Invalid generalization type '{generalization.__name__}'")

    mod = importlib.import_module(module_name)
    base = generalization.__name__.split("_")[-1]
    regex = re.compile(rf"^UMAA_.+(?<!_){re.escape(base)}$")

    out: Dict[str, Type] = {}
    for name, cls in inspect.getmembers(mod, inspect.isclass):
        if cls.__module__ != module_name or not regex.match(name):
            continue

        if not validate_umaa_obj(cls(), UMAAConcept.SPECIALIZATION):
            raise RuntimeError(f"Invalid specialization type '{cls.__name__}'")

        short = name.split("_")[-1]
        out[short] = cls

    return out
