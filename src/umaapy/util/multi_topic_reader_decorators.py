from __future__ import annotations

from typing import Any, Dict, Sequence, List, Optional, Tuple, Iterable

from umaapy.util.multi_topic_support import CombinedSample, get_at_path, guid_equal, guid_key
from umaapy.util.multi_topic_reader import ReaderDecorator, AssemblySignal, ReaderNode


class GenSpecReader(ReaderDecorator):
    """
    Path-aware UMAA Generalization/Specialization reader decorator.

    Parameters
    ----------
    attr_path : Sequence[str]
        Where the generalization object lives inside the base sample.
        Example: ('objective',) for ObjectiveExecutorControlCommandType.objective.

    Behavior
    --------
    - Matches generalization <-> specialization by (specializationTopic, specializationID[, specializationTimestamp]).
    - On match, registers an overlay (at `attr_path` if provided) so callers can do:
          combined.view.<path>.<specialization_fields>
      with specialization attributes taking precedence over base.
    """

    def __init__(self, attr_path: Sequence[str] = ()) -> None:
        super().__init__()
        self.attr_path: Tuple[str, ...] = tuple(attr_path)

        # Internal buffers keyed by *hashable* GUID keys
        # specID_k -> generalization object (at attr_path or top-level)
        self._gen_by_spec_id: Dict[Any, Any] = {}
        # topic -> { specID_k -> specialization object }
        self._spec_by_topic_id: Dict[str, Dict[Any, Any]] = {}
        # specID_k -> parent node assembly key
        self._parent_key_by_spec_id: Dict[Any, Any] = {}

        self.children: Dict[str, ReaderNode] = {}

    @staticmethod
    def _gen_binding(gen: Any) -> Tuple[str, Any, Optional[Any]]:
        """Return (topic, specID, specTS?) from a generalization object."""
        topic = getattr(gen, "specializationTopic")
        sid = getattr(gen, "specializationID")
        sts = getattr(gen, "specializationTimestamp", None)
        return topic, sid, sts

    @staticmethod
    def _spec_binding(spec: Any) -> Tuple[Any, Optional[Any]]:
        """Return (specID, specTS?) from a specialization object."""
        sid = getattr(spec, "specializationID")
        sts = getattr(spec, "specializationTimestamp", None)
        return sid, sts

    def on_reader_data(
        self, node: ReaderNode, key: Any, combined: CombinedSample, sample: Any
    ) -> Iterable[AssemblySignal]:
        # Locate generalization at path (or top-level)
        gen_obj = get_at_path(sample, self.attr_path) if self.attr_path else sample
        topic, sid, sts = self._gen_binding(gen_obj)
        sid_k = guid_key(sid)

        self._gen_by_spec_id[sid_k] = gen_obj
        self._parent_key_by_spec_id[sid_k] = key

        spec = self._spec_by_topic_id.get(topic, {}).get(sid_k)
        if spec is None:
            return ()

        ssid, ssts = self._spec_binding(spec)
        if guid_equal(ssid, sid) and (sts is None or ssts == sts):
            # Register overlay at path, update node cache, and emit completion
            new_comb = combined.add_overlay_at(self.attr_path, spec) if self.attr_path else combined.with_overlay(spec)
            node._combined_by_key[key] = new_comb
            return (AssemblySignal(key, complete=True),)

        return ()

    def on_child_assembled(
        self, node: ReaderNode, child_name: str, key: Any, assembled: CombinedSample
    ) -> Iterable[AssemblySignal]:
        # Child emitted a specialization sample
        spec = assembled.base
        sid, sts = self._spec_binding(spec)
        sid_k = guid_key(sid)

        # Cache specialization by topic+id
        bucket = self._spec_by_topic_id.setdefault(child_name, {})
        bucket[sid_k] = spec

        gen = self._gen_by_spec_id.get(sid_k)
        if gen is None:
            return ()

        topic, gid, gts = self._gen_binding(gen)
        if topic != child_name or not guid_equal(gid, sid) or (gts is not None and gts != sts):
            return ()

        parent_key = self._parent_key_by_spec_id.get(sid_k)
        if parent_key is None:
            return ()

        comb = node._combined_by_key.get(parent_key)
        if comb is None:
            return ()

        new_comb = comb.add_overlay_at(self.attr_path, spec) if self.attr_path else comb.with_overlay(spec)
        node._combined_by_key[parent_key] = new_comb
        return (AssemblySignal(parent_key, complete=True),)


class LargeSetReader(ReaderDecorator):
    """
    UMAA Large Set reader decorator.

    Parameters
    ----------
    set_name : str
        Logical attribute base name, e.g., 'conditionals' for 'conditionalsSetMetadata'.

    Expectations
    ------------
    Parent/base has:   <set_name>SetMetadata with fields: setID, updateElementID[, updateElementTimestamp]
    Element samples have: setID, elementID[, elementTimestamp]

    Behavior
    --------
    Completes when metadata.updateElementID (and optionally updateElementTimestamp) matches a buffered element.
    Emits a list of elements at combined.collections[set_name]. (Order is not defined for sets.)
    """

    def __init__(self, set_name: str) -> None:
        super().__init__()
        self.set_name = set_name

        # Buffers keyed by *hashable* GUID keys
        # setID_k -> { elemID_k -> element_obj }
        self._elems_by_set: Dict[Any, Dict[Any, Any]] = {}
        # setID_k -> parent/base sample (latest)
        self._meta_by_set: Dict[Any, Any] = {}
        # setID_k -> parent node assembly key
        self._parent_key_by_set: Dict[Any, Any] = {}

        self.children: Dict[str, ReaderNode] = {}

    def _meta_struct(self, parent_sample: Any) -> Any:
        return getattr(parent_sample, f"{self.set_name}SetMetadata")

    def _meta_ids(self, parent_sample: Any) -> Tuple[Any, Optional[Any], Optional[Any]]:
        m = self._meta_struct(parent_sample)
        set_id = getattr(m, "setID")
        upd_id = getattr(m, "updateElementID", None)
        upd_ts = getattr(m, "updateElementTimestamp", None)
        return set_id, upd_id, upd_ts

    @staticmethod
    def _elem_ids(elem: Any) -> Tuple[Any, Optional[Any], Optional[Any]]:
        set_id = getattr(elem, "setID")
        elem_id = getattr(elem, "elementID")
        elem_ts = getattr(elem, "elementTimestamp", None)
        return set_id, elem_id, elem_ts

    def on_reader_data(
        self, node: ReaderNode, key: Any, combined: CombinedSample, sample: Any
    ) -> Iterable[AssemblySignal]:
        set_id, upd_id, upd_ts = self._meta_ids(sample)
        set_id_k = guid_key(set_id)
        upd_id_k = guid_key(upd_id) if upd_id is not None else None

        self._meta_by_set[set_id_k] = sample
        self._parent_key_by_set[set_id_k] = key

        if upd_id_k is None:
            return ()

        bucket = self._elems_by_set.get(set_id_k, {})
        maybe_elem = bucket.get(upd_id_k)
        if maybe_elem is None:
            return ()

        # Optional timestamp gate
        _, _, elem_ts = self._elem_ids(maybe_elem)
        if upd_ts is not None and elem_ts != upd_ts:
            return ()

        # Assemble and emit
        coll = list(bucket.values())
        combined.collections[self.set_name] = coll
        node._combined_by_key[key] = combined
        return (AssemblySignal(key, complete=True),)

    def on_child_assembled(
        self, node: ReaderNode, child_name: str, key: Any, assembled: CombinedSample
    ) -> Iterable[AssemblySignal]:
        elem = assembled.base
        set_id, elem_id, elem_ts = self._elem_ids(elem)
        set_id_k = guid_key(set_id)
        elem_id_k = guid_key(elem_id)

        bucket = self._elems_by_set.setdefault(set_id_k, {})
        bucket[elem_id_k] = elem

        parent_sample = self._meta_by_set.get(set_id_k)
        if parent_sample is None:
            return ()

        _, upd_id, upd_ts = self._meta_ids(parent_sample)
        if upd_id is None:
            return ()

        # Gate on presence (and timestamp if provided)
        if not guid_equal(upd_id, elem_id):
            return ()
        if upd_ts is not None and elem_ts != upd_ts:
            return ()

        parent_key = self._parent_key_by_set.get(set_id_k)
        if parent_key is None:
            return ()

        comb = node._combined_by_key.get(parent_key)
        if comb is None:
            return ()

        coll = list(bucket.values())
        comb.collections[self.set_name] = coll
        node._combined_by_key[parent_key] = comb
        return (AssemblySignal(parent_key, complete=True),)


class LargeListReader(ReaderDecorator):
    """
    UMAA Large List reader decorator.

    Parameters
    ----------
    list_name : str
        Logical attribute base name, e.g., 'waypoints' for 'waypointsListMetadata'.

    Expectations
    ------------
    Parent/base has:   <list_name>ListMetadata with fields: listID, startingElementID?, updateElementID[, updateElementTimestamp]
    Element samples have: listID, elementID, nextElementID?, elementTimestamp?

    Behavior
    --------
    Completes when metadata.updateElementID (and optionally updateElementTimestamp) matches a buffered element.
    Emits an ordered list at combined.collections[list_name], built by following nextElementID
    starting from startingElementID. If startingElementID is missing, falls back to an insertion order.
    """

    def __init__(self, list_name: str) -> None:
        super().__init__()
        self.list_name = list_name

        # Buffers keyed by *hashable* GUID keys
        # listID_k -> { elemID_k -> element_obj }
        self._elems_by_list: Dict[Any, Dict[Any, Any]] = {}
        # listID_k -> parent/base sample (latest)
        self._meta_by_list: Dict[Any, Any] = {}
        # listID_k -> parent node assembly key
        self._parent_key_by_list: Dict[Any, Any] = {}

        self.children: Dict[str, ReaderNode] = {}

    def _meta_struct(self, parent_sample: Any) -> Any:
        return getattr(parent_sample, f"{self.list_name}ListMetadata")

    def _meta_ids(self, parent_sample: Any) -> Tuple[Any, Optional[Any], Optional[Any], Optional[Any]]:
        m = self._meta_struct(parent_sample)
        list_id = getattr(m, "listID")
        start_id = getattr(m, "startingElementID", None)
        upd_id = getattr(m, "updateElementID", None)
        upd_ts = getattr(m, "updateElementTimestamp", None)
        return list_id, start_id, upd_id, upd_ts

    @staticmethod
    def _elem_ids(elem: Any) -> Tuple[Any, Any, Optional[Any], Optional[Any]]:
        list_id = getattr(elem, "listID")
        elem_id = getattr(elem, "elementID")
        next_id = getattr(elem, "nextElementID", None)
        elem_ts = getattr(elem, "elementTimestamp", None)
        return list_id, elem_id, next_id, elem_ts

    def _ordered_chain(self, elems_by_id: Dict[Any, Any], start_k: Optional[Any]) -> List[Any]:
        """Build an ordered list following nextElementID, starting at start_k."""
        if not elems_by_id:
            return []

        if start_k is None:
            return list(elems_by_id.values())

        ordered: List[Any] = []
        cur = start_k
        visited = set()

        while cur is not None and cur in elems_by_id and cur not in visited:
            e = elems_by_id[cur]
            ordered.append(e)
            visited.add(cur)
            _, _, nxt, _ = self._elem_ids(e)
            cur = guid_key(nxt) if nxt is not None else None

        return ordered

    def on_reader_data(
        self, node: ReaderNode, key: Any, combined: CombinedSample, sample: Any
    ) -> Iterable[AssemblySignal]:
        list_id, start_id, upd_id, upd_ts = self._meta_ids(sample)
        list_id_k = guid_key(list_id)
        start_k = guid_key(start_id) if start_id is not None else None
        upd_k = guid_key(upd_id) if upd_id is not None else None

        self._meta_by_list[list_id_k] = sample
        self._parent_key_by_list[list_id_k] = key

        if upd_k is None:
            return ()

        bucket = self._elems_by_list.get(list_id_k, {})
        maybe = bucket.get(upd_k)
        if maybe is None:
            return ()

        # Optional timestamp gate
        _, _, _, ets = self._elem_ids(maybe)
        if upd_ts is not None and ets != upd_ts:
            return ()

        ordered = self._ordered_chain(bucket, start_k)
        combined.collections[self.list_name] = ordered
        node._combined_by_key[key] = combined
        return (AssemblySignal(key, complete=True),)

    def on_child_assembled(
        self, node: ReaderNode, child_name: str, key: Any, assembled: CombinedSample
    ) -> Iterable[AssemblySignal]:
        elem = assembled.base
        list_id, elem_id, next_id, elem_ts = self._elem_ids(elem)
        list_id_k = guid_key(list_id)
        elem_id_k = guid_key(elem_id)

        bucket = self._elems_by_list.setdefault(list_id_k, {})
        bucket[elem_id_k] = elem

        parent_sample = self._meta_by_list.get(list_id_k)
        if parent_sample is None:
            return ()

        _, start_id, upd_id, upd_ts = self._meta_ids(parent_sample)
        if upd_id is None:
            return ()

        if not guid_equal(upd_id, elem_id):
            return ()
        if upd_ts is not None and elem_ts != upd_ts:
            return ()

        parent_key = self._parent_key_by_list.get(list_id_k)
        if parent_key is None:
            return ()

        comb = node._combined_by_key.get(parent_key)
        if comb is None:
            return ()

        start_k = guid_key(start_id) if start_id is not None else None
        ordered = self._ordered_chain(bucket, start_k)
        comb.collections[self.list_name] = ordered
        node._combined_by_key[parent_key] = comb
        return (AssemblySignal(parent_key, complete=True),)
