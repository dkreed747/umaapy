"""
Reader-side UMAA decorators:

- :class:`GenSpecReader` — path-aware generalization/specialization merge (UMAA 3.9).
- :class:`LargeSetReader` — assemble unordered sets (UMAA 3.8).
- :class:`LargeListReader` — assemble ordered lists with next-element linking (UMAA 3.8).

All internally handle UMAA NumericGUID keys safely via :func:`guid_key` / :func:`guid_equal`.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import logging
import inspect

from umaapy.util.multi_topic_support import (
    CombinedSample,
    ElementView,
    get_at_path,
)

from umaapy.util.umaa_utils import (
    guid_key,
    guid_equal,
    path_for_set_element,
    path_for_list_element,
)

from umaapy.util.multi_topic_reader import ReaderDecorator, AssemblySignal, ReaderNode

_logger = logging.getLogger(__name__)


class GenSpecReader(ReaderDecorator):
    """
    Path-aware UMAA Generalization/Specialization reader.

    Parameters
    ----------
    attr_path : Sequence[str], optional
        Path of the generalization object inside the base sample
        (e.g., ``('objective',)``). Defaults to top-level.
    """

    def __init__(self, attr_path: Sequence[str] = ()) -> None:
        super().__init__()
        self.attr_path: Tuple[str, ...] = tuple(attr_path)
        self.children: Dict[str, ReaderNode] = {}
        # specID_k -> generalization object
        self._gen_by_spec_id: Dict[Any, Any] = {}
        # topic -> { specID_k -> specialization object }
        self._spec_by_topic_id: Dict[str, Dict[Any, Any]] = {}
        # specID_k -> parent key
        self._parent_key_by_spec_id: Dict[Any, Any] = {}
        # specID_k -> child's CombinedSample (to harvest nested collections on late generalization)
        self._child_comb_by_spec_id: Dict[Any, CombinedSample] = {}

    @staticmethod
    def _gen_binding(gen: Any) -> Tuple[str, Any, Optional[Any]]:
        topic = getattr(gen, "specializationTopic")
        sid = getattr(gen, "specializationID")
        sts = getattr(gen, "specializationTimestamp")
        return topic, sid, sts

    @staticmethod
    def _spec_binding(spec: Any) -> Tuple[Any, Optional[Any]]:
        sid = getattr(spec, "specializationReferenceID")
        sts = getattr(spec, "specializationReferenceTimestamp")
        return sid, sts

    def on_reader_data(
        self, node: ReaderNode, key: Any, combined: CombinedSample, sample: Any
    ) -> Iterable[AssemblySignal]:
        _logger.debug(f"Received new {node.reader.type_name}")

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
            # Merge any nested collections we captured from the child specialization
            child_comb = self._child_comb_by_spec_id.get(sid_k)
            try:
                if child_comb and getattr(child_comb, "collections", None):
                    for cname, cval in child_comb.collections.items():
                        combined.collections[cname] = cval
            except Exception:
                pass
            new_comb = combined.add_overlay_at(spec, self.attr_path)
            node._combined_by_key[key] = new_comb
            return (AssemblySignal(key, complete=True),)
        return ()

    def on_child_assembled(
        self, node: ReaderNode, child_name: str, key: Any, assembled: CombinedSample
    ) -> Iterable[AssemblySignal]:
        _logger.debug(f"Received new {node.reader.type_name}")
        try:
            _logger.debug(f"[GenSpecReader] child '{child_name}' collections keys={list(assembled.collections.keys())}")
        except Exception:
            pass
        spec = assembled.base
        sid, sts = self._spec_binding(spec)
        sid_k = guid_key(sid)

        bucket = self._spec_by_topic_id.setdefault(child_name, {})
        bucket[sid_k] = spec
        # Remember child's combined to later propagate its collections when gen arrives first
        self._child_comb_by_spec_id[sid_k] = assembled

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

        # Propagate any nested collections (e.g., lists/sets under the specialization)
        try:
            if hasattr(assembled, "collections") and assembled.collections:
                for cname, cval in assembled.collections.items():
                    comb.collections[cname] = cval
        except Exception:
            pass

        new_comb = comb.add_overlay_at(spec, self.attr_path)
        node._combined_by_key[parent_key] = new_comb
        return (AssemblySignal(parent_key, complete=True),)


class LargeSetReader(ReaderDecorator):
    def __init__(self, set_name: str, attr_path: Sequence[str] = ()) -> None:
        super().__init__()
        self.set_name = set_name
        self.attr_path: Tuple[str, ...] = tuple(attr_path)

        self._elems_by_set: Dict[Any, Dict[Any, Any]] = {}
        self._meta_by_set: Dict[Any, Any] = {}
        self._parent_key_by_set: Dict[Any, Any] = {}
        self._elem_combined_by_set: Dict[Any, Dict[Any, CombinedSample]] = {}

    def _meta_struct(self, parent_sample: Any) -> Any:
        return get_at_path(parent_sample, self.attr_path)

    def _meta_ids(self, parent_sample: Any) -> Tuple[Any, Optional[Any], Optional[Any]]:
        m = self._meta_struct(parent_sample)
        return getattr(m, "setID"), getattr(m, "updateElementID"), getattr(m, "updateElementTimestamp", None)

    def _set_size(self, parent_sample: Any) -> int:
        m = self._meta_struct(parent_sample)
        return getattr(m, "size", 0)

    def _elem_path(self, elem_id_k: Any) -> tuple:
        # Element nodes are addressed by their set element token only; not under metadata path
        return tuple(path_for_set_element(self.set_name, elem_id_k))

    @staticmethod
    def _elem_ids(elem: Any) -> Tuple[Any, Any, Optional[Any]]:
        return getattr(elem, "setID"), getattr(elem, "elementID"), getattr(elem, "elementTimestamp")

    def on_reader_data(self, node, key, combined, sample):
        _logger.debug(f"[LargeSetReader:{self.set_name}] on_reader_data: sample={type(sample).__name__}")
        set_id, upd_id, upd_ts = self._meta_ids(sample)
        size = self._set_size(sample)

        set_id_k = guid_key(set_id)
        upd_id_k = guid_key(upd_id) if upd_id is not None else None

        self._meta_by_set[set_id_k] = sample
        self._parent_key_by_set[set_id_k] = key

        bucket = self._elems_by_set.get(set_id_k, {})
        comb_bucket = self._elem_combined_by_set.get(set_id_k, {})

        if size == 0:
            # Treat zero-size as truly empty only when no elements have been observed
            if not bucket:
                combined.collections[self.set_name] = []
                node._combined_by_key[key] = combined
                _logger.debug(f"[LargeSetReader:{self.set_name}] complete empty (no elements)")
                return (AssemblySignal(key, complete=True),)

        if size > 0:
            if len(bucket) < size:
                return ()
        else:
            if upd_id_k is None or upd_id_k not in bucket:
                return ()
            _, _, elem_ts = self._elem_ids(bucket[upd_id_k])
            if upd_ts is not None and elem_ts != upd_ts:
                return ()

        views: List[ElementView] = []
        for eid_k, e in bucket.items():
            elem_path = self._elem_path(eid_k)
            child_comb = comb_bucket.get(eid_k)
            if child_comb and child_comb.overlays_by_path:
                for k2, v2 in child_comb.overlays_by_path.items():
                    combined.overlays_by_path[elem_path + k2] = v2
            # Propagate child collections (e.g., nested sets/lists) to the parent combined
            if child_comb and getattr(child_comb, "collections", None):
                for cname, cval in child_comb.collections.items():
                    combined.collections[cname] = cval
            views.append(ElementView(combined, e, elem_path))

        combined.collections[self.set_name] = views
        node._combined_by_key[key] = combined
        _logger.debug(f"[LargeSetReader:{self.set_name}] complete size>0 with {len(views)} elements (size={size})")
        return (AssemblySignal(key, complete=True),)

    def on_child_assembled(self, node, child_name, key, assembled):
        _logger.debug(
            f"[LargeSetReader:{self.set_name}] on_child_assembled: collections={list(assembled.collections.keys())}"
        )
        elem = assembled.base
        set_id, elem_id, elem_ts = self._elem_ids(elem)

        set_id_k = guid_key(set_id)
        elem_id_k = guid_key(elem_id)

        bucket = self._elems_by_set.setdefault(set_id_k, {})
        bucket[elem_id_k] = elem
        comb_bucket = self._elem_combined_by_set.setdefault(set_id_k, {})
        comb_bucket[elem_id_k] = assembled

        parent_sample = self._meta_by_set.get(set_id_k)
        if parent_sample is None:
            return ()

        size = self._set_size(parent_sample)
        _, upd_id, upd_ts = self._meta_ids(parent_sample)

        parent_key = self._parent_key_by_set.get(set_id_k)
        if parent_key is None:
            return ()

        comb = node._combined_by_key.get(parent_key)
        if comb is None:
            return ()

        if size == 0:
            # Complete as empty only if we have no elements recorded
            if not bucket:
                comb.collections[self.set_name] = []
                node._combined_by_key[parent_key] = comb
                _logger.debug(f"[LargeSetReader:{self.set_name}] complete empty on child (no elements)")
                return (AssemblySignal(parent_key, complete=True),)

        if size > 0:
            if len(bucket) < size:
                return ()
        else:
            if upd_id is None or not guid_equal(upd_id, elem_id):
                return ()
            if upd_ts is not None and elem_ts != upd_ts:
                return ()

        views: List[ElementView] = []
        for eid_k, e in bucket.items():
            elem_path = self._elem_path(eid_k)
            child_comb = comb_bucket.get(eid_k)
            if child_comb:
                for k2, v2 in child_comb.overlays_by_path.items():
                    comb.overlays_by_path[elem_path + k2] = v2
                # Propagate child collections (e.g., nested sets/lists) upward
                if getattr(child_comb, "collections", None):
                    for cname, cval in child_comb.collections.items():
                        comb.collections[cname] = cval
            views.append(ElementView(comb, e, elem_path))

        comb.collections[self.set_name] = views
        node._combined_by_key[parent_key] = comb
        _logger.debug(f"[LargeSetReader:{self.set_name}] complete on child with {len(views)} elements (size={size})")
        return (AssemblySignal(parent_key, complete=True),)


class LargeListReader(ReaderDecorator):
    def __init__(self, list_name: str, attr_path: Sequence[str] = ()) -> None:
        super().__init__()
        self.list_name = list_name
        self.attr_path: Tuple[str, ...] = tuple(attr_path)
        self._elems_by_list: Dict[Any, Dict[Any, Any]] = {}
        self._meta_by_list: Dict[Any, Any] = {}
        self._parent_key_by_list: Dict[Any, Any] = {}
        self._elem_combined_by_list: Dict[Any, Dict[Any, CombinedSample]] = {}

    def _meta_struct(self, parent_sample: Any) -> Any:
        return get_at_path(parent_sample, self.attr_path)

    def _meta_ids(self, parent_sample: Any) -> Tuple[Any, Optional[Any], Optional[Any], Optional[Any]]:
        m = self._meta_struct(parent_sample)
        return (
            getattr(m, "listID"),
            getattr(m, "startingElementID"),
            getattr(m, "updateElementID"),
            getattr(m, "updateElementTimestamp", None),
        )

    def _list_size(self, parent_sample: Any) -> int:
        m = self._meta_struct(parent_sample)
        return getattr(m, "size", 0)

    @staticmethod
    def _elem_ids(elem: Any) -> Tuple[Any, Any, Optional[Any], Optional[Any]]:
        return (
            getattr(elem, "listID"),
            getattr(elem, "elementID"),
            getattr(elem, "nextElementID", None),
            getattr(elem, "elementTimestamp"),
        )

    def _elem_path(self, elem_id_k: Any) -> tuple:
        # Element nodes are addressed by their list element token only; not under metadata path
        return tuple(path_for_list_element(self.list_name, elem_id_k))

    def _ordered_chain(self, elems_by_id: Dict[Any, Any], start_k: Optional[Any]) -> List[Any]:
        if not elems_by_id:
            return []
        if start_k is None:
            return list(elems_by_id.values())
        ordered, cur, visited = [], start_k, set()
        while cur is not None and cur in elems_by_id and cur not in visited:
            e = elems_by_id[cur]
            ordered.append(e)
            visited.add(cur)
            _, _, nxt, _ = self._elem_ids(e)
            cur = guid_key(nxt) if nxt is not None else None
        return ordered

    def on_reader_data(self, node, key, combined, sample):
        _logger.debug(f"[LargeListReader:{self.list_name}] on_reader_data: sample={type(sample).__name__}")
        list_id, start_id, upd_id, upd_ts = self._meta_ids(sample)
        size = self._list_size(sample)

        list_k = guid_key(list_id)
        start_k = guid_key(start_id) if start_id is not None else None
        upd_k = guid_key(upd_id) if upd_id is not None else None

        self._meta_by_list[list_k] = sample
        self._parent_key_by_list[list_k] = key

        bucket = self._elems_by_list.get(list_k, {})
        comb_bucket = self._elem_combined_by_list.get(list_k, {})

        _logger.debug(
            f"[LargeListReader:{self.list_name}] metadata: size={size}, bucket_size={len(bucket)}, start_k={start_k}"
        )

        # Check if we can complete the list now
        if size == 0 and not bucket:
            # Empty list case - can complete immediately
            combined.collections[self.list_name] = []
            node._combined_by_key[key] = combined
            _logger.debug(f"[LargeListReader:{self.list_name}] complete empty (no elements)")
            return (AssemblySignal(key, complete=True),)

        # For non-empty lists, check if we already have all elements
        if size > 0 and len(bucket) >= size and start_k is not None:
            # We have all elements and metadata - can complete now
            ordered = self._ordered_chain(bucket, start_k)
            if len(ordered) >= size:
                views: List[ElementView] = []
                for e in ordered:
                    eid_k = guid_key(getattr(e, "elementID"))
                    elem_path = self._elem_path(eid_k)
                    child_comb = comb_bucket.get(eid_k)
                    if child_comb:
                        for k2, v2 in child_comb.overlays_by_path.items():
                            combined.overlays_by_path[elem_path + k2] = v2
                    views.append(ElementView(combined, e, elem_path))

                combined.collections[self.list_name] = views
                node._combined_by_key[key] = combined
                _logger.debug(f"[LargeListReader:{self.list_name}] complete with {len(views)} elements from metadata")
                return (AssemblySignal(key, complete=True),)

        # If we can't complete yet, wait for more elements
        _logger.debug(f"[LargeListReader:{self.list_name}] waiting for more elements or metadata")
        return ()

    def on_child_assembled(self, node, child_name, key, assembled):
        _logger.debug(f"[LargeListReader:{self.list_name}] on_child_assembled: element={type(assembled.base).__name__}")
        elem = assembled.base
        list_id, elem_id, _next_id, elem_ts = self._elem_ids(elem)

        list_k = guid_key(list_id)
        elem_k = guid_key(elem_id)

        bucket = self._elems_by_list.setdefault(list_k, {})
        bucket[elem_k] = elem
        comb_bucket = self._elem_combined_by_list.setdefault(list_k, {})
        comb_bucket[elem_k] = assembled

        _logger.debug(
            f"[LargeListReader:{self.list_name}] stored element: list_k={list_k}, elem_k={elem_k}, bucket_size={len(bucket)}"
        )

        parent_sample = self._meta_by_list.get(list_k)
        if parent_sample is None:
            _logger.debug(f"[LargeListReader:{self.list_name}] no parent metadata found for list_k={list_k}")
            return ()

        _, start_id, upd_id, upd_ts = self._meta_ids(parent_sample)
        size = self._list_size(parent_sample)

        parent_key = self._parent_key_by_list.get(list_k)
        if parent_key is None:
            _logger.debug(f"[LargeListReader:{self.list_name}] no parent key found for list_k={list_k}")
            return ()

        comb = node._combined_by_key.get(parent_key)
        if comb is None:
            _logger.debug(f"[LargeListReader:{self.list_name}] no combined sample found for parent_key={parent_key}")
            return ()

        start_k = guid_key(start_id) if start_id is not None else None

        _logger.debug(
            f"[LargeListReader:{self.list_name}] checking completion: size={size}, bucket_size={len(bucket)}, start_k={start_k}"
        )

        if size == 0:
            if not bucket:
                comb.collections[self.list_name] = []
                node._combined_by_key[parent_key] = comb
                _logger.debug(f"[LargeListReader:{self.list_name}] complete empty (no elements)")
                return (AssemblySignal(parent_key, complete=True),)

        if size > 0:
            if len(bucket) < size or start_k is None:
                _logger.debug(
                    f"[LargeListReader:{self.list_name}] waiting for more elements: have={len(bucket)}, need={size}"
                )
                return ()
        else:
            if upd_id is None or not guid_equal(upd_id, elem_id):
                _logger.debug(f"[LargeListReader:{self.list_name}] update marker mismatch")
                return ()
            if upd_ts is not None and elem_ts != upd_ts:
                _logger.debug(f"[LargeListReader:{self.list_name}] timestamp mismatch")
                return ()
            if start_k is None:
                _logger.debug(f"[LargeListReader:{self.list_name}] no start element")
                return ()

        ordered = self._ordered_chain(bucket, start_k)
        if size is not None and size > 0 and len(ordered) < size:
            _logger.debug(
                f"[LargeListReader:{self.list_name}] ordered chain incomplete: have={len(ordered)}, need={size}"
            )
            return ()

        views: List[ElementView] = []
        for e in ordered:
            eid_k = guid_key(getattr(e, "elementID"))
            elem_path = self._elem_path(eid_k)
            child_comb = comb_bucket.get(eid_k)
            if child_comb:
                for k2, v2 in child_comb.overlays_by_path.items():
                    comb.overlays_by_path[elem_path + k2] = v2
            views.append(ElementView(comb, e, elem_path))

        comb.collections[self.list_name] = views
        node._combined_by_key[parent_key] = comb
        _logger.debug(f"[LargeListReader:{self.list_name}] complete with {len(views)} elements")
        return (AssemblySignal(parent_key, complete=True),)


class PassthroughReader(ReaderDecorator):
    """
    Leaf decorator that marks a node 'complete' immediately upon receiving
    a valid sample. This allows parent decorators (e.g., GenSpecReader or
    Large(Set|List)Reader above it) to proceed with merging.

    Use this for topics that have no UMAA multi-topic structure of their own.
    """

    def on_reader_data(self, node, key, combined: CombinedSample, sample) -> Iterable[AssemblySignal]:
        # We don’t modify 'combined'; just tell the parent we’re done at this key.
        return (AssemblySignal(key, complete=True),)
