"""
Writer-side UMAA decorators:

- :class:`GenSpecWriter` — path-aware generalization/specialization (UMAA 3.9).
- :class:`LargeSetWriter` — publish sets and update metadata markers (UMAA 3.8).
- :class:`LargeListWriter` — publish lists, link nextElementID, update markers (UMAA 3.8).

These decorators automatically allocate IDs (when missing) using :func:`generate_guid`
and spawn child builders per element so arbitrary nesting continues to work.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Tuple, List

from umaapy.util.multi_topic_support import (
    CombinedBuilder,
    get_at_path,
    path_for_set_element,
    path_for_list_element,
)
from umaapy.util.multi_topic_writer import WriterDecorator, WriterNode

from umaapy.util.umaa_utils import topic_from_type

from umaapy.util.uuid_factory import generate_guid, NIL_GUID


def _resolve_items_with_attr_path(
    builder: "CombinedBuilder",
    name: str,
    attr_path: Tuple[str, ...],
) -> List[Any]:
    """
    Robustly fetch a collection for `name` from `builder` at a nested `attr_path`.

    Returns runtime items (list, possibly via to_runtime()).
    """

    bagmap = getattr(builder, "collections_by_path", None)
    if isinstance(bagmap, dict):
        bag = bagmap.get(tuple(attr_path))
        if isinstance(bag, dict) and name in bag:
            coll = bag[name]
            items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
            return items

    if hasattr(builder, "get_collection_at"):
        try:
            coll = builder.get_collection_at(attr_path, name)
            if coll is not None:
                items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
                return items
        except Exception as e:
            pass
    if hasattr(builder, "get_collection"):
        try:
            try:
                coll = builder.get_collection(name, path=attr_path)
            except TypeError:
                coll = builder.get_collection(name, attr_path)  # some code uses positional
            if coll is not None:
                items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
                return items
        except Exception as e:
            pass
    coll_store = getattr(builder, "collections", {})
    node: Any = coll_store
    if attr_path:
        for seg in attr_path:
            if isinstance(node, dict) and seg in node:
                node = node[seg]
            else:
                node = None
                break
        if isinstance(node, dict):
            coll = node.get(name)
            if coll is not None:
                items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
                return items

    if isinstance(coll_store, dict):
        key = (tuple(attr_path), name)
        if key in coll_store:
            coll = coll_store[key]
            items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
            return items

    if not attr_path and isinstance(coll_store, dict) and name in coll_store:
        coll = coll_store.get(name)
        items = coll.to_runtime() if hasattr(coll, "to_runtime") else list(coll)
        return items

    return []


class GenSpecWriter(WriterDecorator):
    """
    Path-aware UMAA Generalization/Specialization writer.

    Parameters
    ----------
    attr_path : Sequence[str], optional
        Where the generalization object lives inside the base (e.g., ``('objective',)``).
    resolve_child_by_topic_name : Callable[[str], str], optional
        Optional mapping hook to resolve child names when specialization topics differ.
    """

    def __init__(self, attr_path: Sequence[str] = (), resolve_child_by_topic_name=None):
        super().__init__()
        self.attr_path: Tuple[str, ...] = tuple(attr_path)
        self._children: Dict[str, WriterNode] = {}
        self._resolve = resolve_child_by_topic_name

    def attach_children(self, **children: WriterNode) -> None:
        super().attach_children(**children)
        self._children = getattr(self, "_children", {})

    @staticmethod
    def _spec_identity(spec: Any):
        return getattr(spec, "specializationReferenceID"), getattr(spec, "specializationReferenceTimestamp")

    @staticmethod
    def _spec_topic_name(spec: Any) -> str:
        return getattr(spec, "specializationTopic", None) or topic_from_type(type(spec))

    @staticmethod
    def _bind_generalization(gen: Any, topic: str, spec_id: Any, spec_ts: Optional[Any]) -> None:
        setattr(gen, "specializationTopic", topic)
        setattr(gen, "specializationID", spec_id)
        if spec_ts is not None:
            setattr(gen, "specializationTimestamp", spec_ts)

    def _resolve_child(self, topic: str) -> Optional[WriterNode]:
        child = self._children.get(topic)
        if child is None and self._resolve is not None:
            mapped = self._resolve(topic)
            child = self._children.get(mapped)
        return child

    def publish(self, node: WriterNode, builder: CombinedBuilder) -> None:
        # Locate specialization and generalization at the path
        if self.attr_path:
            spec = builder.overlays_by_path.get(self.attr_path)
            gen_obj = get_at_path(builder.base, self.attr_path)
            child_collections = builder.collections_by_path.get(self.attr_path, {})
        else:
            spec = builder.overlay
            gen_obj = builder.base
            child_collections = builder.collections

        if spec is None:
            return

        topic = self._spec_topic_name(spec)
        child = self._resolve_child(topic)
        if child is None:
            raise RuntimeError(f"GenSpecWriter: no child WriterNode for specialization topic '{topic}'")

        # Ensure specialization ID/topic exist
        if getattr(spec, "specializationReferenceID") == NIL_GUID:
            try:
                setattr(spec, "specializationReferenceID", generate_guid())
            except Exception:
                pass

        child.publish(CombinedBuilder(base=spec, collections=child_collections))

        sid, sts = self._spec_identity(spec)
        self._bind_generalization(gen_obj, topic, sid, sts)


class LargeSetWriter(WriterDecorator):
    def __init__(
        self,
        set_name: str,
        attr_path: Tuple[str, ...] = (),
        expected_child_topic: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.set_name = set_name
        self.attr_path = tuple(attr_path)
        self.expected_child_topic = expected_child_topic
        self._children: Dict[str, WriterNode] = {}

    def attach_children(self, **children: "WriterNode") -> None:
        super().attach_children(**children)
        self._children = getattr(self, "_children", {})

    def _select_child(self) -> "WriterNode":
        if self.expected_child_topic and self.expected_child_topic in self._children:
            return self._children[self.expected_child_topic]
        if len(self._children) == 1:
            return next(iter(self._children.values()))
        raise RuntimeError(
            f"LargeSetWriter('{self.set_name}'): cannot select child "
            f"(expected={self.expected_child_topic}, available={list(self._children.keys())})"
        )

    def _meta_struct(self, parent: Any) -> Any:
        # Traverse attr_path to the container that should hold the "*SetMetadata"
        container = parent
        for seg in self.attr_path:
            if not hasattr(container, seg):
                raise AttributeError(
                    f"{type(container).__name__} missing segment '{seg}' while resolving attr_path {self.attr_path}"
                )
            container = getattr(container, seg)

        attr = f"{self.set_name}SetMetadata"
        if hasattr(container, attr):
            return getattr(container, attr)

        if hasattr(container, "element") and hasattr(container.element, attr):
            return getattr(container.element, attr)

        raise AttributeError(f"{type(container).__name__} missing {attr}")

    def _get_set_id(self, meta: Any) -> Any:
        return getattr(meta, "setID")

    @staticmethod
    def _set_update_marker(meta: Any, elem_id: Any, elem_ts: Optional[Any]) -> None:
        setattr(meta, "updateElementID", elem_id)
        if elem_ts is not None:
            setattr(meta, "updateElementTimestamp", elem_ts)

    @staticmethod
    def _elem_identity(elem: Any):
        return getattr(elem, "elementID"), getattr(elem, "elementTimestamp", None)

    @staticmethod
    def _ensure_elem_set_id(elem: Any, set_id: Any) -> None:
        setattr(elem, "setID", set_id)

    def publish(self, node: "WriterNode", builder: "CombinedBuilder") -> None:
        base_type = type(builder.base).__name__

        meta = self._meta_struct(builder.base)
        if meta is None:
            return

        current_id = getattr(meta, "setID", None)
        if current_id is None or current_id == NIL_GUID:
            new_id = generate_guid()
            setattr(meta, "setID", new_id)

        set_id = getattr(meta, "setID")

        items = _resolve_items_with_attr_path(builder, self.set_name, self.attr_path)
        setattr(meta, "size", int(len(items)))

        if not items:
            return

        child = self._select_child()

        last_id = last_ts = None
        for idx, e in enumerate(items):
            self._ensure_elem_set_id(e, set_id)
            elem_id, elem_ts = getattr(e, "elementID"), getattr(e, "elementTimestamp", None)

            elem_path = path_for_set_element(self.set_name, elem_id) + tuple(self.attr_path)
            child_b = builder.spawn_child(elem_path, e)
            child.publish(child_b)
            last_id, last_ts = elem_id, elem_ts

        setattr(meta, "updateElementID", last_id)
        if last_ts is not None:
            setattr(meta, "updateElementTimestamp", last_ts)


class LargeListWriter(WriterDecorator):
    def __init__(
        self,
        list_name: str,
        attr_path: Tuple[str, ...] = (),
        expected_child_topic: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.list_name = list_name
        self.attr_path = tuple(attr_path)
        self.expected_child_topic = expected_child_topic
        self._children: Dict[str, WriterNode] = {}

    def attach_children(self, **children: "WriterNode") -> None:
        super().attach_children(**children)
        self._children = getattr(self, "_children", {})

    def _select_child(self) -> "WriterNode":
        if self.expected_child_topic and self.expected_child_topic in self._children:
            return self._children[self.expected_child_topic]
        if len(self._children) == 1:
            return next(iter(self._children.values()))
        raise RuntimeError(
            f"LargeListWriter('{self.list_name}'): cannot select child "
            f"(expected={self.expected_child_topic}, available={list(self._children.keys())})"
        )

    def _meta_struct(self, parent: Any) -> Any:
        container = parent
        for seg in self.attr_path:
            if not hasattr(container, seg):
                raise AttributeError(
                    f"{type(container).__name__} missing segment '{seg}' while resolving attr_path {self.attr_path}"
                )
            container = getattr(container, seg)

        attr = f"{self.list_name}ListMetadata"
        if hasattr(container, attr):
            return getattr(container, attr)

        if hasattr(container, "element") and hasattr(container.element, attr):
            return getattr(container.element, attr)

        raise AttributeError(f"{type(container).__name__} missing {attr}")

    def _get_list_id(self, meta: Any) -> Any:
        return getattr(meta, "listID")

    @staticmethod
    def _set_starting(meta: Any, first_id: Any) -> None:
        setattr(meta, "startingElementID", first_id)

    @staticmethod
    def _set_update_marker(meta: Any, last_id: Any, last_ts: Optional[Any]) -> None:
        setattr(meta, "updateElementID", last_id)
        if last_ts is not None:
            setattr(meta, "updateElementTimestamp", last_ts)

    @staticmethod
    def _elem_id(elem: Any) -> Any:
        return getattr(elem, "elementID")

    @staticmethod
    def _elem_ts(elem: Any) -> Optional[Any]:
        return getattr(elem, "elementTimestamp", None)

    @staticmethod
    def _set_next(elem: Any, next_id: Optional[Any]) -> None:
        setattr(elem, "nextElementID", next_id)

    @staticmethod
    def _ensure_elem_list_id(elem: Any, list_id: Any) -> None:
        setattr(elem, "listID", list_id)

    def publish(self, node: "WriterNode", builder: "CombinedBuilder") -> None:
        base_type = type(builder.base).__name__

        meta = self._meta_struct(builder.base)
        if meta is None:
            return

        if getattr(meta, "listID", None) in (None, NIL_GUID):
            new_id = generate_guid()
            setattr(meta, "listID", new_id)

        list_id = getattr(meta, "listID")

        items = _resolve_items_with_attr_path(builder, self.list_name, self.attr_path)
        setattr(meta, "size", int(len(items)))

        if not items:
            return

        child = self._select_child()

        # link chain
        for i, e in enumerate(items):
            setattr(e, "listID", list_id)
            nxt = getattr(items[i + 1], "elementID") if i + 1 < len(items) else None
            setattr(e, "nextElementID", nxt)
        for e in items:
            elem_id = getattr(e, "elementID")
            elem_path = path_for_list_element(self.list_name, elem_id) + tuple(self.attr_path)
            child_b = builder.spawn_child(elem_path, e)
            child.publish(child_b)

        first_id = getattr(items[0], "elementID")
        last_id = getattr(items[-1], "elementID")
        last_ts = getattr(items[-1], "elementTimestamp", None)
        setattr(meta, "startingElementID", first_id)
        setattr(meta, "updateElementID", last_id)
        if last_ts is not None:
            setattr(meta, "updateElementTimestamp", last_ts)
