from __future__ import annotations

from typing import Any, Dict, Sequence, Optional, Tuple

from umaapy.util.multi_topic_support import CombinedBuilder, SetCollection, ListCollection, get_at_path
from umaapy.util.multi_topic_writer import WriterDecorator, WriterNode


class GenSpecWriter(WriterDecorator):
    """Publishes specialization first, then binds the generalization fields.

    Configuration
    -------------
    - Provide a way to map a specialization 'topic name' to the child WriterNode key.
      By default we try direct lookup in attached children by topic name; you can inject a resolver.
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
    def _spec_identity(spec: Any) -> Tuple[Any, Optional[Any]]:
        return getattr(spec, "specializationID"), getattr(spec, "specializationTimestamp", None)

    @staticmethod
    def _spec_topic_name(spec: Any) -> str:
        return getattr(spec, "specializationTopic", None) or spec.__class__.__name__

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
        # Locate specialization object for this path
        if self.attr_path:
            spec = builder.overlays_by_path.get(self.attr_path)
            gen_obj = get_at_path(builder.base, self.attr_path)
        else:
            spec = builder.overlay
            gen_obj = builder.base

        if spec is None:
            return  # nothing to do

        topic = self._spec_topic_name(spec)
        child = self._resolve_child(topic)
        if child is None:
            raise RuntimeError(f"GenSpecWriter: no child WriterNode for specialization topic '{topic}'")

        # Publish specialization first
        child.publish(CombinedBuilder(base=spec))

        # Bind generalization at the right path
        sid, sts = self._spec_identity(spec)
        self._bind_generalization(gen_obj, topic, sid, sts)


class LargeSetWriter(WriterDecorator):
    """Publishes all set elements, then updates metadata updateElementID/updateElementTimestamp on base.

    Assumptions
    -----------
    - The base object has `<set_name>SetMetadata` with fields:
        setID, updateElementID, updateElementTimestamp?  (UMAA §3.8)
    - Elements have fields:
        setID, elementID, elementTimestamp?
    - Exactly one child WriterNode is attached for the element topic.
    """

    def __init__(self, set_name: str) -> None:
        super().__init__()
        self.set_name = set_name
        self._children: Dict[str, WriterNode] = {}

    def attach_children(self, **children: WriterNode) -> None:
        super().attach_children(**children)
        self._children = getattr(self, "_children", {})

    def _meta_struct(self, meta_parent: Any) -> Any:
        return getattr(meta_parent, f"{self.set_name}SetMetadata")

    @staticmethod
    def _get_set_id(meta_struct: Any) -> Any:
        return getattr(meta_struct, "setID")

    @staticmethod
    def _set_update_marker(meta_struct: Any, element_id: Any, element_ts: Optional[Any]) -> None:
        setattr(meta_struct, "updateElementID", element_id)
        if element_ts is not None:
            setattr(meta_struct, "updateElementTimestamp", element_ts)

    @staticmethod
    def _ensure_elem_set_id(elem: Any, set_id: Any) -> None:
        if getattr(elem, "setID", None) != set_id:
            try:
                setattr(elem, "setID", set_id)
            except Exception:
                # If generated type forbids setting, assume already correct
                pass

    @staticmethod
    def _elem_identity(elem: Any) -> Tuple[Any, Optional[Any]]:
        return getattr(elem, "elementID"), getattr(elem, "elementTimestamp", None)

    def publish(self, node: WriterNode, builder: CombinedBuilder) -> None:
        # Resolve collection
        coll = builder.collections.get(self.set_name)
        if coll is None:
            return  # no set content to publish

        # Normalize to iterable of elements
        if isinstance(coll, SetCollection):
            elements = coll.to_runtime()
        elif isinstance(coll, (list, tuple)):
            elements = list(coll)
        else:
            raise TypeError(f"LargeSetWriter('{self.set_name}') expects SetCollection or list-like, got {type(coll)}")

        if not self._children:
            raise RuntimeError(f"LargeSetWriter('{self.set_name}') requires one attached element WriterNode")

        # Assume single child for set elements
        child = next(iter(self._children.values()))

        meta = self._meta_struct(builder.base)
        set_id = self._get_set_id(meta)

        last_id = last_ts = None
        for e in elements:
            # Ensure the element's setID matches parent metadata
            self._ensure_elem_set_id(e, set_id)
            # Publish each element
            child.publish(CombinedBuilder(base=e))
            # Track last for atomic update marker
            last_id, last_ts = self._elem_identity(e)

        # Update metadata marker to the last element (UMAA atomic update)
        if last_id is not None:
            self._set_update_marker(meta, last_id, last_ts)


class LargeListWriter(WriterDecorator):
    """Links and publishes list elements in order, then updates list metadata on base.

    Assumptions
    -----------
    - The base object has `<list_name>ListMetadata` with fields:
        listID, startingElementID?, updateElementID, updateElementTimestamp?
    - Elements have fields:
        listID, elementID, nextElementID?, elementTimestamp?
    - Exactly one child WriterNode is attached for the element topic.
    """

    def __init__(self, list_name: str) -> None:
        super().__init__()
        self.list_name = list_name
        self._children: Dict[str, WriterNode] = {}

    def attach_children(self, **children: WriterNode) -> None:
        super().attach_children(**children)
        self._children = getattr(self, "_children", {})

    def _meta_struct(self, meta_parent: Any) -> Any:
        return getattr(meta_parent, f"{self.list_name}ListMetadata")

    @staticmethod
    def _get_list_id(meta_struct: Any) -> Any:
        return getattr(meta_struct, "listID")

    @staticmethod
    def _set_starting(meta_struct: Any, first_id: Optional[Any]) -> None:
        try:
            setattr(meta_struct, "startingElementID", first_id)
        except Exception:
            pass

    @staticmethod
    def _set_update_marker(meta_struct: Any, last_id: Any, last_ts: Optional[Any]) -> None:
        setattr(meta_struct, "updateElementID", last_id)
        if last_ts is not None:
            setattr(meta_struct, "updateElementTimestamp", last_ts)

    @staticmethod
    def _ensure_elem_list_id(elem: Any, list_id: Any) -> None:
        if getattr(elem, "listID", None) != list_id:
            try:
                setattr(elem, "listID", list_id)
            except Exception:
                pass

    @staticmethod
    def _elem_id(elem: Any) -> Any:
        return getattr(elem, "elementID")

    @staticmethod
    def _elem_ts(elem: Any) -> Optional[Any]:
        return getattr(elem, "elementTimestamp", None)

    @staticmethod
    def _set_next(elem: Any, next_id: Optional[Any]) -> None:
        try:
            setattr(elem, "nextElementID", next_id)
        except Exception:
            pass

    def publish(self, node: WriterNode, builder: CombinedBuilder) -> None:
        # Resolve collection
        coll = builder.collections.get(self.list_name)
        if coll is None:
            return  # nothing to publish

        if isinstance(coll, ListCollection):
            items = coll.to_runtime()
        elif isinstance(coll, (list, tuple)):
            items = list(coll)
        else:
            raise TypeError(
                f"LargeListWriter('{self.list_name}') expects ListCollection or list-like, got {type(coll)}"
            )

        if not items:
            # For empty lists, you may want to clear startingElementID and update markers explicitly.
            # Leaving as-is by default.
            return

        if not self._children:
            raise RuntimeError(f"LargeListWriter('{self.list_name}') requires one attached element WriterNode")

        child = next(iter(self._children.values()))

        meta = self._meta_struct(builder.base)
        list_id = self._get_list_id(meta)

        # Link the list (nextElementID) and ensure listID is set
        for i, e in enumerate(items):
            self._ensure_elem_list_id(e, list_id)
            nxt = self._elem_id(items[i + 1]) if i + 1 < len(items) else None
            self._set_next(e, nxt)

        # Publish elements in order
        for e in items:
            child.publish(CombinedBuilder(base=e))

        # Update metadata: startingElementID, updateElementID(+ts)
        first_id = self._elem_id(items[0])
        last_id = self._elem_id(items[-1])
        last_ts = self._elem_ts(items[-1])

        self._set_starting(meta, first_id)
        self._set_update_marker(meta, last_id, last_ts)
