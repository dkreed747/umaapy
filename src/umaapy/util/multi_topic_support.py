"""
Shared UMAA support runtime: GUID helpers, path/navigation, combined sample/builder,
editable single-object facade, element views, and top-level reader/writer adapters.

This module is the glue that lets users interact with *any* nested UMAA multi-topic
structure (generalization/specialization, large sets, large lists) as if it were
one normal Python object — both when READING (assembled views) and WRITING
(editable combined builders).

Key ideas
---------
- **GUID helpers**: robust hashing/equality for UMAA's 16-digit NumericGUID.
- **Paths**: every node in the multi-topic tree (root, specialization nodes,
  set/list elements) has a path; special tokens address collection elements.
- **CombinedSample** (reader) & **CombinedBuilder** (writer): carry overlays and
  per-node collection bags keyed by these paths.
- **OverlayView / ElementView** (reader) and **BuilderEditView / CombinedEditHandle**
  (writer): "single-object" interfaces that let users read/edit nested content
  naturally (e.g., ``v.objective.speed`` or ``cmd.objective.collections["waypoints"]``).
- **Adapters**: UmaaReaderAdapter / UmaaFilteredReaderAdapter / UmaaWriterAdapter
  expose DDS-like ergonomics (listeners, read/take) while the UMAA graph handles
  the heavy lifting behind the scenes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from collections import deque
import inspect
from typing import (
    Any,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    List,
    Iterable,
    TYPE_CHECKING,
)

import rti.connextdds as dds

from umaapy.util.umaa_utils import (
    guid_key,
    classify_obj_by_umaa,
    UMAAConcept,
    path_for_list_element,
    path_for_set_element,
)
from umaapy.util.uuid_factory import generate_guid, NIL_GUID

if TYPE_CHECKING:
    from umaapy.util.multi_topic_reader import ReaderNode
    from umaapy.util.multi_topic_writer import WriterNode, TopLevelWriter


class SetCollection:
    """
    Mutable set-like collection for building UMAA Large Sets at runtime.

    - Elements must have an `elementID` attribute.
    - Order is not guaranteed.
    """

    __slots__ = ("_items",)

    def __init__(self) -> None:
        self._items: Dict[Any, Any] = {}

    def add(self, elem: Any) -> None:
        """Add an element; replaces existing entry with same ID."""
        if getattr(elem, "elementID") == NIL_GUID:
            setattr(elem, "elementID", generate_guid())
        eid = getattr(elem, "elementID")
        self._items[guid_key(eid)] = elem

    def discard(self, element_id: Any) -> None:
        """Remove an element by ID if present."""
        self._items.pop(guid_key(element_id), None)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._items)

    def __iter__(self):  # pragma: no cover - trivial
        return iter(self._items.values())

    def to_runtime(self) -> List[Any]:
        """Return a stable list snapshot of elements."""
        return list(self._items.values())


class ListCollection:
    """
    Mutable list-like collection for building UMAA Large Lists at runtime.

    - Elements must have an `elementID` attribute.
    - Append/insert supported; element linking is handled by the writer decorator.
    """

    __slots__ = ("_items",)

    def __init__(self) -> None:
        self._items: List[Any] = []

    def append(self, elem: Any) -> None:
        """Append an element."""
        self._items.append(elem)

    def insert(self, index: int, elem: Any) -> None:
        """Insert an element at a given index."""
        self._items.insert(index, elem)

    def pop(self, index: int = -1) -> Any:  # pragma: no cover - trivial
        """Pop and return an element."""
        return self._items.pop(index)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._items)

    def __iter__(self):  # pragma: no cover - trivial
        return iter(self._items)

    def to_runtime(self) -> List[Any]:
        """Return list snapshot of elements."""
        return list(self._items)


def get_at_path(obj: object, path: Sequence[Any]) -> object:
    """
    Navigate attributes using a path of names.

    Parameters
    ----------
    obj : object
        Root object.
    path : Sequence[Any]
        Sequence of attribute names.

    Returns
    -------
    object
        The nested object.
    """
    cur = obj
    for seg in path:
        cur = getattr(cur, seg, None)
    return cur


def set_at_path(root: object, path: Sequence[Any], value: object) -> None:
    """
    Set an attribute at a nested path.

    Parameters
    ----------
    root : object
        Root object.
    path : Sequence[Any]
        Attribute name path; must be non-empty.
    value : object
        Value to assign.
    """
    if not path:
        raise ValueError("Empty path not supported for set_at_path")
    parent = get_at_path(root, path[:-1])
    setattr(parent, path[-1], value)


class OverlayView:
    """
    Read-only overlay view supporting nested overlays and per-node collections.

    Attribute resolution order:
    1) If a nested overlay is registered for the next attribute hop, return a new
       OverlayView scoped to that subpath.
    2) If the top-level overlay has the attribute, return it.
    3) Otherwise, return the attribute from the base object.
    4) If the attribute equals a collection name, return the collection.
    """

    __slots__ = ("_base", "_collections", "_overlays_by_path", "_path")

    def __init__(
        self,
        base: Any,
        collections: Mapping[str, Any],
        overlays_by_path: Mapping[Tuple[Any, ...], Any] = (),
        path: Tuple[Any, ...] = (),
    ) -> None:
        self._base = base
        self._collections = collections
        self._overlays_by_path = dict(overlays_by_path or {})
        self._path = tuple(path)

    def __getattr__(self, name: str) -> Any:
        sub_path = self._path + (name,)
        if sub_path in self._overlays_by_path:
            base_sub = getattr(self._base, name) if hasattr(self._base, name) else None
            return OverlayView(
                base=base_sub,
                collections=self._collections,
                overlays_by_path=self._overlays_by_path,
                path=sub_path,
            )

        if hasattr(self._base, name):
            return getattr(self._base, name)

        if name in self._collections:
            return self._collections[name]

        raise AttributeError(name)

    def __getitem__(self, key: str) -> Any:  # pragma: no cover - convenience
        return getattr(self, key)


@dataclass(frozen=True)
class CombinedSample:
    """
    Assembled, read-only sample composed from multiple UMAA topics.

    Parameters
    ----------
    base : Any
        The base/root sample (e.g., the metadata or generalization-containing message).
    collections : Dict[str, Any], optional
        Per-node collections bag at the *current node*. Nested nodes use `overlays_by_path`.
    overlays_by_path : Dict[Tuple[Any, ...], Any], optional
        Nested overlays keyed by their absolute attribute/element path.
    """

    base: Any
    collections: Dict[str, Any] = field(default_factory=dict)
    overlays_by_path: Dict[Tuple[Any, ...], Any] = field(default_factory=dict)

    def __post_init__(self):
        # Bolt on collections for convenience
        setattr(self.base, "collections", self.collections)

    @property
    def view(self) -> OverlayView:
        """Return a read-only overlay view for user access."""
        return OverlayView(
            self.base,
            self.collections,
            overlays_by_path=self.overlays_by_path,
            path=(),
        )

    def with_overlay(self, overlay: Any) -> "CombinedSample":
        """Return a new CombinedSample with a top-level overlay set."""
        return CombinedSample(
            base=self.base,
            collections=self.collections,
            overlays_by_path=self.overlays_by_path.update({(), overlay}),
        )

    def clone_with_collections(self, updates: Mapping[str, Any]) -> "CombinedSample":
        """Return a new CombinedSample with updated local collections bag."""
        return CombinedSample(
            base=self.base,
            collections=self.collections.update(updates),
            overlays_by_path=self.overlays_by_path,
        )

    def add_overlay_at(self, overlay_obj: Any, path: Sequence[Any] = ()) -> "CombinedSample":
        """
        Register a nested overlay at an absolute path.

        Parameters
        ----------
        path : Sequence[Any]
            Absolute path (attributes and/or element tokens).
        overlay_obj : Any
            Overlay object to merge at that path.

        Returns
        -------
        CombinedSample
            A new CombinedSample with the overlay registered.
        """
        return CombinedSample(
            base=self.base,
            collections=self.collections,
            overlays_by_path=self.overlays_by_path.update({tuple(path): overlay_obj}),
        )


@dataclass
class CombinedBuilder:
    """
    Editable, path-aware combined sample for publishing nested UMAA graphs.

    Parameters
    ----------
    base : Any
        The base/root object to publish.
    collections_by_path : Dict[Tuple[Any, ...], Dict[str, Any]], optional
        Per-node collections bags keyed by absolute path.
    overlays_by_path : Dict[Tuple[Any, ...], Any], optional
        Per-node specialization overlays keyed by absolute path.
    """

    base: Any
    overlays_by_path: Dict[Tuple[Any, ...], Any] = field(default_factory=dict)
    collections_by_path: Dict[Tuple[Any, ...], Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        setattr(self.base, "collections", self.collections_by_path.setdefault((), {}))

    def ensure_collection_at(self, name: str, kind: str, path: Sequence[str] = ()) -> Any:
        """
        Ensure a per-node collection exists at a given path.

        Parameters
        ----------
        path : Sequence[Any]
            Absolute node path (attributes and/or element tokens).
        name : str
            Collection logical name (e.g., 'waypoints').
        kind : {'set', 'list'}
            Collection kind.

        Returns
        -------
        Any
            A `SetCollection` or `ListCollection` instance.
        """
        p = tuple(path)
        bag = self.collections_by_path.setdefault(p, {})
        existing = bag.get(name)
        if existing is not None:
            return existing
        if kind == "set":
            created = SetCollection()
        elif kind == "list":
            created = ListCollection()
        else:
            raise ValueError("kind must be 'set' or 'list'")
        bag[name] = created
        return created

    def collections_at(self, path: Sequence[str] = ()) -> Dict[str, Any]:
        """
        Get (and create if absent) the per-node collections bag at a given path.

        Parameters
        ----------
        path : Sequence[Any]
            Absolute node path.

        Returns
        -------
        Dict[str, Any]
            The collections bag dictionary.
        """
        return self.collections_by_path.setdefault(tuple(path), {})

    def use_specialization_at(self, spec_obj: Any, path: Sequence[str] = ()) -> None:
        """
        Set a specialization overlay at a given path.

        Parameters
        ----------
        path : Sequence[Any]
            Absolute path where the generalization field lives (e.g., ('objective',)).
        spec_obj : Any
            Specialization object instance.
        """
        self.overlays_by_path[tuple(path)] = spec_obj

    def overlay_at(self, path: Sequence[str] = ()) -> Optional[Any]:
        """Get the specialization overlay at a given path, if any."""
        return self.overlays_by_path.get(tuple(path))

    def spawn_child(self, base_obj: Any, path: Sequence[str] = ()) -> "CombinedBuilder":
        """
        Spawn a child builder scoped to `path`, rebasing nested overlays/collections.

        This is used by writer decorators to publish elements and nested content
        under a collection element or a specialization node.

        Parameters
        ----------
        path : Sequence[Any]
            Absolute node path for the child.
        base_obj : Any
            The child's base object.

        Returns
        -------
        CombinedBuilder
            A child builder that carries only the relevant nested bags/overlays.
        """
        p = tuple(path)

        child_collections_by_path: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for k, v in self.collections_by_path.items():
            if len(k) >= len(p) and tuple(k[: len(p)]) == p:
                rel = tuple(k[len(p) :])
                if rel:
                    child_collections_by_path[rel] = v

        child_overlays: Dict[Tuple[Any, ...], Any] = {}
        for k, v in self.overlays_by_path.items():
            if len(k) >= len(p) and tuple(k[: len(p)]) == p:
                rel = tuple(k[len(p) :])
                child_overlays[rel] = v

        return CombinedBuilder(
            base=base_obj,
            collections_by_path=child_collections_by_path,
            overlays_by_path=child_overlays,
        )


class BuilderEditView:
    """
    Write-through overlay view for `CombinedBuilder`, path-aware.

    - Getting nested attributes returns scoped `BuilderEditView`s when the value
      looks like a user object (has `__dict__`/`__slots__`).
    - Setting attributes writes into the overlay object when present; otherwise
      into the base at the current path.
    - `collections` at any path returns the per-node bag for lists/sets.
    """

    __slots__ = ("_builder", "_path")

    def __init__(self, builder: CombinedBuilder, path: Tuple[Any, ...] = ()) -> None:
        object.__setattr__(self, "_builder", builder)
        object.__setattr__(self, "_path", tuple(path))

    def _base_at(self):
        return get_at_path(self._builder.base, self._path) if self._path else self._builder.base

    def _overlay_at(self):
        return self._builder.overlays_by_path.get(self._path)

    def _child_view(self, name: str) -> "BuilderEditView":
        return BuilderEditView(self._builder, self._path + (name,))

    def __getattr__(self, name: str):
        if name == "collections":
            return self._builder.collections_by_path.setdefault(self._path, {})

        overlay = self._overlay_at()
        base = self._base_at()

        if hasattr(base, name) or (overlay is not None and hasattr(overlay, name)):
            val = getattr(overlay, name) if (overlay is not None and hasattr(overlay, name)) else getattr(base, name)
            is_obj = hasattr(val, "__dict__") or hasattr(val, "__slots__")
            return self._child_view(name) if is_obj else val

        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any):
        if name in BuilderEditView.__slots__ or name.startswith("_"):  # pragma: no cover - defensive
            return object.__setattr__(self, name, value)

        overlay = self._overlay_at()
        base = self._base_at()
        if overlay is not None and hasattr(overlay, name):
            setattr(overlay, name, value)
            return
        if hasattr(base, name):
            setattr(base, name, value)
            return
        if overlay is not None:
            setattr(overlay, name, value)
            return
        setattr(base, name, value)

    def __getitem__(self, key: str):  # pragma: no cover - convenience
        return getattr(self, key)


class CombinedEditHandle:
    """
    One-object facade around a `CombinedBuilder` for ergonomic editing.

    Access attributes directly (write-through), and use nested `.collections`
    to add list/set elements at any node.
    """

    __slots__ = ("_builder", "_root_view")

    def __init__(self, builder: CombinedBuilder) -> None:
        object.__setattr__(self, "_builder", builder)
        object.__setattr__(self, "_root_view", BuilderEditView(builder, ()))

    @property
    def builder(self) -> CombinedBuilder:
        """Return the underlying `CombinedBuilder`."""
        return self._builder

    def __getattr__(self, name: str):
        return getattr(self._root_view, name)

    def __setattr__(self, name: str, value: Any):
        return setattr(self._root_view, name, value)

    def __iter__(self):  # pragma: no cover - defensive
        raise TypeError("CombinedEditHandle is not iterable")


class ElementView:
    """
    Read-only proxy for a collection element that knows its absolute path.

    This proxy allows nested overlay access beneath a collection element, e.g.:

    .. code-block:: python

        for task in combined.view.missionPlan.collections["taskPlan"]:
            print(task.objective.speed)  # specialization under that element
    """

    __slots__ = ("_combined", "_elem", "_path")

    def __init__(self, combined: CombinedSample, elem: Any, path: Tuple[Any, ...]):
        self._combined = combined
        self._elem = elem
        self._path = tuple(path)

    def __getattr__(self, name: str) -> Any:
        # overlays directly at this element node
        sub = self._path + (name,)
        if sub in self._combined.overlays_by_path:
            base_sub = getattr(self._elem, name) if hasattr(self._elem, name) else None
            overlay_sub = self._combined.overlays_by_path[sub]
            return OverlayView(
                base_sub,
                overlay_sub,
                self._combined.collections,
                overlays_by_path=self._combined.overlays_by_path,
                path=sub,
            )

        # direct attribute on the set/list element wrapper
        if hasattr(self._elem, name):
            return getattr(self._elem, name)

        if hasattr(self._elem, "element"):
            elem = self._elem.element
            sub2 = self._path + ("element", name)
            if sub2 in self._combined.overlays_by_path:
                base_sub = getattr(elem, name) if hasattr(elem, name) else None
                overlay_sub = self._combined.overlays_by_path[sub2]
                return OverlayView(
                    base_sub,
                    overlay_sub,
                    self._combined.collections,
                    overlays_by_path=self._combined.overlays_by_path,
                    path=sub2,
                )
            if hasattr(elem, name):
                return getattr(elem, name)

        raise AttributeError(name)


class ElementHandle:
    """
    Tiny writer facade rooted at a collection element node path.

    Provides convenience for choosing specializations and adding nested collections.
    """

    def __init__(self, builder: CombinedBuilder, elem_path: Tuple[Any, ...], base_elem: Any):
        self._b = builder
        self._path = tuple(elem_path)
        self.base = base_elem

    def use_specialization(self, spec_type: type, *, at: Optional[Sequence[str]] = None) -> Any:
        """
        Attach a specialization under the element node.

        Parameters
        ----------
        spec_type : type
            The specialization class to instantiate.
        at : Sequence[str], optional
            Path under this element where the generalization lives (defaults to the only
            generalization if exactly one exists).

        Returns
        -------
        Any
            The specialization object instance for further editing.
        """
        if at is None:
            cmap = classify_obj_by_umaa(self.base)
            gen_paths = [p for p, finfo in cmap.items() if UMAAConcept.GENERALIZATION in finfo.classifications]
            if not gen_paths:
                raise RuntimeError("No generalization found in element; provide 'at='")
            if len(gen_paths) > 1:
                raise RuntimeError(f"Multiple generalizations found {gen_paths}; provide 'at='")
            at = gen_paths[0]
        at = tuple(at)
        spec = spec_type()
        self._b.use_specialization_at(spec, self._path + at)
        return spec

    def ensure_collection(self, name: str, kind: str) -> Any:
        """Ensure a collection under this element node."""
        return self._b.ensure_collection_at(name, kind, self._path)

    @property
    def collections(self) -> Dict[str, Any]:
        """Return the per-node collections bag under this element."""
        return self._b.collections_at(self._path)

    def __getattr__(self, name: str):
        # Delegate unknown attrs to the underlying element
        base = object.__getattribute__(self, "base")
        if hasattr(base, name):
            return getattr(base, name)
        raise AttributeError(name)

    def __setattr__(self, name: str, value):
        # Keep internal fields local; everything else goes to the element
        if name in {"_b", "_path", "base"} or name.startswith("_"):
            return object.__setattr__(self, name, value)
        base = object.__getattribute__(self, "base")
        setattr(base, name, value)


class SetEditor:
    """Convenience API to add/edit elements of a set at a given node path."""

    def __init__(self, builder: CombinedBuilder, node_path: Tuple[Any, ...], set_name: str, element_type: type):
        self._b = builder
        self._node_path = tuple(node_path or ())
        self._name = set_name
        self._elem_type = element_type

    def add_new(self, *, element_id: Optional[Any] = None) -> ElementHandle:
        """Create a new element with (optional) elementID and return its handle."""
        elem = self._elem_type()
        if element_id is None:
            element_id = generate_guid()
        setattr(elem, "elementID", element_id)
        coll = self._b.ensure_collection_at(self._name, "set", self._node_path)
        coll.add(elem)
        elem_path = self._node_path + path_for_set_element(self._name, element_id)
        return ElementHandle(self._b, elem_path, elem)

    def add(self, elem: Any) -> ElementHandle:
        """Add an existing element instance and return its handle."""
        if getattr(elem, "elementID") == NIL_GUID:
            setattr(elem, "elementID", generate_guid())
        element_id = getattr(elem, "elementID")
        coll = self._b.ensure_collection_at(self._name, "set", self._node_path)
        coll.add(elem)
        elem_path = self._node_path + path_for_set_element(self._name, element_id)
        return ElementHandle(self._b, elem_path, elem)


class ListEditor:
    """Convenience API to append/edit elements of a list at a given node path."""

    def __init__(self, builder: CombinedBuilder, node_path: Tuple[Any, ...], list_name: str, element_type: type):
        self._b = builder
        self._node_path = tuple(node_path or ())
        self._name = list_name
        self._elem_type = element_type

    def append_new(self, *, element_id: Optional[Any] = None) -> ElementHandle:
        """Append a new element and return its handle."""
        elem = self._elem_type()
        if element_id is None:
            element_id = generate_guid()
        setattr(elem, "elementID", element_id)
        coll = self._b.ensure_collection_at(self._name, "list", self._node_path)
        coll.append(elem)
        elem_path = self._node_path + path_for_list_element(self._name, element_id)
        return ElementHandle(self._b, elem_path, elem)

    def append(self, elem: Any) -> ElementHandle:
        """Append an existing element instance and return its handle."""
        if getattr(elem, "elementID") == NIL_GUID:
            setattr(elem, "elementID", generate_guid())
        element_id = getattr(elem, "elementID")
        coll = self._b.ensure_collection_at(self._name, "list", self._node_path)
        coll.append(elem)
        elem_path = self._node_path + path_for_list_element(self._name, element_id)
        return ElementHandle(self._b, elem_path, elem)


class ForwardingReaderListener(dds.NoOpDataReaderListener):
    """
    Internal listener installed on the root RTI DataReader.

    - On DATA_AVAILABLE: triggers UMAA assembly via `root_node.poll_once()`,
      then forwards `on_data_available` if enabled by the user's mask.
    - All other reader events are forwarded per the user's mask as-is.
    """

    def __init__(self, adapter: "UmaaReaderAdapter") -> None:
        super().__init__()
        self._adapter = adapter

    # -- Full reader listener surface (modern Connext) -----------------------
    def on_requested_deadline_missed(self, reader, status):
        self._adapter._dispatch("on_requested_deadline_missed", reader, status)

    def on_requested_incompatible_qos(self, reader, status):
        self._adapter._dispatch("on_requested_incompatible_qos", reader, status)

    def on_sample_rejected(self, reader, status):
        self._adapter._dispatch("on_sample_rejected", reader, status)

    def on_liveliness_changed(self, reader, status):
        self._adapter._dispatch("on_liveliness_changed", reader, status)

    def on_data_available(self, reader):
        try:
            self._adapter._root_node.poll_once()
        except Exception:
            pass
        self._adapter._dispatch("on_data_available", reader)

    def on_subscription_matched(self, reader, status):
        self._adapter._dispatch("on_subscription_matched", reader, status)

    def on_sample_lost(self, reader, status):
        self._adapter._dispatch("on_sample_lost", reader, status)

    # Optional reliable-reader diagnostics (available in recent RTI)
    def on_reliable_reader_cache_changed(self, reader, status):
        self._adapter._dispatch("on_reliable_reader_cache_changed", reader, status)

    def on_reliable_reader_activity_changed(self, reader, status):
        self._adapter._dispatch("on_reliable_reader_activity_changed", reader, status)


class UmaaReaderAdapter:
    """
    Adapter that makes a UMAA reader graph feel like an RTI `DataReader`.

    Supports:
    - `set_listener(listener, status_mask)`: full reader listener surface.
    - `read()/take()` -> `(samples, infos)` where `infos[i].valid` can be False (disposes).
    - `read_data()/take_data()` -> valid samples only (no infos).
    - `__getattr__`: delegates unknown attributes to the underlying RTI reader.
    """

    _EVENT_MASKS = {
        # standard reader events
        "on_requested_deadline_missed": getattr(dds.StatusMask, "REQUESTED_DEADLINE_MISSED", dds.StatusMask.NONE),
        "on_requested_incompatible_qos": getattr(dds.StatusMask, "REQUESTED_INCOMPATIBLE_QOS", dds.StatusMask.NONE),
        "on_sample_rejected": getattr(dds.StatusMask, "SAMPLE_REJECTED", dds.StatusMask.NONE),
        "on_liveliness_changed": getattr(dds.StatusMask, "LIVELINESS_CHANGED", dds.StatusMask.NONE),
        "on_data_available": getattr(dds.StatusMask, "DATA_AVAILABLE", dds.StatusMask.NONE),
        "on_subscription_matched": getattr(dds.StatusMask, "SUBSCRIPTION_MATCHED", dds.StatusMask.NONE),
        "on_sample_lost": getattr(dds.StatusMask, "SAMPLE_LOST", dds.StatusMask.NONE),
        # extended (reliable reader)
        "on_reliable_reader_cache_changed": getattr(
            dds.StatusMask, "RELIABLE_READER_CACHE_CHANGED", dds.StatusMask.NONE
        ),
        "on_reliable_reader_activity_changed": getattr(
            dds.StatusMask, "RELIABLE_READER_ACTIVITY_CHANGED", dds.StatusMask.NONE
        ),
    }

    def __init__(self, root_node: "ReaderNode", root_reader: dds.DataReader) -> None:
        self._root_node = root_node
        self._root_reader = root_reader

        # Buffer stores (CombinedSample | None, SampleInfo | None) pairs.
        self._buf = deque()
        self._buf_lock = threading.Lock()

        self._user_listener: Optional[object] = None
        self._user_status_mask: dds.StatusMask = dds.StatusMask.NONE

        # Parent notify from the root UMAA node writes into our buffer.
        def _on_ready(_key: Any, combined: Optional[CombinedSample], info: Optional[object]) -> None:
            with self._buf_lock:
                self._buf.append((combined, info))
            if self._user_listener and (self._user_status_mask & dds.StatusMask.DATA_AVAILABLE):
                cb = getattr(self._user_listener, "on_data_available", None)
                if callable(cb):
                    try:
                        cb(self)
                    except Exception:
                        pass

        self._root_node.parent_notify = _on_ready

        # Install our forwarding listener on the root reader; children keep their own.
        self._internal_listener = ForwardingReaderListener(self)
        self._install_internal_listener()

    def set_listener(self, listener: Optional[object], status_mask: dds.StatusMask = dds.StatusMask.NONE) -> None:
        """
        Register a user `DataReaderListener`-like object with a status mask.

        The listener is *not* installed on the underlying root reader; instead,
        we install a single internal listener and forward events to user callbacks
        after UMAA assembly when appropriate (e.g., on_data_available).
        """
        self._user_listener = listener
        self._user_status_mask = status_mask or dds.StatusMask.NONE

    def read(self):
        """
        Return a snapshot of buffered records **without clearing**.

        Returns
        -------
        (samples, infos) : (list, list)
            `samples[i]` is a `CombinedSample` or `None` if `infos[i].valid == False`.
        """
        with self._buf_lock:
            pairs = list(self._buf)
        samples = [p[0] for p in pairs]
        infos = [p[1] for p in pairs]
        return samples, infos

    def take(self):
        """
        Return and clear buffered records.

        Returns
        -------
        (samples, infos) : (list, list)
        """
        with self._buf_lock:
            pairs = list(self._buf)
            self._buf.clear()
        samples = [p[0] for p in pairs]
        infos = [p[1] for p in pairs]
        return samples, infos

    def read_data(self):
        """Return valid `CombinedSample`s only (no infos), without clearing."""
        samples, infos = self.read()
        out = []
        for s, info in zip(samples, infos):
            if info is None or getattr(info, "valid", True):
                if s is not None:
                    out.append(s)
        return out

    def take_data(self):
        """Return and clear valid `CombinedSample`s only (no infos)."""
        samples, infos = self.take()
        out = []
        for s, info in zip(samples, infos):
            if info is None or getattr(info, "valid", True):
                if s is not None:
                    out.append(s)
        return out

    @property
    def raw_reader(self) -> dds.DataReader:
        """Access the underlying RTI DataReader."""
        return self._root_reader

    def __getattr__(self, name: str):
        return getattr(self._root_reader, name)

    def _install_internal_listener(self) -> None:
        try:
            self._root_reader.set_listener(self._internal_listener, dds.StatusMask.ALL)
        except Exception:
            # Conservative fallback
            self._root_reader.set_listener(self._internal_listener, dds.StatusMask.DATA_AVAILABLE)

    def _dispatch(self, method_name: str, reader, *args) -> None:
        if not self._user_listener:
            return
        mask_required = self._EVENT_MASKS.get(method_name, dds.StatusMask.NONE)
        if mask_required is dds.StatusMask.NONE:
            return
        if not (self._user_status_mask & mask_required):
            return
        cb = getattr(self._user_listener, method_name, None)
        if not callable(cb):
            return
        try:
            cb(self, *args)
        except Exception:
            pass


class UmaaFilteredReaderAdapter(UmaaReaderAdapter):
    """
    Like :class:`UmaaReaderAdapter`, but for a root bound to a `ContentFilteredTopic`.
    """

    def __init__(self, root_node: "ReaderNode", root_reader: dds.DataReader, cft: dds.ContentFilteredTopic) -> None:
        super().__init__(root_node, root_reader)
        self._cft = cft

    def topic_name(self) -> str:
        """Return the filtered topic name."""
        return self._cft.name

    @property
    def content_filtered_topic(self) -> dds.ContentFilteredTopic:
        """Access the root `ContentFilteredTopic`."""
        return self._cft


class ForwardingWriterListener(dds.NoOpDataWriterListener):
    """
    Internal listener installed on each RTI DataWriter in the UMAA writer tree.

    Forwards events to :class:`UmaaWriterAdapter`, which filters per the user's mask.
    """

    def __init__(self, adapter: "UmaaWriterAdapter") -> None:
        super().__init__()
        self._adapter = adapter

    def on_offered_deadline_missed(self, writer, status):
        self._adapter._dispatch("on_offered_deadline_missed", writer, status)

    def on_offered_incompatible_qos(self, writer, status):
        self._adapter._dispatch("on_offered_incompatible_qos", writer, status)

    def on_liveliness_lost(self, writer, status):
        self._adapter._dispatch("on_liveliness_lost", writer, status)

    def on_publication_matched(self, writer, status):
        self._adapter._dispatch("on_publication_matched", writer, status)

    def on_reliable_writer_cache_changed(self, writer, status):
        self._adapter._dispatch("on_reliable_writer_cache_changed", writer, status)

    def on_reliable_reader_activity_changed(self, writer, status):
        self._adapter._dispatch("on_reliable_reader_activity_changed", writer, status)

    def on_instance_replaced(self, writer, handle):
        self._adapter._dispatch("on_instance_replaced", writer, handle)


class UmaaWriterAdapter:
    """
    Adapter that makes a UMAA writer graph feel like an RTI `DataWriter`.

    Supports:
    - `.new()` returning a `CombinedBuilder`,
    - `.new_combined(...)` returning a `CombinedEditHandle` (single-object facade),
    - `.write(...)` accepting either, splitting and linking metadata automatically,
    - `set_listener(listener, status_mask)` forwarding DataWriter events after internal handling,
    - `__getattr__` delegation to the root writer.
    """

    _EVENT_MASKS = {
        "on_offered_deadline_missed": getattr(dds.StatusMask, "OFFERED_DEADLINE_MISSED", dds.StatusMask.NONE),
        "on_offered_incompatible_qos": getattr(dds.StatusMask, "OFFERED_INCOMPATIBLE_QOS", dds.StatusMask.NONE),
        "on_liveliness_lost": getattr(dds.StatusMask, "LIVELINESS_LOST", dds.StatusMask.NONE),
        "on_publication_matched": getattr(dds.StatusMask, "PUBLICATION_MATCHED", dds.StatusMask.NONE),
        "on_reliable_writer_cache_changed": getattr(
            dds.StatusMask, "RELIABLE_WRITER_CACHE_CHANGED", dds.StatusMask.NONE
        ),
        "on_reliable_reader_activity_changed": getattr(
            dds.StatusMask, "RELIABLE_READER_ACTIVITY_CHANGED", dds.StatusMask.NONE
        ),
        "on_instance_replaced": getattr(dds.StatusMask, "INSTANCE_REPLACED", dds.StatusMask.NONE),
    }

    def __init__(self, root_node: "WriterNode", top_level: "TopLevelWriter", root_writer: dds.DataWriter) -> None:
        self._root_node = root_node
        self._top = top_level
        self._root_writer = root_writer
        self._user_listener: Optional[object] = None
        self._user_status_mask: dds.StatusMask = dds.StatusMask.NONE

        self._internal_listener = ForwardingWriterListener(self)
        self._install_internal_listeners()

    def new(self) -> CombinedBuilder:
        """Create a new `CombinedBuilder` using the top-level writer's base factory."""
        return self._top.new()

    def new_combined(
        self,
        *,
        spec_at: Optional[Sequence[str]] = None,
        spec_type: Optional[type] = None,
        auto_init_collections: bool = True,
    ) -> CombinedEditHandle:
        """
        Create a one-object editable combined handle with an optional nested specialization
        and pre-created collections at that path.

        Parameters
        ----------
        spec_at : Sequence[str], optional
            Path (e.g., ``('objective',)``) where a generalization lives.
        spec_type : type, optional
            Specialization class to instantiate at `spec_at`.
        auto_init_collections : bool, default True
            If True, pre-create list/set collections on the specialization path.

        Returns
        -------
        CombinedEditHandle
            The editable facade around a `CombinedBuilder`.
        """
        b = self._top.new()

        if auto_init_collections:
            cmap_base = classify_obj_by_umaa(b.base)
            for path, finfo in cmap_base.items():
                if UMAAConcept.LARGE_SET in finfo.classifications:
                    name = path[-1][: -len("SetMetadata")]
                    b.ensure_collection_at(name, "set", path)
                if UMAAConcept.LARGE_LIST in finfo.classifications:
                    name = path[-1][: -len("ListMetadata")]
                    b.ensure_collection_at(name, "list", path)

        if spec_at is not None and spec_type is not None:
            spec = spec_type()
            p = tuple(spec_at)
            b.use_specialization_at(spec, p)

        return CombinedEditHandle(b)

    def write(self, builder_or_handle: Any) -> None:
        """
        Publish a combined sample.

        Parameters
        ----------
        builder_or_handle : CombinedBuilder or CombinedEditHandle
            The combined builder or its editable façade.

        Notes
        -----
        Splits and links metadata automatically via the UMAA writer graph.
        """
        if isinstance(builder_or_handle, CombinedEditHandle):
            builder = builder_or_handle.builder
        else:
            builder = builder_or_handle
        self._top.write(builder)

    def set_listener(self, listener: Optional[object], status_mask: dds.StatusMask = dds.StatusMask.NONE) -> None:
        """
        Register a user `DataWriterListener`-like object with a status mask.

        The listener is not attached to the underlying RTI writers; instead, we
        attach a single internal listener across the UMAA tree and forward events
        to the user after internal business logic.

        Parameters
        ----------
        listener : object or None
            Listener object implementing relevant `on_*` methods.
        status_mask : StatusMask
            Mask selecting which events to forward.
        """
        self._user_listener = listener
        self._user_status_mask = status_mask or dds.StatusMask.NONE

    def topic_name(self) -> str:
        """Return the topic name of the root writer."""
        return self._root_writer.topic.name

    @property
    def raw_writer(self) -> dds.DataWriter:
        """Access the underlying RTI DataWriter."""
        return self._root_writer

    def editor_for_set(
        self, handle_or_builder: Any, path: Sequence[Any], set_name: str, element_type: type
    ) -> SetEditor:
        """
        Create a :class:`SetEditor` rooted at a node path.

        Parameters
        ----------
        handle_or_builder : CombinedEditHandle or CombinedBuilder
            Source builder or handle.
        path : Sequence[Any]
            Absolute node path.
        set_name : str
            Logical set name (e.g. "taskPlan").
        element_type : type
            Element class.

        Returns
        -------
        SetEditor
        """
        b, node_path = self._extract_builder_and_node_path(handle_or_builder, path)
        return SetEditor(b, node_path, set_name, element_type)

    def editor_for_list(
        self, handle_or_builder: Any, path: Sequence[Any], list_name: str, element_type: type
    ) -> ListEditor:
        """
        Create a :class:`ListEditor` rooted at a node path.

        See Also
        --------
        editor_for_set : sibling helper for sets.
        """
        b, node_path = self._extract_builder_and_node_path(handle_or_builder, path)
        return ListEditor(b, node_path, list_name, element_type)

    def _extract_builder_and_node_path(self, src, path):
        """Return (CombinedBuilder, absolute_node_path_tuple) from a handle or builder."""
        if hasattr(src, "builder"):
            b = src.builder
        elif hasattr(src, "_b"):
            b = src._b
        else:
            b = src

        base = getattr(src, "_path", ())
        p = tuple(path or ())
        node_path = tuple(base) + p
        return b, node_path

    def _install_internal_listeners(self) -> None:
        mask = dds.StatusMask.ALL
        for w in self._walk_writers(self._root_node):
            try:
                w.set_listener(self._internal_listener, mask)
            except Exception:
                pass

    def _walk_writers(self, node: "WriterNode"):
        yield node.writer
        decorators = getattr(node, "_decorators", {}) or {}
        for deco in decorators.values():
            children = getattr(deco, "_children", {}) or {}
            for child in children.values():
                yield from self._walk_writers(child)

    def _dispatch(self, method_name: str, writer, arg) -> None:
        if not self._user_listener:
            return
        mask_required = self._EVENT_MASKS.get(method_name, dds.StatusMask.NONE)
        if mask_required is dds.StatusMask.NONE:
            return
        if not (self._user_status_mask & mask_required):
            return
        cb = getattr(self._user_listener, method_name, None)
        if not callable(cb):
            return
        try:
            cb(self, arg)
        except Exception:
            pass

    def __getattr__(self, name: str):
        return getattr(self._root_writer, name)
