from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING
from collections import deque
import threading

import rti.connextdds as dds

from umaapy.util.umaa_utils import NumericGUID, HashableNumericGUID

if TYPE_CHECKING:
    from umaapy.util.multi_topic_reader import ReaderNode
    from umaapy.util.multi_topic_writer import WriterNode, TopLevelWriter


def _attach_collections_attr(base: Any, collections: Dict[str, Any]) -> None:
    """Best-effort: attach a live 'collections' dict onto the IDL object.
    Some generated types might disallow dynamic attributes—silently ignore in that case.
    """
    try:
        setattr(base, "collections", collections)
    except Exception:
        # Slots or read-only object—skip attaching but keep dict in Combined*.
        pass


def guid_key(value: Any) -> Any:
    """Return a hashable key for UMAA NumericGUID-like values."""
    if isinstance(value, HashableNumericGUID):
        return value
    if isinstance(value, NumericGUID):
        return HashableNumericGUID(value)
    return value


def guid_equal(a: Any, b: Any) -> bool:
    """Robust GUID comparison across (NumericGUID|HashableNumericGUID|other)."""
    ak = guid_key(a)
    bk = guid_key(b)
    try:
        return ak == bk
    except Exception:
        return a == b


def get_at_path(obj: object, path: Sequence[str]) -> object:
    cur = obj
    for seg in path:
        cur = getattr(cur, seg)
    return cur


def set_at_path(root: object, path: Sequence[str], value: object) -> None:
    if not path:
        raise ValueError("Empty path not supported for set_at_path")
    parent = get_at_path(root, path[:-1])
    setattr(parent, path[-1], value)


class OverlayView:
    """A read-only attribute proxy that prefers the overlay object over the base.

    Access order:
      1) If overlay is set and has attribute 'name' -> return overlay.name
      2) Else if base has attribute 'name' -> return base.name
      3) Else if 'name' exists in collections -> return collections[name]
      4) Else AttributeError

    Notes
    -----
    - This is intentionally lightweight: no mutation, no descriptor forwarding.
    - Use `CombinedSample.view` or `CombinedBuilder.view` to get an OverlayView.
    """

    __slots__ = ("_base", "_overlay", "_collections", "_overlays_by_path", "_path")

    def __init__(
        self,
        base: Any,
        overlay: Optional[Any],
        collections: Mapping[str, Any],
        overlays_by_path: Mapping[Tuple[str, ...], Any] = (),
        path: Tuple[str, ...] = (),
    ) -> None:
        self._base = base
        self._overlay = overlay
        self._collections = collections
        self._overlays_by_path = dict(overlays_by_path or {})
        self._path = tuple(path or ())

    def __getattr__(self, name: str) -> Any:
        # 1) Nested overlay registered for this next hop?
        sub_path = self._path + (name,)
        if sub_path in self._overlays_by_path:
            base_sub = getattr(self._base, name) if hasattr(self._base, name) else None
            overlay_sub = self._overlays_by_path[sub_path]
            # Return another OverlayView *scoped* to sub_path
            return OverlayView(
                base=base_sub,
                overlay=overlay_sub,
                collections=self._collections,
                overlays_by_path=self._overlays_by_path,
                path=sub_path,
            )

        # 2) Top-level overlay takes precedence if it has the attribute
        if self._overlay is not None and hasattr(self._overlay, name):
            return getattr(self._overlay, name)

        # 3) Fall back to base
        if hasattr(self._base, name):
            return getattr(self._base, name)

        # 4) Allow dot access to collections
        if name in self._collections:
            return self._collections[name]

        raise AttributeError(name)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass(frozen=True)
class CombinedSample:
    """Immutable runtime view used on the READER side.

    Parameters
    ----------
    base :
        The base IDL object for this node (e.g., a metadata or generalization sample).
    collections :
        A dict of UMAA collection projections (LargeSet / LargeList) keyed by logical name,
        e.g., `{"waypoints": [...], "conditionals": [...]}`.
    overlay :
        Optional specialization IDL object; when present, its attributes take precedence at read time.

    Behavior
    --------
    - Tries to attach `collections` to the base object (best-effort).
    - `view` returns an `OverlayView` that resolves attributes with specialization > base precedence.
    """

    base: Any
    collections: Dict[str, Any] = field(default_factory=dict)
    overlay: Optional[Any] = None  # legacy top-level overlay
    overlays_by_path: Dict[Tuple[str, ...], Any] = field(default_factory=dict)  # NEW

    def __post_init__(self):
        _attach_collections_attr(self.base, self.collections)

    @property
    def view(self) -> OverlayView:
        return OverlayView(
            self.base,
            self.overlay,
            self.collections,
            overlays_by_path=self.overlays_by_path,
            path=(),
        )

    def with_overlay(self, overlay: Any) -> "CombinedSample":
        return CombinedSample(
            base=self.base,
            collections=self.collections,
            overlay=overlay,
            overlays_by_path=self.overlays_by_path,
        )

    def clone_with_collections(self, updates: Mapping[str, Any]) -> "CombinedSample":
        new = dict(self.collections)
        new.update(updates)
        return CombinedSample(
            base=self.base,
            collections=new,
            overlay=self.overlay,
            overlays_by_path=self.overlays_by_path,
        )

    def add_overlay_at(self, path: Sequence[str], overlay_obj: Any) -> "CombinedSample":
        new_overlays = dict(self.overlays_by_path)
        new_overlays[tuple(path)] = overlay_obj
        return CombinedSample(
            base=self.base,
            collections=self.collections,
            overlay=self.overlay,
            overlays_by_path=new_overlays,
        )


class SetCollection:
    """Writer-side set-like container with deterministic identity by key.

    Usage
    -----
    coll = SetCollection()
    coll.add(elem)  # requires elem.elementID
    coll.add(elem2, key=some_uuid)
    list(coll) -> [elem, elem2]
    """

    __slots__ = ("_elems",)

    def __init__(self) -> None:
        self._elems: Dict[Any, Any] = {}

    def add(self, elem: Any, *, key: Optional[Any] = None) -> None:
        k = key if key is not None else getattr(elem, "elementID", None)
        if k is None:
            raise ValueError("Set element must provide 'elementID' or explicit 'key'.")
        self._elems[k] = elem

    def discard(self, key: Any) -> None:
        self._elems.pop(key, None)

    def clear(self) -> None:
        self._elems.clear()

    def __contains__(self, key: Any) -> bool:
        return key in self._elems

    def __len__(self) -> int:
        return len(self._elems)

    def __iter__(self) -> Iterable[Any]:
        return iter(self._elems.values())

    def keys(self) -> Iterable[Any]:
        return self._elems.keys()

    def values(self) -> Iterable[Any]:
        return self._elems.values()

    def items(self) -> Iterable[tuple[Any, Any]]:
        return self._elems.items()

    # Reader parity: easy conversion to a simple runtime structure
    def to_runtime(self) -> List[Any]:
        return list(self._elems.values())


class ListCollection:
    """Writer-side list-like container with explicit order.

    Usage
    -----
    coll = ListCollection()
    coll.append(elem)
    coll.insert(0, first)
    list(coll) -> [first, elem]
    """

    __slots__ = ("_items",)

    def __init__(self) -> None:
        self._items: List[Any] = []

    def append(self, elem: Any) -> None:
        self._items.append(elem)

    def extend(self, elems: Iterable[Any]) -> None:
        self._items.extend(elems)

    def insert(self, idx: int, elem: Any) -> None:
        self._items.insert(idx, elem)

    def pop(self, idx: int = -1) -> Any:
        return self._items.pop(idx)

    def clear(self) -> None:
        self._items.clear()

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterable[Any]:
        return iter(self._items)

    def __getitem__(self, idx: int) -> Any:
        return self._items[idx]

    def __setitem__(self, idx: int, value: Any) -> None:
        self._items[idx] = value

    def to_runtime(self) -> List[Any]:
        return list(self._items)


@dataclass
class CombinedBuilder:
    """Mutable builder used on the WRITER side.

    Parameters
    ----------
    base :
        The base IDL object for this node (e.g., metadata/generalization sample).
    collections :
        A dict (name -> SetCollection/ListCollection). Use `ensure_collection(...)`.
    overlay :
        Optional specialization IDL object. On publish, writer decorators will:
          1) publish `overlay` to its specialization topic, then
          2) bind the base generalization to that specialization (topic/ID/timestamp), then
          3) publish the base.

    Convenience
    -----------
    - `view` provides the same OverlayView semantics as CombinedSample for inspection/debugging.
    - We also attempt to attach the live `collections` dict to `base` so users may do:
        `builder.base.collections["waypoints"].append(...)`
    """

    base: Any
    collections: Dict[str, Any] = field(default_factory=dict)
    overlay: Optional[Any] = None  # legacy top-level
    overlays_by_path: Dict[Tuple[str, ...], Any] = field(default_factory=dict)  # NEW

    def __post_init__(self):
        _attach_collections_attr(self.base, self.collections)

    def ensure_collection(self, name: str, kind: str) -> Any: ...

    def use_specialization(self, spec_obj: Any) -> None:
        self.overlay = spec_obj

    def use_specialization_at(self, path: Sequence[str], spec_obj: Any) -> None:
        self.overlays_by_path[tuple(path)] = spec_obj

    @property
    def view(self) -> OverlayView:
        return OverlayView(
            self.base,
            self.overlay,
            self.collections,
            overlays_by_path=self.overlays_by_path,
            path=(),
        )


class UmaaReaderAdapter:
    """Adapter that makes a UMAA Reader graph feel like an RTI DataReader.

    - set_listener(listener, status_mask): stores a user listener and dispatches
      on_data_available AFTER UMAA assembly completes.
    - read/take and read_data/take_data return assembled CombinedSample objects.
    - Unknown attributes delegate to the underlying RTI DataReader via __getattr__.
    """

    def __init__(self, root_node: "ReaderNode", root_reader: dds.DataReader) -> None:
        self._root_node = root_node
        self._root_reader = root_reader
        self._buf = deque()
        self._buf_lock = threading.Lock()
        self._user_listener: Optional[object] = None
        self._user_status_mask: dds.StatusMask = dds.StatusMask.NONE

        def _on_ready(_key: Any, combined: CombinedSample) -> None:
            # Buffer assembled sample
            with self._buf_lock:
                self._buf.append(combined)

            # After internal business logic, invoke user listener if requested
            if self._user_listener and (self._user_status_mask & dds.StatusMask.DATA_AVAILABLE):
                on_data = getattr(self._user_listener, "on_data_available", None)
                if callable(on_data):
                    try:
                        on_data(self)  # pass THIS adapter so user can call read()/take()
                    except Exception:
                        pass

        # Node emits completed samples via parent_notify
        self._root_node.parent_notify = _on_ready

    def topic_name(self) -> str:
        return self._root_reader.topic.name

    def set_listener(self, listener: Optional[object], status_mask: dds.StatusMask = dds.StatusMask.NONE) -> None:
        """Store a user listener; we do NOT attach it to the underlying RTI reader."""
        self._user_listener = listener
        self._user_status_mask = status_mask or dds.StatusMask.NONE

    def read(self):
        with self._buf_lock:
            return list(self._buf)

    def take(self):
        with self._buf_lock:
            out = list(self._buf)
            self._buf.clear()
            return out

    def read_data(self):
        return self.read()

    def take_data(self):
        return self.take()

    @property
    def raw_reader(self) -> dds.DataReader:
        return self._root_reader

    def __getattr__(self, name: str):
        return getattr(self._root_reader, name)


class UmaaFilteredReaderAdapter(UmaaReaderAdapter):
    """Like UmaaReaderAdapter, but for a root bound to a ContentFilteredTopic."""

    def __init__(self, root_node: "ReaderNode", root_reader: dds.DataReader, cft: dds.ContentFilteredTopic) -> None:
        super().__init__(root_node, root_reader)
        self._cft = cft

    def topic_name(self) -> str:
        return self._cft.name

    @property
    def content_filtered_topic(self) -> dds.ContentFilteredTopic:
        return self._cft


class ForwardingWriterListener(dds.NoOpDataWriterListener):  # type: ignore[attr-defined]
    """Internal listener installed on each RTI DataWriter in the UMAA writer tree.
    It forwards events to a UmaaWriterAdapter (which filters by the user's status mask).
    """

    def __init__(self, adapter: "UmaaWriterAdapter") -> None:
        super().__init__()
        self._adapter = adapter

    # Common DataWriterListener methods
    def on_offered_deadline_missed(self, writer, status):
        self._adapter._dispatch("on_offered_deadline_missed", writer, status)

    def on_offered_incompatible_qos(self, writer, status):
        self._adapter._dispatch("on_offered_incompatible_qos", writer, status)

    def on_liveliness_lost(self, writer, status):
        self._adapter._dispatch("on_liveliness_lost", writer, status)

    def on_publication_matched(self, writer, status):
        self._adapter._dispatch("on_publication_matched", writer, status)

    # Optional/extension events (guarded by StatusMask mapping in adapter)
    def on_reliable_writer_cache_changed(self, writer, status):
        self._adapter._dispatch("on_reliable_writer_cache_changed", writer, status)

    def on_reliable_reader_activity_changed(self, writer, status):
        self._adapter._dispatch("on_reliable_reader_activity_changed", writer, status)

    def on_instance_replaced(self, writer, handle):
        self._adapter._dispatch("on_instance_replaced", writer, handle)


class UmaaWriterAdapter:
    """Adapter that makes a UMAA Writer graph feel like an RTI DataWriter.

    - new()/write() delegate to the TopLevelWriter (fan-out to children, then write base).
    - set_listener(listener, status_mask) registers a user DataWriterListener;
      we install internal listeners on *all* writers we own and invoke the user only after filtering by mask.
    - Unknown attributes delegate to the underlying RTI DataWriter via __getattr__.
    """

    _EVENT_MASKS = {
        "on_offered_deadline_missed": dds.StatusMask.OFFERED_DEADLINE_MISSED,
        "on_offered_incompatible_qos": dds.StatusMask.OFFERED_INCOMPATIBLE_QOS,
        "on_liveliness_lost": dds.StatusMask.LIVELINESS_LOST,
        "on_publication_matched": dds.StatusMask.PUBLICATION_MATCHED,
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

        # Install a single internal listener object across the entire writer tree
        self._internal_listener = ForwardingWriterListener(self)
        self._install_internal_listeners()

    def new(self):
        return self._top.new()

    def write(self, builder):
        self._top.write(builder)
        # DDS will generate events; our internal listener will forward appropriately.

    def set_listener(self, listener: Optional[object], status_mask: dds.StatusMask = dds.StatusMask.NONE) -> None:
        """Store a user listener; we do NOT attach it to the underlying RTI writers."""
        self._user_listener = listener
        self._user_status_mask = status_mask or dds.StatusMask.NONE

    def topic_name(self) -> str:
        return self._root_writer.topic.name

    @property
    def raw_writer(self) -> dds.DataWriter:
        return self._root_writer

    # Delegate unknown attributes to the underlying RTI writer
    def __getattr__(self, name: str):
        return getattr(self._root_writer, name)

    def _install_internal_listeners(self) -> None:
        """Attach our internal listener to every DataWriter in the UMAA writer tree."""
        mask = dds.StatusMask.ANY  # listen to everything; we filter by user's mask in _dispatch()
        for w in self._walk_writers(self._root_node):
            try:
                w.set_listener(self._internal_listener, mask)
            except Exception:
                # Some writers may be shared/externally managed—fail soft
                pass

    def _walk_writers(self, node: "WriterNode"):
        """Yield all RTI DataWriter objects owned by the writer graph."""
        # Yield this node's writer
        yield node.writer
        # Recurse into children attached on decorators
        decorators = getattr(node, "_decorators", {}) or {}
        for deco in decorators.values():
            children = getattr(deco, "_children", {}) or {}
            for child in children.values():
                # child is a WriterNode
                yield from self._walk_writers(child)

    def _dispatch(self, method_name: str, writer, arg) -> None:
        """Forward an internal event to the user listener if their mask includes it."""
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
