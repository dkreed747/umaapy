from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol


import rti.connextdds as dds


from umaapy.util.multi_topic_support import CombinedSample


class AssemblySignal:
    """Decorator -> Node message that a given key has reached 'complete' for this decorator."""

    __slots__ = ("key", "complete")

    def __init__(self, key: Any, complete: bool = True) -> None:
        self.key = key
        self.complete = complete


class ReaderDecorator:
    """Abstract base for UMAA-specific reader decorators.

    A decorator:
      - Mutates the provided CombinedSample in-place (collections / overlay).
      - Yields AssemblySignal when its UMAA atomic-completeness is satisfied for 'key'.
      - Can buffer child/parent arrivals out-of-order.
    """

    name: str = "base"
    required: bool = True

    # The node injects its child nodes via this dict (name -> ReaderNode)
    children: Dict[str, "ReaderNode"]

    def attach_children(self, **children: "ReaderNode") -> None:
        if not hasattr(self, "children"):
            self.children = {}
        self.children.update(children)

    def on_reader_data(
        self, node: "ReaderNode", key: Any, combined: CombinedSample, sample: Any
    ) -> Iterable[AssemblySignal]:
        """Handle a sample published on THIS node's topic (the base)."""
        return ()

    def on_child_assembled(
        self, node: "ReaderNode", child_name: str, key: Any, assembled: CombinedSample
    ) -> Iterable[AssemblySignal]:
        """Handle an assembled sample from a CHILD node (e.g., set/list element, specialization)."""
        return ()


class ReaderNode:
    """A graph node that owns ONE RTI DataReader and zero or more UMAA reader decorators.

    Flow
    ----
    - RTI listener (or polling) pulls valid data samples from the DataReader.
    - For each sample: key = key_fn(sample); combined = new_base_fn(sample)
    - Each registered decorator processes the sample (and any buffered child data),
      mutates `combined` and yields AssemblySignal when its own completeness is satisfied.
    - When all REQUIRED decorators for 'key' have completed, the node emits `parent_notify(key, combined)`.

    Threading
    ---------
    - RTI calls listener callbacks from internal threads. We guard state with a re-entrant lock.
    - If you prefer polling, call `poll_once()` periodically instead of using listeners.
    """

    def __init__(
        self,
        reader: dds.DataReader,
        key_fn: Callable[[Any], Any],
        new_base_fn: Callable[[Any], CombinedSample],
        parent_notify: Optional[Callable[[Any, CombinedSample], None]] = None,
        use_listener: bool = True,
    ) -> None:
        self.reader = reader
        self.key_fn = key_fn
        self.new_base_fn = new_base_fn
        self.parent_notify = parent_notify

        self.decorators: Dict[str, ReaderDecorator] = {}

        # per-key coordination
        self._complete_by_key: Dict[Any, Dict[str, bool]] = {}
        self._combined_by_key: Dict[Any, CombinedSample] = {}

        self._lock = threading.RLock()

        if use_listener:
            self._install_listener()

    def register_decorator(self, name: str, decorator: ReaderDecorator, required: bool = True) -> None:
        """Attach a UMAA reader decorator to this node."""
        decorator.name = name
        decorator.required = required
        self.decorators[name] = decorator

    def attach_child(self, decorator_name: str, child_name: str, child_node: "ReaderNode") -> None:
        """Let a decorator own a CHILD reader node and receive its assembled outputs."""
        self.decorators[decorator_name].attach_children(**{child_name: child_node})
        child_node.parent_notify = lambda key, combined: self._on_child_assembled(
            decorator_name, child_name, key, combined
        )

    def poll_once(self) -> int:
        """Pull samples synchronously (alternative to listener). Returns count of processed valid samples."""
        count = 0
        for data in _yield_valid_data(self.reader):
            self._process_reader_sample(data)
            count += 1
        return count

    def _install_listener(self) -> None:
        node = self

        class _Listener(dds.NoOpDataReaderListener):
            def on_data_available(self, reader: dds.DataReader) -> None:
                for data in _yield_valid_data(reader):
                    node._process_reader_sample(data)

        self.reader.set_listener(_Listener(), dds.StatusMask.DATA_AVAILABLE)

    def _process_reader_sample(self, sample: Any) -> None:
        key = self.key_fn(sample)
        combined = self.new_base_fn(sample)

        with self._lock:
            self._combined_by_key[key] = combined

            # Let each decorator see the base sample
            for deco in self.decorators.values():
                for sig in deco.on_reader_data(self, key, combined, sample):
                    self._mark_complete_and_maybe_emit(key, deco.name, sig.complete)

            # In case decorators completed earlier due to child-first buffering
            self._maybe_emit(key)

    def _on_child_assembled(self, decorator_name: str, child_name: str, key: Any, combined: CombinedSample) -> None:
        """Child node bubbled an assembled sample; route only to the owning decorator."""
        deco = self.decorators[decorator_name]

        with self._lock:
            for sig in deco.on_child_assembled(self, child_name, key, combined):
                self._mark_complete_and_maybe_emit(sig.key, decorator_name, sig.complete)

    def _mark_complete_and_maybe_emit(self, key: Any, decorator_name: str, complete: bool) -> None:
        per = self._complete_by_key.setdefault(key, {})
        per[decorator_name] = bool(complete)
        self._maybe_emit(key)

    def _maybe_emit(self, key: Any) -> None:
        combined = self._combined_by_key.get(key)
        if combined is None:
            return

        # all required decorators done?
        for name, deco in self.decorators.items():
            if deco.required and not self._complete_by_key.get(key, {}).get(name, False):
                return

        # emit upstream
        if self.parent_notify:
            self.parent_notify(key, combined)

        # clear one-shot data for this key
        self._complete_by_key.pop(key, None)
        self._combined_by_key.pop(key, None)


def _yield_valid_data(reader: dds.DataReader) -> Iterable[Any]:
    """Yield valid data samples from an RTI DataReader with a robust fallback chain."""
    # Fast paths (only valid data)
    for attr in ("take_data", "read_data"):
        fn = getattr(reader, attr, None)
        if callable(fn):
            try:
                seq = fn()
                for data in seq:
                    yield data
                return
            except Exception:
                pass

    # Loaned samples fallback
    for attr in ("take", "read"):
        fn = getattr(reader, attr, None)
        if callable(fn):
            try:
                with fn() as samples:
                    for s in samples:
                        info = getattr(s, "info", None)
                        if info is None or getattr(info, "valid", False):
                            yield s.data
                return
            except Exception:
                pass

    # As a last resort, do nothing (no data)
    return
