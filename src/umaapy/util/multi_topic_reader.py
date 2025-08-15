"""
UMAA reader graph runtime: ReaderNode, AssemblySignal, and ReaderDecorator base.

A `ReaderNode` wraps a single RTI `DataReader`. Decorators attached to a node
(e.g., generalization/specialization, large sets/lists) consume raw samples and
emit assembled `CombinedSample` objects upwards when their completion rules are met.

The graph supports arbitrary nesting: parents attach children per concept:
- one-to-many specializations under a generalization (role "gen_spec")
- element readers under large sets/lists (role = set/list logical name)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, List
import types
import inspect
import logging

from umaapy.util.multi_topic_support import CombinedSample

import rti.connextdds as dds

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssemblySignal:
    """
    Signal returned by a decorator to indicate assembly progress at a node.

    Parameters
    ----------
    key : Any
        The parent node's key for the in-progress assembled sample.
    complete : bool
        If True, the node should notify its parent that a complete combined sample is ready.
    """

    key: Any
    complete: bool = False


class ReaderDecorator:
    """
    Base class for reader-side UMAA decorators.

    Override `on_reader_data` to consume a base sample and optionally emit
    completion signals. Override `on_child_assembled` to receive assembled
    samples from child nodes (specializations, set/list elements).
    """

    name: str = ""

    def attach_children(self, **children: "ReaderNode") -> None:
        """Receive child node mapping (topic or alias -> ReaderNode)."""
        self.children = children

    def on_reader_data(
        self,
        node: "ReaderNode",
        key: Any,
        combined: CombinedSample,
        sample: Any,
    ) -> Iterable[AssemblySignal]:
        """Handle a base sample arriving at this node. Default: no-op."""
        return ()

    def on_child_assembled(
        self,
        node: "ReaderNode",
        child_name: str,
        key: Any,
        assembled: CombinedSample,
    ) -> Iterable[AssemblySignal]:
        """Handle a child node emitting an assembled sample. Default: no-op."""
        return ()


class ReaderNode:
    """
    Reader graph node that wraps a single RTI `DataReader`.

    Parameters
    ----------
    reader : dds.DataReader
        RTI reader for this node's topic.
    key_fn : Callable[[Any], Any]
        Function to derive a node-local assembly key from a raw sample (default: `id(sample)`).
    parent_notify : Callable[[Any, CombinedSample | None, Any | None], None], optional
        Callback invoked when this node completes an assembled combined sample **or**
        when an invalid/dispose arrives (combined=None), supplying the root `SampleInfo`.
    use_listener : bool, default True
        If True, install an internal listener to poll automatically; otherwise,
        an external adapter listener may drive polling on data-available.
    """

    def __init__(
        self,
        reader: dds.DataReader,
        key_fn: Callable[[Any], Any] = id,
        parent_notify: Optional[Callable[[Any, Optional[CombinedSample], Optional[object]], None]] = None,
        use_listener: bool = True,
    ) -> None:
        self.reader = reader
        self._key_fn = key_fn
        self.parent_notify = parent_notify
        self._decorators: Dict[str, ReaderDecorator] = {}
        self._children: Dict[str, Dict[str, ReaderNode]] = {}
        self._combined_by_key: Dict[Any, CombinedSample] = {}
        self._info_by_key: Dict[Any, object] = {}

        if use_listener:
            # Install a minimal internal listener that polls on data available.
            class _L(dds.NoOpDataReaderListener):
                def on_data_available(_self, _r):
                    try:
                        self.poll_once()
                    except Exception as e:
                        _logger.warning(f"Error encounter in poll_once - {e}")

            self.reader.set_listener(_L(), dds.StatusMask.DATA_AVAILABLE)

    def register_decorator(self, role: str, decorator: ReaderDecorator, required: bool = True) -> None:
        """Register a decorator under a role (e.g., 'gen_spec', 'waypoints')."""
        if role in self._decorators:
            _logger.debug(
                f"Replacing decorator for role {role}: "
                f"{type(self._decorators[role]).__name__} -> {type(decorator).__name__}"
            )
        decorator.name = role
        _logger.debug(f"Registering decorator {decorator.name} for role {role}")
        self._decorators[role] = decorator

        # If children were already attached for this role, wire them now.
        bucket = self._children.get(role)
        if bucket:
            decorator.attach_children(**bucket)

    def attach_child(self, role: str, child_name: str, child_node: "ReaderNode") -> None:
        """
        Attach a child node for a given role and topic/alias.

        The child's `parent_notify` is wired to call this node's decorators,
        and when a completion occurs we bubble the root `SampleInfo` to our parent.
        """
        bucket = self._children.setdefault(role, {})
        bucket[child_name] = child_node

        def _child_ready(key: Any, assembled: Optional[CombinedSample], _info: Optional[object]) -> None:
            # assembled must be a CombinedSample from the child; we pass it to the owning decorator(s)
            if assembled is None:
                return
            for r, deco in self._decorators.items():
                if r != role:
                    continue
                try:
                    for sig in deco.on_child_assembled(self, child_name, key, assembled):
                        if sig.complete and self.parent_notify is not None:
                            info = self._info_by_key.get(sig.key)
                            self.parent_notify(sig.key, self._combined_by_key.get(sig.key, assembled), info)
                except Exception:
                    _logger.exception(f"Decorator {deco.name} raised in on_child_assembled")

        child_node.parent_notify = _child_ready

        if role in self._decorators:
            self._decorators[role].attach_children(**bucket)

    def has_decorators(self, role: Optional[str] = None) -> bool:
        """
        Return True if this node has any decorators (role is None),
        or if a decorator is registered for the specific role.
        """
        if role is None:
            return bool(self._decorators)
        return role in self._decorators

    def decorator_roles(self) -> tuple[str, ...]:
        """Convenience: the set of decorator role names on this node."""
        return tuple(self._decorators.keys())

    def _read_with_infos(self) -> Tuple[List[Any], List[object]]:
        """
        Fetch samples and infos from the underlying reader, trying common RTI Python patterns:
        - Prefer `take()`; if it returns (data, infos), use those; if it returns only data, synthesize infos.
        - Fallback to `read()` similarly.
        - As a last resort, use `take_data()` or `read_data()` and synthesize valid infos.
        """

    def poll_once(self) -> None:
        """
        Drain some samples from the underlying RTI reader and update decorators.

        On valid samples: run decorators and, upon completion, notify parent with info.
        On invalid/dispose: notify parent immediately with (combined=None, info).
        """
        for sample, info in self.reader.take():
            if info is not None and hasattr(info, "valid") and not info.valid:
                # dispose/unregister/etc.: bubble info upward with no combined
                _logger.debug(f"Received invalid sample: {type(sample)}, info: {info}")
                if self.parent_notify is not None:
                    if sample is None:
                        key = object()  # synthetic key for disposals
                    else:
                        key = self._key_fn(sample)
                    self._info_by_key[key] = info
                    self.parent_notify(key, None, info)
                continue

            if sample is None:
                _logger.debug("Received None sample, skipping")
                continue

            key = self._key_fn(sample)
            self._info_by_key[key] = info  # may be None if synthetic
            combined = self._combined_by_key.get(key)
            if combined is None:
                combined = CombinedSample(base=sample)
                self._combined_by_key[key] = combined

            _logger.debug(f"Forwarding {type(sample).__name__.split("_")[-1]} to {len(self._decorators)} decorators")
            for deco in list(self._decorators.values()):
                _logger.debug(f"Calling decorator {deco.name}")
                try:
                    for sig in deco.on_reader_data(self, key, combined, sample):
                        if sig.complete and self.parent_notify is not None:
                            self.parent_notify(
                                sig.key, self._combined_by_key.get(sig.key, combined), self._info_by_key.get(sig.key)
                            )
                except Exception:
                    _logger.exception(f"Decorator {deco.name} raised in on_reader_data")
