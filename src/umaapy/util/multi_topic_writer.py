"""
UMAA writer graph runtime: WriterNode, WriterDecorator base, and TopLevelWriter.

A `WriterNode` wraps one RTI `DataWriter`. Decorators attached to the node publish
any child topics (specializations, set/list elements) *before* the node writes its
base object, ensuring all metadata links are correct.
"""

from __future__ import annotations

from typing import Any, Dict
import logging

from umaapy.util.multi_topic_support import CombinedBuilder

import rti.connextdds as dds

_logger = logging.getLogger(__name__)


class WriterDecorator:
    """
    Base class for writer-side UMAA decorators.

    Implement `publish(node, builder)` to:
    - publish any dependent child topics (e.g., specializations, set/list elements),
    - set/validate metadata fields on `builder.base` (e.g., link markers),
    - leave the base in a ready-to-write state for the node.
    """

    name: str = ""

    def attach_children(self, **children: "WriterNode") -> None:
        """Receive child mapping (topic or alias -> WriterNode)."""
        self._children = children

    def publish(self, node: "WriterNode", builder: CombinedBuilder) -> None:
        """Publish dependent data and prepare the base; default is no-op."""
        return


class WriterNode:
    """
    Writer graph node that wraps a single RTI `DataWriter`.

    Parameters
    ----------
    writer : dds.DataWriter
        RTI data writer for this node.
    """

    def __init__(self, writer: dds.DataWriter) -> None:
        self.writer = writer
        self._decorators: Dict[str, WriterDecorator] = {}
        self._children: Dict[str, Dict[str, WriterNode]] = {}

    def register_decorator(self, role: str, decorator: WriterDecorator) -> None:
        """Register a decorator under a role (e.g., 'gen_spec', 'waypoints')."""
        decorator.name = role
        self._decorators[role] = decorator

    def attach_child(self, role: str, child_name: str, child_node: "WriterNode") -> None:
        """
        Attach a child node for a given role and topic/alias.
        """
        bucket = self._children.setdefault(role, {})
        bucket[child_name] = child_node
        if role in self._decorators:
            _logger.debug(
                "Attaching child '%s' to '%s'", child_name.split("::")[-1], self.writer.topic_name.split("::")[-1]
            )
            self._decorators[role].attach_children(**bucket)

    def publish(self, builder: CombinedBuilder) -> None:
        """
        Publish all dependent data via decorators, then write the base.

        Decorators must publish children *before* base write, so by the time
        we call `writer.write(builder.base)`, metadata links are complete.
        """
        for name, deco in self._decorators.items():
            deco.publish(self, builder)
        _logger.debug("WriterNode.publish: writing base object '%s'.", type(builder.base).__name__)
        self.writer.write(builder.base)


class TopLevelWriter:
    """
    High-level wrapper for a writer tree. Provides `new()` and `write()`.

    Parameters
    ----------
    root : WriterNode
        Root node of the writer graph.
    base_factory : type or callable
        Callable or type to produce a new base object for `new()`.
    """

    def __init__(self, root: WriterNode, base_factory: Any) -> None:
        self._root = root
        self._base_factory = base_factory

    def new(self) -> CombinedBuilder:
        """Create a fresh `CombinedBuilder` with a new base object."""
        base = self._base_factory() if callable(self._base_factory) else self._base_factory()
        _logger.debug("TopLevelWriter.new: created base object '%s'", type(base).__name__)
        return CombinedBuilder(base=base)

    def write(self, builder: CombinedBuilder) -> None:
        """Publish a combined builder."""
        _logger.debug("TopLevelWriter.write: publishing combined sample")
        self._root.publish(builder)
