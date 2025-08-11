# multi_topic_writer.py
from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Optional, Callable

import rti.connextdds as dds

from umaapy.util.multi_topic_support import CombinedBuilder


class WriterDecorator:
    """Base class for UMAA writer decorators.

    A writer decorator:
      - Splits a CombinedBuilder into writes on its child WriterNodes (e.g., spec or list/set elements),
      - Mutates the builder.base metadata/generalization to point at what it wrote (IDs/timestamps/links),
      - Does NOT write the base; WriterNode writes its base AFTER all decorators publish.
    """

    name: str = "base"

    def attach_children(self, **children: "WriterNode") -> None:
        for _ in children.values():
            if not isinstance(_, WriterNode):
                raise TypeError("attach_children expects WriterNode instances")
        if not hasattr(self, "_children"):
            self._children: Dict[str, WriterNode] = {}
        self._children.update(children)

    def publish(self, node: "WriterNode", builder: CombinedBuilder) -> None:
        """Implement UMAA-specific fan-out in subclasses."""
        pass


class WriterNode:
    """Owns ONE RTI DataWriter and zero or more UMAA writer decorators.

    Publish order:
      1) All decorators publish their children first (specializations, list/set elements, etc.),
      2) Decorators update base metadata/generalization fields (IDs/timestamps/links),
      3) This node writes its base to the writer.

    Notes
    -----
    - Decorator order is registration order (insertion-ordered).
    - If you need to skip writing base for a non-root node, set write_base=False.
    """

    def __init__(self, writer: dds.DataWriter, *, write_base: bool = True) -> None:
        self.writer = writer
        self._decorators: "OrderedDict[str, WriterDecorator]" = OrderedDict()
        self._write_base = write_base

    def register_decorator(self, name: str, decorator: WriterDecorator) -> None:
        decorator.name = name
        self._decorators[name] = decorator

    def attach_child(self, decorator_name: str, child_name: str, child: "WriterNode") -> None:
        deco = self._decorators[decorator_name]
        deco.attach_children(**{child_name: child})

    def publish(self, builder: CombinedBuilder) -> None:
        # First, let decorators publish children and mutate metadata/generalization on base
        for deco in self._decorators.values():
            deco.publish(self, builder)

        # Finally, write the base for this node
        if self._write_base:
            self.writer.write(builder.base)

    # Convenience if a decorator needs to write a one-off sample using THIS node's writer
    def write_now(self, sample: Any) -> None:
        self.writer.write(sample)


class TopLevelWriter:
    """User-facing wrapper for a top-level UMAA type writer.

    Usage:
        root = WriterNode(cmd_writer)
        obj_exec = TopLevelWriter(root, base_factory=idl.ObjectiveExecutorCommandType)
        b = obj_exec.new()
        ... fill b.collections and b.overlay ...
        obj_exec.write(b)
    """

    def __init__(self, root: WriterNode, base_factory: Callable[[], Any]) -> None:
        self.root = root
        self.base_factory = base_factory

    def new(self) -> CombinedBuilder:
        base = self.base_factory()
        return CombinedBuilder(base=base)

    def write(self, builder: CombinedBuilder) -> None:
        self.root.publish(builder)
