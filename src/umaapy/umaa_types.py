"""Compatibility layer for Cyclone DDS IDL-generated UMAA types.

This preserves the historical RTI rtiddsgen-style names (UMAA_*) while
backing them with Cyclone DDS idlc-generated modules under the UMAA package.
"""

from __future__ import annotations

from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from enum import Enum
import importlib
import sys
from typing import Any, Annotated, Sequence, Union, get_args, get_origin, get_type_hints
import collections.abc

from cyclonedds.idl import IdlStruct, IdlUnion
from cyclonedds.idl import types as idl_types


try:
    UMAA = importlib.import_module("UMAA")
except Exception as exc:  # pragma: no cover - exercised indirectly in tests
    raise ImportError(
        "UMAA generated types package is missing. Run `python -m umaapy.tools.generate_types`."
    ) from exc


@dataclass
class UMAA_Common_Measurement_NumericGUID(Sequence[int]):
    """Compatibility wrapper for UMAA NumericGUID (octet[16])."""

    value: Sequence[int] = field(default_factory=lambda: [0] * 16)

    def __post_init__(self) -> None:
        self.value = list(self.value)

    def __len__(self) -> int:
        return len(self.value)

    def __getitem__(self, idx: int) -> int:
        return self.value[idx]

    def __iter__(self):
        return iter(self.value)


_SPECIAL_CASES = {
    "UMAA_Common_Measurement_NumericGUID": UMAA_Common_Measurement_NumericGUID,
}


def _default_value(ann: Any) -> Any:
    origin = get_origin(ann)
    if origin is Annotated:
        base, *metadata = get_args(ann)
        for meta in metadata:
            if isinstance(meta, idl_types.typedef):
                if meta.name == "UMAA.Common.Measurement.NumericGUID":
                    return UMAA_Common_Measurement_NumericGUID()
                return _default_value(meta.subtype)
            if isinstance(meta, idl_types.array):
                return [_default_value(meta.subtype) for _ in range(meta.length)]
        return _default_value(base)

    if origin is Union and type(None) in get_args(ann):
        return None

    if origin in (list, collections.abc.Sequence) or ann in (Sequence, collections.abc.Sequence):
        return []

    if origin is tuple:
        return ()

    if origin is None and isinstance(ann, str):
        ann = _resolve_forward_ref(ann)
        return _default_value(ann)

    if isinstance(ann, type) and issubclass(ann, Enum):
        return next(iter(ann))

    if isinstance(ann, type) and issubclass(ann, IdlStruct):
        _ensure_defaults(ann)
        return ann()
    if isinstance(ann, type) and issubclass(ann, IdlUnion):
        return _default_union(ann)

    if ann in (int, float):
        return ann()
    if ann is bool:
        return False
    if ann is str:
        return ""

    return None


def _resolve_forward_ref(ref: str) -> Any:
    if ref.startswith("UMAA."):
        module_path, _, attr = ref.rpartition(".")
        module = importlib.import_module(module_path)
        return getattr(module, attr)
    return ref


def _default_union(union_cls: type) -> Any:
    default_case = getattr(union_cls, "__idl_default__", None)
    cases = getattr(union_cls, "__idl_cases__", {}) or {}
    if default_case:
        name, subtype = default_case
    elif cases:
        name, subtype = next(iter(cases.values()))
    else:
        return union_cls(discriminator=0, value=None)

    if isinstance(subtype, str):
        subtype = _resolve_forward_ref(subtype)
    return union_cls(**{name: _default_value(subtype)})


def _ensure_defaults(cls: type) -> None:
    if getattr(cls, "__umaa_defaults_patched__", False):
        return
    if not is_dataclass(cls):
        return

    try:
        type_hints = _type_hints_for(cls)
    except Exception:
        type_hints = {}

    original_init = cls.__init__

    def __init__(self, *args, **kwargs):
        if args:
            return original_init(self, *args, **kwargs)
        for f in fields(cls):
            if f.name in kwargs:
                continue
            if f.default is not MISSING:
                kwargs[f.name] = f.default
            elif f.default_factory is not MISSING:  # type: ignore[comparison-overlap]
                kwargs[f.name] = f.default_factory()  # type: ignore[misc]
            else:
                ann = type_hints.get(f.name, f.type)
                kwargs[f.name] = _default_value(ann)
        return original_init(self, **kwargs)

    cls.__init__ = __init__  # type: ignore[assignment]
    cls.__umaa_defaults_patched__ = True


def _type_hints_for(cls: type) -> dict[str, Any]:
    module = sys.modules.get(cls.__module__)
    if module is None:
        return {}
    return get_type_hints(
        cls, globalns=module.__dict__, localns=module.__dict__, include_extras=True
    )


def _resolve_legacy(name: str) -> Any:
    if name in _SPECIAL_CASES:
        return _SPECIAL_CASES[name]
    if not name.startswith("UMAA_"):
        raise AttributeError(f"{__name__} has no attribute {name}")

    parts = name.split("_")[1:]
    for split in range(len(parts) - 1, 0, -1):
        module_path = "UMAA." + ".".join(parts[:split])
        attr_name = "_".join(parts[split:])
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue
        if hasattr(module, attr_name):
            value = getattr(module, attr_name)
            if isinstance(value, type):
                _ensure_defaults(value)
            return value
    raise AttributeError(f"{__name__} has no attribute {name}")


def __getattr__(name: str) -> Any:
    value = _resolve_legacy(name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_SPECIAL_CASES))
