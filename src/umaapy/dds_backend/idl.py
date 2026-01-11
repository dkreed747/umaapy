from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple, Union, get_args, get_origin
import collections.abc

from cyclonedds.idl import IdlStruct, IdlUnion
from cyclonedds.idl import _main as idl_main
from cyclonedds.idl import types as idl_types


class _TypeName:
    def __init__(self, name: str) -> None:
        self.name = name


class _Bound:
    def __init__(self, size: int) -> None:
        self.size = size


class _Array:
    def __init__(self, dims: Sequence[int]) -> None:
        self.dims = list(dims)


class _Default:
    def __init__(self, value: Any) -> None:
        self.value = value


class _Case:
    def __init__(self, labels: Any) -> None:
        self.labels = labels


class _Key:
    pass


key = _Key()


def type_name(name: str) -> _TypeName:
    return _TypeName(name)


def bound(size: int) -> _Bound:
    return _Bound(size)


def array(dims: Sequence[int]) -> _Array:
    return _Array(dims)


def default(value: Any) -> _Default:
    return _Default(value)


def case(label: Any) -> _Case:
    return _Case(label)


def array_factory(element_type: Any, dims: Sequence[int]):
    dims = list(dims)

    def _zero_value(t: Any) -> Any:
        origin = get_origin(t)
        if origin is not None and origin.__module__ == "typing" and origin.__qualname__ == "Annotated":
            t = get_args(t)[0]
        if t in (int, float):
            return t()
        if t is bool:
            return False
        if t is str:
            return ""
        return t() if callable(t) else None

    def _build(level: int) -> Any:
        if level >= len(dims):
            return _zero_value(element_type)
        return [_build(level + 1) for _ in range(dims[level])]

    return lambda: _build(0)


def get_module(name: str):
    return _MODULES.setdefault(name, _ModuleNamespace(name))


def struct(*, type_annotations=None, member_annotations: Optional[Dict[str, Sequence[Any]]] = None, **_kwargs):
    def decorator(cls):
        typename = _resolve_typename(cls.__name__, type_annotations)
        hints = dict(cls.__annotations__)
        field_annotations: Dict[str, Dict[str, Any]] = {}
        for field, ann_list in (member_annotations or {}).items():
            for ann in ann_list:
                if isinstance(ann, _Bound) and field in hints:
                    hints[field] = _apply_bound(hints[field], ann.size)
                elif isinstance(ann, _Default):
                    field_annotations.setdefault(field, {})["default_literal"] = ann.value
                elif ann is key:
                    field_annotations.setdefault(field, {})["key"] = True
        return _build_struct(cls, hints, typename, field_annotations)

    return decorator


def alias(*, annotations: Optional[Sequence[Any]] = None, **_kwargs):
    def decorator(cls):
        typename = cls.__name__
        hints = dict(cls.__annotations__)
        for ann in annotations or []:
            if isinstance(ann, _Array):
                hints = _apply_array_to_value(hints, ann.dims)
        return _build_struct(cls, hints, typename, {})

    return decorator


def union(*, type_annotations=None, **_kwargs):
    def decorator(cls):
        typename = _resolve_typename(cls.__name__, type_annotations)
        discriminator = cls.__annotations__.get("discriminator", int)
        fields: Dict[str, Any] = {}
        for name, hint in cls.__annotations__.items():
            if name in ("discriminator", "value"):
                continue
            val = getattr(cls, name, None)
            if isinstance(val, _Case):
                fields[name] = idl_types.case[val.labels, hint]
            elif isinstance(val, _Default):
                fields[name] = idl_types.default[hint]
        namespace = idl_main.IdlUnionMeta.__prepare__(
            cls.__name__,
            (IdlUnion,),
            typename=typename,
            discriminator=discriminator,
        )
        namespace["__module__"] = cls.__module__
        namespace["__annotations__"] = fields
        new_cls = idl_main.IdlUnionMeta(cls.__name__, (IdlUnion,), namespace)

        def _init(self, **kwargs):
            if not kwargs:
                if new_cls.__idl_default__:
                    name, subtype = new_cls.__idl_default__
                    return IdlUnion.__init__(self, **{name: subtype()})
                if new_cls.__idl_cases__:
                    label = next(iter(new_cls.__idl_cases__.keys()))
                    name, subtype = new_cls.__idl_cases__[label]
                    return IdlUnion.__init__(self, **{name: subtype()})
            return IdlUnion.__init__(self, **kwargs)

        new_cls.__init__ = _init
        return new_cls

    return decorator


def enum(cls):
    if not hasattr(cls, "__idl_annotations__"):
        cls.__idl_annotations__ = {}
    if not hasattr(cls, "__idl_field_annotations__"):
        cls.__idl_field_annotations__ = {}
    if not hasattr(cls, "__idl_typename__"):
        cls.__idl_typename__ = cls.__name__
    return cls


uint8 = idl_types.uint8
uint16 = idl_types.uint16
uint32 = idl_types.uint32
uint64 = idl_types.uint64
int8 = idl_types.int8
int16 = idl_types.int16
int32 = idl_types.int32
int64 = idl_types.int64
char = idl_types.char
wchar = idl_types.wchar


class _ModuleNamespace:
    def __init__(self, name: str) -> None:
        self.__name__ = name

    def __repr__(self) -> str:
        return f"<idl module {self.__name__}>"


_MODULES: Dict[str, _ModuleNamespace] = {}


def _resolve_typename(default_name: str, type_annotations: Optional[Sequence[Any]]) -> str:
    for ann in type_annotations or []:
        if isinstance(ann, _TypeName):
            return ann.name
    return default_name


def _build_struct(
    original: type,
    hints: Dict[str, Any],
    typename: str,
    field_annotations: Optional[Dict[str, Dict[str, Any]]] = None,
) -> type:
    namespace = idl_main.IdlMeta.__prepare__(original.__name__, (IdlStruct,), typename=typename)
    namespace["__module__"] = original.__module__
    namespace["__annotations__"] = hints
    if field_annotations:
        namespace["__idl_field_annotations__"] = field_annotations
    for name in hints:
        if name in original.__dict__:
            namespace[name] = original.__dict__[name]
    cls = idl_main.IdlMeta(original.__name__, (IdlStruct,), namespace)
    return dataclass(cls)


def _apply_array_to_value(hints: Dict[str, Any], dims: Sequence[int]) -> Dict[str, Any]:
    if "value" not in hints:
        return hints
    elem_type = _sequence_subtype(hints["value"])
    t = elem_type
    for dim in reversed(list(dims)):
        t = idl_types.array[t, dim]
    hints = dict(hints)
    hints["value"] = t
    return hints


def _sequence_subtype(type_hint: Any) -> Any:
    origin = get_origin(type_hint)
    if origin in (list, Sequence, tuple, collections.abc.Sequence):
        args = get_args(type_hint)
        if args:
            return args[0]
    return type_hint


def _apply_bound(type_hint: Any, size: int) -> Any:
    origin = get_origin(type_hint)
    if origin is Union:
        args = get_args(type_hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return Optional[_apply_bound(non_none[0], size)]

    if origin is not None and origin.__module__ == "typing" and origin.__qualname__ == "Annotated":
        return type_hint

    if type_hint is str:
        return idl_types.bounded_str[size]

    if origin in (list, Sequence, tuple, collections.abc.Sequence):
        args = get_args(type_hint)
        subtype = args[0] if args else Any
        return idl_types.sequence[subtype, size]

    return type_hint
