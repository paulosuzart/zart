"""Pythonic serialization/deserialization helpers for Zart FFI boundaries."""

from __future__ import annotations

import dataclasses
import types
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Any, Union, get_args, get_origin, get_type_hints
from uuid import UUID

try:
    from pydantic import BaseModel as _PydanticBaseModel
except ImportError:  # pragma: no cover
    _PydanticBaseModel = None  # type: ignore[assignment,misc]


def to_json_value(obj: Any) -> Any:
    """Normalize Python objects into JSON-compatible primitives before crossing FFI.

    Supports primitives (``None``, ``bool``, ``int``, ``float``, ``str``),
    ``UUID``, ``datetime``, ``Enum``, ``dataclasses``, ``pydantic.BaseModel``
    (when pydantic is installed), ``list``/``tuple``, and ``dict``.
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, UUID):
        return str(obj)

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, Enum):
        return to_json_value(obj.value)

    if _PydanticBaseModel is not None and isinstance(obj, _PydanticBaseModel):
        return obj.model_dump(mode="json")

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_json_value(getattr(obj, f.name)) for f in dataclasses.fields(obj)}

    if isinstance(obj, Mapping):
        return {(k if isinstance(k, str) else str(k)): to_json_value(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [to_json_value(item) for item in obj]

    raise TypeError(f"Unsupported type for JSON serialization: {type(obj).__name__}")


def _from_union(data: Any, target_type: Any) -> Any:
    """Deserialize a ``Union`` or ``T | U`` annotation."""
    args = get_args(target_type)
    if data is None and type(None) in args:
        return None
    non_none_args = [a for a in args if a is not type(None)]
    if len(non_none_args) == 1:
        return from_json_value(data, non_none_args[0])
    for arg in non_none_args:
        try:
            return from_json_value(data, arg)
        except TypeError, ValueError, KeyError:
            continue
    raise TypeError(f"Cannot deserialize {data!r} into {target_type!r}")


def _from_dataclass(data: Any, target_type: type[Any]) -> Any:
    """Reconstruct a dataclass instance recursively from a mapping."""
    if not isinstance(data, Mapping):
        raise TypeError(
            f"Expected mapping for dataclass {target_type.__name__}, got {type(data).__name__}"
        )
    type_hints = get_type_hints(target_type)
    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(target_type):
        if f.name in data:
            field_type = type_hints.get(f.name, Any)
            kwargs[f.name] = from_json_value(data[f.name], field_type)
        elif f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING:
            continue
        else:
            raise KeyError(
                f"Missing required field {f.name!r} for dataclass {target_type.__name__}"
            )
    return target_type(**kwargs)


def _from_collection(data: Any, origin: Any, target_type: Any) -> Any:
    """Reconstruct ``list[T]`` or ``dict[K, V]`` annotations."""
    if origin is list or target_type is list:
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data).__name__}")
        args = get_args(target_type)
        elem_type = args[0] if args else Any
        return [from_json_value(item, elem_type) for item in data]

    if not isinstance(data, Mapping):
        raise TypeError(f"Expected dict, got {type(data).__name__}")
    args = get_args(target_type)
    key_type = args[0] if len(args) >= 1 else Any
    val_type = args[1] if len(args) >= 2 else Any
    return {from_json_value(k, key_type): from_json_value(v, val_type) for k, v in data.items()}


def _from_primitive(data: Any, target_type: type[Any]) -> Any:
    """Reconstruct scalar primitives, ``UUID``, ``datetime``, and ``Enum``."""
    if target_type is UUID:
        if isinstance(data, UUID):
            return data
        if isinstance(data, str):
            return UUID(data)
        raise TypeError(f"Expected UUID string, got {type(data).__name__}")

    if target_type is datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, str):
            return datetime.fromisoformat(data)
        raise TypeError(f"Expected ISO datetime string, got {type(data).__name__}")

    if target_type is bool:
        if isinstance(data, bool):
            return data
        raise TypeError(f"Expected bool, got {type(data).__name__}")

    if target_type is int:
        if isinstance(data, int) and not isinstance(data, bool):
            return data
        if isinstance(data, str):
            return int(data)
        raise TypeError(f"Expected int, got {type(data).__name__}")

    if target_type is float:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return float(data)
        if isinstance(data, str):
            return float(data)
        raise TypeError(f"Expected float, got {type(data).__name__}")

    if target_type is str:
        if isinstance(data, str):
            return data
        raise TypeError(f"Expected str, got {type(data).__name__}")

    return target_type(data)


def from_json_value[T](data: Any, target_type: type[T] | Any = None) -> T | Any:
    """Reconstruct an annotated Python type from JSON-compatible primitives.

    If ``target_type`` is ``None`` or ``typing.Any``, returns ``data`` as-is.
    Supports primitives, ``UUID``, ``datetime``, ``Enum``, ``Optional[T]`` / ``T | None``,
    ``list[T]``, ``dict[K, V]``, ``dataclasses``, and ``pydantic.BaseModel``.
    """
    if target_type is None or target_type is Any:
        return data

    if target_type is type(None):
        if data is None:
            return None
        raise TypeError(f"Expected None, got {type(data).__name__}")

    origin = get_origin(target_type)
    if origin is Union or origin is types.UnionType:
        return _from_union(data, target_type)

    if (
        _PydanticBaseModel is not None
        and isinstance(target_type, type)
        and issubclass(target_type, _PydanticBaseModel)
    ):
        return target_type.model_validate(data)

    if dataclasses.is_dataclass(target_type) and isinstance(target_type, type):
        return _from_dataclass(data, target_type)

    if origin in (list, dict) or target_type in (list, dict):
        return _from_collection(data, origin, target_type)

    if target_type in (UUID, datetime, bool, int, float, str) or (
        isinstance(target_type, type) and issubclass(target_type, Enum)
    ):
        return _from_primitive(data, target_type)

    return data
