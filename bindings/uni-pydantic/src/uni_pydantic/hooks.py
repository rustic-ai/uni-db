# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Lifecycle hooks for uni-pydantic models."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from .base import UniEdge, UniNode

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


# Hook marker attributes
_BEFORE_CREATE = "_uni_before_create"
_AFTER_CREATE = "_uni_after_create"
_BEFORE_UPDATE = "_uni_before_update"
_AFTER_UPDATE = "_uni_after_update"
_BEFORE_DELETE = "_uni_before_delete"
_AFTER_DELETE = "_uni_after_delete"
_BEFORE_LOAD = "_uni_before_load"
_AFTER_LOAD = "_uni_after_load"


def _mark_hook(attr: str) -> Callable[[F], F]:
    """Create a decorator that marks a method as a lifecycle hook."""

    def decorator(func: F) -> F:
        setattr(func, attr, True)
        return func

    return decorator


def before_create(func: F) -> F:
    """
    Mark a method to be called before the entity is created in the database.

    The method is called after validation but before the INSERT operation.
    Useful for setting timestamps, generating IDs, or final validation.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...     created_at: datetime | None = None
        ...
        ...     @before_create
        ...     def set_created_at(self):
        ...         self.created_at = datetime.now()
    """
    return _mark_hook(_BEFORE_CREATE)(func)


def after_create(func: F) -> F:
    """
    Mark a method to be called after the entity is created in the database.

    The method is called after the INSERT operation completes successfully.
    The entity will have its vid/eid assigned at this point.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...
        ...     @after_create
        ...     def log_creation(self):
        ...         logger.info(f"Created person {self.name} with vid={self.vid}")
    """
    return _mark_hook(_AFTER_CREATE)(func)


def before_update(func: F) -> F:
    """
    Mark a method to be called before the entity is updated in the database.

    The method is called before the UPDATE operation.
    Useful for validation or updating timestamps.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...     updated_at: datetime | None = None
        ...
        ...     @before_update
        ...     def validate_and_timestamp(self):
        ...         if not self.name:
        ...             raise ValueError("Name cannot be empty")
        ...         self.updated_at = datetime.now()
    """
    return _mark_hook(_BEFORE_UPDATE)(func)


def after_update(func: F) -> F:
    """
    Mark a method to be called after the entity is updated in the database.

    The method is called after the UPDATE operation completes successfully.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...
        ...     @after_update
        ...     def notify_change(self):
        ...         events.emit("person_updated", self.vid)
    """
    return _mark_hook(_AFTER_UPDATE)(func)


def before_delete(func: F) -> F:
    """
    Mark a method to be called before the entity is deleted from the database.

    The method is called before the DELETE operation.
    Useful for cleanup or validation.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...
        ...     @before_delete
        ...     def cleanup(self):
        ...         # Remove related data
        ...         pass
    """
    return _mark_hook(_BEFORE_DELETE)(func)


def after_delete(func: F) -> F:
    """
    Mark a method to be called after the entity is deleted from the database.

    The method is called after the DELETE operation completes successfully.
    The entity's vid/eid will be cleared at this point.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...
        ...     @after_delete
        ...     def log_deletion(self):
        ...         logger.info(f"Deleted person {self.name}")
    """
    return _mark_hook(_AFTER_DELETE)(func)


def before_load(func: F) -> F:
    """
    Mark a method to be called before the entity is loaded from the database.

    This is a class method that receives the raw property dictionary.
    Can be used to transform data before model instantiation.

    Example:
        >>> class Person(UniNode):
        ...     name: str
        ...
        ...     @classmethod
        ...     @before_load
        ...     def transform_data(cls, props: dict) -> dict:
        ...         # Normalize name
        ...         if 'name' in props:
        ...             props['name'] = props['name'].strip()
        ...         return props
    """
    return _mark_hook(_BEFORE_LOAD)(func)


def after_load(func: F) -> F:
    """
    Mark a method to be called after the entity is loaded from the database.

    The method is called after the entity is instantiated from database data.
    Useful for computing derived values or initializing non-persisted state.

    Example:
        >>> class Person(UniNode):
        ...     first_name: str
        ...     last_name: str
        ...     full_name: str | None = None
        ...
        ...     @after_load
        ...     def compute_full_name(self):
        ...         self.full_name = f"{self.first_name} {self.last_name}"
    """
    return _mark_hook(_AFTER_LOAD)(func)


def get_hooks(
    model: type[UniNode] | type[UniEdge],
    hook_type: str,
) -> list[Callable[..., Any]]:
    """Get all methods marked with a specific hook type."""
    hooks = []
    for name in dir(model):
        if name.startswith("_"):
            continue
        # Use vars() to get the raw descriptor from each class in MRO
        raw = None
        for klass in model.__mro__:
            if name in vars(klass):
                raw = vars(klass)[name]
                break
        if raw is None:
            continue

        # Unwrap classmethod/staticmethod descriptors to check for hook marker
        unwrapped = raw
        if isinstance(raw, (classmethod, staticmethod)):
            unwrapped = raw.__func__
        if getattr(unwrapped, hook_type, False):
            hooks.append(raw)
    return hooks


def run_hooks(
    entity: UniNode | UniEdge,
    hook_type: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Run all hooks of a specific type on an entity."""
    model = type(entity)
    hooks = get_hooks(model, hook_type)
    for hook in hooks:
        if isinstance(hook, classmethod):
            # classmethod: call with class as first arg
            hook.__func__(model, entity, *args, **kwargs)
        elif isinstance(hook, staticmethod):
            hook.__func__(entity, *args, **kwargs)
        else:
            # Regular method: bind to instance
            hook(entity, *args, **kwargs)


def run_class_hooks(
    model: type[UniNode] | type[UniEdge],
    hook_type: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run class-level hooks (like before_load) and return transformed result.

    For classmethod hooks: calls hook(data, *extra_args)
    For regular method hooks: skipped (instance doesn't exist yet for before_load)
    """
    hooks = get_hooks(model, hook_type)
    result = args[0] if args else None
    for hook in hooks:
        if isinstance(hook, classmethod):
            # classmethod: call with (data, *extra_args)
            result = hook.__func__(model, result, *args[1:], **kwargs)
        else:
            # For before_load, regular methods can't be called (no instance yet)
            # For after_load, this function shouldn't be used — use run_hooks instead
            pass
    return result
