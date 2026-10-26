"""Task handler registry for jobqueue."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, overload

from jobqueue.models import TaskNotFoundError

HandlerFunc = Callable[[str], Any]


class Registry:
    """Registry mapping task names to callable handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFunc] = {}

    @overload
    def register(
        self, task_name: str, handler: None = None
    ) -> Callable[[HandlerFunc], HandlerFunc]: ...

    @overload
    def register(self, task_name: str, handler: HandlerFunc) -> HandlerFunc: ...

    def register(
        self,
        task_name: str,
        handler: HandlerFunc | None = None,
    ) -> Callable[[HandlerFunc], HandlerFunc] | HandlerFunc:
        """Register a handler callable either directly or as a decorator."""
        if handler is None:

            def decorator(fn: HandlerFunc) -> HandlerFunc:
                self.register(task_name, fn)
                return fn

            return decorator

        self._handlers[task_name] = handler
        return handler

    def get(self, task_name: str) -> HandlerFunc:
        """Retrieve handler for task name, raising TaskNotFoundError if not found."""
        if task_name not in self._handlers:
            msg = f"Unknown task: '{task_name}'. Handler is not registered."
            raise TaskNotFoundError(msg)
        return self._handlers[task_name]

    def contains(self, task_name: str) -> bool:
        """Check if task is registered."""
        return task_name in self._handlers

    @classmethod
    def from_module(cls, module_path: str) -> Registry:
        """Import a module and return its registry instance or discovered handlers."""
        mod = importlib.import_module(module_path)
        if hasattr(mod, "registry") and isinstance(mod.registry, cls):
            return mod.registry

        registry = cls()
        for attr_name in dir(mod):
            val = getattr(mod, attr_name)
            if callable(val) and hasattr(val, "_jobqueue_task_name"):
                registry.register(val._jobqueue_task_name, val)
        return registry


# Global default registry instance
default_registry = Registry()


def task(task_name: str) -> Callable[[HandlerFunc], HandlerFunc]:
    """Decorator to mark a function with a task name and register it on default_registry."""

    def decorator(fn: HandlerFunc) -> HandlerFunc:
        setattr(fn, "_jobqueue_task_name", task_name)  # noqa: B010
        default_registry.register(task_name, fn)
        return fn

    return decorator
