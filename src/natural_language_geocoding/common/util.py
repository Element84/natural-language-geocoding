"""Miscellaneous utility functions."""

import functools
import logging
import os
import textwrap
from collections.abc import Callable, Generator, Iterable
from time import perf_counter
from typing import Any, cast, overload

log = logging.getLogger(__name__)


def get_env_var(name: str, default: str | None = None) -> str:
    """Retrieve the value of an environment variable."""
    value = os.getenv(name) or default
    if value is None:
        msg = f"Env var {name} must be set"
        raise ValueError(msg)
    return value


def singleline(text: str) -> str:
    """Dedent a multi-line string and join its lines into a single line.

    Example:
        >>> singleline('''
        ...     Lorem ipsum dolor sit amet,
        ...     consectetur adipiscing elit.
        ... ''')
        'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'
    """
    return textwrap.dedent(text).strip().replace("\n", " ")


@overload
def timed_function[F: Callable[..., Any]](arg: F) -> F: ...


@overload
def timed_function[F: Callable[..., Any]](
    arg: logging.Logger | None = None,
) -> Callable[[F], F]: ...


def timed_function[F: Callable[..., Any]](
    arg: Callable[..., Any] | logging.Logger | None = None,
) -> Callable[..., Any] | Callable[[F], F]:
    """A decorator that logs how long a function takes to execute.

    Can be used with or without arguments:

    @timed_function
    def func(): ...

    @timed_function(logger)
    def func(): ...
    """

    def decorator(func: F, logger: logging.Logger) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            start_time = perf_counter()
            result = func(*args, **kwargs)
            end_time = perf_counter()
            logger.info("%s took %.4f seconds to execute", func.__name__, end_time - start_time)
            return result

        return cast("F", wrapper)

    if callable(arg):
        return decorator(cast("F", arg), log)

    custom_logger = arg or log
    return functools.partial(decorator, logger=custom_logger)


def unique_by[T, K](
    items: Iterable[T],
    *,
    key_fn: Callable[[T], K],
    duplicate_handler_fn: Callable[[T, K], None] | None = None,
) -> Generator[T, None, None]:
    """Filter an iterable to yield only the first item for each unique key.

    Args:
        items: The input items to be filtered for uniqueness.
        key_fn: A function that extracts the key to determine uniqueness.
        duplicate_handler_fn: Optional function called with each duplicate item and its key.

    Example:
        >>> data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 1, "name": "Alex"}]
        >>> list(unique_by(data, key_fn=lambda x: x["id"]))
        [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    """
    keys: set[K] = set()

    for item in items:
        key = key_fn(item)
        if key not in keys:
            keys.add(key)
            yield item
        elif duplicate_handler_fn:
            duplicate_handler_fn(item, key)
