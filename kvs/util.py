import asyncio
import hashlib
from functools import WRAPPER_ASSIGNMENTS, wraps
from typing import Callable, Coroutine, ParamSpec, TypeVar


# from https://stackoverflow.com/a/17511341/8762161
def integer_ceiling_divide(a: int, b: int, /) -> int:
    """returns the ceiling of `a / b`."""
    return -(a // -b)

def hash_key(key: str, modulo: int) -> int:
    key_hash = int.from_bytes(hashlib.sha256(bytes(key, encoding='utf-8')).digest()[:4], 'little')
    return key_hash % modulo


T_ret = TypeVar('T_ret')

def wraps_and_consumes_props(func, *props: str) -> Callable[[Callable[..., T_ret]], Callable[..., T_ret]]:
    def decorator(wrapper):
        # copy various properties (e.g. name, docstring) from the wrapped function onto our wrapper.
        # flask will actually throw up some errors if we don't do this and also use the decorator on
        # more than one endpoint, since it thinks they're the same function
        for k in WRAPPER_ASSIGNMENTS:
            if k == '__annotations__':
                continue
            try:
                v = getattr(func, k)
            except ValueError:
                pass
            else:
                setattr(wrapper, k, v)
        # copy the annotations from the wrapped function onto our wrapper, excluding the annotations
        # for arguments we consume.
        try:
            func_annotations = getattr(func, '__annotations__')
        except AttributeError:
            pass
        else:
            setattr(wrapper, '__annotations__', {name: annotation for name, annotation in func_annotations.items() if name not in props})
        return wrapper
    return decorator


P = ParamSpec('P')

def deasyncify_endpoint(func: Callable[P, Coroutine[None, None, T_ret]]) -> Callable[P, T_ret]:
    @wraps(func)  # TODO update return type annotation?
    def wrapper(*args, **kwargs):
        coro = func(*args, **kwargs)
        # loop = asyncio.new_event_loop()
        # loop.run_until_complete(coro)
        return asyncio.run(coro)
    return wrapper
