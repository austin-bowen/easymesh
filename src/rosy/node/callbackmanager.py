from typing import Callable, Generic, TypeVar

K = TypeVar('K')
C = TypeVar('C', bound=Callable)


class CallbackManager(Generic[K, C]):
    def __init__(self):
        self._handlers: dict[K, C] = {}

    @property
    def keys(self) -> set[K]:
        return set(self._handlers.keys())

    def get_callback(self, key: K) -> Callable | None:
        return self._handlers.get(key)

    def set_callback(self, key: K, callback: Callable) -> None:
        self._handlers[key] = callback

    def remove_callback(self, key: K) -> Callable | None:
        return self._handlers.pop(key, None)
