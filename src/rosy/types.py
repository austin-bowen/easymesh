from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

Data = Any

Topic = str
TopicCallback = Callable[[Topic], Awaitable[None]] | Callable[[Topic, ...], Awaitable[None]]

Service = str
ServiceCallback = Callable[[Service], Awaitable[Data]] | Callable[[Service, ...], Awaitable[Data]]

Host = str
ServerHost = Host | Sequence[Host] | None
Port = int


@dataclass
class Endpoint:
    host: Host
    port: Port

    def __str__(self):
        return f'{self.host}:{self.port}'


class Buffer(Protocol):
    """Not available in std lib until Python 3.12."""

    def __buffer__(self, *args, **kwargs):
        ...  # pragma: no cover
