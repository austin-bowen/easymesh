from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, NamedTuple, Protocol

Host = str
ServerHost = Host | Sequence[Host] | None
Port = int


@dataclass
class Endpoint:
    host: Host
    port: Port

    def __str__(self):
        return f'{self.host}:{self.port}'


Topic = str
Data = Any
TopicCallback = Callable[[Topic, Data], None]


class Message(NamedTuple):
    topic: Topic
    data: Data


Service = str
ServiceCallback = Callable[[Service, Data], Awaitable[Data]]


class Buffer(Protocol):
    """Not available in std lib until Python 3.12."""

    def __buffer__(self, *args, **kwargs):
        ...
