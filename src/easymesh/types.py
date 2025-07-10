from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, NamedTuple, Protocol, Union

Host = str
ServerHost = Union[Host, Sequence[Host], None]
Port = int


@dataclass
class Endpoint:
    host: Host
    port: Port

    def __str__(self):
        return f'{self.host}:{self.port}'


Topic = str
Service = str
Data = Any
TopicCallback = Callable[[Topic, Data], None]


class Message(NamedTuple):
    topic: Topic
    data: Data


ServiceName = str
ServiceResponse = Any
ServiceCallback = Callable[[Topic, Data], Awaitable[ServiceResponse]]
RequestId = int


class ServiceRequest(NamedTuple):
    id: RequestId
    service: ServiceName
    data: Data


class ServiceResponse(NamedTuple):
    id: RequestId
    data: Data = None
    error: str | None = None


class Buffer(Protocol):
    """Not available in std lib until Python 3.12."""

    def __buffer__(self, *args, **kwargs):
        ...
