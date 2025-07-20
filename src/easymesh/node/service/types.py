from typing import NamedTuple

from easymesh.node.types import Args, KWArgs
from easymesh.types import Data, Service

RequestId = int


class ServiceRequest(NamedTuple):
    id: RequestId
    service: Service
    args: Args
    kwargs: KWArgs


class ServiceResponse(NamedTuple):
    id: RequestId
    result: Data = None
    error: str | None = None
