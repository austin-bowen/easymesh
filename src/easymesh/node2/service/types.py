from typing import NamedTuple

from easymesh.types import Data, Service

RequestId = int


class ServiceRequest(NamedTuple):
    id: RequestId
    service: Service
    data: Data


class ServiceResponse(NamedTuple):
    id: RequestId
    data: Data = None
    error: str | None = None
