from abc import ABC, abstractmethod
from asyncio import Future, wait_for
from uuid import UUID

from easymesh.types import Data, ServiceCallback, ServiceName, ServiceResponse, Topic

RequestId = int


class ServicesManager(ABC):
    @abstractmethod
    async def start(self, node) -> None:
        ...

    @abstractmethod
    async def request(
            self,
            service: ServiceName,
            data: Data = None,
            timeout: float = None,
    ) -> ServiceResponse:
        ...

    @abstractmethod
    async def add_service(self, service: ServiceName, handler: ServiceCallback) -> None:
        ...


class BasicServicesOverTopicsManager(ServicesManager):
    """
    A basic services manager that uses topics to handle service requests.

    WARNING: A memory leak can occur in a very specific case:
    1. The task calling `request` is cancelled before the response is received
       (e.g. the `request` call is wrapped in a `wait_for` timeout), and
    2. A response to the request is never received.

    To avoid this scenario, use the `request` method's built-in `timeout` parameter.
    """

    def __init__(self):
        self.node = None

        self._request_counter: RequestId = 0
        self._response_futures: dict[RequestId, Future] = {}

    @property
    def uuid(self) -> UUID:
        return self.node.id.uuid

    async def start(self, node) -> None:
        self.node = node

        response_topic = get_response_topic(self.uuid)

        async def response_handler(_: Topic, data: tuple[RequestId, Data]) -> None:
            request_id, response_data = data

            response_future = self._response_futures.pop(request_id, None)
            if response_future is None:
                raise RuntimeError(f'Received response for unknown/timed out request_id={request_id}')

            response_future.set_result(response_data)

        await self.node.listen(response_topic, response_handler)

    async def request(
            self,
            service: ServiceName,
            data: Data = None,
            timeout: float = None,
    ) -> ServiceResponse:
        service_topic = get_service_topic(service)
        if not await self.node.topic_has_listeners(service_topic):
            raise NoServiceError(service)

        request_id = self._next_request_id()
        data = (self.uuid.bytes, request_id, data)

        response_future = self._response_futures[request_id] = Future()
        try:
            await self.node.send(service_topic, data)
            return await wait_for(response_future, timeout=timeout)
        finally:
            self._response_futures.pop(request_id, None)

    def _next_request_id(self) -> RequestId:
        request_id = self._request_counter
        self._request_counter += 1
        return request_id

    async def add_service(self, service: ServiceName, handler: ServiceCallback) -> None:
        service_topic = get_service_topic(service)

        async def meta_handler(_: Topic, data: tuple[bytes, RequestId, Data]) -> None:
            client_uuid_bytes, request_id, data = data

            response = await handler(service, data)
            response = (request_id, response)

            client_uuid = UUID(bytes=client_uuid_bytes)
            response_topic = get_response_topic(client_uuid)

            await self.node.send(response_topic, response)

        await self.node.listen(service_topic, meta_handler)


def get_service_topic(service: ServiceName) -> Topic:
    return f's/{service}'


def get_response_topic(uuid: UUID) -> Topic:
    return f'r/{uuid}'


class NoServiceError(Exception):
    def __init__(self, service: ServiceName):
        super().__init__(f'No nodes currently hosting service={service!r}')
        self.service = service
