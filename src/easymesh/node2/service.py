import asyncio
import logging
from asyncio import Future

from easymesh.asyncio import Reader
from easymesh.codec2 import NodeMessageCodec
from easymesh.node2.peer import PeerConnectionSelector
from easymesh.types import Data, RequestId, ServiceRequest, ServiceResponse

logger = logging.getLogger(__name__)


class ServiceCaller:
    def __init__(
            self,
            connection_selector: PeerConnectionSelector,
            node_message_codec: NodeMessageCodec,
            max_request_ids: int,
    ):
        self.connection_selector = connection_selector
        self.node_message_codec = node_message_codec
        self.max_request_ids = max_request_ids

        self._next_request_id: RequestId = 0
        self._response_futures: dict[RequestId, Future] = {}
        self._response_handler_readers: set[Reader] = set()

    async def request(self, service: str, data: Data) -> Data:
        connection = await self.connection_selector.get_connection_for_service(service)
        if connection is None:
            raise ValueError(f'No node hosting service={service!r}')

        self._start_response_handler(connection.reader)

        request_id, response_future = self._get_request_id_and_response_future()
        try:
            request = ServiceRequest(request_id, service, data)
            request = await self.node_message_codec.encode_service_request(request)

            async with connection.writer as writer:
                await writer.write(request)
                await writer.drain()

            response: ServiceResponse = await response_future
        finally:
            self._response_futures.pop(request_id, None)

        if response.error:
            raise ServiceResponseError(response.error)

        return response.data

    def _get_request_id_and_response_future(self) -> tuple[RequestId, Future]:
        request_id = self._get_request_id()
        response_future = self._response_futures[request_id] = Future()
        return request_id, response_future

    def _get_request_id(self) -> RequestId:
        request_id = self._find_next_available_request_id()
        self._inc_next_request_id()
        return request_id

    def _find_next_available_request_id(self) -> RequestId:
        for _ in range(self.max_request_ids):
            if self._next_request_id not in self._response_futures:
                return self._next_request_id

            self._inc_next_request_id()

        raise ServiceRequestError(f'All {self.max_request_ids} request IDs are in use')

    def _inc_next_request_id(self) -> None:
        self._next_request_id = (self._next_request_id + 1) % self.max_request_ids

    def _start_response_handler(self, reader: Reader) -> None:
        if reader not in self._response_handler_readers:
            self._response_handler_readers.add(reader)
            asyncio.create_task(self._response_handler(reader), name='ServiceResponseHandler')

    async def _response_handler(self, reader: Reader) -> None:
        try:
            while True:
                await self._handle_one_response(reader)
        finally:
            self._response_handler_readers.remove(reader)

    async def _handle_one_response(self, reader: Reader) -> None:
        response = await self.node_message_codec.decode_service_response(reader)

        response_future = self._response_futures.get(response.id)
        if response_future is None:
            logger.warning(f'Received response for unknown request id={response.id}')
            return

        response_future.set_result(response)


class ServiceRequestError(Exception):
    pass


class ServiceResponseError(Exception):
    pass
