import asyncio
import logging
from asyncio import Future
from collections.abc import Iterator
from contextlib import contextmanager
from weakref import WeakKeyDictionary

from easymesh.asyncio import Reader
from easymesh.node.codec import NodeMessageCodec
from easymesh.node.peer import PeerConnectionManager, PeerSelector
from easymesh.node.service.types import RequestId, ServiceRequest, ServiceResponse
from easymesh.node.types import Args, KWArgs
from easymesh.types import Data

logger = logging.getLogger(__name__)


class ServiceCaller:
    def __init__(
            self,
            peer_selector: PeerSelector,
            connection_manager: PeerConnectionManager,
            node_message_codec: NodeMessageCodec,
            max_request_ids: int,
    ):
        self.peer_selector = peer_selector
        self.connection_selector = connection_manager
        self.node_message_codec = node_message_codec
        self.max_request_ids = max_request_ids

        self._next_request_id: RequestId = 0
        self._response_futures: WeakKeyDictionary[
            Reader,
            dict[RequestId, Future]
        ] = WeakKeyDictionary()

    async def call(self, service: str, args: Args, kwargs: KWArgs) -> Data:
        node = self.peer_selector.get_node_for_service(service)
        if node is None:
            raise ValueError(f'No node hosting service={service!r}')

        connection = await self.connection_selector.get_connection(node)

        self._start_response_handler(connection.reader)

        with self._get_request_id_and_response_future(connection.reader) as (request_id, response_future):
            request = ServiceRequest(request_id, service, args, kwargs)
            request = await self.node_message_codec.encode_service_request(request)

            async with connection.writer as writer:
                writer.write(request)
                await writer.drain()

            response: ServiceResponse = await response_future

        if response.error:
            raise ServiceResponseError(response.error)

        return response.result

    @contextmanager
    def _get_request_id_and_response_future(
            self,
            reader: Reader,
    ) -> Iterator[tuple[RequestId, Future]]:
        request_id = self._get_new_request_id(reader)
        response_future = Future()
        self._response_futures[reader][request_id] = response_future

        yield request_id, response_future

        futures = self._response_futures.get(reader)
        if futures:
            futures.pop(request_id, None)

    def _get_new_request_id(self, reader: Reader) -> RequestId:
        request_id = self._find_next_available_request_id(reader)
        self._inc_next_request_id()
        return request_id

    def _find_next_available_request_id(self, reader: Reader) -> RequestId:
        response_futures = self._response_futures[reader]
        for _ in range(self.max_request_ids):
            if self._next_request_id not in response_futures:
                return self._next_request_id

            self._inc_next_request_id()

        raise ServiceRequestError(
            f'All {self.max_request_ids} request IDs are in use for reader={reader!r}'
        )

    def _inc_next_request_id(self) -> None:
        self._next_request_id = (self._next_request_id + 1) % self.max_request_ids

    def _start_response_handler(self, reader: Reader) -> None:
        if reader not in self._response_futures:
            self._response_futures[reader] = {}
            asyncio.create_task(self._response_handler(reader), name='ServiceResponseHandler')

    async def _response_handler(self, reader: Reader) -> None:
        try:
            while True:
                await self._handle_one_response(reader)
        finally:
            self._fail_pending_response_futures_for(reader)
            self._response_futures.pop(reader)

    async def _handle_one_response(self, reader: Reader) -> None:
        response = await self.node_message_codec.decode_service_response(reader)

        response_future = self._response_futures[reader].get(response.id)
        if response_future is None:
            logger.warning(
                f'Received response for unknown request '
                f'id={response.id} on reader={reader!r}'
            )
            return

        if not response_future.done():
            response_future.set_result(response)

    def _fail_pending_response_futures_for(self, reader: Reader) -> None:
        error = ServiceResponseError(f'Reader {reader!r} was closed before response was received')
        response_futures = self._response_futures[reader].values()
        for response_future in response_futures:
            if not response_future.done():
                response_future.set_exception(error)


class ServiceRequestError(Exception):
    pass


class ServiceResponseError(Exception):
    pass
