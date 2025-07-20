from unittest.mock import AsyncMock

import pytest

from easymesh.asyncio import LockableWriter
from easymesh.node.codec import NodeMessageCodec
from easymesh.node.peer import PeerConnection, PeerConnectionManager, PeerSelector
from easymesh.node.service.caller import ServiceCaller, ServiceRequestError, ServiceResponseError
from easymesh.node.service.types import ServiceResponse
from easymesh.specs import MeshNodeSpec
from easymesh.types import Service


class TestServiceCaller:
    def setup_method(self):
        self.connection = AsyncMock(spec=PeerConnection)
        self.connection.writer = AsyncMock(spec=LockableWriter)
        self.connection.writer.__aenter__.return_value = self.connection.writer

        node = AsyncMock(spec=MeshNodeSpec)

        peer_selector = AsyncMock(spec=PeerSelector)
        peer_selector.get_node_for_service.side_effect = (
            lambda service: node if service == 'service' else None
        )

        connection_manager = AsyncMock(spec=PeerConnectionManager)
        connection_manager.get_connection.return_value = self.connection

        self.node_message_codec = AsyncMock(spec=NodeMessageCodec)
        self.node_message_codec.decode_service_response.side_effect = []

        self.service_caller = ServiceCaller(
            peer_selector,
            connection_manager,
            self.node_message_codec,
            max_request_ids=10,
        )

    @pytest.mark.asyncio
    async def test_request_with_success_response_returns_response_data(self):
        self.node_message_codec.decode_service_response.side_effect = [
            ServiceResponse(id=0, result='response'),
        ]

        response = await self._call('service')
        assert response == 'response'

        assert self.service_caller._next_request_id == 1
        self._assert_no_pending_requests()
        assert self.connection.writer.write.call_count == 1
        assert self.connection.writer.drain.await_count == 1

    @pytest.mark.asyncio
    async def test_request_with_error_response_raises_ServiceErrorResponse(self):
        self.node_message_codec.decode_service_response.side_effect = [
            ServiceResponse(id=0, error='error message'),
        ]

        with pytest.raises(ServiceResponseError, match='error message'):
            await self._call('service')

        self._assert_no_pending_requests()
        assert self.connection.writer.write.call_count == 1
        assert self.connection.writer.drain.await_count == 1

    @pytest.mark.asyncio
    async def test_request_with_unknown_service_raises_ValueError(self):
        with pytest.raises(ValueError, match="No node hosting service='unknown_service'"):
            await self._call('unknown_service')

        self._assert_no_pending_requests()
        self.connection.writer.write.assert_not_called()
        self.connection.writer.drain.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_request_raises_ServiceRequestError_when_all_request_ids_are_taken(self):
        self.service_caller._response_futures[self.connection.reader] = {
            i: AsyncMock() for i in range(self.service_caller.max_request_ids)
        }

        with pytest.raises(ServiceRequestError):
            await self._call('service')

    @pytest.mark.asyncio
    async def test_requests_fail_for_reader_with_error(self):
        self.node_message_codec.decode_service_response.side_effect = ConnectionError()

        with pytest.raises(
                ServiceResponseError,
                match='Reader .* was closed before response was received',
        ):
            await self._call('service')

        self._assert_no_pending_requests()

    async def _call(self, service: Service):
        return await self.service_caller.call(service, ['arg'], {'key': 'value'})

    def _assert_no_pending_requests(self):
        for response_futures in self.service_caller._response_futures.values():
            assert not response_futures
