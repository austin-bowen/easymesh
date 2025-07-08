from unittest.mock import AsyncMock

import pytest

from easymesh.codec2 import NodeMessageCodec
from easymesh.node2.peer import LockableWriter, PeerConnection, PeerConnectionSelector
from easymesh.node2.service import ServiceCaller, ServiceResponseError
from easymesh.types import ServiceResponse


class TestServiceCaller:
    def setup_method(self):
        self.connection = AsyncMock(spec=PeerConnection)
        self.connection.writer = AsyncMock(spec=LockableWriter)
        self.connection.writer.__aenter__.return_value = self.connection.writer

        connection_selector = AsyncMock(spec=PeerConnectionSelector)
        connection_selector.get_connection_for_service.side_effect = (
            lambda service: self.connection if service == 'service' else None
        )

        self.node_message_codec = AsyncMock(spec=NodeMessageCodec)

        self.service_caller = ServiceCaller(
            connection_selector,
            self.node_message_codec,
        )

    @pytest.mark.asyncio
    async def test_request_with_success_response_returns_response_data(self):
        self.node_message_codec.decode_service_response.side_effect = [
            ServiceResponse(id=0, data='response'),
        ]

        response = await self.service_caller.request('service', 'data')
        assert response == 'response'

        assert self.connection.writer.write.await_count == 1
        assert self.connection.writer.drain.await_count == 1

    @pytest.mark.asyncio
    async def test_request_with_error_response_raises_ServiceErrorResponse(self):
        self.node_message_codec.decode_service_response.side_effect = [
            ServiceResponse(id=0, error='error message'),
        ]

        with pytest.raises(ServiceResponseError, match='error message'):
            await self.service_caller.request('service', 'data')

        assert self.connection.writer.write.await_count == 1
        assert self.connection.writer.drain.await_count == 1

    @pytest.mark.asyncio
    async def test_request_with_unknown_service_raises_ValueError(self):
        with pytest.raises(ValueError, match="No node hosting service='unknown_service'"):
            await self.service_caller.request('unknown_service', 'data')

        self.connection.writer.write.assert_not_awaited()
        self.connection.writer.drain.assert_not_awaited()
