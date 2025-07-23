from unittest.mock import AsyncMock, create_autospec

import pytest

from rosy.asyncio import LockableWriter
from rosy.node.callbackmanager import CallbackManager
from rosy.node.codec import NodeMessageCodec
from rosy.node.service.requesthandler import ServiceRequestHandler
from rosy.node.service.types import ServiceRequest, ServiceResponse


class TestServiceRequestHandler:
    def setup_method(self):
        self.service_handler_manager = create_autospec(CallbackManager)
        self.node_message_codec = create_autospec(NodeMessageCodec)

        self.writer = create_autospec(LockableWriter)

        self.handler = ServiceRequestHandler(
            self.service_handler_manager,
            self.node_message_codec,
        )

    @pytest.mark.asyncio
    async def test_handle_request_sends_result_on_success(self):
        handler = AsyncMock(return_value='result')
        self.service_handler_manager.get_callback.return_value = handler

        request = ServiceRequest(
            id=0,
            service='service',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        assert await self.handler.handle_request(request, self.writer) is None

        expected_response = ServiceResponse(id=0, result='result', error=None)

        self.service_handler_manager.get_callback.assert_called_once_with('service')
        handler.assert_awaited_once_with(
            'service',
            'arg',
            key='value',
        )
        self.node_message_codec.encode_service_response.assert_awaited_once_with(
            self.writer,
            expected_response,
        )

    @pytest.mark.asyncio
    async def test_handle_request_sends_error_if_exception_occurs_in_handler(self):
        handler = AsyncMock(side_effect=ValueError('error occurred'))
        self.service_handler_manager.get_callback.return_value = handler

        request = ServiceRequest(
            id=0,
            service='service',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        assert await self.handler.handle_request(request, self.writer) is None

        expected_response = ServiceResponse(
            id=0,
            result=None,
            error="ValueError('error occurred')",
        )

        self.service_handler_manager.get_callback.assert_called_once_with('service')
        handler.assert_awaited_once_with(
            'service',
            'arg',
            key='value',
        )
        self.node_message_codec.encode_service_response.assert_awaited_once_with(
            self.writer,
            expected_response,
        )

    @pytest.mark.asyncio
    async def test_handle_request_sends_error_if_service_not_provided(self):
        self.service_handler_manager.get_callback.return_value = None

        request = ServiceRequest(
            id=0,
            service='unknown_service',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        assert await self.handler.handle_request(request, self.writer) is None

        expected_response = ServiceResponse(
            id=0,
            result=None,
            error="service='unknown_service' is not provided by this node",
        )

        self.service_handler_manager.get_callback.assert_called_once_with('unknown_service')
        self.node_message_codec.encode_service_response.assert_awaited_once_with(
            self.writer,
            expected_response,
        )
