import asyncio
from unittest.mock import ANY, create_autospec

import pytest

from rosy.asyncio import LockableWriter, Reader, Writer
from rosy.authentication import Authenticator
from rosy.node.clienthandler import ClientHandler
from rosy.node.codec import NodeMessageCodec
from rosy.node.service.requesthandler import ServiceRequestHandler
from rosy.node.service.types import ServiceRequest
from rosy.node.topic.messagehandler import TopicMessageHandler
from rosy.node.topic.types import TopicMessage


class TestClientHandler:
    def setup_method(self):
        self.reader = create_autospec(Reader)

        self.writer = create_autospec(Writer)
        self.writer.get_extra_info.side_effect = lambda key: key

        self.authenticator = create_autospec(Authenticator)
        self.node_message_codec = create_autospec(NodeMessageCodec)
        self.topic_message_handler = create_autospec(TopicMessageHandler)
        self.service_request_handler = create_autospec(ServiceRequestHandler)

        self.handler = ClientHandler(
            self.authenticator,
            self.node_message_codec,
            self.topic_message_handler,
            self.service_request_handler,
        )

    @pytest.mark.asyncio
    async def test_receive_topic_message_calls_topic_message_handler(self):
        message = TopicMessage(topic='topic', args=['arg'], kwargs={'key': 'value'})

        self.node_message_codec.decode_topic_message_or_service_request.side_effect = [
            message,
            EOFError(),
        ]

        assert await self.handler.handle_client(self.reader, self.writer) is None

        self.authenticator.authenticate.assert_awaited_once_with(self.reader, self.writer)
        self.node_message_codec.decode_topic_message_or_service_request.assert_called_with(self.reader)
        self.topic_message_handler.handle_message.assert_awaited_once_with(message)
        self.service_request_handler.handle_request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_receive_service_request_calls_service_request_handler(self):
        message = ServiceRequest(
            id=0,
            service='service',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        self.node_message_codec.decode_topic_message_or_service_request.side_effect = [
            message,
            EOFError(),
        ]

        assert await self.handler.handle_client(self.reader, self.writer) is None

        # Service requests are handled in a separate task;
        # yield to allow the task to run.
        await asyncio.sleep(0)

        self.authenticator.authenticate.assert_awaited_once_with(self.reader, self.writer)
        self.node_message_codec.decode_topic_message_or_service_request.assert_called_with(self.reader)
        self.service_request_handler.handle_request.assert_awaited_once_with(message, ANY)
        self.topic_message_handler.handle_message.assert_not_awaited()

        actual_writer = self.service_request_handler.handle_request.call_args[0][1]
        assert isinstance(actual_writer, LockableWriter)
        assert actual_writer.writer is self.writer

    @pytest.mark.asyncio
    async def test_receive_unknown_object_raises_RuntimeError(self):
        message = object()

        self.node_message_codec.decode_topic_message_or_service_request.side_effect = [
            message,
            EOFError(),
        ]

        with pytest.raises(RuntimeError, match='Unreachable code'):
            await self.handler.handle_client(self.reader, self.writer)

        self.authenticator.authenticate.assert_awaited_once_with(self.reader, self.writer)
        self.node_message_codec.decode_topic_message_or_service_request.assert_called_once_with(self.reader)
        self.service_request_handler.handle_request.assert_not_awaited()
        self.topic_message_handler.handle_message.assert_not_awaited()
