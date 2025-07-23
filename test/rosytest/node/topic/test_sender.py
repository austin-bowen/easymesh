from unittest.mock import AsyncMock

import pytest

from rosy.asyncio import LockableWriter
from rosy.node.codec import NodeMessageCodec
from rosy.node.peer import PeerConnection, PeerConnectionManager, PeerSelector
from rosy.node.topic.sender import TopicSender
from rosy.node.topic.types import TopicMessage
from rosy.specs import MeshNodeSpec


class TestTopicSender:
    def setup_method(self) -> None:
        self.connection = AsyncMock(spec=PeerConnection)
        self.connection.writer = AsyncMock(spec=LockableWriter)
        self.connection.writer.__aenter__.return_value = self.connection.writer

        nodes = [
            AsyncMock(spec=MeshNodeSpec),
            AsyncMock(spec=MeshNodeSpec),
        ]

        self.connections = {}
        for node in nodes:
            connection = AsyncMock(spec=PeerConnection)
            connection.writer = AsyncMock(spec=LockableWriter)
            connection.writer.__aenter__.return_value = connection.writer
            self.connections[node] = connection

        self.peer_selector = AsyncMock(spec=PeerSelector)
        self.peer_selector.get_nodes_for_topic.return_value = nodes

        self.connection_manager = AsyncMock(spec=PeerConnectionManager)
        self.connection_manager.get_connection.side_effect = (
            lambda node: self.connections[node]
        )

        self.encoded_data = b'encoded data'
        self.node_message_codec = AsyncMock(spec=NodeMessageCodec)
        self.node_message_codec.encode_topic_message.return_value = self.encoded_data

        self.topic_sender = TopicSender(
            self.peer_selector,
            self.connection_manager,
            self.node_message_codec,
        )

    @pytest.mark.asyncio
    async def test_send(self):
        message = TopicMessage('topic', ['arg'], {'key': 'value'})

        await self.topic_sender.send(message.topic, message.args, message.kwargs)

        self.peer_selector.get_nodes_for_topic.assert_called_once_with(message.topic)
        self.node_message_codec.encode_topic_message.assert_awaited_once_with(message)

        for connection in self.connections.values():
            connection.writer.write.assert_called_once_with(self.encoded_data)
            connection.writer.drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send_with_no_listening_nodes(self):
        self.peer_selector.get_nodes_for_topic.return_value = []

        message = TopicMessage('topic', ['arg'], {'key': 'value'})

        await self.topic_sender.send(message.topic, message.args, message.kwargs)

        self.peer_selector.get_nodes_for_topic.assert_called_once_with(message.topic)
        self.node_message_codec.encode_topic_message.assert_not_awaited()
        self.connection_manager.get_connection.assert_not_awaited()
