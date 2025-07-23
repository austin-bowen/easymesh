from unittest.mock import AsyncMock, Mock

import pytest

from rosy.node.topic.listenermanager import TopicListenerManager
from rosy.node.topic.messagehandler import TopicMessageHandler
from rosy.node.topic.types import TopicMessage
from rosy.types import TopicCallback


class TestTopicMessageHandler:
    def setup_method(self):
        self.message = TopicMessage('topic', args=['arg'], kwargs={'key': 'value'})

        self.listener_manager = Mock(TopicListenerManager)

        self.handler = TopicMessageHandler(self.listener_manager)

    @pytest.mark.asyncio
    async def test_handle_message_with_callback(self):
        callback = AsyncMock(TopicCallback)
        self.listener_manager.get_callback.return_value = callback

        assert await self.handler.handle_message(self.message) is None

        callback.assert_awaited_once_with('topic', 'arg', key='value')

    @pytest.mark.asyncio
    async def test_handle_message_without_callback(self):
        self.listener_manager.get_callback.return_value = None

        assert await self.handler.handle_message(self.message) is None
