from unittest.mock import create_autospec

import pytest

from rosy.node.node import Node, ServiceProxy, TopicProxy


class TestNode:
    @pytest.mark.asyncio
    async def test(self):
        # TODO
        pytest.fail()


class TestTopicProxy:
    def setup_method(self):
        self.node = create_autospec(Node)
        self.topic = TopicProxy(self.node, 'topic')

    def test_str(self):
        assert str(self.topic) == "TopicProxy(topic='topic')"

    @pytest.mark.asyncio
    async def test_send(self):
        await self.topic.send('arg', key='value')

        self.node.send.assert_called_once_with(self.topic.topic, 'arg', key='value')

    @pytest.mark.asyncio
    async def test_has_listeners(self):
        self.node.topic_has_listeners.return_value = True

        assert await self.topic.has_listeners() is True

        self.node.topic_has_listeners.assert_called_once_with(self.topic.topic)

    @pytest.mark.asyncio
    async def test_wait_for_listener(self):
        await self.topic.wait_for_listener(poll_interval=0.5)

        self.node.wait_for_listener.assert_called_once_with(
            self.topic.topic,
            poll_interval=0.5,
        )

    def test_depends_on_listener(self):
        expected = object()
        self.node.depends_on_listener.return_value = expected

        result = self.topic.depends_on_listener(poll_interval=0.5)

        assert result is expected

        self.node.depends_on_listener.assert_called_once_with(
            self.topic.topic,
            poll_interval=0.5,
        )


class TestServiceProxy:
    def setup_method(self):
        self.node = create_autospec(Node)
        self.service = ServiceProxy(self.node, 'service')

    def test_str(self):
        assert str(self.service) == "ServiceProxy(service='service')"

    @pytest.mark.asyncio
    async def test_magic_call(self):
        await self.service('arg', key='value')

        self.node.call.assert_called_once_with(self.service.service, 'arg', key='value')

    @pytest.mark.asyncio
    async def test_call(self):
        await self.service.call('arg', key='value')

        self.node.call.assert_awaited_once_with(self.service.service, 'arg', key='value')

    @pytest.mark.asyncio
    async def test_has_providers(self):
        self.node.service_has_providers.return_value = True

        assert await self.service.has_providers() is True

        self.node.service_has_providers.assert_awaited_once_with(self.service.service)

    @pytest.mark.asyncio
    async def test_wait_for_provider(self):
        await self.service.wait_for_provider(poll_interval=0.5)

        self.node.wait_for_service_provider.assert_called_once_with(
            self.service.service,
            poll_interval=0.5,
        )
