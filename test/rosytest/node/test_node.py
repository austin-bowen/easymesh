from unittest.mock import AsyncMock, create_autospec

import pytest

from rosy.coordinator.client import MeshCoordinatorClient
from rosy.node.callbackmanager import CallbackManager
from rosy.node.node import Node, ServiceProxy, TopicProxy
from rosy.node.peer import PeerConnectionManager
from rosy.node.servers import ServersManager
from rosy.node.service.caller import ServiceCaller
from rosy.node.topic.sender import TopicSender
from rosy.node.topology import MeshTopologyManager
from rosy.specs import MeshNodeSpec, NodeId


class TestNode:
    def setup_method(self):
        self.id = NodeId('node')
        self.coordinator_client = create_autospec(MeshCoordinatorClient)
        self.servers_manager = create_autospec(ServersManager)
        self.topology_manager = create_autospec(MeshTopologyManager)
        self.connection_manager = create_autospec(PeerConnectionManager)
        self.topic_sender = create_autospec(TopicSender)
        self.topic_listener_manager = create_autospec(CallbackManager)
        self.service_caller = create_autospec(ServiceCaller)
        self.service_handler_manager = create_autospec(CallbackManager)

        self.node = Node(
            id=self.id,
            coordinator_client=self.coordinator_client,
            servers_manager=self.servers_manager,
            topology_manager=self.topology_manager,
            connection_manager=self.connection_manager,
            topic_sender=self.topic_sender,
            topic_listener_manager=self.topic_listener_manager,
            service_caller=self.service_caller,
            service_handler_manager=self.service_handler_manager,
        )

    def test_constructor_sets_up_coordinator_client(self):
        self.coordinator_client.set_broadcast_handler.assert_called_once_with(
            self.node._handle_topology_broadcast,
        )

    def test_id_property_is_read_only(self):
        assert self.node.id is self.id

        with pytest.raises(AttributeError):
            self.node.id = NodeId('new_node')

    def test_str(self):
        assert str(self.node) == str(self.id)

    @pytest.mark.asyncio
    async def test_start(self):
        await self.node.start()

        self.servers_manager.start_servers.assert_awaited_once()
        self.coordinator_client.register_node.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_send(self):
        await self.node.send('topic', 'arg', key='value')

        self.topic_sender.send.assert_awaited_once_with(
            'topic',
            ('arg',),
            {'key': 'value'},
        )

    @pytest.mark.asyncio
    async def test_listen(self):
        callback = AsyncMock()

        await self.node.listen('topic', callback)

        self.topic_listener_manager.set_callback.assert_called_once_with('topic', callback)
        self.coordinator_client.register_node.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_listening_to_valid_topic_registers_node(self):
        callback = AsyncMock()
        self.topic_listener_manager.remove_callback.return_value = callback

        assert await self.node.stop_listening('topic') is None

        self.topic_listener_manager.remove_callback.assert_called_once_with('topic')
        self.coordinator_client.register_node.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_listening_to_invalid_topic_does_not_register_node(self):
        self.topic_listener_manager.remove_callback.return_value = None

        assert await self.node.stop_listening('topic') is None

        self.topic_listener_manager.remove_callback.assert_called_once_with('topic')
        self.coordinator_client.register_node.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_topic_has_listeners_returns_True(self):
        self.topology_manager.get_nodes_listening_to_topic.return_value = [
            create_autospec(MeshNodeSpec),
        ]

        assert await self.node.topic_has_listeners('topic') is True

    @pytest.mark.asyncio
    async def test_topic_has_listeners_returns_False(self):
        self.topology_manager.get_nodes_listening_to_topic.return_value = []

        assert await self.node.topic_has_listeners('topic') is False

    @pytest.mark.asyncio
    async def test_wait_for_listener(self):
        self.topology_manager.get_nodes_listening_to_topic.side_effect = [
            [],  # First call, no listeners
            [create_autospec(MeshNodeSpec)],  # Second call, listener found
        ]

        assert await self.node.wait_for_listener('topic', poll_interval=0.) is None

        assert self.topology_manager.get_nodes_listening_to_topic.call_count == 2

    def test_get_topic(self):
        topic = self.node.get_topic('topic')

        assert isinstance(topic, TopicProxy)
        assert topic.node is self.node
        assert topic.topic == 'topic'


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
