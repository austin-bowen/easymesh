from unittest.mock import call, create_autospec, patch

import pytest

from rosy.asyncio import LockableWriter, Reader, Writer
from rosy.authentication import Authenticator
from rosy.node.loadbalancing import ServiceLoadBalancer, TopicLoadBalancer
from rosy.node.peer import PeerConnection, PeerConnectionBuilder, PeerConnectionManager, PeerSelector
from rosy.node.topology import MeshTopologyManager
from rosy.specs import IpConnectionSpec, MeshNodeSpec, NodeId, UnixConnectionSpec


class TestPeerConnection:
    def setup_method(self):
        self.reader = create_autospec(Reader)
        self.writer = create_autospec(Writer)

        self.connection = PeerConnection(self.reader, self.writer)

    @pytest.mark.asyncio
    async def test_close(self):
        await self.connection.close()

        self.writer.close.assert_called_once()
        self.writer.wait_closed.assert_awaited_once()

    def test_is_closing(self):
        expected = True
        self.writer.is_closing.return_value = expected

        assert self.connection.is_closing() == expected

        self.writer.is_closing.assert_called_once()


class TestPeerConnectionBuilder:
    def setup_method(self):
        self.authenticator = create_autospec(Authenticator)

        self.conn_builder = PeerConnectionBuilder(
            self.authenticator,
            host='host',
        )

    @pytest.mark.asyncio
    async def test_build_with_IPConnectionSpec(self, open_connection_mock):
        reader = create_autospec(Reader)
        writer = create_autospec(Writer)
        open_connection_mock.return_value = reader, writer

        conn_spec = IpConnectionSpec('host', 8080)

        result = await self.conn_builder.build([conn_spec])

        assert result == (reader, writer)

        open_connection_mock.assert_awaited_once_with(host='host', port=8080)
        self.authenticator.authenticate.assert_awaited_once_with(reader, writer)

    @pytest.mark.asyncio
    async def test_build_with_UnixConnectionSpec_on_same_host_succeeds(self, open_unix_connection_mock):
        reader = create_autospec(Reader)
        writer = create_autospec(Writer)
        open_unix_connection_mock.return_value = reader, writer

        conn_spec = UnixConnectionSpec('path', 'host')

        result = await self.conn_builder.build([conn_spec])

        assert result == (reader, writer)

        open_unix_connection_mock.assert_awaited_once_with(path='path')
        self.authenticator.authenticate.assert_awaited_once_with(reader, writer)

    @pytest.mark.asyncio
    async def test_build_with_UnixConnectionSpec_on_different_host_fails(self, open_unix_connection_mock):
        conn_spec = UnixConnectionSpec('path', 'other-host')

        with pytest.raises(ConnectionError):
            await self.conn_builder.build([conn_spec])

        open_unix_connection_mock.assert_not_awaited()
        self.authenticator.authenticate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_build_with_multiple_specs_uses_first_successful(self, open_connection_mock):
        reader = create_autospec(Reader)
        writer = create_autospec(Writer)
        open_connection_mock.side_effect = [
            ConnectionError('Connection failed'),
            (reader, writer),  # Second spec succeeds
        ]

        conn_specs = [
            IpConnectionSpec('host1', 8080),
            IpConnectionSpec('host2', 8080),
        ]

        result = await self.conn_builder.build(conn_specs)

        assert result == (reader, writer)

        assert open_connection_mock.call_args_list == [
            call(host='host1', port=8080),
            call(host='host2', port=8080),
        ]
        self.authenticator.authenticate.assert_awaited_once_with(reader, writer)

    @pytest.mark.asyncio
    async def test_build_raises_ConnectionError_if_no_specs_succeed(self, open_connection_mock):
        open_connection_mock.side_effect = [
            ConnectionError('Connection failed'),
            ConnectionError('Connection failed'),
        ]

        conn_specs = [
            IpConnectionSpec('host1', 8080),
            IpConnectionSpec('host2', 8080),
        ]

        with pytest.raises(ConnectionError, match='Could not connect to any connection spec'):
            await self.conn_builder.build(conn_specs)

        assert open_connection_mock.call_args_list == [
            call(host='host1', port=8080),
            call(host='host2', port=8080),
        ]
        self.authenticator.authenticate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_build_raises_ValueError_if_unknown_connection_spec_given(self):
        with pytest.raises(ValueError, match='Unrecognized connection spec:'):
            not_a_connection_spec = object()
            await self.conn_builder._get_connection([not_a_connection_spec])

        self.authenticator.authenticate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_build_raises_ConnectionError_if_no_specs_provided(self):
        with pytest.raises(ConnectionError, match='Could not connect to any connection spec'):
            await self.conn_builder.build([])

        self.authenticator.authenticate.assert_not_awaited()


@pytest.fixture
def open_connection_mock():
    with patch('rosy.node.peer.open_connection') as mock:
        yield mock


@pytest.fixture
def open_unix_connection_mock():
    with patch('rosy.node.peer.open_unix_connection') as mock:
        yield mock


class TestPeerConnectionManager:
    def setup_method(self):
        self.reader = create_autospec(Reader)

        self.writer = create_autospec(Writer)
        self.writer.is_closing.return_value = False

        self.conn_builder = create_autospec(PeerConnectionBuilder)
        self.conn_builder.build.return_value = self.reader, self.writer

        self.manager = PeerConnectionManager(self.conn_builder)

    @pytest.mark.asyncio
    async def test_get_connection_returns_new_connection_on_first_call(self):
        node = mock_node('node')

        connection = await self.manager.get_connection(node)

        assert connection.reader is self.reader
        assert isinstance(connection.writer, LockableWriter)
        assert connection.writer.writer is self.writer

        self.conn_builder.build.assert_awaited_once_with(node.connection_specs)

    @pytest.mark.asyncio
    async def test_get_connection_returns_cached_connection_on_second_call(self):
        node = mock_node('node')

        connection1 = await self.manager.get_connection(node)

        self.conn_builder.build.assert_awaited_once_with(node.connection_specs)

        connection2 = await self.manager.get_connection(node)

        assert connection2 is connection1

        self.conn_builder.build.assert_awaited_once_with(node.connection_specs)

    @pytest.mark.asyncio
    async def test_get_connection_returns_new_connection_on_second_call_when_cached_connection_closed(self):
        node = mock_node('node')

        connection1 = await self.manager.get_connection(node)

        self.writer.is_closing.return_value = True

        connection2 = await self.manager.get_connection(node)

        assert connection2 is not connection1

        assert self.conn_builder.build.call_args_list == [
            call(node.connection_specs),
            call(node.connection_specs),
        ]


def mock_node(name: str):
    node = create_autospec(MeshNodeSpec)

    node.id = NodeId(name)
    node.connection_specs = [create_autospec(IpConnectionSpec)]

    return node


class TestPeerSelector:
    def setup_method(self):
        self.topology_manager = create_autospec(MeshTopologyManager)
        self.topic_load_balancer = create_autospec(TopicLoadBalancer)
        self.service_load_balancer = create_autospec(ServiceLoadBalancer)

        self.selector = PeerSelector(
            self.topology_manager,
            self.topic_load_balancer,
            self.service_load_balancer,
        )

    def test_get_nodes_for_topic(self):
        nodes = ['node0', 'node1']
        self.topology_manager.get_nodes_listening_to_topic.return_value = nodes
        self.topic_load_balancer.choose_nodes.return_value = ['node1']

        topic = 'topic'

        result = self.selector.get_nodes_for_topic(topic)

        assert result == ['node1']

        self.topology_manager.get_nodes_listening_to_topic.assert_called_once_with(topic)
        self.topic_load_balancer.choose_nodes.assert_called_once_with(nodes, topic)
        self.service_load_balancer.choose_node.assert_not_called()

    def test_get_node_for_service(self):
        nodes = ['node0', 'node1']
        self.topology_manager.get_nodes_providing_service.return_value = nodes
        self.service_load_balancer.choose_node.return_value = 'node1'

        service = 'service'

        result = self.selector.get_node_for_service(service)

        assert result == 'node1'

        self.topology_manager.get_nodes_providing_service.assert_called_once_with(service)
        self.service_load_balancer.choose_node.assert_called_once_with(nodes, service)
        self.topic_load_balancer.choose_nodes.assert_not_called()
