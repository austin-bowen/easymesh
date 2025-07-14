from asyncio import Lock
from collections.abc import Awaitable, Iterable
from typing import NamedTuple

from easymesh.asyncio import (
    LockableWriter,
    Reader,
    Writer,
    open_connection,
    open_unix_connection,
)
from easymesh.authentication import Authenticator
from easymesh.network import get_hostname
from easymesh.node2.loadbalancing import ServiceLoadBalancer, TopicLoadBalancer
from easymesh.node2.topology import MeshTopologyManager
from easymesh.specs import ConnectionSpec, IpConnectionSpec, MeshNodeSpec, NodeId, UnixConnectionSpec
from easymesh.types import Host, Service, Topic


class PeerConnection(NamedTuple):
    reader: Reader
    writer: LockableWriter

    async def close(self) -> None:
        await self.writer.close()
        await self.writer.wait_closed()


class PeerConnectionBuilder:
    def __init__(self, authenticator: Authenticator, host: Host = None):
        self.authenticator = authenticator
        self.host = host or get_hostname()

    async def build(self, conn_specs: Iterable[ConnectionSpec]) -> tuple[Reader, Writer]:
        reader_writer = None
        for conn_spec in conn_specs:
            try:
                reader_writer = await self._get_connection(conn_spec)
            except ConnectionError as e:
                print(f'Error connecting to {conn_spec}: {e}')
                continue

            if reader_writer is not None:
                break

        if reader_writer is None:
            raise ConnectionError('Could not connect to any connection spec')

        reader, writer = reader_writer
        await self.authenticator.authenticate(reader, writer)

        return reader, writer

    async def _get_connection(self, conn_spec: ConnectionSpec) -> tuple[Reader, Writer] | None:
        if isinstance(conn_spec, IpConnectionSpec):
            return await open_connection(
                host=conn_spec.host,
                port=conn_spec.port,
            )
        elif isinstance(conn_spec, UnixConnectionSpec):
            if conn_spec.host != self.host:
                return None

            return await open_unix_connection(path=conn_spec.path)
        else:
            raise ValueError(f'Invalid connection spec: {conn_spec}')


class PeerConnectionManager:
    def __init__(self, conn_builder: PeerConnectionBuilder):
        self.conn_builder = conn_builder

        self._connections: dict[NodeId, PeerConnection] = {}
        self._connections_lock = Lock()

    async def get_connection(self, node: MeshNodeSpec) -> PeerConnection:
        async with self._connections_lock:
            connection = self._connections.get(node.id, None)
            if connection:
                return connection

            reader, writer = await self.conn_builder.build(node.connection_specs)

            reader = self._Reader(reader, self, node)
            writer = self._Writer(writer, self, node)
            writer = LockableWriter(writer)

            connection = PeerConnection(reader, writer)
            self._connections[node.id] = connection
            return connection

    async def close_connection(self, node: MeshNodeSpec) -> None:
        async with self._connections_lock:
            connection = self._connections.pop(node.id, None)

        if connection:
            await connection.close()

    class _Managed:
        def __init__(self, manager: 'PeerConnectionManager', node: MeshNodeSpec):
            self.manager = manager
            self.node = node

        async def _close_on_error(self, awaitable: Awaitable):
            try:
                return await awaitable
            except Exception:
                await self.manager.close_connection(self.node)
                raise

    class _Reader(_Managed, Reader):
        def __init__(
                self,
                reader: Reader,
                manager: 'PeerConnectionManager',
                node: MeshNodeSpec,
        ):
            super().__init__(manager, node)
            self.reader = reader

        async def readexactly(self, n: int) -> bytes:
            return await self._close_on_error(
                self.reader.readexactly(n)
            )

        async def readuntil(self, separator: bytes) -> bytes:
            return await self._close_on_error(
                self.reader.readuntil(separator)
            )

    class _Writer(_Managed, Writer):
        def __init__(
                self,
                writer: Writer,
                manager: 'PeerConnectionManager',
                node: MeshNodeSpec,
        ):
            super().__init__(manager, node)
            self.writer = writer

        async def write(self, data: bytes) -> None:
            await self._close_on_error(
                self.writer.write(data)
            )

        async def drain(self) -> None:
            await self._close_on_error(
                self.writer.drain()
            )

        async def close(self) -> None:
            await self.writer.close()

        async def wait_closed(self) -> None:
            await self.writer.wait_closed()


class PeerSelector:
    def __init__(
            self,
            topology_manager: MeshTopologyManager,
            topic_load_balancer: TopicLoadBalancer,
            service_load_balancer: ServiceLoadBalancer,
    ):
        self.topology_manager = topology_manager
        self.topic_load_balancer = topic_load_balancer
        self.service_load_balancer = service_load_balancer

    def get_nodes_for_topic(self, topic: Topic) -> list[MeshNodeSpec]:
        peers = self.topology_manager.get_nodes_listening_to_topic(topic)
        return self.topic_load_balancer.choose_nodes(peers, topic)

    def get_node_for_service(self, service: Service) -> MeshNodeSpec | None:
        peers = self.topology_manager.get_nodes_providing_service(service)
        return self.service_load_balancer.choose_node(peers, service)
