import logging
from asyncio import Lock, open_connection, open_unix_connection
from collections.abc import Iterable
from typing import NamedTuple

from easymesh.asyncio import (
    LockableWriter,
    Reader,
    Writer,
)
from easymesh.authentication import Authenticator
from easymesh.network import get_hostname
from easymesh.node.loadbalancing import ServiceLoadBalancer, TopicLoadBalancer
from easymesh.node.topology import MeshTopologyManager
from easymesh.specs import ConnectionSpec, IpConnectionSpec, MeshNodeSpec, NodeId, UnixConnectionSpec
from easymesh.types import Host, Service, Topic

logger = logging.getLogger(__name__)


class PeerConnection(NamedTuple):
    reader: Reader
    writer: LockableWriter

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    def is_closing(self) -> bool:
        return self.writer.is_closing()


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
                logger.exception(f'Error connecting to {conn_spec}', exc_info=e)
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
            connection = await self._get_cached_connection(node)
            if connection:
                return connection

            logger.debug(f'Connecting to node: {node.id}')
            reader, writer = await self.conn_builder.build(node.connection_specs)

            writer = LockableWriter(writer)

            connection = PeerConnection(reader, writer)
            self._connections[node.id] = connection
            return connection

    async def _get_cached_connection(self, node: MeshNodeSpec) -> PeerConnection | None:
        connection = self._connections.get(node.id, None)
        if not connection:
            return None

        if connection.is_closing():
            await self.close_connection(node)
            return None

        return connection

    async def close_connection(self, node: MeshNodeSpec) -> None:
        connection = self._connections.pop(node.id, None)
        if connection:
            await connection.close()


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
