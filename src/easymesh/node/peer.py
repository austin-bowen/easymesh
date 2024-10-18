import asyncio
import socket
from abc import abstractmethod
from asyncio import open_connection, open_unix_connection
from typing import Iterable, Optional

from easymesh.codec import Codec
from easymesh.objectstreamio import MessageStreamIO, ObjectStreamIO
from easymesh.specs import (
    ConnectionSpec,
    IpConnectionSpec,
    MeshNodeSpec,
    MeshTopologySpec,
    NodeName,
    UnixConnectionSpec,
)
from easymesh.types import Body, Message, Topic


class PeerConnection:
    @abstractmethod
    async def send(self, message: Message) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class ObjectStreamPeerConnection(PeerConnection):
    def __init__(self, obj_io: ObjectStreamIO[Message]):
        self.obj_io = obj_io

    async def send(self, message: Message) -> None:
        await self.obj_io.write_object(message)

    async def close(self) -> None:
        self.obj_io.writer.close()
        await self.obj_io.writer.wait_closed()


class PeerConnectionBuilder:
    def __init__(
            self,
            codec: Codec[Body],
            host: str = socket.gethostname(),
    ):
        self.codec = codec
        self.host = host

    async def build(self, conn_specs: Iterable[ConnectionSpec]) -> PeerConnection:
        reader, writer = None, None
        for conn_spec in conn_specs:
            try:
                reader, writer = await self._get_connection(conn_spec)
            except ConnectionError as e:
                print(f'Error connecting to {conn_spec}: {e}')
                continue
            else:
                break

        if not reader:
            raise ConnectionError('Could not connect to any connection spec')

        return ObjectStreamPeerConnection(
            obj_io=MessageStreamIO(reader, writer, codec=self.codec),
        )

    async def _get_connection(self, conn_spec: ConnectionSpec):
        if isinstance(conn_spec, IpConnectionSpec):
            return await open_connection(
                host=conn_spec.host,
                port=conn_spec.port,
            )
        elif isinstance(conn_spec, UnixConnectionSpec):
            if conn_spec.host != self.host:
                raise ConnectionError(
                    f'Unix connection host={conn_spec.host} '
                    f'does not match local host={self.host}'
                )

            return await open_unix_connection(path=conn_spec.path)
        else:
            raise ValueError(f'Invalid connection spec: {conn_spec}')


class PeerConnectionPool:
    def __init__(self, connection_builder: PeerConnectionBuilder):
        self.connection_builder = connection_builder
        self._connections: dict[str, PeerConnection] = {}

    def clear(self) -> None:
        self._connections = {}

    async def get_connection_for(self, peer_spec: MeshNodeSpec) -> PeerConnection:
        conn = self._connections.get(peer_spec.name, None)

        if conn is None:
            conn = await self.connection_builder.build(peer_spec.connections)
            self._connections[peer_spec.name] = conn

        return conn

    def get_node_names_with_connections(self) -> set[NodeName]:
        return set(self._connections.keys())

    def remove_connection_for(self, node_name: NodeName) -> Optional[PeerConnection]:
        return self._connections.pop(node_name, None)


class LazyPeerConnection(PeerConnection):
    def __init__(
            self,
            peer_spec: MeshNodeSpec,
            peer_connection_pool: PeerConnectionPool,
    ):
        self.peer_spec = peer_spec
        self.connection_pool = peer_connection_pool

    async def send(self, message: Message) -> None:
        connection = await self.connection_pool.get_connection_for(self.peer_spec)

        try:
            return await connection.send(message)
        except ConnectionError:
            await self.close()
            raise

    async def close(self) -> None:
        connection = self.connection_pool.remove_connection_for(self.peer_spec.name)
        if connection is not None:
            await connection.close()


class MeshPeer:
    def __init__(
            self,
            name: NodeName,
            topics: set[Topic],
            connection: PeerConnection,
    ):
        self.name = name
        self.topics = topics
        self.connection = connection

    async def send(self, message: Message) -> None:
        await self.connection.send(message)

    async def is_listening_to(self, topic: Topic) -> bool:
        return topic in self.topics


class PeerManager:
    def __init__(self, codec: Codec[Body]):
        self._connection_pool = PeerConnectionPool(
            connection_builder=PeerConnectionBuilder(codec),
        )
        self._mesh_topology = MeshTopologySpec(nodes={})

    async def get_peers(self) -> list[MeshPeer]:
        return [
            MeshPeer(
                name=node.name,
                topics=node.listening_to_topics,
                connection=LazyPeerConnection(
                    peer_spec=node,
                    peer_connection_pool=self._connection_pool,
                ),
            ) for node in self._mesh_topology.nodes.values()
        ]

    async def set_mesh_topology(self, mesh_topo: MeshTopologySpec) -> None:
        self._mesh_topology = mesh_topo

        old_node_names_with_conns = self._connection_pool.get_node_names_with_connections()
        new_node_names = set(mesh_topo.nodes.keys())
        nodes_to_remove = old_node_names_with_conns - new_node_names

        connections_to_close = (
            self._connection_pool.remove_connection_for(node_name)
            for node_name in nodes_to_remove
        )

        await asyncio.gather(*(
            conn.close() for conn in connections_to_close
        ))