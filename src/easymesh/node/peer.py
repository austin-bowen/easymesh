from abc import abstractmethod
from asyncio import open_connection, open_unix_connection
from dataclasses import dataclass
from typing import Iterable, Protocol

from easymesh.asyncio import LockableWriter, Reader, Writer, close_ignoring_errors
from easymesh.authentication import Authenticator
from easymesh.network import get_hostname
from easymesh.specs import (
    ConnectionSpec,
    IpConnectionSpec,
    MeshNodeSpec,
    MeshTopologySpec,
    NodeId,
    UnixConnectionSpec,
)
from easymesh.types import Host, Topic


class PeerWriterBuilder:
    def __init__(self, authenticator: Authenticator, host: Host = None):
        self.authenticator = authenticator
        self.host = host or get_hostname()

    async def build(self, conn_specs: Iterable[ConnectionSpec]) -> Writer:
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

        return writer

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


class PeerWriterPool:
    def __init__(self, writer_builder: PeerWriterBuilder):
        self.writer_builder = writer_builder
        self._writers: dict[NodeId, LockableWriter] = {}

    def clear(self) -> None:
        self._writers = {}

    async def get_writer_for(self, peer_spec: MeshNodeSpec) -> LockableWriter:
        writer = self._writers.get(peer_spec.id, None)
        if writer is not None:
            return writer

        try:
            writer = await self.writer_builder.build(peer_spec.connections)
        except Exception as e:
            raise ConnectionError(f'Error connecting to {peer_spec.id}: {e!r}')
        else:
            print(f'Connected to {peer_spec.id}')

        writer = LockableWriter(writer)
        self._writers[peer_spec.id] = writer

        return writer

    def get_node_ids_with_writers(self) -> set[NodeId]:
        return set(self._writers.keys())

    async def close_writer_for(self, node_id: NodeId) -> None:
        writer = self._writers.pop(node_id, None)
        if writer is not None:
            await close_ignoring_errors(writer)


class PeerConnection:
    @abstractmethod
    async def get_writer(self) -> LockableWriter:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class LazyPeerConnection(PeerConnection):
    def __init__(
            self,
            peer_spec: MeshNodeSpec,
            peer_connection_pool: PeerWriterPool,
    ):
        self.peer_spec = peer_spec
        self.connection_pool = peer_connection_pool

    async def get_writer(self) -> LockableWriter:
        return await self.connection_pool.get_writer_for(self.peer_spec)

    async def close(self) -> None:
        await self.connection_pool.close_writer_for(self.peer_spec.id)


@dataclass
class MeshPeer:
    id: NodeId
    topics: set[Topic]
    connection: PeerConnection

    async def is_listening_to(self, topic: Topic) -> bool:
        return topic in self.topics


class PeerManager:
    def __init__(self, authenticator: Authenticator):
        self._connection_pool = PeerWriterPool(
            writer_builder=PeerWriterBuilder(authenticator),
        )
        self._peers: list[MeshPeer] = []

    def get_peers(self) -> list[MeshPeer]:
        return self._peers

    async def set_mesh_topology(self, mesh_topology: MeshTopologySpec) -> None:
        self._set_peers(mesh_topology)

        old_nodes_with_conns = self._connection_pool.get_node_ids_with_writers()
        new_nodes = set(node.id for node in mesh_topology.nodes)
        nodes_to_remove = old_nodes_with_conns - new_nodes

        for node_id in nodes_to_remove:
            await self._connection_pool.close_writer_for(node_id)

    def _set_peers(self, mesh_topology: MeshTopologySpec) -> None:
        self._peers = [
            MeshPeer(
                id=node.id,
                topics=node.listening_to_topics,
                connection=LazyPeerConnection(
                    peer_spec=node,
                    peer_connection_pool=self._connection_pool,
                ),
            ) for node in mesh_topology.nodes
        ]


Reader2 = Reader


class Writer2(Protocol):
    async def write(self, data: bytes) -> None:
        ...

    async def drain(self) -> None:
        ...

    async def close(self) -> None:
        ...


@dataclass
class PeerConnection2:
    reader: Reader2
    writer: Writer2

    async def close(self) -> None:
        await self.writer.close()


class PeerConnectionBuilder:
    def __init__(self, authenticator: Authenticator, host: Host = None):
        self.authenticator = authenticator
        self.host = host or get_hostname()

    async def build(self, conn_specs: Iterable[ConnectionSpec]) -> PeerConnection2:
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

        return PeerConnection2(reader, writer)

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
        self._connections: dict[NodeId, PeerConnection2] = {}

    async def get_connection(self, peer_spec: MeshNodeSpec) -> PeerConnection2:
        return PeerConnection2(
            reader=LazyReader(self, peer_spec),
            writer=LazyWriter(self, peer_spec),
        )

    # TODO Rename this
    async def _get_connection_for_real(self, peer_spec: MeshNodeSpec) -> PeerConnection2:
        # TODO should probably lock self._connections
        conn = self._connections.get(peer_spec.id, None)
        if conn:
            return conn

        conn = await self.conn_builder.build(peer_spec.connections)

        self._connections[peer_spec.id] = conn
        return conn

    async def close_connection(self, peer_spec: MeshNodeSpec) -> None:
        conn = self._connections.pop(peer_spec.id, None)
        if conn:
            await conn.close()


@dataclass
class LazyWriter(Writer2):
    conn_manager: PeerConnectionManager
    peer_spec: MeshNodeSpec

    async def write(self, data: bytes) -> None:
        writer = await self._get_writer()
        writer.write(data)

    async def drain(self) -> None:
        writer = await self._get_writer()
        await writer.drain()

    async def close(self) -> None:
        await self.conn_manager.close_connection(self.peer_spec)

    async def _get_writer(self) -> Writer:
        conn = await self.conn_manager._get_connection_for_real(self.peer_spec)
        return conn.writer


@dataclass
class LazyReader(Reader2):
    conn_manager: PeerConnectionManager
    peer_spec: MeshNodeSpec

    async def readexactly(self, n: int) -> bytes:
        reader = await self._get_reader()
        return await reader.readexactly(n)

    async def _get_reader(self) -> Reader:
        conn = await self.conn_manager._get_connection_for_real(self.peer_spec)
        return conn.reader
