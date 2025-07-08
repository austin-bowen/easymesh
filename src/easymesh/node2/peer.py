from asyncio import Lock
from collections.abc import Iterable
from typing import NamedTuple, Protocol

from easymesh.asyncio import Reader
from easymesh.node2.loadbalancing import ServiceLoadBalancer, TopicLoadBalancer
from easymesh.node2.topology import MeshTopologyManager
from easymesh.specs import MeshNodeSpec
from easymesh.types import Buffer, Service, Topic


# TODO move this
class Writer(Protocol):
    async def write(self, data: bytes) -> None:
        ...

    async def drain(self) -> None:
        ...

    async def close(self) -> None:
        ...

    async def wait_closed(self) -> None:
        ...


# TODO move this
class BufferWriter(bytearray, Buffer, Writer):
    async def write(self, data: bytes) -> None:
        self.extend(data)

    async def drain(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


class LockableWriter(Writer):
    def __init__(self, writer: Writer):
        self.writer = writer
        self._lock = Lock()

    @property
    def lock(self) -> Lock:
        return self._lock

    async def __aenter__(self) -> 'LockableWriter':
        await self.lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.lock.release()

    async def write(self, data: bytes) -> None:
        assert self.lock.locked()
        await self.writer.write(data)

    async def drain(self) -> None:
        assert self.lock.locked()
        await self.writer.drain()

    async def close(self) -> None:
        assert self.lock.locked()
        await self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()


class PeerConnection(NamedTuple):
    reader: Reader
    writer: LockableWriter

    async def close(self) -> None:
        await self.writer.close()


class PeerConnectionManager:
    async def get_connection(self, peer: MeshNodeSpec) -> PeerConnection:
        # TODO
        ...

    async def get_connections(self, peers: Iterable[MeshNodeSpec]) -> list[PeerConnection]:
        return [await self.get_connection(peer) for peer in peers]


class PeerConnectionSelector:
    def __init__(
            self,
            topology_manager: MeshTopologyManager,
            topic_load_balancer: TopicLoadBalancer,
            service_load_balancer: ServiceLoadBalancer,
            connection_manager: PeerConnectionManager,
    ):
        self.topology_manager = topology_manager
        self.topic_load_balancer = topic_load_balancer
        self.service_load_balancer = service_load_balancer
        self.connection_manager = connection_manager

    async def get_connections_for_topic(self, topic: Topic) -> list[PeerConnection]:
        peers = self.topology_manager.get_nodes_listening_to_topic(topic)
        peers = self.topic_load_balancer.choose_nodes(peers, topic)
        return await self.connection_manager.get_connections(peers)

    async def get_connection_for_service(self, service: Service) -> PeerConnection | None:
        peers = self.topology_manager.get_nodes_providing_service(service)
        peer = self.service_load_balancer.choose_node(peers, service)
        return await self.connection_manager.get_connection(peer) if peer else None
