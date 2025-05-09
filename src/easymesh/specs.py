from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Union
from uuid import UUID, uuid4

from easymesh.network import get_hostname
from easymesh.types import Host, Port, Topic


@dataclass
class IpConnectionSpec:
    host: Host
    port: Port


@dataclass
class UnixConnectionSpec:
    path: str
    host: Host = field(default_factory=get_hostname)


ConnectionSpec = Union[IpConnectionSpec, UnixConnectionSpec]

NodeName = str
NodeUUID = UUID


@dataclass(order=True, frozen=True)
class NodeId:
    name: NodeName
    hostname: Host = field(default_factory=get_hostname)
    uuid: NodeUUID = field(default_factory=uuid4)

    def __str__(self) -> str:
        short_uuid = str(self.uuid)[:4]
        return f'{self.name}@{self.hostname} ({short_uuid})'


@dataclass
class MeshNodeSpec:
    id: NodeId
    connections: list[ConnectionSpec]
    listening_to_topics: set[Topic]


@dataclass
class MeshTopologySpec:
    nodes: Collection[MeshNodeSpec]
