from collections import defaultdict

from easymesh.specs import MeshNodeSpec, MeshTopologySpec
from easymesh.types import Service, Topic


class MeshTopologyManager:
    def __init__(self):
        self._topology: MeshTopologySpec
        self._topic_nodes: dict[Topic, list[MeshNodeSpec]]
        self._service_nodes: dict[Service, list[MeshNodeSpec]]

        self.set_topology(MeshTopologySpec(nodes=[]))

    @property
    def topology(self) -> MeshTopologySpec:
        return self._topology

    def set_topology(self, value: MeshTopologySpec) -> None:
        self._topology = value

        self._cache_topic_nodes()
        self._cache_service_nodes()

    def _cache_topic_nodes(self) -> None:
        topic_nodes = defaultdict(list)

        for node in self.topology.nodes:
            for topic in node.topics:
                topic_nodes[topic].append(node)

        self._topic_nodes_cache = topic_nodes

    def _cache_service_nodes(self) -> None:
        service_nodes = defaultdict(list)

        for node in self.topology.nodes:
            for service in node.services:
                service_nodes[service].append(node)

        self._service_nodes_cache = service_nodes

    def get_nodes_listening_to_topic(self, topic: Topic) -> list[MeshNodeSpec]:
        return self._topic_nodes_cache[topic]

    def get_nodes_providing_service(self, service: str) -> list[MeshNodeSpec]:
        return self._service_nodes_cache[service]

    def get_removed_nodes(
            self,
            new_topology: MeshTopologySpec,
    ) -> list[MeshNodeSpec]:
        """
        Returns a list of nodes that were removed in the new topology.
        """

        new_node_ids = {node.id for node in new_topology.nodes}

        return [
            node for node in self.topology.nodes
            if node.id not in new_node_ids
        ]
