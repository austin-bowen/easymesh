from easymesh.node.topology import MeshTopologyManager
from easymesh.specs import MeshNodeSpec, MeshTopologySpec, NodeId


class TestMeshTopologyManager:
    def setup_method(self):
        self.node1 = MeshNodeSpec(
            id=NodeId('node1'),
            connection_specs=[],
            topics={'topic1', 'topic2'},
            services=set(),
        )

        self.node2 = MeshNodeSpec(
            id=NodeId('node2'),
            connection_specs=[],
            topics={'topic1', 'topic3'},
            services={'service1', 'service2'},
        )

        self.node3 = MeshNodeSpec(
            id=NodeId('node3'),
            connection_specs=[],
            topics=set(),
            services={'service1', 'service3'},
        )

        self.topology = MeshTopologySpec(nodes=[
            self.node1,
            self.node2,
            self.node3,
        ])

        self.topology_manager = MeshTopologyManager()
        self.topology_manager.set_topology(self.topology)

    def test_topology_property(self):
        assert self.topology_manager.topology is self.topology

    def test_get_nodes_listening_to_topic_returns_nodes(self):
        result = self.topology_manager.get_nodes_listening_to_topic('topic1')
        assert result == [self.node1, self.node2]

    def test_get_nodes_listening_to_topic_returns_empty_list_for_unknown_topic(self):
        result = self.topology_manager.get_nodes_listening_to_topic('unknown_topic')
        assert result == []

    def test_get_nodes_providing_service_returns_nodes(self):
        result = self.topology_manager.get_nodes_providing_service('service1')
        assert result == [self.node2, self.node3]

    def test_get_nodes_providing_service_returns_empty_list_for_unknown_service(self):
        result = self.topology_manager.get_nodes_providing_service('unknown_service')
        assert result == []

    def test_get_removed_nodes_returns_removed_nodes(self):
        removed_node = self.node1
        new_node = mesh_node_spec('node4')

        new_topology = MeshTopologySpec(nodes=[
            self.node2,
            self.node3,
            new_node,
        ])

        removed_nodes = self.topology_manager.get_removed_nodes(new_topology)

        assert removed_nodes == [removed_node]

    def test_get_removed_nodes_returns_empty_list_when_no_nodes_removed(self):
        new_node = mesh_node_spec('node2')

        new_topology = MeshTopologySpec(nodes=[
            self.node1,
            self.node2,
            self.node3,
            new_node,
        ])

        removed_nodes = self.topology_manager.get_removed_nodes(new_topology)

        assert removed_nodes == []


def mesh_node_spec(name: str) -> MeshNodeSpec:
    return MeshNodeSpec(
        id=NodeId(name),
        connection_specs=[],
        topics=set(),
        services=set(),
    )
