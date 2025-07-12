from easymesh.node2.topology import MeshTopologyManager, get_removed_nodes
from easymesh.specs import MeshNodeSpec, MeshTopologySpec, NodeId


class TestMeshTopologyManager:
    def setup_method(self):
        self.node1 = MeshNodeSpec(
            id=NodeId('node1'),
            connections=[],
            topics={'topic1', 'topic2'},
            services=set(),
        )

        self.node2 = MeshNodeSpec(
            id=NodeId('node2'),
            connections=[],
            topics={'topic1', 'topic3'},
            services={'service1', 'service2'},
        )

        self.node3 = MeshNodeSpec(
            id=NodeId('node3'),
            connections=[],
            topics=set(),
            services={'service1', 'service3'},
        )

        self.topology = MeshTopologySpec(nodes=[
            self.node1,
            self.node2,
            self.node3,
        ])

        self.topology_manager = MeshTopologyManager()
        self.topology_manager.topology = self.topology

    def test_topology_property(self):
        assert self.topology_manager.topology is self.topology

    def test_get_nodes_listening_to_topic(self):
        result = self.topology_manager.get_nodes_listening_to_topic('topic1')
        assert result == [self.node1, self.node2]

    def test_get_nodes_providing_service(self):
        result = self.topology_manager.get_nodes_providing_service('service1')
        assert result == [self.node2, self.node3]


class TestGetRemovedNodes:
    def test_returns_removed_nodes_when_nodes_are_removed(self):
        removed_node = mesh_node_spec('node1')
        kept_node = mesh_node_spec('node2')
        new_node = mesh_node_spec('node3')

        old_topology = MeshTopologySpec(nodes=[
            removed_node,
            kept_node,
        ])
        new_topology = MeshTopologySpec(nodes=[
            kept_node,
            new_node,
        ])

        removed_nodes = get_removed_nodes(old_topology, new_topology)

        assert removed_nodes == [removed_node]

    def test_returns_empty_list_when_no_nodes_removed(self):
        kept_node = mesh_node_spec('node1')
        new_node = mesh_node_spec('node2')

        old_topology = MeshTopologySpec(nodes=[
            kept_node,
        ])
        new_topology = MeshTopologySpec(nodes=[
            kept_node,
            new_node,
        ])

        removed_nodes = get_removed_nodes(old_topology, new_topology)

        assert removed_nodes == []


def mesh_node_spec(name: str) -> MeshNodeSpec:
    return MeshNodeSpec(
        id=NodeId(name),
        connections=[],
        topics=set(),
        services=set(),
    )
