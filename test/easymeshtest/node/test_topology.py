from easymesh.node2.topology import get_removed_nodes
from easymesh.specs import MeshNodeSpec, MeshTopologySpec, NodeId


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
