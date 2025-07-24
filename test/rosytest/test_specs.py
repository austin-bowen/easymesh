from rosy.specs import NodeId, NodeUUID


def test_NodeId_str():
    node_id = NodeId(
        name='name',
        hostname='hostname',
        uuid=NodeUUID('beef0000-0000-0000-0000-000000000000'),
    )
    assert str(node_id) == 'name@hostname (beef)'
