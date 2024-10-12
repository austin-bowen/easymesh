from dataclasses import dataclass

from easymesh.specs import MeshNodeSpec, MeshTopologySpec


@dataclass
class RegisterNodeRequest:
    node_spec: MeshNodeSpec


class RegisterNodeResponse:
    pass


@dataclass
class MeshTopologyBroadcast:
    mesh_topology: MeshTopologySpec
