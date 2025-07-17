import asyncio
from abc import abstractmethod
from asyncio import StreamReader
from codecs import StreamWriter

from easymesh.asyncio import close_ignoring_errors, many
from easymesh.authentication import AuthKey, Authenticator, optional_authkey_authenticator
from easymesh.coordinator.constants import DEFAULT_COORDINATOR_HOST, DEFAULT_COORDINATOR_PORT
from easymesh.objectio import (
    CodecObjectReader,
    CodecObjectWriter,
    ObjectIO,
)
from easymesh.reqres import MeshTopologyBroadcast, RegisterNodeRequest, RegisterNodeResponse
from easymesh.rpc import ObjectIORPC, RPC
from easymesh.specs import MeshNodeSpec, MeshTopologySpec, NodeId
from easymesh.types import Port, ServerHost


class MeshCoordinatorServer:
    @abstractmethod
    async def start(self) -> None:
        ...


class RPCMeshCoordinatorServer(MeshCoordinatorServer):
    def __init__(
            self,
            start_stream_server,
            build_rpc,
            authenticator: Authenticator,
            log_heartbeats: bool,
    ):
        self.start_stream_server = start_stream_server
        self.build_rpc = build_rpc
        self.authenticator = authenticator
        self.log_heartbeats = log_heartbeats

        self._node_clients: dict[RPC, NodeId | None] = {}
        self._nodes: dict[NodeId, MeshNodeSpec] = {}

    async def start(self) -> None:
        server = await self.start_stream_server(self._handle_connection)

    async def _handle_connection(self, reader: StreamReader, writer: StreamWriter) -> None:
        peer_name = writer.get_extra_info('peername') or writer.get_extra_info('sockname')
        print(f'New connection from: {peer_name}')

        await self.authenticator.authenticate(reader, writer)

        rpc = self.build_rpc(reader, writer)
        rpc.request_handler = lambda r: self._handle_request(r, rpc, peer_name)
        self._node_clients[rpc] = None

        try:
            await rpc.run_forever()
        except (ConnectionResetError, EOFError):
            print('Client disconnected')
        finally:
            try:
                await self._remove_node(rpc)
            finally:
                await close_ignoring_errors(writer)

    async def _handle_request(self, request, rpc: RPC, peer_name: str):
        if request == b'ping':
            if self.log_heartbeats:
                print(f'Received heartbeat from {peer_name}')
            return b'pong'
        elif isinstance(request, RegisterNodeRequest):
            return await self._handle_register_node(request, rpc)
        else:
            raise Exception(f'Received invalid request object of type={type(request)}')

    async def _handle_register_node(
            self,
            request: RegisterNodeRequest,
            rpc: RPC,
    ) -> RegisterNodeResponse:
        print(f'Got register node request: {request}')

        node_spec = request.node_spec
        self._node_clients[rpc] = node_spec.id
        self._nodes[node_spec.id] = node_spec

        await self._broadcast_topology()

        return RegisterNodeResponse()

    async def _remove_node(self, rpc: RPC) -> None:
        node_id = self._node_clients.pop(rpc)
        self._nodes.pop(node_id, None)

        await self._broadcast_topology()

    async def _broadcast_topology(self) -> None:
        print(f'\nBroadcasting topology to {len(self._nodes)} nodes...')
        if not self._nodes:
            return

        nodes = sorted(self._nodes.values(), key=lambda n: n.id)
        print('Mesh nodes:')
        for node in nodes:
            print(f'- {node.id}: {node}')

        mesh_topology = MeshTopologySpec(nodes=nodes)
        message = MeshTopologyBroadcast(mesh_topology)

        await many(
            node_client.send_message(message)
            for node_client in self._node_clients.keys()
        )


def build_mesh_coordinator_server(
        host: ServerHost = DEFAULT_COORDINATOR_HOST,
        port: Port = DEFAULT_COORDINATOR_PORT,
        authkey: AuthKey = None,
        authenticator: Authenticator = None,
        log_heartbeats: bool = False,
) -> MeshCoordinatorServer:
    async def start_stream_server(cb):
        return await asyncio.start_server(cb, host=host, port=port)

    def build_rpc(reader, writer):
        return ObjectIORPC(ObjectIO(
            reader=CodecObjectReader(reader),
            writer=CodecObjectWriter(writer),
        ))

    authenticator = authenticator or optional_authkey_authenticator(authkey)

    return RPCMeshCoordinatorServer(
        start_stream_server,
        build_rpc,
        authenticator,
        log_heartbeats,
    )
