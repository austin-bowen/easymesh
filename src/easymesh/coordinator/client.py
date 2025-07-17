import asyncio
from abc import ABC, abstractmethod
from asyncio import Lock, open_connection, wait_for
from collections.abc import Awaitable, Callable

from easymesh.authentication import Authenticator, NoAuthenticator
from easymesh.coordinator.constants import DEFAULT_COORDINATOR_PORT
from easymesh.objectio import CodecObjectReader, CodecObjectWriter, ObjectIO
from easymesh.reqres import MeshTopologyBroadcast, RegisterNodeRequest, RegisterNodeResponse
from easymesh.rpc import ObjectIORPC, RPC
from easymesh.specs import MeshNodeSpec
from easymesh.types import Host, Port

MeshTopologyBroadcastHandler = Callable[[MeshTopologyBroadcast], Awaitable[None]]


class MeshCoordinatorClient(ABC):
    mesh_topology_broadcast_handler: MeshTopologyBroadcastHandler

    def set_broadcast_handler(
            self, handler: MeshTopologyBroadcastHandler
    ) -> None:
        self.mesh_topology_broadcast_handler = handler

    @abstractmethod
    async def send_heartbeat(self) -> None:
        ...

    @abstractmethod
    async def register_node(self, node_spec: MeshNodeSpec) -> None:
        ...


class RPCMeshCoordinatorClient(MeshCoordinatorClient):
    def __init__(self, rpc: RPC):
        self.rpc = rpc
        rpc.message_handler = self._handle_rpc_message

    async def send_heartbeat(self) -> None:
        response = await self.rpc.send_request(b'ping')
        if response != b'pong':
            raise Exception(f'Got unexpected response={response} from heartbeat.')

    async def register_node(self, node_spec: MeshNodeSpec) -> None:
        request = RegisterNodeRequest(node_spec)

        response = await self.rpc.send_request(request)

        if not isinstance(response, RegisterNodeResponse):
            raise Exception(f'Failed to register node; got response={response}')

    async def _handle_rpc_message(self, data) -> None:
        if isinstance(data, MeshTopologyBroadcast):
            await self.mesh_topology_broadcast_handler(data)
        else:
            print(f'Received unknown message={data}')


MeshCoordinatorClientBuilder = Callable[[], Awaitable[MeshCoordinatorClient]]


class ReconnectMeshCoordinatorClient(MeshCoordinatorClient):
    """
    Automatically reconnects to the coordinator when it detects that the
    connection has been lost.

    When a re-connection is made, the most recent node spec is re-registered
    with the coordinator, if applicable.
    """

    def __init__(
            self,
            client_builder: MeshCoordinatorClientBuilder,
            timeout: float,
    ):
        self.client_builder = client_builder
        self.timeout = timeout

        self._client: MeshCoordinatorClient | None = None
        self._client_lock = Lock()
        self._node_spec: MeshNodeSpec | None = None
        self._mesh_topology_broadcast_handler = None
        self._connection_monitor_task: asyncio.Task | None = None

    @property
    async def client(self) -> MeshCoordinatorClient:
        async with self._client_lock:
            if self._client is not None:
                return self._client

            try:
                self._client = await self.client_builder()
                self._client.mesh_topology_broadcast_handler = self.mesh_topology_broadcast_handler
            finally:
                if self._connection_monitor_task is None:
                    self._connection_monitor_task = asyncio.create_task(self._monitor_connection())

            return self._client

    @property
    def mesh_topology_broadcast_handler(self) -> MeshTopologyBroadcastHandler:
        return self._mesh_topology_broadcast_handler

    @mesh_topology_broadcast_handler.setter
    def mesh_topology_broadcast_handler(self, handler: MeshTopologyBroadcastHandler) -> None:
        self._mesh_topology_broadcast_handler = handler

        if self._client is not None:
            self._client.mesh_topology_broadcast_handler = handler

    async def send_heartbeat(self) -> None:
        await self._call_client_method('send_heartbeat')

    async def register_node(self, node_spec: MeshNodeSpec) -> None:
        self._node_spec = node_spec

        try:
            await self._call_client_method('register_node', node_spec)
        except Exception as e:
            print(
                f'[coordinator client] Failed to register node. '
                f'Registration will be retried. {e!r}'
            )

    async def _call_client_method(self, method: str, *args, **kwargs) -> None:
        try:
            client = await self.client
            client_method = getattr(client, method)

            await wait_for(
                client_method(*args, **kwargs),
                timeout=self.timeout,
            )
        except Exception:
            self._client = None
            raise

    async def _monitor_connection(self) -> None:
        prev_error = False
        while True:
            try:
                await self.send_heartbeat()
            except Exception as e:
                print(f'[coordinator client] Heartbeat failed: {e!r}')
                error = True
            else:
                error = False

            if prev_error and not error:
                print('[coordinator client] Connection to coordinator restored.')

                if self._node_spec:
                    await self.register_node(self._node_spec)

            await asyncio.sleep(self.timeout)
            prev_error = error


async def build_coordinator_client(
        host: Host,
        port: Port = DEFAULT_COORDINATOR_PORT,
        authenticator: Authenticator = None,
        reconnect_timeout: float | None = 5.0,
) -> MeshCoordinatorClient:
    if authenticator is None:
        authenticator = NoAuthenticator()

    async def client_builder():
        reader, writer = await open_connection(host, port)
        await authenticator.authenticate(reader, writer)

        rpc = ObjectIORPC(ObjectIO(
            reader=CodecObjectReader(reader),
            writer=CodecObjectWriter(writer),
        ))
        client_ = RPCMeshCoordinatorClient(rpc)
        await rpc.start()

        return client_

    if reconnect_timeout is not None and reconnect_timeout > 0:
        return ReconnectMeshCoordinatorClient(
            client_builder,
            timeout=reconnect_timeout,
        )
    else:
        return await client_builder()
