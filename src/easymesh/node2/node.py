import logging

from easymesh.asyncio import Reader, Writer, close_ignoring_errors
from easymesh.authentication import Authenticator, optional_authkey_authenticator
from easymesh.codec2 import (
    Codec,
    FixedLengthIntCodec,
    LengthPrefixedStringCodec,
    NodeMessageCodec,
    ServiceRequestCodec,
    ServiceResponseCodec,
    TopicMessageCodec,
    pickle_codec,
)
from easymesh.coordinator.client import MeshCoordinatorClient, build_coordinator_client
from easymesh.coordinator.constants import DEFAULT_COORDINATOR_PORT
from easymesh.network import get_lan_hostname
from easymesh.node.servers import PortScanTcpServerProvider, ServerProvider, ServersManager, TmpUnixServerProvider
from easymesh.node2.loadbalancing import RoundRobinLoadBalancer, ServiceLoadBalancer, TopicLoadBalancer
from easymesh.node2.peer import PeerConnectionBuilder, PeerConnectionManager, PeerSelector
from easymesh.node2.service import ServiceCaller
from easymesh.node2.topic import TopicListenerCallback, TopicListenerManager, TopicSender
from easymesh.node2.topology import MeshTopologyManager, get_removed_nodes
from easymesh.reqres import MeshTopologyBroadcast
from easymesh.specs import MeshNodeSpec, NodeId
from easymesh.types import Data, Host, Message, Port, ServerHost, Service, ServiceRequest, Topic

logger = logging.getLogger(__name__)


class Node:
    def __init__(
            self,
            id: NodeId,
            coordinator_client: MeshCoordinatorClient,
            servers_manager: ServersManager,
            client_handler: 'ClientHandler',
            topology_manager: MeshTopologyManager,
            connection_manager: PeerConnectionManager,
            topic_sender: TopicSender,
            topic_listener_manager: TopicListenerManager,
            service_caller: ServiceCaller,
    ):
        self._id = id
        self.coordinator_client = coordinator_client
        self.servers_manager = servers_manager
        self.client_handler = client_handler
        self.topology_manager = topology_manager
        self.connection_manager = connection_manager
        self.topic_sender = topic_sender
        self.topic_listener_manager = topic_listener_manager
        self.service_caller = service_caller

        coordinator_client.set_broadcast_handler(self._handle_topology_broadcast)

    @property
    def id(self) -> NodeId:
        return self._id

    async def start(self) -> None:
        logger.info(f'Starting node {self.id}')

        logger.debug('Starting servers')
        await self.servers_manager.start_servers(self.client_handler.handle_client)

        await self.register()

    async def send(self, topic: Topic, data: Data = None) -> None:
        await self.topic_sender.send(topic, data)

    async def listen(
            self,
            topic: Topic,
            callback: TopicListenerCallback,
    ) -> None:
        self.topic_listener_manager.set_listener(topic, callback)
        await self.register()

    async def stop_listening(self, topic: Topic) -> None:
        callback = self.topic_listener_manager.remove_listener(topic)

        if callback is not None:
            await self.register()
        else:
            logger.warning(f"Attempted to remove non-existing listener for topic={topic!r}")

    async def request(self, service: Service, data: Data = None) -> Data:
        return await self.service_caller.request(service, data)

    async def register(self) -> None:
        node_spec = self._build_node_spec()
        logger.info('Registering node with coordinator')
        logger.debug(f'node_spec={node_spec}')
        await self.coordinator_client.register_node(node_spec)

    def _build_node_spec(self) -> MeshNodeSpec:
        return MeshNodeSpec(
            id=self.id,
            connection_specs=self.servers_manager.connection_specs,
            topics=self.topic_listener_manager.topics,
            services=set(),  # TODO
        )

    async def _handle_topology_broadcast(self, broadcast: MeshTopologyBroadcast) -> None:
        logger.debug(
            f'Received mesh topology broadcast with '
            f'{len(broadcast.mesh_topology.nodes)} nodes.'
        )

        removed_nodes = get_removed_nodes(
            old_topology=self.topology_manager.topology,
            new_topology=broadcast.mesh_topology,
        )
        logger.debug(f'Removed nodes: {removed_nodes}')

        self.topology_manager.topology = broadcast.mesh_topology

        for node in removed_nodes:
            await self.connection_manager.close_connection(node)


class ClientHandler:
    def __init__(
            self,
            node_message_codec: NodeMessageCodec,
            topic_listener_manager: TopicListenerManager,
    ):
        self.node_message_codec = node_message_codec
        self.topic_listener_manager = topic_listener_manager

    async def handle_client(self, reader: Reader, writer: Writer) -> None:
        peer_name = writer.get_extra_info('peername') or writer.get_extra_info('sockname')
        logger.debug(f'New connection from: {peer_name}')

        while True:
            try:
                message = await self.node_message_codec.decode_topic_message_or_service_request(reader)
            except EOFError:
                logger.debug(f'Closed connection from: {peer_name}')
                await close_ignoring_errors(writer)
                return
            except Exception as e:
                logger.exception(f'Error reading from peer={peer_name}', exc_info=e)
                await close_ignoring_errors(writer)
                return

            if isinstance(message, Message):
                await self._handle_topic_message(message)
            elif isinstance(message, ServiceRequest):
                await self._handle_service_request(message)
            else:
                raise RuntimeError('Unreachable code')

    async def _handle_topic_message(self, message: Message) -> None:
        listener = self.topic_listener_manager.get_listener(message.topic)

        if listener:
            await listener(message.topic, message.data)
        else:
            logger.warning(
                f'Received message for topic={message.topic!r} '
                f'but no listener is registered for it.'
            )

    async def _handle_service_request(self, request: ServiceRequest) -> None:
        # TODO
        ...


async def build_node(
        name: str,
        coordinator_host: Host = 'localhost',
        coordinator_port: Port = DEFAULT_COORDINATOR_PORT,
        coordinator_reconnect_timeout: float | None = 5.0,
        allow_unix_connections: bool = True,
        allow_tcp_connections: bool = True,
        node_server_host: ServerHost = None,
        node_client_host: Host = None,
        data_codec: Codec[Data] = pickle_codec,
        topic_load_balancer: TopicLoadBalancer = None,
        service_load_balancer: ServiceLoadBalancer = None,
        authkey: bytes = None,
        authenticator: Authenticator = None,
        start: bool = True,
) -> Node:
    authenticator = authenticator or optional_authkey_authenticator(authkey)

    coordinator_client = await build_coordinator_client(
        coordinator_host,
        coordinator_port,
        authenticator,
        reconnect_timeout=coordinator_reconnect_timeout,
    )

    server_providers = build_server_providers(
        allow_unix_connections,
        allow_tcp_connections,
        node_server_host,
        node_client_host,
    )
    servers_manager = ServersManager(server_providers)

    topology_manager = MeshTopologyManager()

    peer_selector = build_peer_selector(
        topology_manager,
        topic_load_balancer,
        service_load_balancer,
    )

    connection_manager = PeerConnectionManager(
        PeerConnectionBuilder(authenticator),
    )

    node_message_codec = build_node_message_codec(data_codec)

    topic_sender = TopicSender(peer_selector, connection_manager, node_message_codec)

    topic_listener_manager = TopicListenerManager()

    client_handler = ClientHandler(node_message_codec, topic_listener_manager)

    service_caller = ServiceCaller(
        peer_selector,
        connection_manager,
        node_message_codec,
        max_request_ids=2 ** (8 * 2),  # Codec uses 2 bytes for request ID
    )

    node = Node(
        id=NodeId(name),
        coordinator_client=coordinator_client,
        servers_manager=servers_manager,
        client_handler=client_handler,
        topology_manager=topology_manager,
        connection_manager=connection_manager,
        topic_sender=topic_sender,
        topic_listener_manager=topic_listener_manager,
        service_caller=service_caller,
    )

    if start:
        await node.start()

    return node


def build_server_providers(
        allow_unix_connections: bool,
        allow_tcp_connections: bool,
        node_server_host: ServerHost | None,
        node_client_host: Host | None,
) -> list[ServerProvider]:
    server_providers = []

    if allow_unix_connections:
        server_providers.append(TmpUnixServerProvider())

    if allow_tcp_connections:
        if not node_client_host:
            node_client_host = get_lan_hostname()

        provider = PortScanTcpServerProvider(node_server_host, node_client_host)
        server_providers.append(provider)

    if not server_providers:
        raise ValueError('Must allow at least one type of connection')

    return server_providers


def build_peer_selector(
        topology_manager: MeshTopologyManager,
        topic_load_balancer: TopicLoadBalancer | None,
        service_load_balancer: ServiceLoadBalancer | None,
) -> PeerSelector:
    round_robin_load_balancer = RoundRobinLoadBalancer()

    return PeerSelector(
        topology_manager,
        topic_load_balancer=topic_load_balancer or round_robin_load_balancer,
        service_load_balancer=service_load_balancer or round_robin_load_balancer,
    )


def build_node_message_codec(
        data_codec: Codec[Data],
) -> NodeMessageCodec:
    short_string_codec = LengthPrefixedStringCodec(
        len_prefix_codec=FixedLengthIntCodec(length=1)
    )

    request_id_codec = FixedLengthIntCodec(length=2)

    return NodeMessageCodec(
        topic_message_codec=TopicMessageCodec(
            topic_codec=short_string_codec,
            data_codec=data_codec,
        ),
        service_request_codec=ServiceRequestCodec(
            request_id_codec,
            service_codec=short_string_codec,
            data_codec=data_codec,
        ),
        service_response_codec=ServiceResponseCodec(
            request_id_codec,
            data_codec=data_codec,
            error_codec=LengthPrefixedStringCodec(
                len_prefix_codec=FixedLengthIntCodec(length=2),
            )
        ),
    )
