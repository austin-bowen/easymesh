import asyncio
import logging
from argparse import Namespace
from functools import wraps
from typing import NamedTuple

from easymesh.argparse import get_node_arg_parser
from easymesh.asyncio import forever
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
from easymesh.node.clienthandler import ClientHandler
from easymesh.node.loadbalancing import (
    GroupingTopicLoadBalancer,
    RoundRobinLoadBalancer,
    ServiceLoadBalancer,
    TopicLoadBalancer,
    node_name_group_key,
)
from easymesh.node.peer import PeerConnectionBuilder, PeerConnectionManager, PeerSelector
from easymesh.node.servers import PortScanTcpServerProvider, ServerProvider, ServersManager, TmpUnixServerProvider
from easymesh.node.service.caller import ServiceCaller
from easymesh.node.service.handlermanager import ServiceHandlerManager
from easymesh.node.service.types import ServiceResponse
from easymesh.node.topic import TopicListenerCallback, TopicListenerManager, TopicMessageHandler, TopicSender
from easymesh.node.topology import MeshTopologyManager, get_removed_nodes
from easymesh.reqres import MeshTopologyBroadcast
from easymesh.specs import MeshNodeSpec, NodeId
from easymesh.types import Data, Host, Port, ServerHost, Service, ServiceCallback, Topic

logger = logging.getLogger(__name__)


class Node:
    def __init__(
            self,
            id: NodeId,
            coordinator_client: MeshCoordinatorClient,
            servers_manager: ServersManager,
            topology_manager: MeshTopologyManager,
            connection_manager: PeerConnectionManager,
            topic_sender: TopicSender,
            topic_listener_manager: TopicListenerManager,
            service_caller: ServiceCaller,
            service_handler_manager: ServiceHandlerManager,
    ):
        self._id = id
        self.coordinator_client = coordinator_client
        self.servers_manager = servers_manager
        self.topology_manager = topology_manager
        self.connection_manager = connection_manager
        self.topic_sender = topic_sender
        self.topic_listener_manager = topic_listener_manager
        self.service_caller = service_caller
        self.service_handler_manager = service_handler_manager

        coordinator_client.set_broadcast_handler(self._handle_topology_broadcast)

    @property
    def id(self) -> NodeId:
        return self._id

    def __str__(self) -> str:
        return str(self.id)

    async def start(self) -> None:
        logger.info(f'Starting node {self.id}')

        logger.debug('Starting servers')
        await self.servers_manager.start_servers()

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

    async def topic_has_listeners(self, topic: Topic) -> bool:
        listeners = self.topology_manager.get_nodes_listening_to_topic(topic)
        return bool(listeners)

    async def wait_for_listener(self, topic: Topic, poll_interval: float = 1.) -> None:
        """
        Wait until there is a listener for a topic.

        Useful for send-only nodes to avoid doing unnecessary work when there
        are no listeners for a topic.

        Combine this with ``depends_on_listener`` in intermediate nodes to make all
        nodes in a chain wait until there is a listener at the end of the chain.
        """

        while not await self.topic_has_listeners(topic):
            await asyncio.sleep(poll_interval)

    def depends_on_listener(self, downstream_topic: Topic, poll_interval: float = 1.):
        """
        Decorator for callback functions that send messages to a downstream
        topic. If there is no listener for the downstream topic, then the node
        will stop listening to the upstream topic until there is a listener for
        the downstream topic.

        Useful for nodes that do intermediate processing, i.e. nodes that
        receive a message on a topic, process it, and then send the result on
        another topic.

        Example:
            @node.depends_on_listener('bar')
            async def handle_foo(topic, data):
                await node.send('bar', data)

            await node.listen('foo', handle_foo)

        Combine this with ``wait_for_listener`` in send-only nodes to make all
        nodes in a chain wait until there is a listener at the end of the chain.
        """

        def decorator(callback):
            @wraps(callback)
            async def wrapper(topic: Topic, data: Data) -> None:
                if await self.topic_has_listeners(downstream_topic):
                    await callback(topic, data)
                    return

                await self.stop_listening(topic)

                async def wait_for_listener_then_listen():
                    await self.wait_for_listener(downstream_topic, poll_interval)
                    await self.listen(topic, wrapper)

                asyncio.create_task(wait_for_listener_then_listen())

            return wrapper

        return decorator

    def get_topic(self, topic: Topic) -> 'TopicProxy':
        return TopicProxy(self, topic)

    async def call(self, service: Service, data: Data = None) -> Data:
        return await self.service_caller.call(service, data)

    async def add_service(self, service: Service, handler: ServiceCallback) -> None:
        """Add a service to the node that other nodes can call."""
        self.service_handler_manager.set_handler(service, handler)
        await self.register()

    async def remove_service(self, service: Service) -> None:
        """Stop providing a service."""
        self.service_handler_manager.remove_handler(service)
        await self.register()

    async def service_has_providers(self, service: Service) -> bool:
        """Check if there are any nodes that provide the service."""
        providers = self.topology_manager.get_nodes_providing_service(service)
        return bool(providers)

    async def wait_for_service(self, service: Service, poll_interval: float = 1.) -> None:
        """Wait until there is a provider for a service."""
        while not await self.service_has_providers(service):
            await asyncio.sleep(poll_interval)

    def get_service(self, service: Service) -> 'ServiceProxy':
        """
        Returns a convenient way to call a service if used more than once.

        Example:
            >>> math_service = node.get_service('math')
            >>> result = await math_service('2 + 2')
            >>> # ... is equivalent to ...
            >>> result = await node.call('math', '2 + 2')
        """

        return ServiceProxy(self, service)

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
            services=self.service_handler_manager.services,
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
        logger.debug(
            f'Removed {len(removed_nodes)} nodes: '
            f'{[str(node.id) for node in removed_nodes]}'
        )

        self.topology_manager.topology = broadcast.mesh_topology

        for node in removed_nodes:
            await self.connection_manager.close_connection(node)

    async def forever(self) -> None:
        """
        Does nothing forever. Convenience method to prevent your main function
        from exiting while the node is running.
        """
        await forever()


class TopicProxy(NamedTuple):
    node: Node
    topic: Topic

    async def send(self, data: Data = None) -> None:
        await self.node.send(self.topic, data)

    async def has_listeners(self) -> bool:
        return await self.node.topic_has_listeners(self.topic)

    async def wait_for_listener(self, poll_interval: float = 1.) -> None:
        await self.node.wait_for_listener(self.topic, poll_interval)

    def depends_on_listener(self, poll_interval: float = 1.):
        return self.node.depends_on_listener(self.topic, poll_interval)


class ServiceProxy(NamedTuple):
    node: Node
    service: Service

    def __str__(self) -> str:
        name = self.__class__.__name__
        return f'{name}(service={self.service})'

    async def __call__(self, data: Data = None) -> ServiceResponse:
        return await self.call(data)

    async def call(self, data: Data = None) -> Data:
        return await self.node.call(self.service, data)

    async def has_providers(self) -> bool:
        return await self.node.service_has_providers(self.service)

    async def wait_for_provider(self, poll_interval: float = 1.) -> None:
        await self.node.wait_for_service(self.service, poll_interval)


async def build_node_from_args(
        default_node_name: str = None,
        args: Namespace = None,
        **kwargs,
) -> Node:
    """
    Builds a node from command line arguments.

    Args:
        default_node_name:
            Default node name. If not given, the argument is required.
            Ignored if `args` is given.
        args:
            Arguments from an argument parser. If not given, an argument parser
            is created using `get_node_arg_parser` and is used to parse args.
            This is useful if you create your own argument parser.
        kwargs:
            Additional keyword arguments to pass to `build_node`.
            These will override anything specified in `args`.
    """

    if args is None:
        args = get_node_arg_parser(default_node_name).parse_args()

    build_args = vars(args)

    if hasattr(args, 'coordinator'):
        build_args['coordinator_host'] = args.coordinator.host
        build_args['coordinator_port'] = args.coordinator.port

    build_args.update(kwargs)

    return await build_node(**build_args)


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
        **kwargs,
) -> Node:
    authenticator = authenticator or optional_authkey_authenticator(authkey)

    coordinator_client = await build_coordinator_client(
        coordinator_host,
        coordinator_port,
        authenticator,
        reconnect_timeout=coordinator_reconnect_timeout,
    )

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
    topic_message_handler = TopicMessageHandler(topic_listener_manager)

    service_handler_manager = ServiceHandlerManager()

    client_handler = ClientHandler(
        node_message_codec,
        topic_message_handler,
        service_handler_manager,
    )

    server_providers = build_server_providers(
        allow_unix_connections,
        allow_tcp_connections,
        node_server_host,
        node_client_host,
    )
    servers_manager = ServersManager(server_providers, client_handler.handle_client)

    request_id_bytes = 2  # Codec uses 2 bytes for request ID
    service_caller = ServiceCaller(
        peer_selector,
        connection_manager,
        node_message_codec,
        max_request_ids=2 ** (8 * request_id_bytes),
    )

    node = Node(
        id=NodeId(name),
        coordinator_client=coordinator_client,
        servers_manager=servers_manager,
        topology_manager=topology_manager,
        connection_manager=connection_manager,
        topic_sender=topic_sender,
        topic_listener_manager=topic_listener_manager,
        service_caller=service_caller,
        service_handler_manager=service_handler_manager,
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

    default_topic_load_balancer = GroupingTopicLoadBalancer(
        group_key=node_name_group_key,
        load_balancer=round_robin_load_balancer,
    )

    return PeerSelector(
        topology_manager,
        topic_load_balancer=topic_load_balancer or default_topic_load_balancer,
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
