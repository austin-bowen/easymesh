import asyncio
from argparse import Namespace
from asyncio import StreamReader, StreamWriter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from inspect import isawaitable
from typing import Literal, Optional, TypeVar, Union

from easymesh.argparse import get_node_arg_parser
from easymesh.asyncio import MultiWriter, close_ignoring_errors, many
from easymesh.authentication import Authenticator, optional_authkey_authenticator
from easymesh.codec import Codec, pickle_codec
from easymesh.coordinator.client import MeshCoordinatorClient, build_coordinator_client
from easymesh.coordinator.constants import DEFAULT_COORDINATOR_PORT
from easymesh.network import get_lan_hostname
from easymesh.node.listenermanager import ListenerCallback, ListenerManager, SerialTopicsListenerManager
from easymesh.node.loadbalancing import (
    GroupingLoadBalancer,
    LoadBalancer,
    NoopLoadBalancer,
    RoundRobinLoadBalancer,
    node_name_group_key,
)
from easymesh.node.peer import MeshPeer, PeerManager
from easymesh.node.servers import (
    PortScanTcpServerProvider,
    ServerProvider,
    TmpUnixServerProvider,
    UnsupportedProviderError,
)
from easymesh.node.servicesmanager import BasicServicesOverTopicsManager, ServicesManager
from easymesh.objectio import MessageReader, MessageWriter
from easymesh.reqres import MeshTopologyBroadcast
from easymesh.specs import MeshNodeSpec, NodeId
from easymesh.types import (
    Data,
    Host,
    Message,
    Port,
    ServerHost,
    Service,
    Topic,
)
from easymesh.node2.service.types import ServiceResponse

try:
    from easymesh.codec import msgpack_codec
except ImportError:
    msgpack_codec = None

T = TypeVar('T')


class MeshNode:
    load_balancer: LoadBalancer

    def __init__(
            self,
            id: NodeId,
            mesh_coordinator_client: MeshCoordinatorClient,
            server_providers: Iterable[ServerProvider],
            listener_manager: ListenerManager,
            peer_manager: PeerManager,
            message_codec: Codec[Data],
            authenticator: Authenticator,
            load_balancer: LoadBalancer,
            services_manager: ServicesManager,
    ):
        self._id = id
        self._mesh_coordinator_client = mesh_coordinator_client
        self._server_providers = server_providers
        self._listener_manager = listener_manager
        self._peer_manager = peer_manager
        self._message_codec = message_codec
        self._authenticator = authenticator
        self.load_balancer = load_balancer
        self._services_manager = services_manager

        self._connection_specs = []

        mesh_coordinator_client.mesh_topology_broadcast_handler = self._handle_topology_broadcast

    @property
    def id(self) -> NodeId:
        return self._id

    def __str__(self) -> str:
        return str(self.id)

    def log(self, *values, **kwargs) -> None:
        now = datetime.now()
        print(f'[{now}] [{self}]', *values, **kwargs)

    async def start(self) -> None:
        self.log('Starting node servers...')
        self._connection_specs = []
        for server_provider in self._server_providers:
            try:
                server, connection_spec = await server_provider.start_server(
                    self._handle_connection
                )
            except UnsupportedProviderError as e:
                self.log(e)
            else:
                self.log(f'Started node server with connection_spec={connection_spec}')
                self._connection_specs.append(connection_spec)

        if not self._connection_specs:
            raise RuntimeError('Unable to start any node servers')

        await self._register_node()

        await self._services_manager.start(self)

    async def _register_node(self) -> None:
        node_spec = MeshNodeSpec(
            id=self.id,
            connection_specs=self._connection_specs,
            topics=self._listener_manager.get_topics(),
        )

        self.log(f'node_spec={node_spec}')
        self.log('Registering node with server...')
        await self._mesh_coordinator_client.register_node(node_spec)

    async def _handle_connection(self, reader: StreamReader, writer: StreamWriter) -> None:
        peer_name = writer.get_extra_info('peername') or writer.get_extra_info('sockname')
        self.log(f'New connection from: {peer_name}')

        await self._authenticator.authenticate(reader, writer)

        # Don't need the writer
        writer.write_eof()

        message_reader = MessageReader(reader, codec=self._message_codec)

        try:
            async for message in message_reader:
                await self._listener_manager.handle_message(message)
        except EOFError:
            self.log(f'Closed connection from: {peer_name}')
        finally:
            await close_ignoring_errors(writer)

    async def _handle_topology_broadcast(self, broadcast: MeshTopologyBroadcast) -> None:
        self.log(
            f'Received mesh topology broadcast with '
            f'{len(broadcast.mesh_topology.nodes)} nodes.'
        )

        topology = broadcast.mesh_topology
        await self._peer_manager.set_mesh_topology(topology)

    async def send(self, topic: Topic, data: Data = None):
        peers = await self._get_peers_for_topic(topic)
        if not peers:
            return

        peers = self.load_balancer.choose_nodes(peers, topic)

        peers_without_self = []
        send_to_self = False
        for peer in peers:
            if peer.id == self.id:
                assert not send_to_self
                send_to_self = True
            else:
                peers_without_self.append(peer)

        message = Message(topic, data)

        tasks = []
        if peers_without_self:
            tasks.append(self._send_to_peers(peers_without_self, message))
        if send_to_self:
            tasks.append(self._listener_manager.handle_message(message))

        await many(tasks)

    async def _send_to_peers(self, peers: list[MeshPeer], message: Message) -> None:
        writers = [await peer.connection.get_writer() for peer in peers]

        multi_writer = MultiWriter(writers)
        message_writer = MessageWriter(multi_writer, codec=self._message_codec)

        [await writer.lock.acquire() for writer in writers]

        try:
            await message_writer.write(message, drain=False)

            to_close = []
            for peer, writer in zip(peers, writers):
                try:
                    await writer.drain()
                except Exception as e:
                    self.log(
                        f'Error sending message with topic={message.topic} '
                        f'to node {peer.id}: {e!r}'
                    )

                    to_close.append(peer.connection)
        finally:
            [writer.lock.release() for writer in writers]

        for connection in to_close:
            await connection.close()

    async def send_result(
            self,
            topic: Topic,
            fn: Callable[[...], T],
            *args,
            **kwargs,
    ) -> tuple[bool, T]:
        """
        Send the result of a function to all listeners of a topic.

        This is "lazy" in that it will only call the function if there are
        listeners for the topic. This is useful in avoiding unnecessary
        computation when no one is listening.

        Returns a tuple of (did_send, result). If there were no listeners, then
        this returns (False, None).

        Args:
            topic:
                Topic to send the result to, if there are listeners.
            fn:
                Function (either "normal" or async) to generate the result.
            args:
                Passed to ``fn``.
            kwargs:
                Passed to ``fn``.
        """

        if not await self.topic_has_listeners(topic):
            return False, None

        result = fn(*args, **kwargs)
        if isawaitable(result):
            result = await result

        await self.send(topic, result)

        return True, result

    async def topic_has_listeners(self, topic: Topic) -> bool:
        peers = await self._get_peers_for_topic(topic)
        return bool(peers)

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

    async def _get_peers_for_topic(self, topic: Topic) -> list[MeshPeer]:
        peers = self._peer_manager.get_peers()
        return [p for p in peers if await p.is_listening_to(topic)]

    def get_topic_sender(self, topic: Topic) -> 'TopicSender':
        return TopicSender(self, topic)

    async def listen(self, topic: Topic, callback: ListenerCallback) -> None:
        self._listener_manager.set_listener(topic, callback)
        await self._register_node()

    async def stop_listening(self, topic: Topic) -> Optional[ListenerCallback]:
        callback = self._listener_manager.remove_listener(topic)

        if callback is not None:
            await self._register_node()

        return callback

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
                    return await callback(topic, data)

                await self.stop_listening(topic)

                async def wait_for_listener_then_listen():
                    await self.wait_for_listener(downstream_topic, poll_interval)
                    await self.listen(topic, wrapper)

                asyncio.create_task(wait_for_listener_then_listen())

            return wrapper

        return decorator

    async def request(
            self,
            service: Service,
            data: Data = None,
            timeout: float = None,
    ) -> ServiceResponse:
        """Send a request to a service and return the response."""
        return await self._services_manager.request(service, data, timeout)

    async def add_service(self, service: Service, handler) -> None:
        """Add a service to the node that other nodes can send requests to."""
        await self._services_manager.add_service(service, handler)

    def get_service(self, service: Service) -> 'ServiceCaller':
        """
        Returns a convenient way to call a service if used more than once.

        Example:
            >>> math_service = node.get_service('math')
            >>> result = await math_service('2 + 2')
            >>> # ... is equivalent to ...
            >>> result = await node.request('math', '2 + 2')
        """

        return ServiceCaller(service, self._services_manager)


@dataclass
class TopicSender:
    node: MeshNode
    topic: Topic

    async def send(self, data: Data = None) -> None:
        await self.node.send(self.topic, data)

    async def send_result(
            self,
            fn: Callable[[...], T],
            *args,
            **kwargs,
    ) -> tuple[bool, T]:
        return await self.node.send_result(self.topic, fn, *args, **kwargs)

    async def has_listeners(self) -> bool:
        return await self.node.topic_has_listeners(self.topic)

    async def wait_for_listener(self, poll_interval: float = 1.) -> None:
        await self.node.wait_for_listener(self.topic, poll_interval)

    def depends_on_listener(self, poll_interval: float = 1.):
        return self.node.depends_on_listener(self.topic, poll_interval)


class ServiceCaller:
    service: Service

    def __init__(self, service: Service, service_manager: ServicesManager):
        self.service = service
        self._service_manager = service_manager

    def __str__(self) -> str:
        name = self.__class__.__name__
        return f'{name}(service={self.service})'

    async def __call__(self, *args, **kwargs) -> ServiceResponse:
        return await self.request(*args, **kwargs)

    async def request(self, data: Data = None, timeout: float = None) -> ServiceResponse:
        return await self._service_manager.request(self.service, data, timeout)


async def build_mesh_node(
        name: str,
        coordinator_host: Host = 'localhost',
        coordinator_port: Port = DEFAULT_COORDINATOR_PORT,
        coordinator_reconnect_timeout: Optional[float] = 5.0,
        allow_unix_connections: bool = True,
        allow_tcp_connections: bool = True,
        node_server_host: ServerHost = None,
        node_client_host: Host = None,
        message_queue_maxsize: int = 10,
        message_codec: Union[Codec[Data], Literal['pickle', 'msgpack']] = 'pickle',
        authkey: bytes = None,
        authenticator: Authenticator = None,
        load_balancer: Union[LoadBalancer, Literal['default'], None] = 'default',
        services_manager: ServicesManager = None,
        start: bool = True,
        **kwargs,
) -> MeshNode:
    authenticator = authenticator or optional_authkey_authenticator(authkey)

    mesh_coordinator_client = await build_coordinator_client(
        coordinator_host,
        coordinator_port,
        authenticator,
        reconnect_timeout=coordinator_reconnect_timeout,
    )

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

    listener_manager = SerialTopicsListenerManager(message_queue_maxsize)
    peer_manager = PeerManager(authenticator)

    if message_codec == 'pickle':
        message_codec = pickle_codec
    elif message_codec == 'msgpack':
        if not msgpack_codec:
            raise ValueError('msgpack is not installed')
        message_codec = msgpack_codec

    if load_balancer == 'default':
        load_balancer = GroupingLoadBalancer(node_name_group_key, RoundRobinLoadBalancer())
    elif load_balancer is None:
        load_balancer = NoopLoadBalancer()

    services_manager = services_manager or BasicServicesOverTopicsManager()

    node = MeshNode(
        NodeId(name),
        mesh_coordinator_client,
        server_providers,
        listener_manager,
        peer_manager,
        message_codec,
        authenticator,
        load_balancer,
        services_manager,
    )

    if start:
        await node.start()

    return node


async def build_mesh_node_from_args(
        default_node_name: str = None,
        args: Namespace = None,
        **kwargs,
) -> MeshNode:
    """
    Builds a mesh node from command line arguments.

    Args:
        default_node_name:
            Default node name. If not given, the argument is required.
            Ignored if `args` is given.
        args:
            Arguments from an argument parser. If not given, an argument parser
            is created using `get_node_arg_parser` and is used to parse args.
            This is useful if you create your own argument parser.
        kwargs:
            Additional keyword arguments to pass to `build_mesh_node`.
            These will override anything specified in `args`.
    """

    if args is None:
        args = get_node_arg_parser(default_node_name).parse_args()

    build_args = vars(args)

    if hasattr(args, 'coordinator'):
        build_args['coordinator_host'] = args.coordinator.host
        build_args['coordinator_port'] = args.coordinator.port

    build_args.update(kwargs)

    return await build_mesh_node(**build_args)
