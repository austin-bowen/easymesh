import asyncio
import logging
from functools import wraps
from typing import NamedTuple

from rosy.asyncio import forever
from rosy.coordinator.client import MeshCoordinatorClient
from rosy.node.peer import PeerConnectionManager
from rosy.node.servers import ServersManager
from rosy.node.service.caller import ServiceCaller
from rosy.node.service.handlermanager import ServiceHandlerManager
from rosy.node.topic.listenermanager import TopicListenerManager
from rosy.node.topic.sender import TopicSender
from rosy.node.topology import MeshTopologyManager
from rosy.reqres import MeshTopologyBroadcast
from rosy.specs import MeshNodeSpec, NodeId
from rosy.types import Data, Service, ServiceCallback, Topic, TopicCallback

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

    async def send(self, topic: Topic, *args: Data, **kwargs: Data) -> None:
        await self.topic_sender.send(topic, args, kwargs)

    async def listen(
            self,
            topic: Topic,
            callback: TopicCallback,
    ) -> None:
        self.topic_listener_manager.set_callback(topic, callback)
        await self.register()

    async def stop_listening(self, topic: Topic) -> None:
        callback = self.topic_listener_manager.remove_callback(topic)

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

    async def call(self, service: Service, *args, **kwargs) -> Data:
        return await self.service_caller.call(service, args, kwargs)

    async def add_service(self, service: Service, handler: ServiceCallback) -> None:
        """Add a service to the node that other nodes can call."""
        self.service_handler_manager.set_callback(service, handler)
        await self.register()

    async def remove_service(self, service: Service) -> None:
        """Stop providing a service."""
        callback = self.service_handler_manager.remove_callback(service)

        if callback is not None:
            await self.register()
        else:
            logger.warning(f"Attempted to remove non-existing service={service!r}")

    async def service_has_providers(self, service: Service) -> bool:
        """Check if there are any nodes that provide the service."""
        providers = self.topology_manager.get_nodes_providing_service(service)
        return bool(providers)

    async def wait_for_service_provider(self, service: Service, poll_interval: float = 1.) -> None:
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
            topics=self.topic_listener_manager.keys,
            services=self.service_handler_manager.keys,
        )

    async def _handle_topology_broadcast(self, broadcast: MeshTopologyBroadcast) -> None:
        new_topology = broadcast.mesh_topology

        logger.debug(
            f'Received mesh topology broadcast with '
            f'{len(new_topology.nodes)} nodes.'
        )

        removed_nodes = self.topology_manager.get_removed_nodes(new_topology)
        logger.debug(
            f'Removed {len(removed_nodes)} nodes: '
            f'{[str(node.id) for node in removed_nodes]}'
        )

        self.topology_manager.set_topology(new_topology)

        for node in removed_nodes:
            await self.connection_manager.close_connection(node)

    async def forever(self) -> None:
        """
        Does nothing forever. Convenience method to prevent your main function
        from exiting while the node is running.
        """
        await forever()  # pragma: no cover


class TopicProxy(NamedTuple):
    node: Node
    topic: Topic

    def __str__(self):
        name = self.__class__.__name__
        return f'{name}(topic={self.topic!r})'

    async def send(self, *args: Data, **kwargs: Data) -> None:
        await self.node.send(self.topic, *args, **kwargs)

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
        return f'{name}(service={self.service!r})'

    async def __call__(self, *args: Data, **kwargs: Data) -> Data:
        return await self.call(*args, **kwargs)

    async def call(self, *args: Data, **kwargs: Data) -> Data:
        return await self.node.call(self.service, *args, **kwargs)

    async def has_providers(self) -> bool:
        return await self.node.service_has_providers(self.service)

    async def wait_for_provider(self, poll_interval: float = 1.) -> None:
        await self.node.wait_for_service_provider(self.service, poll_interval)
