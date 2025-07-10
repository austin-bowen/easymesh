import logging

from easymesh.codec import Codec, pickle_codec
from easymesh.codec2 import (
    FixedLengthIntCodec,
    LengthPrefixedStringCodec,
    NodeMessageCodec,
    ServiceRequestCodec,
    ServiceResponseCodec,
    TopicMessageCodec,
)
from easymesh.node2.loadbalancing import RoundRobinLoadBalancer, ServiceLoadBalancer, TopicLoadBalancer
from easymesh.node2.peer import PeerConnectionManager, PeerConnectionSelector
from easymesh.node2.service import ServiceCaller
from easymesh.node2.topic import TopicListenerCallback, TopicListenerManager, TopicSender
from easymesh.node2.topology import MeshTopologyManager
from easymesh.types import Data, Service, Topic

logger = logging.getLogger(__name__)


class Node:
    def __init__(
            self,
            topic_sender: TopicSender,
            topic_listener_manager: TopicListenerManager,
            service_caller: ServiceCaller,
    ):
        self.topic_sender = topic_sender
        self.topic_listener_manager = topic_listener_manager
        self.service_caller = service_caller

    async def send(self, topic: Topic, data: Data = None) -> None:
        await self.topic_sender.send(topic, data)

    async def add_listener(
            self,
            topic: Topic,
            callback: TopicListenerCallback,
    ) -> None:
        self.topic_listener_manager.set_listener(topic, callback)
        await self.register()

    async def remove_listener(
            self,
            topic: Topic,
    ) -> None:
        callback = self.topic_listener_manager.remove_listener(topic)

        if callback is not None:
            await self.register()
        else:
            logger.warning(f"Attempted to remove non-existing listener for topic={topic!r}")

    async def request(self, service: Service, data: Data = None) -> Data:
        return await self.service_caller.request(service, data)

    async def register(self) -> None:
        # TODO
        ...


async def build_node(
        data_codec: Codec[Data] = pickle_codec,
        topic_load_balancer: TopicLoadBalancer = None,
        service_load_balancer: ServiceLoadBalancer = None,
) -> Node:
    connection_selector = build_peer_connection_selector(
        topic_load_balancer,
        service_load_balancer,
    )

    node_message_codec = build_node_message_codec(data_codec)

    topic_sender = TopicSender(connection_selector, node_message_codec)

    topic_listener_manager = TopicListenerManager()

    service_caller = ServiceCaller(
        connection_selector,
        node_message_codec,
        max_request_ids=2 ** (8 * 2),  # 2 bytes for request ID
    )

    return Node(
        topic_sender=topic_sender,
        topic_listener_manager=topic_listener_manager,
        service_caller=service_caller,
    )


def build_peer_connection_selector(
        topic_load_balancer: TopicLoadBalancer | None,
        service_load_balancer: ServiceLoadBalancer | None,
) -> PeerConnectionSelector:
    round_robin_load_balancer = RoundRobinLoadBalancer()

    return PeerConnectionSelector(
        topology_manager=MeshTopologyManager(),
        topic_load_balancer=topic_load_balancer or round_robin_load_balancer,
        service_load_balancer=service_load_balancer or round_robin_load_balancer,
        connection_manager=PeerConnectionManager(),
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
