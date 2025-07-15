import logging
from collections.abc import Awaitable, Callable

from typing_extensions import Buffer

from easymesh.asyncio import log_error, many
from easymesh.codec2 import NodeMessageCodec
from easymesh.node.peer import PeerConnectionManager, PeerSelector
from easymesh.specs import MeshNodeSpec
from easymesh.types import Data, Message, Topic

TopicListenerCallback = Callable[[Topic, Data], Awaitable[None]]

logger = logging.getLogger(__name__)


class TopicSender:
    def __init__(
            self,
            peer_selector: PeerSelector,
            connection_manager: PeerConnectionManager,
            node_message_codec: NodeMessageCodec,
    ):
        self.peer_selector = peer_selector
        self.connection_manager = connection_manager
        self.node_message_codec = node_message_codec

    async def send(self, topic: Topic, data: Data) -> None:
        # TODO handle case of self-sending more efficiently

        nodes = self.peer_selector.get_nodes_for_topic(topic)
        data = await self.node_message_codec.encode_topic_message(Message(topic, data))

        await many([
            log_error(self._send_to_one(n, data))
            for n in nodes
        ])

    async def _send_to_one(self, node: MeshNodeSpec, data: Buffer) -> None:
        connection = await self.connection_manager.get_connection(node)

        async with connection.writer as writer:
            await writer.write(data)
            await writer.drain()


class TopicListenerManager:
    def __init__(self):
        self._listeners: dict[Topic, TopicListenerCallback] = {}

    @property
    def topics(self) -> set[Topic]:
        return set(self._listeners.keys())

    def get_listener(self, topic: Topic) -> TopicListenerCallback | None:
        return self._listeners.get(topic)

    def set_listener(self, topic: Topic, callback: TopicListenerCallback) -> None:
        self._listeners[topic] = callback

    def remove_listener(self, topic: Topic) -> TopicListenerCallback | None:
        return self._listeners.pop(topic, None)

    def has_listener(self, topic: Topic) -> bool:
        return topic in self._listeners


class TopicMessageHandler:
    def __init__(
            self,
            listener_manager: TopicListenerManager,
    ):
        self.listener_manager = listener_manager

    async def handle_message(self, message: Message) -> None:
        callback = self.listener_manager.get_listener(message.topic)

        if callback:
            await log_error(callback(message.topic, message.data))
        else:
            logger.warning(
                f'Received message for topic={message.topic!r} '
                f'but no listener is registered.'
            )
