import logging

from easymesh.codec import Codec, pickle_codec
from easymesh.node2.peer import PeerConnectionSelector
from easymesh.node2.topic import TopicListenerCallback, TopicListenerManager, TopicSender
from easymesh.types import Data, Message, Topic

logger = logging.getLogger(__name__)


class Node:
    def __init__(
            self,
            topic_sender: TopicSender,
            topic_listener_manager: TopicListenerManager,
    ):
        self.topic_sender = topic_sender
        self.topic_listener_manager = topic_listener_manager

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

    async def register(self) -> None:
        # TODO
        ...


async def build_node(
        message_codec: Codec[Message] = pickle_codec,
) -> Node:
    connection_selector = PeerConnectionSelector()

    topic_sender = TopicSender(connection_selector, message_codec)

    topic_listener_manager = TopicListenerManager()

    return Node(
        topic_sender=topic_sender,
        topic_listener_manager=topic_listener_manager,
    )
