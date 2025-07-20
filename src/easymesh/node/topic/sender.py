from typing_extensions import Buffer

from easymesh.asyncio import log_error, many
from easymesh.node.codec import NodeMessageCodec
from easymesh.node.peer import PeerConnectionManager, PeerSelector
from easymesh.node.topic.types import TopicMessage
from easymesh.node.types import Args, KWArgs
from easymesh.specs import MeshNodeSpec
from easymesh.types import Topic


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

    async def send(self, topic: Topic, args: Args, kwargs: KWArgs) -> None:
        # TODO handle case of self-sending more efficiently

        nodes = self.peer_selector.get_nodes_for_topic(topic)
        if not nodes:
            return

        message = TopicMessage(topic, args, kwargs)
        data = await self.node_message_codec.encode_topic_message(message)

        await many([
            log_error(self._send_to_one(n, data))
            for n in nodes
        ])

    async def _send_to_one(self, node: MeshNodeSpec, data: Buffer) -> None:
        connection = await self.connection_manager.get_connection(node)

        async with connection.writer as writer:
            writer.write(data)
            await writer.drain()
