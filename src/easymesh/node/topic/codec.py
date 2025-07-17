from easymesh.asyncio import Reader, Writer
from easymesh.codec import Codec
from easymesh.types import Data, Topic
from easymesh.node.topic.types import Message


class TopicMessageCodec(Codec[Message]):
    def __init__(
            self,
            topic_codec: Codec[Topic],
            data_codec: Codec[Data],
    ):
        self.topic_codec = topic_codec
        self.data_codec = data_codec

    async def encode(self, writer: Writer, message: Message) -> None:
        await self.topic_codec.encode(writer, message.topic)
        await self.data_codec.encode(writer, message.data)

    async def decode(self, reader: Reader) -> Message:
        topic = await self.topic_codec.decode(reader)
        data = await self.data_codec.decode(reader)
        return Message(topic, data)
