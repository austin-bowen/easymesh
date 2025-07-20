from easymesh.asyncio import Reader, Writer
from easymesh.codec import Codec
from easymesh.node.topic.types import TopicMessage
from easymesh.node.types import Args, KWArgs
from easymesh.types import Topic


class TopicMessageCodec(Codec[TopicMessage]):
    def __init__(
            self,
            topic_codec: Codec[Topic],
            args_codec: Codec[Args],
            kwargs_codec: Codec[KWArgs],
    ):
        self.topic_codec = topic_codec
        self.args_codec = args_codec
        self.kwargs_codec = kwargs_codec

    async def encode(self, writer: Writer, message: TopicMessage) -> None:
        await self.topic_codec.encode(writer, message.topic)
        await self.args_codec.encode(writer, message.args)
        await self.kwargs_codec.encode(writer, message.kwargs)

    async def decode(self, reader: Reader) -> TopicMessage:
        topic = await self.topic_codec.decode(reader)
        args = await self.args_codec.decode(reader)
        kwargs = await self.kwargs_codec.decode(reader)
        return TopicMessage(topic, args, kwargs)
