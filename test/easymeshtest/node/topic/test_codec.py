import pytest

from easymesh.codec import FixedLengthIntCodec, LengthPrefixedStringCodec
from easymesh.node.topic.codec import TopicMessageCodec
from easymesh.node.topic.types import TopicMessage
from easymeshtest.test_codec import CodecTest


class TestTopicMessageCodec(CodecTest):
    codec: TopicMessageCodec

    def setup_method(self):
        len_prefix_codec = FixedLengthIntCodec(length=1)
        self.topic_codec = LengthPrefixedStringCodec(len_prefix_codec)
        self.data_codec = LengthPrefixedStringCodec(len_prefix_codec)

        self.codec = TopicMessageCodec(self.topic_codec, self.data_codec)

    def test_constructor_defaults(self):
        assert self.codec.topic_codec == self.topic_codec
        assert self.codec.data_codec == self.data_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        await self.assert_encode_decode(TopicMessage('topic', 'data'))

    @pytest.mark.asyncio
    async def test_encode(self):
        await self.assert_encode_writes(
            TopicMessage('topic', 'data'),
            [
                b'\x05',
                b'topic',
                b'\x04',
                b'data',
            ],
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        await self.assert_decode_returns(
            b'\x05topic\x04data',
            TopicMessage('topic', 'data'),
        )
