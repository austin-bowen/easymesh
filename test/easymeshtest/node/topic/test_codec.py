from unittest.mock import call

import pytest

from easymesh.node.topic.codec import TopicMessageCodec
from easymesh.node.topic.types import TopicMessage
from easymeshtest.test_codec import CodecTest


class TestTopicMessageCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.message = TopicMessage(
            topic='topic',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        self.topic_codec = self.add_tracked_codec_mock()
        self.args_codec = self.add_tracked_codec_mock()
        self.kwargs_codec = self.add_tracked_codec_mock()

        self.codec = TopicMessageCodec(
            topic_codec=self.topic_codec,
            args_codec=self.args_codec,
            kwargs_codec=self.kwargs_codec,
        )

    @pytest.mark.asyncio
    async def test_encode(self):
        writer, message = self.writer, self.message

        await self.assert_encode_returns_None(message)

        self.call_tracker.assert_calls(
            (self.topic_codec.encode, call(writer, message.topic)),
            (self.args_codec.encode, call(writer, message.args)),
            (self.kwargs_codec.encode, call(writer, message.kwargs)),
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        self.call_tracker.track(self.topic_codec.decode, return_value='topic')
        self.call_tracker.track(self.args_codec.decode, return_value=['arg'])
        self.call_tracker.track(self.kwargs_codec.decode, return_value={'key': 'value'})

        reader, message = self.reader, self.message

        await self.assert_decode_returns(message)

        self.call_tracker.assert_calls(
            (self.topic_codec.decode, call(reader)),
            (self.args_codec.decode, call(reader)),
            (self.kwargs_codec.decode, call(reader)),
        )
