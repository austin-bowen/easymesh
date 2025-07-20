import pytest

from easymesh.asyncio import BufferReader, BufferWriter
from easymesh.codec import FixedLengthIntCodec, LengthPrefixedStringCodec
from easymesh.node.builder import build_node_message_codec
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymesh.node.topic.types import TopicMessage


class TestNodeMessageCodec:
    def setup_method(self):
        self.topic_message = TopicMessage('topic', ['arg'], {'key': 'value'})
        self.encoded_topic_message = b't\x05topic\x01\x03arg\x01\x03key\x05value'

        self.service_request = ServiceRequest(
            id=1,
            service='service',
            args=['arg'],
            kwargs={'key': 'value'}
        )
        self.encoded_service_request = (
            b's'
            b'\x01'
            b'\x00\x07service'
            b'\x01\x03arg'
            b'\x01\x03key\x05value'
        )

        self.service_response = ServiceResponse(id=1, data='data', error=None)
        self.encoded_service_response = b'\x01\x00\x00\x04data'

        self.codec = build_node_message_codec(
            data_codec=LengthPrefixedStringCodec(FixedLengthIntCodec(length=1)),
        )

    @pytest.mark.asyncio
    async def test_encode_topic_message(self):
        result = await self.codec.encode_topic_message(self.topic_message)
        assert result == self.encoded_topic_message

    @pytest.mark.asyncio
    async def test_encode_service_request(self):
        result = await self.codec.encode_service_request(self.service_request)
        assert result == self.encoded_service_request

    @pytest.mark.asyncio
    async def test_encode_service_response(self):
        writer = BufferWriter()
        await self.codec.encode_service_response(writer, self.service_response)
        assert bytes(writer) == self.encoded_service_response

    @pytest.mark.asyncio
    async def test_decode_topic_message_or_service_request(self):
        reader = BufferReader(
            self.encoded_topic_message +
            self.encoded_service_request
        )

        message = await self.codec.decode_topic_message_or_service_request(reader)
        assert message == self.topic_message

        request = await self.codec.decode_topic_message_or_service_request(reader)
        assert request == self.service_request

    @pytest.mark.asyncio
    async def test_decode_service_response(self):
        reader = BufferReader(self.encoded_service_response)
        response = await self.codec.decode_service_response(reader)
        assert response == self.service_response
