import pytest

from easymesh.asyncio import BufferReader, BufferWriter
from easymesh.codec import FixedLengthIntCodec, LengthPrefixedStringCodec
from easymesh.node.codec import NodeMessageCodec
from easymesh.node.service.codec import ServiceRequestCodec, ServiceResponseCodec
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymesh.node.topic.codec import TopicMessageCodec
from easymesh.node.topic.types import TopicMessage


class TestNodeMessageCodec:
    def setup_method(self):
        topic_codec = LengthPrefixedStringCodec(FixedLengthIntCodec(length=1))
        data_codec = LengthPrefixedStringCodec(FixedLengthIntCodec(length=1))

        self.topic_message_codec = TopicMessageCodec(topic_codec, data_codec)

        id_codec = FixedLengthIntCodec(length=2)
        service_codec = topic_codec

        self.service_request_codec = ServiceRequestCodec(
            id_codec,
            service_codec,
            data_codec,
        )

        error_codec = topic_codec

        self.service_response_codec = ServiceResponseCodec(
            id_codec,
            data_codec,
            error_codec,
        )

        self.codec = NodeMessageCodec(
            self.topic_message_codec,
            self.service_request_codec,
            self.service_response_codec,
        )

    @pytest.mark.asyncio
    async def test_encode_topic_message(self):
        message = TopicMessage('topic', 'data')
        result = await self.codec.encode_topic_message(message)
        assert result == b't\x05topic\x04data'

    @pytest.mark.asyncio
    async def test_encode_service_request(self):
        request = ServiceRequest(id=1, service='service', data='data')
        result = await self.codec.encode_service_request(request)
        assert result == b's\x01\x00\x07service\x04data'

    @pytest.mark.asyncio
    async def test_encode_service_response(self):
        writer = BufferWriter()
        response = ServiceResponse(id=1, data='data')
        await self.codec.encode_service_response(writer, response)
        assert bytes(writer) == b'\x01\x00\x00\x04data'

    @pytest.mark.asyncio
    async def test_decode_topic_message_or_service_request(self):
        reader = BufferReader(
            b't\x05topic\x04data'
            b's\x01\x00\x07service\x04data'
        )

        message = await self.codec.decode_topic_message_or_service_request(reader)
        assert message == TopicMessage('topic', 'data')

        request = await self.codec.decode_topic_message_or_service_request(reader)
        assert request == ServiceRequest(id=1, service='service', data='data')

    @pytest.mark.asyncio
    async def test_decode_service_response(self):
        reader = BufferReader(b'\x01\x00\x00\x04data')
        response = await self.codec.decode_service_response(reader)
        assert response == ServiceResponse(id=1, data='data')
