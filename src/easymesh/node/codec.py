from easymesh.asyncio import BufferWriter, Reader, Writer
from easymesh.codec import Codec
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymesh.node.topic.types import TopicMessage
from easymesh.types import Buffer
from easymesh.utils import require


class NodeMessageCodec:
    def __init__(
            self,
            topic_message_codec: Codec[TopicMessage],
            service_request_codec: Codec[ServiceRequest],
            service_response_codec: Codec[ServiceResponse],
            topic_message_prefix: bytes = b't',
            service_request_prefix: bytes = b's',
    ):
        require(len(topic_message_prefix) == 1, 'Topic message prefix must be a single byte')
        require(len(service_request_prefix) == 1, 'Service request prefix must be a single byte')

        self.topic_message_codec = topic_message_codec
        self.service_request_codec = service_request_codec
        self.service_response_codec = service_response_codec
        self.topic_message_prefix = topic_message_prefix
        self.service_request_prefix = service_request_prefix

    async def encode_topic_message(self, message: TopicMessage) -> Buffer:
        buffer = BufferWriter()
        buffer.write(self.topic_message_prefix)
        await self.topic_message_codec.encode(buffer, message)
        return buffer

    async def encode_service_request(self, request: ServiceRequest) -> Buffer:
        buffer = BufferWriter()
        buffer.write(self.service_request_prefix)
        await self.service_request_codec.encode(buffer, request)
        return buffer

    async def encode_service_response(
            self,
            writer: Writer,
            response: ServiceResponse,
    ) -> None:
        await self.service_response_codec.encode(writer, response)

    async def decode_topic_message_or_service_request(self, reader: Reader) -> TopicMessage | ServiceRequest:
        prefix = await reader.readexactly(1)

        if prefix == self.topic_message_prefix:
            return await self.topic_message_codec.decode(reader)
        elif prefix == self.service_request_prefix:
            return await self.service_request_codec.decode(reader)
        else:
            raise ValueError(f'Unknown prefix={prefix!r}')

    async def decode_service_response(self, reader: Reader) -> ServiceResponse:
        return await self.service_response_codec.decode(reader)
