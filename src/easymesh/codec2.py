import pickle
from abc import ABC, abstractmethod
from typing import Any, Generic, Literal, TypeVar, Union

from easymesh.asyncio import Reader
from easymesh.node2.peer import BufferWriter, Writer
from easymesh.types import Buffer, Data, Message, RequestId, Service, ServiceRequest, ServiceResponse, Topic
from easymesh.utils import require

try:
    import msgpack
except ImportError:
    msgpack = None

T = TypeVar('T')

ByteOrder = Literal['big', 'little']
DEFAULT_BYTE_ORDER: ByteOrder = 'little'

DEFAULT_MAX_HEADER_LEN: int = 8

DEFAULT_MAX_TOPIC_LENGTH: int = 256
DEFAULT_TOPIC_ENCODING: str = 'utf-8'


class Codec(Generic[T], ABC):
    @abstractmethod
    async def encode(self, writer: Writer, obj: T) -> None:
        ...

    @abstractmethod
    async def decode(self, reader: Reader) -> T:
        ...


class UsingLenHeader:
    def __init__(
            self,
            byte_order: ByteOrder,
            max_header_len: int,
    ):
        self.max_header_len = max_header_len
        self.byte_order = byte_order

    async def _read_data_with_len_header(self, reader: Reader) -> bytes:
        header_len = (await reader.readexactly(1))[0]

        if not header_len:
            return b''

        require(
            header_len <= self.max_header_len,
            f'Received header_len={header_len} > max_header_len={self.max_header_len}',
        )

        header = await reader.readexactly(header_len)
        data_len = self._bytes_to_int(header)

        return await reader.readexactly(data_len)

    def _bytes_to_int(self, data: bytes) -> int:
        return int.from_bytes(data, byteorder=self.byte_order, signed=False)

    async def _write_data_with_len_header(self, writer: Writer, data: bytes) -> None:
        data_len = len(data)

        header_len = (data_len.bit_length() + 7) // 8

        require(
            header_len <= self.max_header_len,
            f'Computed header_len={header_len} > max_header_len={self.max_header_len}',
        )

        await writer.write(self._int_to_bytes(header_len, length=1))

        if not data_len:
            return

        header = self._int_to_bytes(data_len, length=header_len)

        await writer.write(header)
        await writer.write(data)

    def _int_to_bytes(self, value: int, length: int) -> bytes:
        return value.to_bytes(length, byteorder=self.byte_order, signed=False)


class PickleCodec(Codec[Any], UsingLenHeader):
    def __init__(
            self,
            protocol: int = pickle.HIGHEST_PROTOCOL,
            dump_kwargs: dict[str, Any] = None,
            load_kwargs: dict[str, Any] = None,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            max_header_len: int = DEFAULT_MAX_HEADER_LEN,
    ):
        UsingLenHeader.__init__(self, byte_order, max_header_len)

        self.protocol = protocol
        self.dump_kwargs = dump_kwargs or {}
        self.load_kwargs = load_kwargs or {}

    async def encode(self, writer: Writer, obj: T) -> None:
        data = pickle.dumps(obj, protocol=self.protocol, **self.dump_kwargs)
        await self._write_data_with_len_header(writer, data)

    async def decode(self, reader: Reader) -> T:
        data = await self._read_data_with_len_header(reader)
        return pickle.loads(data, **self.load_kwargs)


pickle_codec = PickleCodec()

if msgpack:
    MsgpackTypes = Union[None, bool, int, float, str, bytes, bytearray, list, tuple, dict]


    class MsgpackCodec(Codec[MsgpackTypes], UsingLenHeader):
        def __init__(
                self,
                pack_kwargs: dict[str, Any] = None,
                unpack_kwargs: dict[str, Any] = None,
                byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
                max_header_len: int = DEFAULT_MAX_HEADER_LEN,
        ):
            UsingLenHeader.__init__(self, byte_order, max_header_len)

            self.pack_kwargs = pack_kwargs or {}
            self.unpack_kwargs = unpack_kwargs or {}

        async def encode(self, writer: Writer, obj: T) -> None:
            data = msgpack.packb(obj, **self.pack_kwargs)
            await self._write_data_with_len_header(writer, data)

        async def decode(self, reader: Reader) -> T:
            data = await self._read_data_with_len_header(reader)
            return msgpack.unpackb(data, **self.unpack_kwargs)


    msgpack_codec = MsgpackCodec()


class FixedLengthIntCodec(Codec[int]):
    def __init__(
            self,
            length: int,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            signed: bool = False,
    ):
        self.length = length
        self.byte_order = byte_order
        self.signed = signed

    async def encode(self, writer: Writer, value: int) -> None:
        data = value.to_bytes(self.length, byteorder=self.byte_order, signed=self.signed)
        await writer.write(data)

    async def decode(self, reader: Reader) -> int:
        data = await reader.readexactly(self.length)
        return int.from_bytes(data, byteorder=self.byte_order, signed=self.signed)


class LengthPrefixedStringCodec(Codec[str]):
    def __init__(
            self,
            len_prefix_codec: Codec[int],
            encoding: str = 'utf-8',
    ):
        self.len_prefix_codec = len_prefix_codec
        self.encoding = encoding

    async def encode(self, writer: Writer, data: str) -> None:
        data = data.encode(encoding=self.encoding)
        await self.len_prefix_codec.encode(writer, len(data))
        if data:
            await writer.write(data)

    async def decode(self, reader: Reader) -> str:
        length = await self.len_prefix_codec.decode(reader)
        if length == 0:
            return ''

        data = await reader.readexactly(length)
        return data.decode(encoding=self.encoding)


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


class ServiceRequestCodec(Codec[ServiceRequest]):
    def __init__(
            self,
            id_codec: Codec[RequestId],
            service_codec: Codec[Service],
            data_codec: Codec[Data],
    ):
        self.id_codec = id_codec
        self.service_codec = service_codec
        self.data_codec = data_codec

    async def encode(self, writer: Writer, request: ServiceRequest) -> None:
        await self.id_codec.encode(writer, request.id)
        await self.service_codec.encode(writer, request.service)
        await self.data_codec.encode(writer, request.data)

    async def decode(self, reader: Reader) -> ServiceRequest:
        id = await self.id_codec.decode(reader)
        service = await self.service_codec.decode(reader)
        data = await self.data_codec.decode(reader)
        return ServiceRequest(id, service, data)


class ServiceResponseCodec(Codec[ServiceResponse]):
    def __init__(
            self,
            id_codec: Codec[RequestId],
            data_codec: Codec[Data],
            error_codec: Codec[str],
            success_status_code: bytes = b'\x00',
            error_status_code: bytes = b'\xEE',
    ):
        self.id_codec = id_codec
        self.data_codec = data_codec
        self.error_codec = error_codec
        self.success_status_code = success_status_code
        self.error_status_code = error_status_code

    async def encode(self, writer: Writer, response: ServiceResponse) -> None:
        await self.id_codec.encode(writer, response.id)

        if response.error:
            await writer.write(self.error_status_code)
            await self.error_codec.encode(writer, response.error)
        else:
            await writer.write(self.success_status_code)
            await self.data_codec.encode(writer, response.data)

    async def decode(self, reader: Reader) -> ServiceResponse:
        id = await self.id_codec.decode(reader)

        status_code = await reader.readexactly(1)
        if status_code == self.success_status_code:
            data = await self.data_codec.decode(reader)
            error = None
        elif status_code == self.error_status_code:
            data = None
            error = await self.error_codec.decode(reader)
        else:
            raise RuntimeError(f'Received unknown status code={status_code!r}')

        return ServiceResponse(id, data, error)


class NodeMessageCodec:
    def __init__(
            self,
            topic_message_codec: Codec[Message],
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

    async def encode_topic_message(self, message: Message) -> Buffer:
        buffer = BufferWriter()
        await buffer.write(self.topic_message_prefix)
        await self.topic_message_codec.encode(buffer, message)
        return buffer

    async def encode_service_request(self, request: ServiceRequest) -> Buffer:
        buffer = BufferWriter()
        await buffer.write(self.service_request_prefix)
        await self.service_request_codec.encode(buffer, request)
        return buffer

    async def encode_service_response(self, response: ServiceResponse) -> Buffer:
        buffer = BufferWriter()
        await self.service_response_codec.encode(buffer, response)
        return buffer

    async def decode_topic_message_or_service_request(self, reader: Reader) -> Message | ServiceRequest:
        prefix = await reader.readexactly(1)

        if prefix == self.topic_message_prefix:
            return await self.topic_message_codec.decode(reader)
        elif prefix == self.service_request_prefix:
            return await self.service_request_codec.decode(reader)
        else:
            raise ValueError(f"Unknown prefix: {prefix!r}")

    async def decode_service_response(self, reader: Reader) -> ServiceResponse:
        return await self.service_response_codec.decode(reader)
