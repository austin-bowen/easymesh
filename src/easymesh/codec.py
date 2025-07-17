import pickle
from abc import ABC, abstractmethod
from typing import Any, Generic, Literal, TypeVar

from easymesh.asyncio import BufferWriter, Reader, Writer
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymesh.types import Buffer
from easymesh.node.topic.types import TopicMessage
from easymesh.utils import require

try:
    import msgpack
except ImportError:
    msgpack = None

T = TypeVar('T')

ByteOrder = Literal['big', 'little']
DEFAULT_BYTE_ORDER: ByteOrder = 'little'

DEFAULT_MAX_BYTE_LENGTH: int = 8

DEFAULT_MAX_TOPIC_LENGTH: int = 256
DEFAULT_TOPIC_ENCODING: str = 'utf-8'


class Codec(Generic[T], ABC):
    @abstractmethod
    async def encode(self, writer: Writer, obj: T) -> None:
        ...

    @abstractmethod
    async def decode(self, reader: Reader) -> T:
        ...


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
        writer.write(data)

    async def decode(self, reader: Reader) -> int:
        data = await reader.readexactly(self.length)
        return int.from_bytes(data, byteorder=self.byte_order, signed=self.signed)


class VariableLengthIntCodec(Codec[int]):
    def __init__(
            self,
            max_byte_length: int = DEFAULT_MAX_BYTE_LENGTH,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            signed: bool = False,
    ):
        require(
            0 < max_byte_length <= 255,
            f'max_byte_length must be in range (0, 255]; got {max_byte_length}.',
        )

        self.max_byte_length = max_byte_length
        self.byte_order = byte_order
        self.signed = signed

    async def encode(self, writer: Writer, value: int) -> None:
        int_byte_length = byte_length(value)

        require(
            int_byte_length <= self.max_byte_length,
            f'Computed byte_length={int_byte_length} > max_byte_length={self.max_byte_length}',
        )

        header = bytes([int_byte_length])
        writer.write(header)

        if int_byte_length > 0:
            data = value.to_bytes(int_byte_length, byteorder=self.byte_order, signed=self.signed)
            writer.write(data)

    async def decode(self, reader: Reader) -> int:
        header = await reader.readexactly(1)

        int_byte_length = header[0]
        if int_byte_length == 0:
            return 0

        require(
            int_byte_length <= self.max_byte_length,
            f'Received byte_length={int_byte_length} > max_byte_length={self.max_byte_length}',
        )

        data = await reader.readexactly(int_byte_length)
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
            writer.write(data)

    async def decode(self, reader: Reader) -> str:
        length = await self.len_prefix_codec.decode(reader)
        if length == 0:
            return ''

        data = await reader.readexactly(length)
        return data.decode(encoding=self.encoding)


def byte_length(value: int) -> int:
    """Returns the number of bytes required to represent an integer."""
    return (value.bit_length() + 7) // 8


class PickleCodec(Codec[Any]):
    def __init__(
            self,
            protocol: int = pickle.HIGHEST_PROTOCOL,
            dump_kwargs: dict[str, Any] = None,
            load_kwargs: dict[str, Any] = None,
            len_header_bytes: int = 4,
            len_header_codec: Codec[int] = None,
    ):
        self.protocol = protocol
        self.dump_kwargs = dump_kwargs or {}
        self.load_kwargs = load_kwargs or {}
        self.len_header_codec = len_header_codec or FixedLengthIntCodec(len_header_bytes)

    async def encode(self, writer: Writer, obj: T) -> None:
        buffer = BufferWriter()
        pickle.dump(obj, buffer, protocol=self.protocol, **self.dump_kwargs)

        await self.len_header_codec.encode(writer, len(buffer))
        writer.write(buffer)

    async def decode(self, reader: Reader) -> T:
        data_len = await self.len_header_codec.decode(reader)
        data = await reader.readexactly(data_len)
        return pickle.loads(data, **self.load_kwargs)


pickle_codec = PickleCodec()
"""Pickle codec with default settings. Encoded data can be up to 4 GiB in size."""

if msgpack:
    MsgpackTypes = None | bool | int | float | str | bytes | bytearray | list | tuple | dict


    class MsgpackCodec(Codec[MsgpackTypes]):
        def __init__(
                self,
                pack_kwargs: dict[str, Any] = None,
                unpack_kwargs: dict[str, Any] = None,
                len_header_bytes: int = 4,
                len_header_codec: Codec[int] = None,
        ):
            self.pack_kwargs = pack_kwargs or {}
            self.unpack_kwargs = unpack_kwargs or {}
            self.len_header_codec = len_header_codec or FixedLengthIntCodec(len_header_bytes)

        async def encode(self, writer: Writer, obj: T) -> None:
            data = msgpack.packb(obj, **self.pack_kwargs)
            await self.len_header_codec.encode(writer, len(data))
            writer.write(data)

        async def decode(self, reader: Reader) -> T:
            data_len = await self.len_header_codec.decode(reader)
            data = await reader.readexactly(data_len)
            return msgpack.unpackb(data, **self.unpack_kwargs)


    msgpack_codec = MsgpackCodec()


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
