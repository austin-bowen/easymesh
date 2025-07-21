import pickle
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, Literal, TypeVar

import orjson

from easymesh.asyncio import BufferWriter, Reader, Writer
from easymesh.utils import require

try:
    import msgpack
except ImportError:
    msgpack = None

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

ByteOrder = Literal['big', 'little']
DEFAULT_BYTE_ORDER: ByteOrder = 'little'

DEFAULT_MAX_BYTE_LENGTH: int = 8

DEFAULT_MAX_TOPIC_LENGTH: int = 256
DEFAULT_TOPIC_ENCODING: str = 'utf-8'


class Codec(Generic[T], ABC):
    @abstractmethod
    async def encode(self, writer: Writer, obj: T) -> None:
        ...  # pragma: no cover

    @abstractmethod
    async def decode(self, reader: Reader) -> T:
        ...  # pragma: no cover


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

        if int_byte_length > self.max_byte_length:
            raise OverflowError(
                f'Computed byte_length={int_byte_length} is greater than '
                f'max_byte_length={self.max_byte_length}'
            )

        header = bytes([int_byte_length])

        if int_byte_length > 0:
            data = value.to_bytes(int_byte_length, byteorder=self.byte_order, signed=self.signed)
        else:
            data = None

        writer.write(header)
        if data:
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


class SequenceCodec(Generic[T], Codec[Sequence[T]]):
    def __init__(
            self,
            len_header_codec: Codec[int],
            item_codec: Codec[T],
    ):
        self.len_header_codec = len_header_codec
        self.item_codec = item_codec

    async def encode(self, writer: Writer, sequence: Sequence[T]) -> None:
        await self.len_header_codec.encode(writer, len(sequence))

        for item in sequence:
            await self.item_codec.encode(writer, item)

    async def decode(self, reader: Reader) -> Sequence[T]:
        length = await self.len_header_codec.decode(reader)

        return [
            await self.item_codec.decode(reader)
            for _ in range(length)
        ]


class DictCodec(Generic[K, V], Codec[dict[K, V]]):
    def __init__(
            self,
            len_header_codec: Codec[int],
            key_codec: Codec[K],
            value_codec: Codec[V],
    ):
        self.len_header_codec = len_header_codec
        self.key_codec = key_codec
        self.value_codec = value_codec

    async def encode(self, writer: Writer, dict_: dict[K, V]) -> None:
        await self.len_header_codec.encode(writer, len(dict_))

        for key, value in dict_.items():
            await self.key_codec.encode(writer, key)
            await self.value_codec.encode(writer, value)

    async def decode(self, reader: Reader) -> dict[K, V]:
        length = await self.len_header_codec.decode(reader)

        result = {}
        for _ in range(length):
            key = await self.key_codec.decode(reader)
            value = await self.value_codec.decode(reader)
            result[key] = value

        return result


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

    async def encode(self, writer: Writer, obj: Any) -> None:
        buffer = BufferWriter()
        pickle.dump(obj, buffer, protocol=self.protocol, **self.dump_kwargs)

        await self.len_header_codec.encode(writer, len(buffer))
        writer.write(buffer)

    async def decode(self, reader: Reader) -> Any:
        data_len = await self.len_header_codec.decode(reader)
        data = await reader.readexactly(data_len)
        return pickle.loads(data, **self.load_kwargs)


pickle_codec = PickleCodec()
"""Pickle codec with default settings. Encoded data can be up to 4 GiB in size."""


class JsonCodec(Codec[Any]):
    def __init__(
            self,
            dumps_kwargs: dict[str, Any] = None,
            len_header_bytes: int = 4,
            len_header_codec: Codec[int] = None,
    ):
        self.dumps_kwargs = dumps_kwargs or {}
        self.len_header_codec = len_header_codec or FixedLengthIntCodec(len_header_bytes)

    async def encode(self, writer: Writer, obj: T) -> None:
        data = orjson.dumps(obj, **self.dumps_kwargs)
        await self.len_header_codec.encode(writer, len(data))
        writer.write(data)

    async def decode(self, reader: Reader) -> T:
        data_len = await self.len_header_codec.decode(reader)
        data = await reader.readexactly(data_len)
        return orjson.loads(data)


json_codec = JsonCodec()
"""JSON codec with default settings. Encoded data can be up to 4 GiB in size."""

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

        async def encode(self, writer: Writer, obj: MsgpackTypes) -> None:
            data = msgpack.packb(obj, **self.pack_kwargs)
            await self.len_header_codec.encode(writer, len(data))
            writer.write(data)

        async def decode(self, reader: Reader) -> MsgpackTypes:
            data_len = await self.len_header_codec.decode(reader)
            data = await reader.readexactly(data_len)
            return msgpack.unpackb(data, **self.unpack_kwargs)


    msgpack_codec = MsgpackCodec()
