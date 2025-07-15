from abc import ABC, abstractmethod
from asyncio import Lock
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

from easymesh.asyncio import BufferReader, BufferWriter, Reader, Writer
from easymesh.codec import Codec, pickle_codec
from easymesh.types import Data, Message
from easymesh.utils import require

DEFAULT_TOPIC_ENCODING: str = 'utf-8'

ByteOrder = Literal['big', 'little']
DEFAULT_BYTE_ORDER: ByteOrder = 'little'

DEFAULT_MAX_HEADER_LEN: int = 8

T = TypeVar('T')


class ObjectReader(Generic[T], ABC):
    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        return await self.read()

    @abstractmethod
    async def read(self) -> T:
        ...


class ObjectWriter(Generic[T], ABC):
    @abstractmethod
    async def write(self, obj: T) -> None:
        ...


@dataclass
class ObjectIO(Generic[T]):
    reader: ObjectReader[T]
    writer: ObjectWriter[T]


class StreamObjectReader(ObjectReader[T]):
    def __init__(
            self,
            reader: Reader,
            byte_order: ByteOrder,
            max_header_len: int,
    ):
        self.reader = reader
        self.byte_order = byte_order
        self.max_header_len = max_header_len

        self._read_lock = Lock()

    async def read(self) -> T:
        async with self._read_lock:
            return await self._read()

    @abstractmethod
    async def _read(self) -> T:
        ...

    async def _read_data_with_len_header(self) -> bytes:
        header_len = (await self.reader.readexactly(1))[0]

        if not header_len:
            return b''

        require(
            header_len <= self.max_header_len,
            f'Received header_len={header_len} > max_header_len={self.max_header_len}',
        )

        header = await self.reader.readexactly(header_len)
        data_len = self._bytes_to_int(header)

        return await self.reader.readexactly(data_len)

    def _bytes_to_int(self, data: bytes) -> int:
        return int.from_bytes(data, byteorder=self.byte_order, signed=False)


class StreamObjectWriter(ObjectWriter[T], ABC):
    def __init__(
            self,
            writer: Writer,
            byte_order: ByteOrder,
            max_header_len: int,
    ):
        self.writer = writer
        self.byte_order = byte_order
        self.max_header_len = max_header_len

        self._write_lock = Lock()

    async def write(self, obj: T, drain: bool = True) -> None:
        async with self._write_lock:
            await self._write(obj)

            if drain:
                await self.writer.drain()

    @abstractmethod
    async def _write(self, obj: T) -> None:
        ...

    async def _write_data_with_len_header(self, data: bytes | bytearray) -> None:
        data_len = len(data)

        header_len = (data_len.bit_length() + 7) // 8

        require(
            header_len <= self.max_header_len,
            f'Computed header_len={header_len} > max_header_len={self.max_header_len}',
        )

        await self.writer.write(self._int_to_bytes(header_len, length=1))

        if not data_len:
            return

        header = self._int_to_bytes(data_len, length=header_len)

        await self.writer.write(header)
        await self.writer.write(data)

    def _int_to_bytes(self, value: int, length: int) -> bytes:
        return value.to_bytes(length, byteorder=self.byte_order, signed=False)


class CodecObjectReader(StreamObjectReader[T]):
    def __init__(
            self,
            reader: Reader,
            codec: Codec[T] = pickle_codec,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            max_header_len: int = DEFAULT_MAX_HEADER_LEN,
    ):
        super().__init__(reader, byte_order, max_header_len)
        self.codec = codec

    async def _read(self) -> T:
        data = await self._read_data_with_len_header()
        data = BufferReader(data)
        return await self.codec.decode(data)


class CodecObjectWriter(StreamObjectWriter[T]):
    def __init__(
            self,
            writer: Writer,
            codec: Codec[T] = pickle_codec,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            max_header_len: int = DEFAULT_MAX_HEADER_LEN,
    ):
        super().__init__(writer, byte_order, max_header_len)
        self.codec = codec

    async def _write(self, obj: T) -> None:
        data = BufferWriter()
        await self.codec.encode(data, obj)
        await self._write_data_with_len_header(data)


class MessageReader(StreamObjectReader[Message]):
    def __init__(
            self,
            reader: Reader,
            codec: Codec[Data],
            topic_encoding: str = DEFAULT_TOPIC_ENCODING,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            max_header_len: int = DEFAULT_MAX_HEADER_LEN,
    ):
        super().__init__(reader, byte_order, max_header_len)
        self.codec = codec
        self.topic_encoding = topic_encoding

    async def _read(self) -> Message:
        topic = await self._read_data_with_len_header()
        topic = topic.decode(self.topic_encoding)

        data = await self._read_data_with_len_header()
        data = BufferReader(data)
        data = await self.codec.decode(data) if data else None

        return Message(topic, data)


class MessageWriter(StreamObjectWriter[Message]):
    def __init__(
            self,
            writer: Writer,
            codec: Codec[Data],
            topic_encoding: str = DEFAULT_TOPIC_ENCODING,
            byte_order: ByteOrder = DEFAULT_BYTE_ORDER,
            max_header_len: int = DEFAULT_MAX_HEADER_LEN,
    ):
        super().__init__(writer, byte_order, max_header_len)
        self.codec = codec
        self.topic_encoding = topic_encoding

    async def _write(self, message: Message) -> None:
        topic = message.topic.encode(self.topic_encoding)
        await self._write_data_with_len_header(topic)

        data = BufferWriter()
        if message.data is not None:
            await self.codec.encode(data, message.data)
        await self._write_data_with_len_header(data)
