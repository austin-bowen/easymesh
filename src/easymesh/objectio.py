from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

from easymesh.asyncio import Reader, Writer
from easymesh.codec import Codec, pickle_codec

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


class CodecObjectReader(ObjectReader[T]):
    def __init__(
            self,
            reader: Reader,
            codec: Codec[T] = pickle_codec,
    ):
        self.reader = reader
        self.codec = codec

    async def read(self) -> T:
        return await self.codec.decode(self.reader)


class CodecObjectWriter(ObjectWriter[T]):
    def __init__(
            self,
            writer: Writer,
            codec: Codec[T] = pickle_codec,
    ):
        self.writer = writer
        self.codec = codec

    async def write(self, obj: T) -> None:
        await self.codec.encode(self.writer, obj)
        await self.writer.drain()
