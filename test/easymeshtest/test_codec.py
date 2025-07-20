from unittest.mock import AsyncMock, call

import pytest

from easymesh.asyncio import BufferReader, Reader, Writer
from easymesh.codec import (
    Codec,
    FixedLengthIntCodec,
    LengthPrefixedStringCodec,
    MsgpackCodec,
    PickleCodec,
    msgpack_codec,
    pickle_codec,
)
from easymeshtest.calltracker import CallTracker


# TODO Remove this
class CodecTest:
    codec: Codec

    async def assert_encode_decode(self, value) -> None:
        writer = AsyncMock(spec=Writer)
        await self.codec.encode(writer, value)

        written_data = b''.join(
            args.args[0] for args in writer.write.call_args_list
        )

        # Simulate a stream with repeated data
        reader = BufferReader(written_data * 2)

        decoded_objects = [
            await self.codec.decode(reader),
            await self.codec.decode(reader),
        ]

        assert decoded_objects == [value, value]

    async def assert_encode_writes(self, value, expected: list[bytes]):
        writer = AsyncMock(spec=Writer)
        await self.codec.encode(writer, value)
        expected_calls = [call(data) for data in expected]
        assert writer.write.call_args_list == expected_calls

    async def assert_decode_returns(self, data: bytes, expected) -> None:
        reader = BufferReader(data)
        actual = await self.codec.decode(reader)
        assert actual == expected


# TODO Rename this
class CodecTest2:
    codec: Codec
    reader: AsyncMock
    writer: AsyncMock
    call_tracker: CallTracker

    def setup_method(self):
        self.reader = AsyncMock(Reader)
        self.writer = AsyncMock(Writer)

        self.call_tracker = CallTracker()
        self.call_tracker.track(self.reader.readexactly)
        self.call_tracker.track(self.writer.write)
        self.call_tracker.track(self.writer.drain)

    def add_tracked_codec_mock(self) -> AsyncMock:
        codec_mock = AsyncMock(Codec)
        self.call_tracker.track(codec_mock.encode)
        self.call_tracker.track(codec_mock.decode)
        return codec_mock

    async def assert_encode_returns_None(self, obj) -> None:
        result = await self.codec.encode(self.writer, obj)
        assert result is None

    async def assert_decode_returns(self, expected) -> None:
        result = await self.codec.decode(self.reader)
        assert result == expected


class TestPickleCodec(CodecTest):
    codec: PickleCodec

    def setup_method(self):
        self.codec = pickle_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        data = dict(key='value', number=42, float_list=[1.2, 3.4])
        await self.assert_encode_decode(data)


class TestMsgpackCodec(CodecTest):
    codec: MsgpackCodec

    def setup_method(self):
        self.codec = msgpack_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        data = dict(key='value', number=42, float_list=[1.2, 3.4])
        await self.assert_encode_decode(data)


class TestFixedLengthIntCodec(CodecTest):
    codec: FixedLengthIntCodec

    def setup_method(self):
        self.codec = FixedLengthIntCodec(
            length=2,
        )

    def test_constructor_defaults(self):
        assert self.codec.length == 2
        assert self.codec.byte_order == 'little'
        assert self.codec.signed is False

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        await self.assert_encode_decode(42)

    @pytest.mark.parametrize('value, expected', [
        (0, b'\x00\x00'),
        (1, b'\x01\x00'),
        (255, b'\xFF\x00'),
        (256, b'\x00\x01'),
        (65535, b'\xFF\xFF'),
    ])
    @pytest.mark.asyncio
    async def test_encode(self, value: int, expected: bytes):
        await self.assert_encode_writes(value, [expected])

    @pytest.mark.parametrize('value', [-1, 65536])
    @pytest.mark.asyncio
    async def test_encode_with_invalid_value_raises_OverflowError(self, value: int):
        writer = AsyncMock(spec=Writer)

        with pytest.raises(OverflowError):
            await self.codec.encode(writer, value)

        writer.write.assert_not_called()

    @pytest.mark.parametrize('data, expected', [
        (b'\x00\x00', 0),
        (b'\x01\x00', 1),
        (b'\xFF\x00', 255),
        (b'\x00\x01', 256),
        (b'\xFF\xFF', 65535),
    ])
    @pytest.mark.asyncio
    async def test_decode(self, data: bytes, expected: int):
        await self.assert_decode_returns(data, expected)


class TestVariableLengthIntCodec(CodecTest):
    # TODO
    ...


class TestLengthPrefixedStringCodec(CodecTest):
    codec: LengthPrefixedStringCodec

    def setup_method(self):
        self.len_prefix_codec = FixedLengthIntCodec(length=1)

        self.codec = LengthPrefixedStringCodec(self.len_prefix_codec)

    def test_constructor_defaults(self):
        assert self.codec.len_prefix_codec is self.len_prefix_codec
        assert self.codec.encoding == 'utf-8'

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        await self.assert_encode_decode('hello world')

    @pytest.mark.parametrize('data, expected', [
        ('', [b'\x00']),
        ('hello world', [b'\x0B', b'hello world']),
    ])
    @pytest.mark.asyncio
    async def test_encode(self, data: str, expected: list[bytes]):
        await self.assert_encode_writes(data, expected)

    @pytest.mark.parametrize('data, expected', [
        (b'\x00', ''),
        (b'\x0Bhello world', 'hello world'),
        (b'\x05hello world', 'hello'),
    ])
    @pytest.mark.asyncio
    async def test_decode(self, data: bytes, expected: str):
        await self.assert_decode_returns(data, expected)
