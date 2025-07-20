import pickle
from unittest.mock import ANY, AsyncMock, call, patch

import pytest

from easymesh.asyncio import BufferReader, Reader, Writer
from easymesh.codec import (
    Codec,
    FixedLengthIntCodec,
    LengthPrefixedStringCodec,
    MsgpackCodec,
    PickleCodec,
    VariableLengthIntCodec, msgpack_codec,
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


class TestPickleCodec(CodecTest2):
    codec: PickleCodec

    def setup_method(self):
        super().setup_method()

        self.len_header_codec = self.add_tracked_codec_mock()

        self.codec = PickleCodec(
            len_header_codec=self.len_header_codec,
        )

    @patch('easymesh.codec.pickle.dump')
    @pytest.mark.asyncio
    async def test_encode(self, dump):
        def dump_side_effect(obj, file, protocol):
            file.write(b'data')

        self.call_tracker.track(dump, side_effect=dump_side_effect)

        await self.assert_encode_returns_None('data')

        self.call_tracker.assert_calls(
            (dump, call('data', ANY, protocol=pickle.HIGHEST_PROTOCOL)),
            (self.len_header_codec.encode, call(self.writer, 4)),
            (self.writer.write, call(b'data')),
        )

    @patch('easymesh.codec.pickle.loads')
    @pytest.mark.asyncio
    async def test_decode(self, loads):
        self.call_tracker.track(self.len_header_codec.decode, return_value=4)
        self.call_tracker.track(self.reader.readexactly, return_value=b'data')
        self.call_tracker.track(loads, return_value='data')

        await self.assert_decode_returns('data')

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
            (self.reader.readexactly, call(4)),
            (loads, call(b'data')),
        )

class TestMsgpackCodec(CodecTest):
    codec: MsgpackCodec

    def setup_method(self):
        self.codec = msgpack_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        data = dict(key='value', number=42, float_list=[1.2, 3.4])
        await self.assert_encode_decode(data)


class TestFixedLengthIntCodec(CodecTest2):
    codec: FixedLengthIntCodec

    def setup_method(self):
        super().setup_method()

        self.codec = FixedLengthIntCodec(
            length=2,
        )

    def test_constructor_defaults(self):
        assert self.codec.length == 2
        assert self.codec.byte_order == 'little'
        assert self.codec.signed is False

    @pytest.mark.parametrize('value, expected', [
        (0, b'\x00\x00'),
        (1, b'\x01\x00'),
        (255, b'\xFF\x00'),
        (256, b'\x00\x01'),
        (65535, b'\xFF\xFF'),
    ])
    @pytest.mark.asyncio
    async def test_encode(self, value: int, expected: bytes):
        await self.assert_encode_returns_None(value)

        self.call_tracker.assert_calls(
            (self.writer.write, call(expected)),
        )

    @pytest.mark.parametrize('value', [-1, 65536])
    @pytest.mark.asyncio
    async def test_encode_with_invalid_value_raises_OverflowError(self, value: int):
        with pytest.raises(OverflowError):
            await self.assert_encode_returns_None(value)

        self.call_tracker.assert_calls()

    @pytest.mark.parametrize('data, expected', [
        (b'\x00\x00', 0),
        (b'\x01\x00', 1),
        (b'\xFF\x00', 255),
        (b'\x00\x01', 256),
        (b'\xFF\xFF', 65535),
    ])
    @pytest.mark.asyncio
    async def test_decode(self, data: bytes, expected: int):
        self.call_tracker.track(self.reader.readexactly, return_value=data)

        await self.assert_decode_returns(expected)

        self.call_tracker.assert_calls(
            (self.reader.readexactly, call(2)),
        )


class TestVariableLengthIntCodec(CodecTest2):
    def setup_method(self):
        super().setup_method()

        self.codec = VariableLengthIntCodec()

    @pytest.mark.asyncio
    async def test_encode(self):
        pytest.fail()

    @pytest.mark.asyncio
    async def test_decode(self):
        pytest.fail()


class TestLengthPrefixedStringCodec(CodecTest2):
    codec: LengthPrefixedStringCodec

    def setup_method(self):
        super().setup_method()

        self.len_prefix_codec = self.add_tracked_codec_mock()

        self.codec = LengthPrefixedStringCodec(self.len_prefix_codec)

    def test_constructor_defaults(self):
        assert self.codec.len_prefix_codec is self.len_prefix_codec
        assert self.codec.encoding == 'utf-8'

    @pytest.mark.asyncio
    async def test_encode(self):
        await self.assert_encode_returns_None('hello world')

        self.call_tracker.assert_calls(
            (self.len_prefix_codec.encode, call(self.writer, 11)),
            (self.writer.write, call(b'hello world')),
        )

    @pytest.mark.asyncio
    async def test_encode_empty_string(self):
        await self.assert_encode_returns_None('')

        self.call_tracker.assert_calls(
            (self.len_prefix_codec.encode, call(self.writer, 0)),
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        self.call_tracker.track(self.len_prefix_codec.decode, return_value=11)
        self.call_tracker.track(self.reader.readexactly, return_value=b'hello world')

        await self.assert_decode_returns('hello world')

        self.call_tracker.assert_calls(
            (self.len_prefix_codec.decode, call(self.reader)),
            (self.reader.readexactly, call(11)),
        )
