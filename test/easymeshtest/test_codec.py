import pickle
from unittest.mock import ANY, AsyncMock, call, patch

import pytest

from easymesh.asyncio import Reader, Writer
from easymesh.codec import (
    Codec,
    DictCodec, FixedLengthIntCodec,
    JsonCodec, LengthPrefixedStringCodec,
    MsgpackCodec,
    PickleCodec,
    SequenceCodec, VariableLengthIntCodec, )
from easymeshtest.calltracker import CallTracker


class CodecTest:
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

    def setup_reader(self, *results: bytes) -> None:
        results = list(results)

        self.call_tracker.track(
            self.reader.readexactly,
            side_effect=lambda n: results.pop(0),
        )

    async def assert_encode_returns_None(self, obj) -> None:
        result = await self.codec.encode(self.writer, obj)
        assert result is None

    async def assert_decode_returns(self, expected) -> None:
        result = await self.codec.decode(self.reader)
        assert result == expected


class TestFixedLengthIntCodec(CodecTest):
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


class TestVariableLengthIntCodec(CodecTest):
    codec: VariableLengthIntCodec

    def setup_method(self):
        super().setup_method()

        self.codec = VariableLengthIntCodec(
            max_byte_length=2,
        )

    def test_constructor_defaults(self):
        codec = VariableLengthIntCodec()
        assert codec.max_byte_length == 8
        assert codec.byte_order == 'little'
        assert codec.signed is False

    @pytest.mark.parametrize('value, len_byte, encoded_bytes', [
        (0, b'\x00', b''),
        (1, b'\x01', b'\x01'),
        (255, b'\x01', b'\xFF'),
        (256, b'\x02', b'\x00\x01'),
        (65535, b'\x02', b'\xFF\xFF'),
    ])
    @pytest.mark.asyncio
    async def test_encode(self, value: int, len_byte: bytes, encoded_bytes: bytes):
        await self.assert_encode_returns_None(value)

        expected_calls = [
            (self.writer.write, call(len_byte)),
        ]
        if encoded_bytes:
            expected_calls.append((self.writer.write, call(encoded_bytes)))

        self.call_tracker.assert_calls(*expected_calls)

    @pytest.mark.parametrize('value', [-1, 65536])
    @pytest.mark.asyncio
    async def test_encode_with_invalid_value_raises_OverflowError(self, value: int):
        with pytest.raises(OverflowError):
            await self.assert_encode_returns_None(value)

        self.call_tracker.assert_calls()

    @pytest.mark.parametrize('len_byte, encoded_bytes, expected', [
        (b'\x00', b'', 0),
        (b'\x01', b'\x01', 1),
        (b'\x01', b'\xFF', 255),
        (b'\x02', b'\x00\x01', 256),
        (b'\x02', b'\xFF\xFF', 65535),
    ])
    @pytest.mark.asyncio
    async def test_decode(self, len_byte: bytes, encoded_bytes: bytes, expected: int):
        self.setup_reader(len_byte, encoded_bytes)

        await self.assert_decode_returns(expected)


class TestLengthPrefixedStringCodec(CodecTest):
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

    @pytest.mark.asyncio
    async def test_decode_empty_string(self):
        self.call_tracker.track(self.len_prefix_codec.decode, return_value=0)

        await self.assert_decode_returns('')

        self.call_tracker.assert_calls(
            (self.len_prefix_codec.decode, call(self.reader)),
        )


class TestSequenceCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.len_header_codec = self.add_tracked_codec_mock()
        self.item_codec = self.add_tracked_codec_mock()

        self.codec = SequenceCodec(
            self.len_header_codec,
            self.item_codec,
        )

    @pytest.mark.asyncio
    async def test_encode(self):
        await self.assert_encode_returns_None([1, 2, 3])

        self.call_tracker.assert_calls(
            (self.len_header_codec.encode, call(self.writer, 3)),
            (self.item_codec.encode, call(self.writer, 1)),
            (self.item_codec.encode, call(self.writer, 2)),
            (self.item_codec.encode, call(self.writer, 3)),
        )

    @pytest.mark.asyncio
    async def test_encode_empty_sequence(self):
        await self.assert_encode_returns_None([])

        self.call_tracker.assert_calls(
            (self.len_header_codec.encode, call(self.writer, 0)),
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        results = [1, 2, 3]
        self.call_tracker.track(self.len_header_codec.decode, return_value=3)
        self.call_tracker.track(
            self.item_codec.decode,
            side_effect=lambda reader: results.pop(0),
        )

        await self.assert_decode_returns([1, 2, 3])

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
            (self.item_codec.decode, call(self.reader)),
            (self.item_codec.decode, call(self.reader)),
            (self.item_codec.decode, call(self.reader)),
        )

    @pytest.mark.asyncio
    async def test_decode_empty_sequence(self):
        self.call_tracker.track(self.len_header_codec.decode, return_value=0)

        await self.assert_decode_returns([])

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
        )


class TestDictCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.len_header_codec = self.add_tracked_codec_mock()
        self.key_codec = self.add_tracked_codec_mock()
        self.value_codec = self.add_tracked_codec_mock()

        self.codec = DictCodec(
            self.len_header_codec,
            self.key_codec,
            self.value_codec,
        )

    @pytest.mark.asyncio
    async def test_encode(self):
        await self.assert_encode_returns_None({'key1': 'value1', 'key2': 'value2'})

        self.call_tracker.assert_calls(
            (self.len_header_codec.encode, call(self.writer, 2)),
            (self.key_codec.encode, call(self.writer, 'key1')),
            (self.value_codec.encode, call(self.writer, 'value1')),
            (self.key_codec.encode, call(self.writer, 'key2')),
            (self.value_codec.encode, call(self.writer, 'value2')),
        )

    @pytest.mark.asyncio
    async def test_encode_empty_dict(self):
        await self.assert_encode_returns_None({})

        self.call_tracker.assert_calls(
            (self.len_header_codec.encode, call(self.writer, 0)),
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        key_results = ['key1', 'key2']
        value_results = ['value1', 'value2']
        self.call_tracker.track(self.len_header_codec.decode, return_value=2)
        self.call_tracker.track(
            self.key_codec.decode,
            side_effect=lambda reader: key_results.pop(0),
        )
        self.call_tracker.track(
            self.value_codec.decode,
            side_effect=lambda reader: value_results.pop(0),
        )

        await self.assert_decode_returns({'key1': 'value1', 'key2': 'value2'})

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
            (self.key_codec.decode, call(self.reader)),
            (self.value_codec.decode, call(self.reader)),
            (self.key_codec.decode, call(self.reader)),
            (self.value_codec.decode, call(self.reader)),
        )

    @pytest.mark.asyncio
    async def test_decode_empty_dict(self):
        self.call_tracker.track(self.len_header_codec.decode, return_value=0)

        await self.assert_decode_returns({})

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
        )


class TestPickleCodec(CodecTest):
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


class TestJsonCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.len_header_codec = self.add_tracked_codec_mock()

        self.codec = JsonCodec(
            len_header_codec=self.len_header_codec,
        )

    @patch('easymesh.codec.orjson.dumps')
    @pytest.mark.asyncio
    async def test_encode(self, dumps):
        self.call_tracker.track(dumps, return_value=b'data')

        await self.assert_encode_returns_None('data')

        self.call_tracker.assert_calls(
            (dumps, call('data')),
            (self.len_header_codec.encode, call(self.writer, 4)),
            (self.writer.write, call(b'data')),
        )

    @patch('easymesh.codec.orjson.loads')
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
    def setup_method(self):
        super().setup_method()

        self.len_header_codec = self.add_tracked_codec_mock()

        self.codec = MsgpackCodec(
            len_header_codec=self.len_header_codec,
        )

    @patch('easymesh.codec.msgpack.packb')
    @pytest.mark.asyncio
    async def test_encode(self, packb):
        self.call_tracker.track(packb, return_value=b'data')

        await self.assert_encode_returns_None('data')

        self.call_tracker.assert_calls(
            (packb, call('data')),
            (self.len_header_codec.encode, call(self.writer, 4)),
            (self.writer.write, call(b'data')),
        )

    @patch('easymesh.codec.msgpack.unpackb')
    @pytest.mark.asyncio
    async def test_decode(self, unpackb):
        self.call_tracker.track(self.len_header_codec.decode, return_value=4)
        self.call_tracker.track(self.reader.readexactly, return_value=b'data')
        self.call_tracker.track(unpackb, return_value='data')

        await self.assert_decode_returns('data')

        self.call_tracker.assert_calls(
            (self.len_header_codec.decode, call(self.reader)),
            (self.reader.readexactly, call(4)),
            (unpackb, call(b'data')),
        )
