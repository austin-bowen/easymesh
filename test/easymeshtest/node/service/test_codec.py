import pytest

from easymesh.codec import FixedLengthIntCodec, LengthPrefixedStringCodec
from easymesh.node.service.codec import ServiceRequestCodec, ServiceResponseCodec
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymeshtest.test_codec import CodecTest


class TestServiceRequestCodec(CodecTest):
    codec: ServiceRequestCodec

    def setup_method(self):
        self.id_codec = FixedLengthIntCodec(length=2)

        len_prefix_codec = FixedLengthIntCodec(length=1)
        self.service_codec = LengthPrefixedStringCodec(len_prefix_codec)
        self.data_codec = LengthPrefixedStringCodec(len_prefix_codec)

        self.codec = ServiceRequestCodec(
            self.id_codec,
            self.service_codec,
            self.data_codec,
        )

    def test_constructor_defaults(self):
        assert self.codec.id_codec is self.id_codec
        assert self.codec.service_codec is self.service_codec
        assert self.codec.data_codec is self.data_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        await self.assert_encode_decode(
            ServiceRequest(id=1, service='service', data='data')
        )

    @pytest.mark.asyncio
    async def test_encode(self):
        await self.assert_encode_writes(
            ServiceRequest(id=1, service='service', data='data'),
            [
                b'\x01\x00',
                b'\x07',
                b'service',
                b'\x04',
                b'data',
            ],
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        await self.assert_decode_returns(
            b'\x01\x00\x07service\x04data',
            ServiceRequest(id=1, service='service', data='data'),
        )


class TestServiceResponseCodec(CodecTest):
    codec: ServiceResponseCodec

    def setup_method(self):
        self.id_codec = FixedLengthIntCodec(length=2)

        len_prefix_codec = FixedLengthIntCodec(length=1)
        self.data_codec = LengthPrefixedStringCodec(len_prefix_codec)
        self.error_codec = LengthPrefixedStringCodec(len_prefix_codec)

        self.codec = ServiceResponseCodec(
            self.id_codec,
            self.data_codec,
            self.error_codec,
        )

    def test_constructor_defaults(self):
        assert self.codec.id_codec is self.id_codec
        assert self.codec.data_codec is self.data_codec
        assert self.codec.error_codec is self.error_codec

    @pytest.mark.asyncio
    async def test_encode_decode(self):
        await self.assert_encode_decode(
            ServiceResponse(id=1, data='data')
        )

    @pytest.mark.asyncio
    async def test_encode_success_response(self):
        await self.assert_encode_writes(
            ServiceResponse(id=1, data='data'),
            [
                b'\x01\x00',  # id
                b'\x00',  # success status code
                b'\x04',  # data
                b'data',
            ],
        )

    @pytest.mark.asyncio
    async def test_decode_success_response(self):
        await self.assert_decode_returns(
            b'\x01\x00\x00\x04data',
            ServiceResponse(id=1, data='data'),
        )

    @pytest.mark.asyncio
    async def test_encode_failure_response(self):
        await self.assert_encode_writes(
            ServiceResponse(id=1, error='error'),
            [
                b'\x01\x00',  # id
                b'\xEE',  # failure status code
                b'\x05',  # error
                b'error',
            ],
        )

    @pytest.mark.asyncio
    async def test_decode_failure_response(self):
        await self.assert_decode_returns(
            b'\x01\x00\xEE\x05error',
            ServiceResponse(id=1, error='error'),
        )
