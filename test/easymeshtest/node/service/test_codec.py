from unittest.mock import call

import pytest

from easymesh.node.service.codec import ServiceRequestCodec, ServiceResponseCodec
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymeshtest.test_codec import CodecTest


class TestServiceRequestCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.id_codec = self.add_tracked_codec_mock()
        self.service_codec = self.add_tracked_codec_mock()
        self.args_codec = self.add_tracked_codec_mock()
        self.kwargs_codec = self.add_tracked_codec_mock()

        self.request = ServiceRequest(
            id=1,
            service='service',
            args=['arg'],
            kwargs={'key': 'value'},
        )

        self.codec = ServiceRequestCodec(
            self.id_codec,
            self.service_codec,
            self.args_codec,
            self.kwargs_codec,
        )

    @pytest.mark.asyncio
    async def test_encode(self):
        writer, request = self.writer, self.request

        await self.assert_encode_returns_None(request)

        self.call_tracker.assert_calls(
            (self.id_codec.encode, call(writer, request.id)),
            (self.service_codec.encode, call(writer, request.service)),
            (self.args_codec.encode, call(writer, request.args)),
            (self.kwargs_codec.encode, call(writer, request.kwargs)),
        )

    @pytest.mark.asyncio
    async def test_decode(self):
        reader, request = self.reader, self.request

        self.call_tracker.track(self.id_codec.decode, return_value=request.id)
        self.call_tracker.track(self.service_codec.decode, return_value=request.service)
        self.call_tracker.track(self.args_codec.decode, return_value=request.args)
        self.call_tracker.track(self.kwargs_codec.decode, return_value=request.kwargs)

        await self.assert_decode_returns(request)

        self.call_tracker.assert_calls(
            (self.id_codec.decode, call(reader)),
            (self.service_codec.decode, call(reader)),
            (self.args_codec.decode, call(reader)),
            (self.kwargs_codec.decode, call(reader)),
        )


class TestServiceResponseCodec(CodecTest):
    def setup_method(self):
        super().setup_method()

        self.success_response = ServiceResponse(id=1, result='data')
        self.failure_response = ServiceResponse(id=1, error='error')

        self.id_codec = self.add_tracked_codec_mock()
        self.data_codec = self.add_tracked_codec_mock()
        self.error_codec = self.add_tracked_codec_mock()

        self.codec = ServiceResponseCodec(
            self.id_codec,
            self.data_codec,
            self.error_codec,
        )

    @pytest.mark.asyncio
    async def test_encode_success_response(self):
        writer, response = self.writer, self.success_response

        await self.assert_encode_returns_None(response)

        self.call_tracker.assert_calls(
            (self.id_codec.encode, call(writer, response.id)),
            (writer.write, call(b'\x00')),  # success status code
            (self.data_codec.encode, call(writer, response.result)),
        )

    @pytest.mark.asyncio
    async def test_decode_success_response(self):
        reader, response = self.reader, self.success_response

        self.call_tracker.track(self.id_codec.decode, return_value=response.id)
        self.call_tracker.track(self.reader.readexactly, return_value=b'\x00')
        self.call_tracker.track(self.data_codec.decode, return_value=response.result)
        self.call_tracker.track(self.error_codec.decode)

        await self.assert_decode_returns(response)

        self.call_tracker.assert_calls(
            (self.id_codec.decode, call(reader)),
            (self.reader.readexactly, call(1)),  # read status code
            (self.data_codec.decode, call(reader)),
        )

    @pytest.mark.asyncio
    async def test_encode_failure_response(self):
        writer, response = self.writer, self.failure_response

        await self.assert_encode_returns_None(response)

        self.call_tracker.assert_calls(
            (self.id_codec.encode, call(writer, response.id)),
            (self.writer.write, call(b'\xEE')),  # failure status code
            (self.error_codec.encode, call(writer, response.error)),
        )

    @pytest.mark.asyncio
    async def test_decode_failure_response(self):
        reader, response = self.reader, self.failure_response

        self.call_tracker.track(self.id_codec.decode, return_value=response.id)
        self.call_tracker.track(self.reader.readexactly, return_value=b'\xEE')
        self.call_tracker.track(self.error_codec.decode, return_value=response.error)
        self.call_tracker.track(self.data_codec.decode)

        await self.assert_decode_returns(response)

        self.call_tracker.assert_calls(
            (self.id_codec.decode, call(reader)),
            (self.reader.readexactly, call(1)),  # read status code
            (self.error_codec.decode, call(reader)),
        )

    @pytest.mark.asyncio
    async def test_decode_unknown_status_code_raises_ValueError(self):
        self.call_tracker.track(self.id_codec.decode, return_value=1)
        self.call_tracker.track(self.reader.readexactly, return_value=b'?')

        with pytest.raises(ValueError, match=f'Received unknown status code='):
            await self.codec.decode(self.reader)

        self.call_tracker.assert_calls(
            (self.id_codec.decode, call(self.reader)),
            (self.reader.readexactly, call(1)),  # read status code
        )
