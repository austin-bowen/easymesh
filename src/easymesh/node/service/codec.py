from easymesh.asyncio import Reader, Writer
from easymesh.codec import Codec
from easymesh.node.service.types import RequestId, ServiceRequest, ServiceResponse
from easymesh.types import Data, Service


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
            writer.write(self.error_status_code)
            await self.error_codec.encode(writer, response.error)
        else:
            writer.write(self.success_status_code)
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
