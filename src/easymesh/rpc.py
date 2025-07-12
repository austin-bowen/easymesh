import asyncio
from abc import ABC, abstractmethod
from asyncio import Event
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from easymesh.objectio import ObjectIO

RequestId = int
Data = Any
RequestHandler = Callable[[Data], Awaitable[Data]]
MessageHandler = Callable[[Data], Awaitable[None]]


@dataclass
class Request:
    id: RequestId
    data: Data


@dataclass
class Response:
    id: RequestId
    data: Data


@dataclass
class Message:
    data: Data


@dataclass
class PendingResponse:
    data: Data = None
    received: Event = field(default_factory=Event)


class RPC(ABC):
    request_handler: RequestHandler = None
    message_handler: MessageHandler = None

    async def start(self) -> None:
        asyncio.create_task(self.run_forever())

    async def run_forever(self) -> None:
        pass

    @abstractmethod
    async def send_request(self, data: Data) -> Data:
        ...

    @abstractmethod
    async def send_message(self, data: Data) -> None:
        ...

    async def handle_request(self, data: Data) -> Data:
        if self.request_handler is not None:
            return await self.request_handler(data)
        else:
            raise NotImplementedError()

    async def handle_message(self, data: Data) -> None:
        if self.message_handler is not None:
            await self.message_handler(data)
        else:
            raise NotImplementedError()


class ObjectIORPC(RPC):
    def __init__(self, obj_io: ObjectIO[Any]):
        self.obj_io = obj_io

        self._request_id_counter: RequestId = -1
        self._pending_responses: dict[RequestId, PendingResponse] = {}

    async def run_forever(self) -> None:
        async for obj in self.obj_io.reader:
            if isinstance(obj, Request):
                await self._handle_received_request(obj)
            elif isinstance(obj, Response):
                await self._handle_received_response(obj)
            elif isinstance(obj, Message):
                await self.handle_message(obj.data)
            else:
                raise Exception(f'Received invalid object of type={type(obj)}')

    async def _handle_received_request(self, request: Request) -> None:
        response_data = await self.handle_request(request.data)
        response = Response(id=request.id, data=response_data)
        await self.obj_io.writer.write(response)

    async def _handle_received_response(self, response: Response) -> None:
        pending_response = self._pending_responses.pop(response.id)
        pending_response.data = response.data
        pending_response.received.set()

    async def send_request(self, data: Data) -> Data:
        request = Request(id=self._get_request_id(), data=data)

        pending_response = PendingResponse()
        self._pending_responses[request.id] = pending_response

        await self.obj_io.writer.write(request)
        await pending_response.received.wait()

        return pending_response.data

    def _get_request_id(self) -> int:
        self._request_id_counter += 1
        return self._request_id_counter

    async def send_message(self, data: Data) -> None:
        await self.obj_io.writer.write(Message(data))
