import asyncio
import logging

from easymesh.asyncio import LockableWriter, Reader, Writer, close_ignoring_errors
from easymesh.authentication import Authenticator
from easymesh.codec import NodeMessageCodec
from easymesh.node.service.handlermanager import ServiceHandlerManager
from easymesh.node.service.types import ServiceRequest, ServiceResponse
from easymesh.node.topic.messagehandler import TopicMessageHandler
from easymesh.node.topic.types import Message

logger = logging.getLogger(__name__)


class ClientHandler:
    def __init__(
            self,
            authenticator: Authenticator,
            node_message_codec: NodeMessageCodec,
            topic_message_handler: TopicMessageHandler,
            service_handler_manager: ServiceHandlerManager,
    ):
        self.authenticator = authenticator
        self.node_message_codec = node_message_codec
        self.topic_message_handler = topic_message_handler
        self.service_handler_manager = service_handler_manager

    async def handle_client(self, reader: Reader, writer: Writer) -> None:
        peer_name = writer.get_extra_info('peername') or writer.get_extra_info('sockname')
        logger.debug(f'New connection from: {peer_name}')

        await self.authenticator.authenticate(reader, writer)
        writer = LockableWriter(writer)

        while True:
            try:
                message = await self.node_message_codec.decode_topic_message_or_service_request(reader)
            except EOFError:
                logger.debug(f'Closed connection from: {peer_name}')
                async with writer:
                    await close_ignoring_errors(writer)
                return
            except Exception as e:
                logger.exception(f'Error reading from peer={peer_name}', exc_info=e)
                async with writer:
                    await close_ignoring_errors(writer)
                return

            if isinstance(message, Message):
                await self.topic_message_handler.handle_message(message)
            elif isinstance(message, ServiceRequest):
                asyncio.create_task(
                    self._handle_service_request(message, writer),
                    name=f'Handle service request {message.id} from {peer_name}',
                )
            else:
                raise RuntimeError('Unreachable code')

    # TODO move into its own thing
    async def _handle_service_request(
            self,
            request: ServiceRequest,
            writer: LockableWriter,
    ) -> None:
        handler = self.service_handler_manager.get_handler(request.service)

        data, error = None, None

        if handler is None:
            logger.warning(
                f'Received service request for service={request.service!r} '
                f'but no handler is registered for it.'
            )

            error = f'service={request.service!r} is not provided by this node'
        else:
            try:
                data = await handler(request.service, request.data)
            except Exception as e:
                logger.exception(
                    f'Error handling service request={request}',
                    exc_info=e,
                )
                error = repr(e)

        response = ServiceResponse(request.id, data, error)

        async with writer:
            await self.node_message_codec.encode_service_response(
                writer,
                response,
            )
