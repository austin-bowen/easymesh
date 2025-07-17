import asyncio
import logging

from easymesh.asyncio import LockableWriter, Reader, Writer
from easymesh.authentication import Authenticator
from easymesh.node.codec import NodeMessageCodec
from easymesh.node.service.requesthandler import ServiceRequestHandler
from easymesh.node.service.types import ServiceRequest
from easymesh.node.topic.messagehandler import TopicMessageHandler
from easymesh.node.topic.types import TopicMessage

logger = logging.getLogger(__name__)


class ClientHandler:
    def __init__(
            self,
            authenticator: Authenticator,
            node_message_codec: NodeMessageCodec,
            topic_message_handler: TopicMessageHandler,
            service_request_handler: ServiceRequestHandler,
    ):
        self.authenticator = authenticator
        self.node_message_codec = node_message_codec
        self.topic_message_handler = topic_message_handler
        self.service_request_handler = service_request_handler

    async def handle_client(self, reader: Reader, writer: Writer) -> None:
        peer_name = writer.get_extra_info('peername') or writer.get_extra_info('sockname')
        logger.debug(f'New connection from: {peer_name}')

        await self.authenticator.authenticate(reader, writer)
        writer = LockableWriter(writer)

        while True:
            try:
                obj = await self.node_message_codec.decode_topic_message_or_service_request(reader)
            except EOFError:
                logger.debug(f'Closed connection from: {peer_name}')
                return

            if isinstance(obj, TopicMessage):
                await self.topic_message_handler.handle_message(obj)
            elif isinstance(obj, ServiceRequest):
                asyncio.create_task(
                    self.service_request_handler.handle_request(obj, writer),
                    name=f'Handle service request {obj.id} from {peer_name}',
                )
            else:
                raise RuntimeError('Unreachable code')
