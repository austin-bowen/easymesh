import logging

from rosy.asyncio import LockableWriter
from rosy.node.codec import NodeMessageCodec
from rosy.node.service.handlermanager import ServiceHandlerManager
from rosy.node.service.types import ServiceRequest, ServiceResponse

logger = logging.getLogger(__name__)


class ServiceRequestHandler:
    def __init__(
            self,
            service_handler_manager: ServiceHandlerManager,
            node_message_codec: NodeMessageCodec,
    ):
        self.service_handler_manager = service_handler_manager
        self.node_message_codec = node_message_codec

    async def handle_request(
            self,
            request: ServiceRequest,
            writer: LockableWriter,
    ) -> None:
        handler = self.service_handler_manager.get_callback(request.service)

        result, error = None, None

        if handler is None:
            logger.warning(
                f'Received service request for service={request.service!r} '
                f'but no handler is registered for it.'
            )

            error = f'service={request.service!r} is not provided by this node'
        else:
            try:
                result = await handler(request.service, *request.args, **request.kwargs)
            except Exception as e:
                logger.exception(
                    f'Error handling service request={request}',
                    exc_info=e,
                )
                error = repr(e)

        response = ServiceResponse(request.id, result, error)

        async with writer:
            await self.node_message_codec.encode_service_response(
                writer,
                response,
            )
