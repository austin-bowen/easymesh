import logging

from rosy.asyncio import log_error
from rosy.node.topic.listenermanager import TopicListenerManager
from rosy.node.topic.types import TopicMessage

logger = logging.getLogger(__name__)


class TopicMessageHandler:
    def __init__(
            self,
            listener_manager: TopicListenerManager,
    ):
        self.listener_manager = listener_manager

    async def handle_message(self, message: TopicMessage) -> None:
        callback = self.listener_manager.get_callback(message.topic)

        if callback:
            await log_error(callback(message.topic, *message.args, **message.kwargs))
        else:
            logger.warning(
                f'Received message for topic={message.topic!r} '
                f'but no listener is registered.'
            )
