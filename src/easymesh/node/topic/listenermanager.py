from typing import Awaitable, Callable

from easymesh.types import Data, Topic

TopicListenerCallback = Callable[[Topic, Data], Awaitable[None]]


class TopicListenerManager:
    def __init__(self):
        self._listeners: dict[Topic, TopicListenerCallback] = {}

    @property
    def topics(self) -> set[Topic]:
        return set(self._listeners.keys())

    def get_listener(self, topic: Topic) -> TopicListenerCallback | None:
        return self._listeners.get(topic)

    def set_listener(self, topic: Topic, callback: TopicListenerCallback) -> None:
        self._listeners[topic] = callback

    def remove_listener(self, topic: Topic) -> TopicListenerCallback | None:
        return self._listeners.pop(topic, None)

    def has_listener(self, topic: Topic) -> bool:
        return topic in self._listeners
