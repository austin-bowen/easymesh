from rosy.node.callbackmanager import CallbackManager
from rosy.types import Topic, TopicCallback

TopicListenerManager = CallbackManager[Topic, TopicCallback]
