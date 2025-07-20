from typing import NamedTuple

from easymesh.node.types import Args, KWArgs
from easymesh.types import Topic


class TopicMessage(NamedTuple):
    topic: Topic
    args: Args
    kwargs: KWArgs
