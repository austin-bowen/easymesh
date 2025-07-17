from typing import NamedTuple

from easymesh.types import Data, Topic


class TopicMessage(NamedTuple):
    topic: Topic
    data: Data
