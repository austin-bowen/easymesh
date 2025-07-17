from typing import NamedTuple

from easymesh.types import Data, Topic


class Message(NamedTuple):
    topic: Topic
    data: Data
