import pickle
from datetime import datetime
from pathlib import Path
from typing import Iterable

from easymesh.node.types import Args, KWArgs
from easymesh.types import Topic

Message = tuple[datetime, Topic, Args, KWArgs]


def get_bag_file_messages(bag_file_path: Path) -> Iterable[Message]:
    with open(bag_file_path, 'rb') as bag_file:
        while True:
            try:
                yield pickle.load(bag_file)
            except EOFError:
                break


def get_most_recent_bag_file_path() -> Path:
    matching_files = Path('.').glob(
        'record_????-??-??-??-??-??.bag'
    )

    try:
        return max(matching_files)
    except ValueError:
        raise FileNotFoundError(
            'No bag files found in the current directory. '
            'Use --input to specify a file.'
        )
