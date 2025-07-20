from argparse import Namespace
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from easymesh.bag.file import get_bag_file_messages, get_most_recent_bag_file_path


async def display_info(args: Namespace) -> None:
    bag_file_path = args.input or get_most_recent_bag_file_path()

    info = get_info(bag_file_path)

    print(f'''
path:     {info.path}
duration: {info.duration}
start:    {info.start}
end:      {info.end}
size:     {info.size} {info.size_unit}
messages: {info.messages}
'''.strip())

    if info.topics:
        print('topics:')
        for topic, count in info.topics.items():
            pct = round(100 * count / info.messages)
            print(f'- {topic!r}:\t{count} ({pct}%)')
    else:
        print('topics:   None')


def add_info_args(subparsers) -> None:
    parser = subparsers.add_parser('info', help='Get info about recorded messages in a file')

    parser.add_argument(
        '--input', '-i',
        type=Path,
        help='Input file path. Default: The most recent '
             'record_*.bag file in the current directory.',
    )


def get_info(bag_file_path: Path) -> 'BagInfo':
    size = bag_file_path.stat().st_size
    size, size_unit = get_human_readable_size(size)

    start_time = end_time = None
    messages = 0
    topics = Counter()

    for instant, topic, *_ in get_bag_file_messages(bag_file_path):
        if not start_time:
            start_time = instant

        end_time = instant
        messages += 1
        topics[topic] += 1

    return BagInfo(
        bag_file_path,
        start_time,
        end_time,
        size,
        size_unit,
        messages,
        topics,
    )


def get_human_readable_size(size: int) -> tuple[int, str]:
    for unit in ['B', 'KB', 'MB']:
        if size < 1024:
            return round(size), unit
        size /= 1024

    return round(size), 'GB'



@dataclass
class BagInfo:
    path: Path
    start: datetime
    end: datetime
    size: int
    size_unit: str
    messages: int
    topics: dict[str, int]

    @property
    def duration(self) -> timedelta:
        start, end = self.start, self.end
        return (end - start) if start and end else None
