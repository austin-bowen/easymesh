import asyncio
import logging
from argparse import Namespace

from easymesh.argparse import get_node_arg_parser
from easymesh.asyncio import forever
from easymesh.node2.node import build_node_from_args

logging.basicConfig(level=logging.INFO)


async def main(args: Namespace):
    logging.basicConfig(level=args.log)

    node = await build_node_from_args(args=args)

    async def handle_message(topic, data):
        print(f'Received topic={topic!r} data={data!r}')

    for topic in args.topics:
        await node.listen(topic, handle_message)

    print(f'Listening to topics: {args.topics}')
    await forever()


def parse_args() -> Namespace:
    parser = get_node_arg_parser(default_node_name='receiver')

    parser.add_argument(
        '--topics', '-t',
        nargs='+',
        default=['some-topic'],
        help='The topics to listen to. Default: %(default)s',
    )

    parser.add_argument(
        '--log',
        default='INFO',
        help='Log level; DEBUG, INFO, ERROR, etc. Default: %(default)s'
    )

    return parser.parse_args()


if __name__ == '__main__':
    asyncio.run(main(parse_args()))
