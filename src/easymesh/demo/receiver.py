import asyncio
from argparse import Namespace

from easymesh import build_mesh_node_from_args
from easymesh.argparse import get_node_arg_parser
from easymesh.asyncio import forever


async def main(args: Namespace):
    node = await build_mesh_node_from_args(args=args)

    async def handle_message(topic, data):
        node.log(f'Received topic={topic!r} data={data!r}')

    for topic in args.topics:
        await node.listen(topic, handle_message)

    node.log(f'Listening to topics: {args.topics}')
    await forever()


def parse_args() -> Namespace:
    parser = get_node_arg_parser(default_node_name='receiver')

    parser.add_argument(
        '--topics', '-t',
        nargs='+',
        default=['some-topic'],
        help='The topics to listen to. Default: %(default)s',
    )

    return parser.parse_args()


if __name__ == '__main__':
    asyncio.run(main(parse_args()))
