import asyncio
from argparse import Namespace

from easymesh import build_node_from_args
from easymesh.argparse import get_node_arg_parser
from easymesh.bag.info import add_info_args, display_info
from easymesh.bag.play import add_play_args, play
from easymesh.bag.record import add_record_args, record


def main() -> None:
    try:
        asyncio.run(_main(parse_args()))
    except KeyboardInterrupt:
        pass


async def _main(args: Namespace):
    if args.command == 'record':
        node = await build_node_from_args(args=args)
        await record(node, args)
    elif args.command == 'play':
        node = await build_node_from_args(args=args)
        await play(node, args)
    elif args.command == 'info':
        await display_info(args)
    else:
        raise ValueError(f'Unknown command: {args.command}')


def parse_args() -> Namespace:
    parser = get_node_arg_parser(
        default_node_name='meshbag',
        description='Tool for recording and playing back messages. '
                    'Based on the `rosbag` ROS command line tool.'
    )

    subparsers = parser.add_subparsers(dest='command', required=True)

    add_record_args(subparsers)
    add_play_args(subparsers)
    add_info_args(subparsers)

    return parser.parse_args()


if __name__ == '__main__':
    main()
