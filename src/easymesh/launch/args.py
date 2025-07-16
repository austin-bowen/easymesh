from dataclasses import dataclass


@dataclass
class ProcessArgs:
    args: str | list[str]

    def append(self, arg: str) -> None:
        if isinstance(self.args, str):
            arg = quote_arg(arg)
            self.args = f'{self.args} {arg}'
        else:
            self.args.append(arg)

    def extend(self, args: list[str]) -> None:
        for arg in args:
            self.append(arg)


def quote_arg(arg: str) -> str:
    """Add double-quotes around an arg if necessary."""

    if not arg:
        return '""'
    elif ' ' in arg and not (arg[0] == arg[-1] == '"'):
        return f'"{arg}"'
    else:
        return arg
