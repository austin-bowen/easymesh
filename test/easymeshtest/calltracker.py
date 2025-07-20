from unittest.mock import Mock, call


class CallTracker:
    calls: list[tuple[Mock, call]]

    def __init__(self):
        self.calls = []

    def track(
            self,
            mock: Mock,
            return_value=None,
            side_effect=None,
    ) -> None:
        def side_effect_(*args, **kwargs):
            self.calls.append((mock, call(*args, **kwargs)))
            return side_effect(*args, **kwargs) if side_effect else return_value

        mock.side_effect = side_effect_

    def assert_calls(self, *expected: tuple[Mock, call]) -> None:
        expected = list(expected)
        assert self.calls == expected, f'''
Expected calls do not match actual calls.

Expected calls:
{format_calls_(expected)}

Actual calls:
{format_calls_(self.calls)}
        '''.strip()


def format_calls_(calls) -> str:
    return '\n'.join(f'{i}: {c}' for i, c in enumerate(calls)) or '[None]'
