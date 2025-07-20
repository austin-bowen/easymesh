from unittest.mock import Mock, call


class CallTracker:
    calls: list[tuple[Mock, call]]

    def __init__(self):
        self.calls = []

    def track(self, mock: Mock, return_value=None) -> None:
        def side_effect(*args, **kwargs):
            self.calls.append((mock, call(*args, **kwargs)))
            return return_value

        mock.side_effect = side_effect

    def assert_calls(self, *expected: tuple[Mock, call]) -> None:
        expected = list(expected)
        assert self.calls == expected
