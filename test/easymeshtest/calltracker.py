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

    def assert_calls(self, *all_expected: tuple[Mock, call]) -> None:
        for actual, expected in zip(self.calls, all_expected):
            assert actual == expected

        assert len(self.calls) == len(all_expected)
