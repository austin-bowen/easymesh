from collections.abc import Callable
from unittest.mock import Mock

from rosy.node.callbackmanager import CallbackManager


class TestCallbackManager:
    def setup_method(self):
        self.key = 'key'
        self.callback = Mock(Callable)

        self.manager = CallbackManager()

    def test_keys_is_empty_after_init(self):
        assert self.manager.keys == set()

    def test_keys_contains_keys_after_set_handler(self):
        self.manager.set_callback(self.key, self.callback)
        assert self.manager.keys == {self.key}

        self.manager.set_callback('another_key', self.callback)
        assert self.manager.keys == {self.key, 'another_key'}

    def test_set_and_get_handler(self):
        self.manager.set_callback(self.key, self.callback)
        assert self.manager.get_callback(self.key) is self.callback

    def test_get_handler_for_unknown_key_returns_None(self):
        assert self.manager.get_callback('unknown_key') is None

    def test_remove_handler(self):
        self.manager.set_callback(self.key, self.callback)
        assert self.manager.get_callback(self.key) is not None

        removed_handler = self.manager.remove_callback(self.key)

        assert removed_handler is self.callback
        assert self.manager.get_callback(self.key) is None
