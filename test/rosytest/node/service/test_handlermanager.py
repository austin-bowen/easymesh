from unittest.mock import AsyncMock

from rosy.node.service.handlermanager import ServiceHandlerManager


class TestServiceHandlerManager:
    def setup_method(self):
        self.service = 'service'
        self.callback = AsyncMock()

        self.manager = ServiceHandlerManager()

    def test_services_is_empty_after_init(self):
        assert self.manager.services == set()

    def test_services_contains_services_after_set_handler(self):
        self.manager.set_handler(self.service, self.callback)
        assert self.manager.services == {self.service}

        self.manager.set_handler('another_service', self.callback)
        assert self.manager.services == {self.service, 'another_service'}

    def test_set_and_get_handler(self):
        self.manager.set_handler(self.service, self.callback)
        assert self.manager.get_handler(self.service) is self.callback

    def test_get_handler_for_unknown_service_returns_None(self):
        assert self.manager.get_handler('unknown_service') is None

    def test_remove_handler(self):
        self.manager.set_handler(self.service, self.callback)
        assert self.manager.get_handler(self.service) is not None

        removed_handler = self.manager.remove_handler(self.service)

        assert removed_handler is self.callback
        assert self.manager.get_handler(self.service) is None
