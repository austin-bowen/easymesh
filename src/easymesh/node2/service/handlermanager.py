from easymesh.types import Service, ServiceCallback


class ServiceHandlerManager:
    def __init__(self):
        self._handlers: dict[Service, ServiceCallback] = {}

    @property
    def services(self) -> set[Service]:
        return set(self._handlers.keys())

    def get_handler(self, service: Service) -> ServiceCallback | None:
        return self._handlers.get(service)

    def set_handler(self, service: Service, callback: ServiceCallback) -> None:
        self._handlers[service] = callback

    def remove_handler(self, service: Service) -> ServiceCallback | None:
        return self._handlers.pop(service, None)

    def has_handler(self, service: Service) -> bool:
        return service in self._handlers
