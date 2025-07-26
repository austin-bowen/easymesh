import asyncio
import logging
import tempfile
from abc import ABC, abstractmethod
from asyncio import Server, StreamReader, StreamWriter
from collections.abc import Awaitable, Callable, Iterable

from rosy.asyncio import Reader, Writer
from rosy.specs import ConnectionSpec, IpConnectionSpec, UnixConnectionSpec
from rosy.types import Host, Port, ServerHost

ClientConnectedCallback = Callable[[Reader, Writer], Awaitable[None]]

logger = logging.getLogger(__name__)


class ServerProvider(ABC):
    @abstractmethod
    async def start_server(
            self,
            client_connected_cb,
    ) -> tuple[Server, ConnectionSpec]:
        """Raises ``UnsupportedProviderError`` if not supported on the system."""
        ...  # pragma: no cover


class PortScanTcpServerProvider(ServerProvider):
    """Starts a TCP server on the first available port."""

    def __init__(
            self,
            server_host: ServerHost,
            client_host: Host,
            start_port: Port = 49152,
            max_ports: Port = 1024,
            **kwargs,
    ):
        """
        Args:
            server_host:
                The interface(s) that the server will listen on.
            client_host:
                The host that clients will use to connect to the server.
            start_port:
                The port to start scanning for an open port.
            max_ports:
                Maximum number of ports to scan.
            kwargs:
                Additional keyword arguments will be passed to the
                ``asyncio.start_server`` call.
        """

        self.server_host = server_host
        self.client_host = client_host
        self.start_port = start_port
        self.max_ports = max_ports
        self.kwargs = kwargs

    @property
    def end_port(self) -> int:
        return self.start_port + self.max_ports - 1

    async def start_server(
            self,
            client_connected_cb,
    ) -> tuple[Server, ConnectionSpec]:
        last_error = None

        for port in range(self.start_port, self.end_port + 1):
            try:
                server = await asyncio.start_server(
                    client_connected_cb,
                    host=self.server_host,
                    port=port,
                    **self.kwargs,
                )
            except OSError as e:
                last_error = e
            else:
                conn_spec = IpConnectionSpec(self.client_host, port)
                return server, conn_spec

        raise OSError(
            f'Unable to start a server on any port in range '
            f'{self.start_port}-{self.end_port}. '
            f'Last error: {last_error!r}'
        )


class TmpUnixServerProvider(ServerProvider):
    """Starts a Unix server on a tmp file."""

    def __init__(
            self,
            prefix: str | None = 'mesh-node-server.',
            suffix: str | None = '.sock',
            dir=None,
            **kwargs,
    ):
        """
        Args:
            prefix:
                The prefix for the temporary Unix socket file.
            suffix:
                The suffix for the temporary Unix socket file.
            dir:
                The directory to create the temporary Unix socket file in.
                If not set, the system's default temporary directory will be used.
            kwargs:
                Additional keyword arguments will be passed to the
                ``asyncio.start_unix_server`` call.
        """

        self.prefix = prefix
        self.suffix = suffix
        self.dir = dir
        self.kwargs = kwargs

    async def start_server(
            self,
            client_connected_cb,
    ) -> tuple[Server, ConnectionSpec]:
        with tempfile.NamedTemporaryFile(
                prefix=self.prefix,
                suffix=self.suffix,
                dir=self.dir,
        ) as file:
            path = file.name

        try:
            server = await asyncio.start_unix_server(client_connected_cb, path=path, **self.kwargs)
        except NotImplementedError as e:
            raise UnsupportedProviderError(self, repr(e))

        conn_spec = UnixConnectionSpec(path=path)

        return server, conn_spec


class ServersManager:
    def __init__(
            self,
            server_providers: Iterable[ServerProvider],
            client_connected_cb: ClientConnectedCallback,
    ):
        self.server_providers = server_providers
        self.client_connected_cb = client_connected_cb

        self._connection_specs: list[ConnectionSpec] = []

    @property
    def connection_specs(self) -> list[ConnectionSpec]:
        return list(self._connection_specs)

    async def start_servers(self) -> None:
        if self._connection_specs:
            raise RuntimeError('Servers have already been started.')

        client_connected_cb = _close_on_return(self.client_connected_cb)

        for provider in self.server_providers:
            try:
                server, connection_spec = await provider.start_server(client_connected_cb)
            except UnsupportedProviderError as e:
                logger.exception(f'Failed to start server using provider={provider}', exc_info=e)
            else:
                logger.debug(f'Started node server with connection_spec={connection_spec}')
                self._connection_specs.append(connection_spec)

        if not self._connection_specs:
            raise RuntimeError('Unable to start any server with the given server providers.')


class UnsupportedProviderError(Exception):
    """Raised when trying to start a server from an unsupported server provider."""

    def __init__(self, provider: ServerProvider, message: str):
        super().__init__(
            f'Unsupported server provider {provider.__class__}: {message}'
        )


def _close_on_return(
        callback: ClientConnectedCallback,
) -> Callable[[StreamReader, StreamWriter], Awaitable[None]]:
    async def wrapped_callback(reader: StreamReader, writer: StreamWriter) -> None:
        try:
            await callback(reader, writer)
        except Exception as e:
            logger.exception('Error in client connected callback', exc_info=e)
        finally:
            writer.close()
            await writer.wait_closed()

    return wrapped_callback
