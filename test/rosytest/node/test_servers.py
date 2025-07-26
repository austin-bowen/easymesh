from asyncio import Server
from collections.abc import Callable
from unittest.mock import ANY, AsyncMock, call, create_autospec, patch

import pytest

from rosy.asyncio import Reader, Writer
from rosy.node.servers import (
    PortScanTcpServerProvider,
    ServerProvider,
    ServersManager,
    TmpUnixServerProvider,
    UnsupportedProviderError,
    _close_on_return,
)
from rosy.specs import IpConnectionSpec, UnixConnectionSpec


class TestPortScanTcpServerProvider:
    def setup_method(self):
        self.provider = PortScanTcpServerProvider(
            server_host='server-host',
            client_host='client-host',
        )

    def test_start_port(self):
        assert self.provider.start_port == 49152

    def test_max_ports(self):
        assert self.provider.max_ports == 1024

    def test_end_port(self):
        assert self.provider.end_port == 49152 + 1024 - 1

    @patch('rosy.node.servers.asyncio.start_server')
    @pytest.mark.asyncio
    async def test_start_server_uses_first_available_port(self, start_server_mock):
        expected_server = create_autospec(Server)

        start_server_mock.side_effect = [
            OSError('Address already in use'),
            OSError('Address already in use'),
            expected_server,
        ]

        client_connected_cb = create_autospec(Callable)

        server, conn_spec = await self.provider.start_server(client_connected_cb)

        assert server is expected_server
        assert conn_spec == IpConnectionSpec('client-host', 49154)

        def expected_call(port: int) -> call:
            return call(
                client_connected_cb,
                host='server-host',
                port=port,
            )

        assert start_server_mock.call_args_list == [
            expected_call(49152),
            expected_call(49153),
            expected_call(49154),
        ]

    @patch('rosy.node.servers.asyncio.start_server')
    @pytest.mark.asyncio
    async def test_start_server_raises_OSError_if_no_ports_available(self, start_server_mock):
        start_server_mock.side_effect = OSError('Address already in use')

        client_connected_cb = create_autospec(Callable)

        with pytest.raises(OSError):
            await self.provider.start_server(client_connected_cb)


class TestTmpUnixServerProvider:
    def setup_method(self):
        self.provider = TmpUnixServerProvider()

    @patch('rosy.node.servers.asyncio.start_unix_server')
    @pytest.mark.asyncio
    async def test_start_server(self, start_unix_server_mock):
        expected_server = create_autospec(Server)
        start_unix_server_mock.return_value = expected_server

        client_connected_cb = create_autospec(Callable)

        server, conn_spec = await self.provider.start_server(client_connected_cb)

        start_unix_server_mock.assert_called_once_with(
            client_connected_cb,
            path=ANY,
        )
        sock_path = start_unix_server_mock.call_args[1]['path']

        assert server is expected_server
        assert conn_spec == UnixConnectionSpec(sock_path)

    @patch('rosy.node.servers.asyncio.start_unix_server')
    @pytest.mark.asyncio
    async def test_start_server_raises_UnsupportedProviderError(self, start_unix_server_mock):
        start_unix_server_mock.side_effect = NotImplementedError()

        client_connected_cb = create_autospec(Callable)

        with pytest.raises(UnsupportedProviderError):
            await self.provider.start_server(client_connected_cb)


class TestServersManager:
    def setup_method(self):
        servers = [
            create_autospec(Server),
            create_autospec(Server),
        ]

        self.conn_specs = [
            create_autospec(IpConnectionSpec),
            create_autospec(UnixConnectionSpec),
        ]

        self.server_providers = [
            create_autospec(ServerProvider),
            create_autospec(ServerProvider),
        ]

        for provider, server, conn_spec in zip(
                self.server_providers, servers, self.conn_specs
        ):
            provider.start_server.return_value = (server, conn_spec)

        self.client_connected_cb = create_autospec(Callable)

        self.manager = ServersManager(
            self.server_providers,
            self.client_connected_cb,
        )

    def test_connection_specs_initially_empty(self):
        assert self.manager.connection_specs == []

    @patch('rosy.node.servers._close_on_return')
    @pytest.mark.asyncio
    async def test_start_servers(self, close_on_return_mock):
        wrapper_cb = create_autospec(Callable)
        close_on_return_mock.return_value = wrapper_cb

        await self.manager.start_servers()

        assert self.manager.connection_specs == self.conn_specs

        close_on_return_mock.assert_called_once_with(self.client_connected_cb)

        for provider in self.server_providers:
            provider.start_server.assert_awaited_once_with(wrapper_cb)

    @pytest.mark.asyncio
    async def test_start_servers_raises_RuntimeError_when_already_started(self):
        await self.manager.start_servers()

        with pytest.raises(RuntimeError, match='Servers have already been started.'):
            await self.manager.start_servers()

    @pytest.mark.asyncio
    async def test_start_servers_raises_RuntimeError_when_no_servers_started(self, logger_mock):
        for provider in self.server_providers:
            provider.start_server.side_effect = UnsupportedProviderError(provider, 'Unsupported')

        with pytest.raises(RuntimeError, match='Unable to start any server with the given server providers.'):
            await self.manager.start_servers()

        assert logger_mock.exception.call_count == len(self.server_providers)


class TestCloseOnReturn:
    def setup_method(self):
        self.reader = create_autospec(Reader)
        self.writer = create_autospec(Writer)
        self.callback = AsyncMock()
        self.callback_wrapper = _close_on_return(self.callback)

    @pytest.mark.asyncio
    async def test_when_success(self):
        assert await self.callback_wrapper(self.reader, self.writer) is None

        self.callback.assert_awaited_once_with(self.reader, self.writer)

        self.writer.close.assert_called_once()
        self.writer.wait_closed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_when_error(self, logger_mock):
        error = Exception()
        self.callback.side_effect = error

        assert await self.callback_wrapper(self.reader, self.writer) is None

        self.callback.assert_awaited_once_with(self.reader, self.writer)

        logger_mock.exception.assert_called_once_with(
            'Error in client connected callback',
            exc_info=error,
        )

        self.writer.close.assert_called_once()
        self.writer.wait_closed.assert_awaited_once()


@pytest.fixture
def logger_mock():
    with patch('rosy.node.servers.logger') as mock:
        yield mock
