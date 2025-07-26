from asyncio import Server
from collections.abc import Callable
from unittest.mock import call, create_autospec, patch

import pytest

from rosy.node.servers import (
    PortScanTcpServerProvider,
    ServerProvider,
    ServersManager,
    TmpUnixServerProvider,
)
from rosy.specs import IpConnectionSpec


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

    @pytest.mark.asyncio
    async def test_start_server(self):
        ...


class TestServersManager:
    def setup_method(self):
        self.server_providers = [
            create_autospec(ServerProvider),
            create_autospec(ServerProvider),
        ]

        self.client_connected_cb = create_autospec(Callable)

        self.manager = ServersManager(
            self.server_providers,
            self.client_connected_cb,
        )

    @pytest.mark.asyncio
    async def test_start_servers(self):
        ...
