import secrets
from unittest.mock import AsyncMock, Mock, call

import pytest

from easymesh.asyncio import Reader, Writer
from easymesh.authentication import (
    AuthenticationError,
    HMACAuthenticator,
    NoAuthenticator,
    optional_authkey_authenticator,
)


class TestNoAuthenticator:
    @pytest.mark.asyncio
    async def test_authenticate_should_do_nothing(self):
        auth = NoAuthenticator()

        reader = AsyncMock(Reader)
        writer = AsyncMock(Writer)

        assert await auth.authenticate(reader, writer) is None

        assert not reader.mock_calls
        assert not writer.mock_calls


class TestHMACAuthenticator:
    def setup_method(self):
        self.reader = AsyncMock(Reader)
        self.writer = AsyncMock(Writer)

        # To be sent to the client in response to their challenge
        self.hmac_to_client = (
            b'\xbcB\x14\xa6\xad\x8e\xd9\xb3\xf8\xbd\\\x84\x1b\xc2'
            b'(\x81-c\xdd7*\xcd\x11\xfc\xd2\xd7v(\xec\x1a\x1a`'
        )

        # To be read from the client in response to our challenge
        self.hmac_from_client = (
            b'V\x1e\x8c\x9a\xe8`\xbf\xed\xcd\x8eCt\xd1\xca0\x8b'
            b'\xba|\xe7\xdb\x00\xf8\xe2g:\xfd\x87\x81*q\xd4\xa9'
        )

        self.bad_hmac_from_client = b'bad' + self.hmac_from_client[3:]

        self.authkey = b'secret key'

        self.challenge_to_client = b'random sent challenge'
        self.challenge_from_client = b'random recv challenge'
        assert len(self.challenge_to_client) == len(self.challenge_from_client)
        self.challenge_length = len(self.challenge_to_client)

        self.get_random_bytes = Mock(return_value=self.challenge_to_client)

        self.auth = HMACAuthenticator(
            self.authkey,
            challenge_length=self.challenge_length,
            get_random_bytes=self.get_random_bytes,
        )

    def test_default_constructor_values(self):
        auth = HMACAuthenticator(self.authkey)

        assert auth.authkey == self.authkey
        assert auth.challenge_length == 32
        assert auth.digest == 'sha256'
        assert auth.timeout == 10.
        assert auth.get_random_bytes is secrets.token_bytes

    @pytest.mark.asyncio
    async def test_authenticate_should_return_on_success(self):
        self.reader.readexactly.side_effect = [
            self.challenge_from_client,
            self.hmac_from_client,
        ]

        assert await self.auth.authenticate(self.reader, self.writer) is None

        assert self.reader.readexactly.await_count == 2
        self.reader.readexactly.assert_has_calls([
            call(self.challenge_length),
            call(len(self.hmac_from_client)),
        ])

        assert self.writer.write.call_count == 2
        self.writer.write.assert_has_calls([
            call(self.challenge_to_client),
            call(self.hmac_to_client),
        ])
        assert self.writer.drain.await_count == 2

    @pytest.mark.asyncio
    async def test_authenticate_should_raise_AuthenticationError_when_client_fails_challenge(self):
        self.reader.readexactly.side_effect = [
            self.challenge_from_client,
            self.bad_hmac_from_client,
        ]

        with pytest.raises(AuthenticationError):
            await self.auth.authenticate(self.reader, self.writer)

        assert self.reader.readexactly.await_count == 2
        self.reader.readexactly.assert_has_calls([
            call(self.challenge_length),
            call(len(self.bad_hmac_from_client)),
        ])

        assert self.writer.write.call_count == 2
        self.writer.write.assert_has_calls([
            call(self.challenge_to_client),
            call(self.hmac_to_client),
        ])
        assert self.writer.drain.await_count == 2


class TestOptionalAuthkeyAuthenticator:
    def test_returns_NoAuthenticator_when_authkey_is_None(self):
        auth = optional_authkey_authenticator(authkey=None)
        assert isinstance(auth, NoAuthenticator)

    def test_returns_HMACAuthenticator_when_authkey_is_not_None(self):
        authkey = b'authkey'
        auth = optional_authkey_authenticator(authkey)
        assert isinstance(auth, HMACAuthenticator)
        assert auth.authkey == authkey
