import asyncio
import hmac
import secrets
from abc import ABC, abstractmethod
from asyncio import IncompleteReadError
from collections.abc import Callable

from easymesh.asyncio import Reader, Writer
from easymesh.utils import require

AuthKey = bytes


class Authenticator(ABC):
    @abstractmethod
    async def authenticate(self, reader: Reader, writer: Writer) -> None:
        """Raises AuthenticationError if authentication fails."""
        ...


class NoAuthenticator(Authenticator):
    """Performs no authentication."""

    async def authenticate(self, reader: Reader, writer: Writer) -> None:
        pass


class HMACAuthenticator(Authenticator):
    """
    Authenticates using symmetric HMAC challenge-response with a shared secret key.
    """

    def __init__(
            self,
            authkey: AuthKey,
            challenge_length: int = 32,
            digest='sha256',
            timeout: float | None = 10.,
            get_random_bytes: Callable[[int], bytes] = secrets.token_bytes,
    ):
        require(authkey, 'authkey must not be empty.')
        require(challenge_length > 0, 'challenge_length must be > 0.')

        self.authkey = authkey
        self.challenge_length = challenge_length
        self.digest = digest
        self.timeout = timeout
        self.get_random_bytes = get_random_bytes

    async def authenticate(self, reader: Reader, writer: Writer) -> None:
        challenge_to_client = self.get_random_bytes(self.challenge_length)

        writer.write(challenge_to_client)
        await writer.drain()

        challenge_from_client = await self._read_exactly(reader, self.challenge_length)

        hmac_to_client = self._hmac_digest(challenge_from_client)

        writer.write(hmac_to_client)
        await writer.drain()

        expected_hmac = self._hmac_digest(challenge_to_client)

        hmac_from_client = await self._read_exactly(reader, len(expected_hmac))

        if not hmac.compare_digest(hmac_from_client, expected_hmac):
            raise AuthenticationError('Received HMAC digest does not match expected digest.')

    async def _read_exactly(self, reader: Reader, n: int) -> bytes:
        try:
            return await asyncio.wait_for(
                reader.readexactly(n),
                timeout=self.timeout,
            )
        except TimeoutError:
            raise AuthenticationError(f'Timeout after {self.timeout}s waiting for authkey.')
        except IncompleteReadError as e:
            raise AuthenticationError(e)

    def _hmac_digest(self, challenge: bytes) -> bytes:
        return hmac.digest(self.authkey, challenge, self.digest)


class AuthenticationError(Exception):
    pass


def optional_authkey_authenticator(
        authkey: AuthKey | None,
) -> HMACAuthenticator | NoAuthenticator:
    return HMACAuthenticator(authkey) if authkey else NoAuthenticator()
