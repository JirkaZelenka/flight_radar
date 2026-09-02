"""Unit tests for OpenSky OAuth2 token refresh (no live API)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import requests

from flight_db import (
    TOKEN_REFRESH_MARGIN,
    OpenSkyClient,
    TokenManager,
)


def _token_response(access_token: str = "tok-1", expires_in: int = 1800) -> MagicMock:
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "access_token": access_token,
        "expires_in": expires_in,
    }
    return response


class TokenManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tm = TokenManager(client_id="id", client_secret="secret")

    @patch("flight_db.requests.post")
    def test_get_token_fetches_once_and_reuses(self, post: MagicMock) -> None:
        post.return_value = _token_response("tok-1")
        first = self.tm.get_token()
        second = self.tm.get_token()
        self.assertEqual(first, "tok-1")
        self.assertEqual(second, "tok-1")
        self.assertEqual(post.call_count, 1)
        _, kwargs = post.call_args
        self.assertEqual(
            kwargs["data"],
            {
                "grant_type": "client_credentials",
                "client_id": "id",
                "client_secret": "secret",
            },
        )

    @patch("flight_db.requests.post")
    def test_refreshes_when_expired(self, post: MagicMock) -> None:
        post.side_effect = [_token_response("tok-1"), _token_response("tok-2")]
        self.tm.get_token()
        self.tm.expires_at = datetime.now() - timedelta(seconds=1)
        self.assertEqual(self.tm.get_token(), "tok-2")
        self.assertEqual(post.call_count, 2)

    @patch("flight_db.requests.post")
    def test_refreshes_within_margin(self, post: MagicMock) -> None:
        post.return_value = _token_response("tok-1", expires_in=1800)
        self.tm.get_token()
        remaining = (self.tm.expires_at - datetime.now()).total_seconds()
        self.assertLessEqual(remaining, 1800 - TOKEN_REFRESH_MARGIN + 1)
        self.assertGreater(remaining, 1800 - TOKEN_REFRESH_MARGIN - 5)

    @patch("flight_db.requests.post")
    def test_short_ttl_does_not_expire_immediately(self, post: MagicMock) -> None:
        post.return_value = _token_response("tok-short", expires_in=10)
        token = self.tm.get_token()
        self.assertEqual(token, "tok-short")
        self.assertIsNotNone(self.tm.expires_at)
        self.assertGreater(self.tm.expires_at, datetime.now())

    @patch("flight_db.requests.post")
    def test_invalidate_forces_refresh(self, post: MagicMock) -> None:
        post.side_effect = [_token_response("tok-1"), _token_response("tok-2")]
        self.tm.get_token()
        self.tm.invalidate()
        self.assertEqual(self.tm.get_token(), "tok-2")
        self.assertEqual(post.call_count, 2)

    def test_missing_credentials_raise(self) -> None:
        with patch("flight_db._load_dotenv"):
            with patch.dict("os.environ", {"CLIENT_ID": "", "CLIENT_SECRET": ""}, clear=False):
                with self.assertRaises(RuntimeError):
                    TokenManager(client_id=None, client_secret=None)


class OpenSkyClientRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tm = TokenManager(client_id="id", client_secret="secret")
        self.tm.token = "stale"
        self.tm.expires_at = datetime.now() + timedelta(minutes=20)

    def _client(self) -> OpenSkyClient:
        with patch("flight_db.get_tokens", return_value=self.tm):
            return OpenSkyClient(
                params={"lamin": 1, "lamax": 2, "lomin": 3, "lomax": 4}
            )

    @patch("flight_db.requests.post")
    @patch("flight_db.requests.get")
    def test_retries_once_on_401(self, get: MagicMock, post: MagicMock) -> None:
        unauthorized = MagicMock()
        unauthorized.status_code = 401
        unauthorized.headers = {}
        unauthorized.raise_for_status.side_effect = requests.HTTPError("401")

        ok = MagicMock()
        ok.status_code = 200
        ok.headers = {"X-Rate-Limit-Remaining": "3999"}
        ok.raise_for_status.return_value = None
        ok.json.return_value = {"time": 1, "states": []}
        get.side_effect = [unauthorized, ok]
        post.return_value = _token_response("tok-fresh")

        client = self._client()
        data = client.fetch()
        self.assertEqual(data, {"time": 1, "states": []})
        self.assertEqual(get.call_count, 2)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.tm.token, "tok-fresh")
        auth_header = get.call_args_list[1].kwargs["headers"]["Authorization"]
        self.assertEqual(auth_header, "Bearer tok-fresh")

    @patch("flight_db.requests.post")
    @patch("flight_db.requests.get")
    def test_401_twice_raises(self, get: MagicMock, post: MagicMock) -> None:
        unauthorized = MagicMock()
        unauthorized.status_code = 401
        unauthorized.headers = {}
        unauthorized.raise_for_status.side_effect = requests.HTTPError("401")
        get.return_value = unauthorized
        post.return_value = _token_response("tok-fresh")

        client = self._client()
        with self.assertRaises(requests.HTTPError):
            client.fetch()
        self.assertEqual(get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
