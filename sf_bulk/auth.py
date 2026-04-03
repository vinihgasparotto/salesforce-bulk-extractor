from __future__ import annotations

import json
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import requests

from .config import Settings

TOKEN_FILE = Path(".sf_tokens")


class SalesforceSession:
    def __init__(self, instance_url: str, access_token: str, api_version: str) -> None:
        self.instance_url = instance_url
        self.api_version = api_version
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {access_token}"})
        self._base = f"{instance_url}/services/data/v{api_version}"

    def get(self, path: str, **kwargs) -> requests.Response:
        return self._session.get(f"{self._base}{path}", **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self._session.post(f"{self._base}{path}", **kwargs)

    def delete(self, path: str, **kwargs) -> requests.Response:
        return self._session.delete(f"{self._base}{path}", **kwargs)


def _raise_sf_error(response: requests.Response) -> None:
    try:
        body = response.json()
    except Exception:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")

    # OAuth error shape: {"error": "...", "error_description": "..."}
    if isinstance(body, dict) and "error_description" in body:
        raise RuntimeError(body["error_description"])

    # REST API error shape: [{"errorCode": "...", "message": "..."}]
    if isinstance(body, list) and body and "errorCode" in body[0]:
        raise RuntimeError(f"{body[0]['errorCode']}: {body[0]['message']}")

    # Bulk API error shape: {"errorCode": "...", "message": "..."}
    if isinstance(body, dict) and "errorCode" in body:
        raise RuntimeError(f"{body['errorCode']}: {body['message']}")

    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")


def _save_tokens(refresh_token: str, instance_url: str) -> None:
    TOKEN_FILE.write_text(
        json.dumps({"refresh_token": refresh_token, "instance_url": instance_url}),
        encoding="utf-8",
    )


def _load_tokens() -> Optional[dict]:
    if TOKEN_FILE.exists():
        try:
            return json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _refresh_token_login(settings: Settings, cached: dict) -> SalesforceSession:
    resp = requests.post(
        f"{settings.login_url}/services/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
            "refresh_token": cached["refresh_token"],
        },
        timeout=30,
    )
    if not resp.ok:
        _raise_sf_error(resp)
    data = resp.json()
    new_refresh = data.get("refresh_token", cached["refresh_token"])
    instance_url = data.get("instance_url", cached["instance_url"])
    _save_tokens(new_refresh, instance_url)
    return SalesforceSession(instance_url, data["access_token"], settings.api_version)


class _CallbackHandler(BaseHTTPRequestHandler):
    auth_code: Optional[str] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/callback":
            params = urllib.parse.parse_qs(parsed.query)
            _CallbackHandler.auth_code = params.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Authentication successful!</h2>"
                b"<p>You can close this tab and return to the terminal.</p>"
                b"</body></html>"
            )
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # suppress server logs
        pass


def _browser_oauth_flow(settings: Settings) -> SalesforceSession:
    redirect_uri = f"http://localhost:{settings.callback_port}/callback"
    auth_url = (
        f"{settings.login_url}/services/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(settings.client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )

    server = HTTPServer(("localhost", settings.callback_port), _CallbackHandler)
    _CallbackHandler.auth_code = None

    def serve():
        while _CallbackHandler.auth_code is None:
            server.handle_request()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    print(f"\nOpening browser for Salesforce login...")
    print(f"If the browser does not open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    thread.join(timeout=120)
    server.server_close()

    code = _CallbackHandler.auth_code
    if not code:
        raise RuntimeError("OAuth flow timed out — no authorization code received.")

    resp = requests.post(
        f"{settings.login_url}/services/oauth2/token",
        data={
            "grant_type": "authorization_code",
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
            "redirect_uri": redirect_uri,
            "code": code,
        },
        timeout=30,
    )
    if not resp.ok:
        _raise_sf_error(resp)

    data = resp.json()
    access_token = data["access_token"]
    refresh_token = data.get("refresh_token", "")
    instance_url = data["instance_url"]

    if refresh_token:
        _save_tokens(refresh_token, instance_url)

    return SalesforceSession(instance_url, access_token, settings.api_version)


def get_session(settings: Settings) -> SalesforceSession:
    cached = _load_tokens()
    if cached and cached.get("refresh_token"):
        try:
            return _refresh_token_login(settings, cached)
        except RuntimeError:
            # Token expired or revoked — fall through to browser flow
            TOKEN_FILE.unlink(missing_ok=True)

    return _browser_oauth_flow(settings)
