from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import requests

from .config import Settings

TOKEN_FILE = Path(".sf_tokens")

# Same client ID Salesforce Data Loader uses for Bulk API — public client,
# no Connected App or client secret required. Uses PKCE instead.
_DATALOADER_BULK_CLIENT_ID = "DataLoaderBulkUI/"

_SOAP_ENVELOPE = """\
<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:urn="urn:partner.soap.sforce.com">
  <soapenv:Body>
    <urn:login>
      <urn:username>{username}</urn:username>
      <urn:password>{password}</urn:password>
    </urn:login>
  </soapenv:Body>
</soapenv:Envelope>"""


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

    if isinstance(body, dict) and "error_description" in body:
        raise RuntimeError(body["error_description"])
    if isinstance(body, list) and body and "errorCode" in body[0]:
        raise RuntimeError(f"{body[0]['errorCode']}: {body[0]['message']}")
    if isinstance(body, dict) and "errorCode" in body:
        raise RuntimeError(f"{body['errorCode']}: {body['message']}")

    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")


def _save_tokens(refresh_token: str, instance_url: str, client_id: str) -> None:
    TOKEN_FILE.write_text(
        json.dumps({
            "refresh_token": refresh_token,
            "instance_url": instance_url,
            "client_id": client_id,
        }),
        encoding="utf-8",
    )


def _load_tokens() -> Optional[dict]:
    if TOKEN_FILE.exists():
        try:
            return json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _pkce_pair() -> tuple[str, str]:
    code_verifier = secrets.token_urlsafe(96)[:128]
    digest = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return code_verifier, code_challenge


# ── Password / SOAP login (no Connected App required) ────────────────────────

def _soap_login(settings: Settings) -> SalesforceSession:
    password = (settings.password or "") + (settings.security_token or "")
    soap_url = f"{settings.login_url}/services/Soap/u/{settings.api_version}"
    body = _SOAP_ENVELOPE.format(username=settings.username, password=password)
    resp = requests.post(
        soap_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": "login"},
        timeout=30,
    )

    if resp.status_code != 200:
        fault = re.search(r"<faultstring>(.*?)</faultstring>", resp.text, re.DOTALL)
        msg = fault.group(1).strip() if fault else resp.text[:300]
        raise RuntimeError(f"SOAP login failed: {msg}")

    session_id = re.search(r"<sessionId>(.*?)</sessionId>", resp.text)
    server_url = re.search(r"<serverUrl>(.*?)</serverUrl>", resp.text)

    if not session_id or not server_url:
        raise RuntimeError("SOAP login: could not parse sessionId or serverUrl.")

    instance_url = "/".join(server_url.group(1).strip().split("/")[:3])
    return SalesforceSession(instance_url, session_id.group(1).strip(), settings.api_version)


# ── OAuth browser flow ────────────────────────────────────────────────────────

class _CallbackHandler(BaseHTTPRequestHandler):
    auth_code: Optional[str] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/callback", "/OauthRedirect"):
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

    def log_message(self, format, *args):
        pass


def _browser_oauth_flow(settings: Settings) -> SalesforceSession:
    # Use Data Loader's public client ID (PKCE, no secret) unless the user
    # has configured their own Connected App.
    use_pkce = not settings.client_id
    client_id = settings.client_id or _DATALOADER_BULK_CLIENT_ID
    # /OauthRedirect is the path registered for the Data Loader client in Salesforce.
    # Custom Connected Apps can use /callback instead.
    redirect_path = "/OauthRedirect" if use_pkce else "/callback"
    redirect_uri = f"http://localhost:{settings.callback_port}{redirect_path}"

    code_verifier, code_challenge = _pkce_pair()

    auth_url = (
        f"{settings.login_url}/services/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )

    server = HTTPServer(("localhost", settings.callback_port), _CallbackHandler)
    _CallbackHandler.auth_code = None

    def serve():
        while _CallbackHandler.auth_code is None:
            server.handle_request()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    print("\nOpening browser for Salesforce login...")
    print(f"If the browser does not open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    thread.join(timeout=120)
    server.server_close()

    code = _CallbackHandler.auth_code
    if not code:
        raise RuntimeError("OAuth flow timed out — no authorization code received.")

    token_data: dict = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code": code,
        "code_verifier": code_verifier,
    }
    if not use_pkce and settings.client_secret:
        token_data["client_secret"] = settings.client_secret

    resp = requests.post(
        f"{settings.login_url}/services/oauth2/token",
        data=token_data,
        timeout=30,
    )
    if not resp.ok:
        _raise_sf_error(resp)

    data = resp.json()
    access_token = data["access_token"]
    refresh_token = data.get("refresh_token", "")
    instance_url = data["instance_url"]

    if refresh_token:
        _save_tokens(refresh_token, instance_url, client_id)

    return SalesforceSession(instance_url, access_token, settings.api_version)


def _refresh_token_login(settings: Settings, cached: dict) -> SalesforceSession:
    client_id = cached.get("client_id", settings.client_id or _DATALOADER_BULK_CLIENT_ID)
    token_data: dict = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": cached["refresh_token"],
    }
    if settings.client_secret:
        token_data["client_secret"] = settings.client_secret

    resp = requests.post(
        f"{settings.login_url}/services/oauth2/token",
        data=token_data,
        timeout=30,
    )
    if not resp.ok:
        _raise_sf_error(resp)

    data = resp.json()
    new_refresh = data.get("refresh_token", cached["refresh_token"])
    instance_url = data.get("instance_url", cached["instance_url"])
    _save_tokens(new_refresh, instance_url, client_id)
    return SalesforceSession(instance_url, data["access_token"], settings.api_version)


# ── Public entry point ────────────────────────────────────────────────────────

def get_session(settings: Settings) -> SalesforceSession:
    if settings.auth_method == "password":
        return _soap_login(settings)

    # OAuth: try cached refresh token first
    cached = _load_tokens()
    if cached and cached.get("refresh_token"):
        try:
            return _refresh_token_login(settings, cached)
        except RuntimeError:
            TOKEN_FILE.unlink(missing_ok=True)

    return _browser_oauth_flow(settings)
