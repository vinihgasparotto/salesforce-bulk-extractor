"""
yaml_builder/builder.py
-----------------------
Self-contained helper that lets you pick Salesforce objects interactively,
optionally attach a template, and copies a ready-to-use queue_import.yaml
snippet to your clipboard.

Run from the project root:
    python yaml_builder/builder.py

Reads: .env, .sf_tokens, templates.json  (project root)
Writes: .sf_tokens (token cache, shared with main tool)
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import sys
import threading
import urllib.parse
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import pyperclip
import requests
import yaml
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from dotenv import load_dotenv
from rich.console import Console
from rich.rule import Rule
from rich.syntax import Syntax

# ── Locate project root (.env lives there) ───────────────────────────────────

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent  # yaml_builder/ sits one level below the project root

# Load .env from project root
load_dotenv(_ROOT / ".env")

console = Console()

# ── Config ────────────────────────────────────────────────────────────────────

_DATALOADER_BULK_CLIENT_ID = "DataLoaderBulkUI/"
_TOKEN_FILE = _ROOT / ".sf_tokens"
_TEMPLATES_FILE = _ROOT / "templates.json"

LOGIN_URL      = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com").strip()
AUTH_METHOD    = os.getenv("SF_AUTH_METHOD", "password").strip().lower()
API_VERSION    = os.getenv("SF_API_VERSION", "59.0").strip()
USERNAME       = os.getenv("SF_USERNAME", "").strip()
PASSWORD       = os.getenv("SF_PASSWORD", "").strip()
SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN", "").strip()
CLIENT_ID      = os.getenv("SF_CLIENT_ID", "").strip() or None
CLIENT_SECRET  = os.getenv("SF_CLIENT_SECRET", "").strip() or None
CALLBACK_PORT  = int(os.getenv("SF_CALLBACK_PORT", "8080").strip())

# ── Auth ──────────────────────────────────────────────────────────────────────

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


@dataclass
class Session:
    instance_url: str
    access_token: str
    _http: requests.Session = field(default_factory=requests.Session, repr=False)

    def __post_init__(self):
        self._http.headers["Authorization"] = f"Bearer {self.access_token}"
        self._base = f"{self.instance_url}/services/data/v{API_VERSION}"

    def get(self, path: str, **kw) -> requests.Response:
        return self._http.get(f"{self._base}{path}", **kw)


def _save_tokens(refresh_token: str, instance_url: str, client_id: str) -> None:
    _TOKEN_FILE.write_text(
        json.dumps({"refresh_token": refresh_token, "instance_url": instance_url, "client_id": client_id}),
        encoding="utf-8",
    )


def _load_tokens() -> Optional[dict]:
    if _TOKEN_FILE.exists():
        try:
            return json.loads(_TOKEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _raise_for(resp: requests.Response) -> None:
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if isinstance(body, dict) and "error_description" in body:
        raise RuntimeError(body["error_description"])
    if isinstance(body, list) and body and "message" in body[0]:
        raise RuntimeError(body[0]["message"])
    if isinstance(body, dict) and "message" in body:
        raise RuntimeError(body["message"])
    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")


def _soap_login() -> Session:
    password = PASSWORD + SECURITY_TOKEN
    body = _SOAP_ENVELOPE.format(username=USERNAME, password=password)
    resp = requests.post(
        f"{LOGIN_URL}/services/Soap/u/{API_VERSION}",
        data=body.encode(),
        headers={"Content-Type": "text/xml; charset=UTF-8", "SOAPAction": "login"},
        timeout=30,
    )
    if resp.status_code != 200:
        fault = re.search(r"<faultstring>(.*?)</faultstring>", resp.text, re.DOTALL)
        raise RuntimeError(fault.group(1).strip() if fault else resp.text[:200])
    session_id = re.search(r"<sessionId>(.*?)</sessionId>", resp.text)
    server_url = re.search(r"<serverUrl>(.*?)</serverUrl>", resp.text)
    if not session_id or not server_url:
        raise RuntimeError("SOAP login: could not parse sessionId/serverUrl.")
    instance_url = "/".join(server_url.group(1).strip().split("/")[:3])
    return Session(instance_url=instance_url, access_token=session_id.group(1).strip())


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(96)[:128]
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


class _CBHandler(BaseHTTPRequestHandler):
    code: Optional[str] = None

    def do_GET(self):
        if urllib.parse.urlparse(self.path).path in ("/OauthRedirect", "/callback"):
            _CBHandler.code = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Authenticated!</h2><p>You can close this tab.</p></body></html>")
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, *a): pass


def _oauth_login() -> Session:
    # Try cached refresh token first
    cached = _load_tokens()
    if cached and cached.get("refresh_token"):
        client_id = cached.get("client_id", CLIENT_ID or _DATALOADER_BULK_CLIENT_ID)
        data: dict = {"grant_type": "refresh_token", "client_id": client_id, "refresh_token": cached["refresh_token"]}
        if CLIENT_SECRET:
            data["client_secret"] = CLIENT_SECRET
        resp = requests.post(f"{LOGIN_URL}/services/oauth2/token", data=data, timeout=30)
        if resp.ok:
            d = resp.json()
            new_refresh = d.get("refresh_token", cached["refresh_token"])
            instance_url = d.get("instance_url", cached["instance_url"])
            _save_tokens(new_refresh, instance_url, client_id)
            return Session(instance_url=instance_url, access_token=d["access_token"])
        _TOKEN_FILE.unlink(missing_ok=True)

    # Browser flow
    use_pkce = not CLIENT_ID
    client_id = CLIENT_ID or _DATALOADER_BULK_CLIENT_ID
    redirect_path = "/OauthRedirect" if use_pkce else "/callback"
    redirect_uri = f"http://localhost:{CALLBACK_PORT}{redirect_path}"
    verifier, challenge = _pkce_pair()

    auth_url = (
        f"{LOGIN_URL}/services/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&code_challenge={challenge}&code_challenge_method=S256"
    )

    server = HTTPServer(("localhost", CALLBACK_PORT), _CBHandler)
    _CBHandler.code = None

    def serve():
        while _CBHandler.code is None:
            server.handle_request()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    console.print("\nOpening browser for Salesforce login...")
    webbrowser.open(auth_url)
    t.join(timeout=120)
    server.server_close()

    code = _CBHandler.code
    if not code:
        raise RuntimeError("OAuth timed out — no code received.")

    token_data: dict = {
        "grant_type": "authorization_code", "client_id": client_id,
        "redirect_uri": redirect_uri, "code": code, "code_verifier": verifier,
    }
    if not use_pkce and CLIENT_SECRET:
        token_data["client_secret"] = CLIENT_SECRET

    resp = requests.post(f"{LOGIN_URL}/services/oauth2/token", data=token_data, timeout=30)
    if not resp.ok:
        _raise_for(resp)
    d = resp.json()
    if d.get("refresh_token"):
        _save_tokens(d["refresh_token"], d["instance_url"], client_id)
    return Session(instance_url=d["instance_url"], access_token=d["access_token"])


def authenticate() -> Session:
    if AUTH_METHOD == "password":
        if not USERNAME or not PASSWORD:
            console.print("[red][ERROR][/red] SF_USERNAME and SF_PASSWORD must be set in .env")
            sys.exit(1)
        return _soap_login()
    return _oauth_login()


# ── Salesforce data ───────────────────────────────────────────────────────────

def fetch_queryable_objects(session: Session) -> list[dict]:
    resp = session.get("/sobjects", timeout=30)
    if not resp.ok:
        _raise_for(resp)
    objects = [o for o in resp.json().get("sobjects", []) if o.get("queryable")]
    objects.sort(key=lambda o: o["label"].lower())
    return objects


# ── Templates ─────────────────────────────────────────────────────────────────

def load_template_names() -> list[str]:
    """Return list of template names from templates.json, preserving default-first order."""
    if not _TEMPLATES_FILE.exists():
        return []
    try:
        raw = json.loads(_TEMPLATES_FILE.read_text(encoding="utf-8"))
        default_first = sorted(raw, key=lambda t: (not t.get("is_default", False), t["name"].lower()))
        return [t["name"] for t in default_first]
    except Exception:
        return []


# ── YAML generation ───────────────────────────────────────────────────────────

def build_yaml(objects: list[dict], template_map: dict[str, Optional[str]]) -> str:
    """
    objects:      list of {name, label}
    template_map: {object_name -> template_name or None}
    """
    entries = []
    for obj in objects:
        entry: dict = {"object": obj["name"]}
        tmpl = template_map.get(obj["name"])
        if tmpl:
            entry["template"] = tmpl
        entries.append(entry)

    return yaml.dump(entries, default_flow_style=False, allow_unicode=True, sort_keys=False)


# ── UI ────────────────────────────────────────────────────────────────────────

def pick_objects(objects: list[dict]) -> list[dict]:
    choices = [
        Choice(value=obj, name=f"{obj['label']} ({obj['name']})")
        for obj in objects
    ]
    selected = inquirer.fuzzy(
        message="Select objects to include:",
        choices=choices,
        multiselect=True,
        keybindings={"toggle": [{"key": "tab"}]},
        long_instruction="Type to filter | Tab to toggle | Enter to confirm",
        max_height="60%",
    ).execute()
    return [{"name": o["name"], "label": o["label"]} for o in selected]


def pick_template_assignment(objects: list[dict], template_names: list[str]) -> dict[str, Optional[str]]:
    """Returns {object_name -> template_name or None}."""
    if not template_names:
        return {obj["name"]: None for obj in objects}

    mode = inquirer.select(
        message="Apply a template?",
        choices=[
            {"name": "Same template for all", "value": "all"},
            {"name": "Pick per object", "value": "per"},
            {"name": "No template", "value": "none"},
        ],
    ).execute()

    if mode == "none":
        return {obj["name"]: None for obj in objects}

    if mode == "all":
        tmpl = inquirer.select(
            message="Select template:",
            choices=template_names,
            default=template_names[0],
        ).execute()
        return {obj["name"]: tmpl for obj in objects}

    # per object
    result = {}
    for obj in objects:
        tmpl = inquirer.select(
            message=f"Template for {obj['label']} ({obj['name']}):",
            choices=["(none)"] + template_names,
            default=template_names[0],
        ).execute()
        result[obj["name"]] = None if tmpl == "(none)" else tmpl
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    console.print(Rule("[bold]Salesforce YAML Queue Builder[/bold]", style="dim"))

    with console.status("[dim]Authenticating...[/dim]"):
        try:
            session = authenticate()
        except RuntimeError as exc:
            console.print(f"[red][ERROR][/red] {exc}")
            sys.exit(1)

    org = session.instance_url.replace("https://", "").split(".")[0]
    console.print(f"[green][OK][/green] Connected to [bold]{org}[/bold]\n")

    with console.status("[dim]Fetching objects...[/dim]"):
        objects = fetch_queryable_objects(session)

    template_names = load_template_names()
    if template_names:
        console.print(f"  [dim]Found {len(template_names)} template(s): {', '.join(template_names)}[/dim]\n")

    # Object selection
    selected = pick_objects(objects)
    if not selected:
        console.print("[yellow][WARN][/yellow] No objects selected.")
        sys.exit(0)

    # Template assignment
    template_map = pick_template_assignment(selected, template_names)

    # Generate YAML
    yaml_str = build_yaml(selected, template_map)

    # Preview
    console.print()
    console.print(Rule("[bold]Generated YAML[/bold]", style="dim"))
    console.print(Syntax(yaml_str, "yaml", theme="monokai", line_numbers=False))

    # Output options
    action = inquirer.select(
        message="What would you like to do?",
        choices=[
            {"name": "Copy to clipboard", "value": "copy"},
            {"name": "Save to file", "value": "save"},
            {"name": "Copy and save", "value": "both"},
        ],
    ).execute()

    if action in ("copy", "both"):
        try:
            pyperclip.copy(yaml_str)
            console.print("[green][OK][/green] Copied to clipboard.")
        except Exception as exc:
            console.print(f"[yellow][WARN][/yellow] Clipboard failed: {exc}")
            console.print("Copy the YAML above manually.")

    if action in ("save", "both"):
        default_path = str(_ROOT / "queue_import.yaml")
        raw = inquirer.text(message="Save to file:", default=default_path).execute().strip()
        path = Path(raw)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml_str, encoding="utf-8")
        console.print(f"[green][OK][/green] Saved to [bold]{path}[/bold]")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\nExiting.")
