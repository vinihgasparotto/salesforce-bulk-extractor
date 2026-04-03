from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Literal, Optional

from dotenv import load_dotenv

load_dotenv()

AuthMethod = Literal["password", "oauth"]


@dataclass(frozen=True)
class Settings:
    login_url: str
    auth_method: AuthMethod
    api_version: str
    output_dir: str
    # password auth fields
    username: Optional[str]
    password: Optional[str]
    security_token: Optional[str]
    # oauth fields
    client_id: Optional[str]
    client_secret: Optional[str]
    callback_port: int


def load_settings() -> Settings:
    login_url = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com").strip()
    auth_method = os.getenv("SF_AUTH_METHOD", "password").strip().lower()
    api_version = os.getenv("SF_API_VERSION", "59.0").strip()
    output_dir = os.getenv("SF_OUTPUT_DIR", "output").strip()

    port_str = os.getenv("SF_CALLBACK_PORT", "8080").strip()
    try:
        callback_port = int(port_str)
    except ValueError:
        print(f"[ERROR] SF_CALLBACK_PORT must be an integer, got: {port_str!r}")
        sys.exit(1)

    if auth_method not in ("password", "oauth"):
        print(f"[ERROR] SF_AUTH_METHOD must be 'password' or 'oauth', got: {auth_method!r}")
        sys.exit(1)

    missing = []

    def get(key: str) -> Optional[str]:
        value = os.getenv(key, "").strip() or None
        return value

    def require(key: str) -> str:
        value = get(key)
        if not value:
            missing.append(key)
        return value or ""

    username = client_id = client_secret = security_token = password = None

    if auth_method == "password":
        username = require("SF_USERNAME")
        password = require("SF_PASSWORD")
        security_token = os.getenv("SF_SECURITY_TOKEN", "").strip() or None  # optional for IP-trusted orgs
    else:
        client_id = require("SF_CLIENT_ID")
        client_secret = require("SF_CLIENT_SECRET")

    if missing:
        print("[ERROR] Missing required environment variables for auth_method=" + auth_method + ":")
        for key in missing:
            print(f"  - {key}")
        print("\nCopy .env.example to .env and fill in the required values.")
        sys.exit(1)

    return Settings(
        login_url=login_url,
        auth_method=auth_method,  # type: ignore[arg-type]
        api_version=api_version,
        output_dir=output_dir,
        username=username,
        password=password,
        security_token=security_token,
        client_id=client_id,
        client_secret=client_secret,
        callback_port=callback_port,
    )
