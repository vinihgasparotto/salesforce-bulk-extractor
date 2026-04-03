from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    login_url: str
    client_id: str
    client_secret: str
    callback_port: int
    api_version: str
    output_dir: str


def load_settings() -> Settings:
    missing = []

    def require(key: str) -> str:
        value = os.getenv(key, "").strip()
        if not value:
            missing.append(key)
        return value

    login_url = require("SF_LOGIN_URL")
    client_id = require("SF_CLIENT_ID")
    client_secret = require("SF_CLIENT_SECRET")

    port_str = os.getenv("SF_CALLBACK_PORT", "8080").strip()
    try:
        callback_port = int(port_str)
    except ValueError:
        print(f"[ERROR] SF_CALLBACK_PORT must be an integer, got: {port_str!r}")
        sys.exit(1)

    api_version = os.getenv("SF_API_VERSION", "59.0").strip()
    output_dir = os.getenv("SF_OUTPUT_DIR", "output").strip()

    if missing:
        print("[ERROR] Missing required environment variables:")
        for key in missing:
            print(f"  - {key}")
        print("\nCopy .env.example to .env and fill in the required values.")
        sys.exit(1)

    return Settings(
        login_url=login_url,
        client_id=client_id,
        client_secret=client_secret,
        callback_port=callback_port,
        api_version=api_version,
        output_dir=output_dir,
    )
