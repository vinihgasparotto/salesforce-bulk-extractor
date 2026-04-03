"""
yaml_builder/app.py
-------------------
Local web UI for building queue_import.yaml files.

Run from the project root:
    python yaml_builder/app.py

Authenticates, then opens http://localhost:5001 in your browser.
Reads: .env, .sf_tokens, templates.json  (project root)
"""
from __future__ import annotations

import os
import sys
import threading
import webbrowser
from pathlib import Path

try:
    from flask import Flask, jsonify, request
except ImportError:
    print("Flask not installed. Run: pip install -r yaml_builder/requirements.txt")
    sys.exit(1)

# ── Path setup ────────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent

# Make builder.py importable as a sibling module
sys.path.insert(0, str(_HERE))
from builder import (  # noqa: E402
    authenticate,
    fetch_queryable_objects,
    load_template_names,
    console,
)

# ── App ───────────────────────────────────────────────────────────────────────

app = Flask(
    __name__,
    template_folder=str(_HERE / "templates"),
    static_folder=str(_HERE / "static"),
)

_objects: list[dict] = []
_org_name: str = ""

# ── Routes ────────────────────────────────────────────────────────────────────


@app.route("/")
def index():
    from flask import render_template
    return render_template("index.html")


@app.route("/api/objects")
def api_objects():
    return jsonify({"objects": _objects, "org": _org_name})


@app.route("/api/templates")
def api_templates():
    return jsonify({"templates": load_template_names()})


# ── Entry point ───────────────────────────────────────────────────────────────

WEB_PORT = int(os.getenv("YAML_BUILDER_PORT", "5001"))


def main() -> None:
    global _objects, _org_name

    from rich.rule import Rule

    console.print(Rule("[bold]Salesforce YAML Builder[/bold]", style="dim"))

    with console.status("[dim]Authenticating...[/dim]"):
        try:
            session = authenticate()
        except RuntimeError as exc:
            console.print(f"[red][ERROR][/red] {exc}")
            sys.exit(1)

    _org_name = session.instance_url.replace("https://", "").split(".")[0]
    console.print(f"[green][OK][/green] Connected to [bold]{_org_name}[/bold]")

    with console.status("[dim]Fetching objects...[/dim]"):
        _objects = fetch_queryable_objects(session)

    console.print(f"[green][OK][/green] {len(_objects)} queryable objects loaded")

    url = f"http://localhost:{WEB_PORT}"
    console.print(f"\nOpening [bold]{url}[/bold]\n")
    console.print("[dim]Press Ctrl+C to stop the server.[/dim]\n")

    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    app.run(port=WEB_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\nStopped.")
