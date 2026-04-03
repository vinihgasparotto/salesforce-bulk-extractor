from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from InquirerPy import inquirer

from .queue import FORMAT_LABELS

TEMPLATES_FILE = Path("templates.json")

FIELD_STRATEGY_LABELS = {
    "all": "All fields (skip picker)",
    "ask": "Let me choose fields each time",
}

FILENAME_STRATEGY_LABELS = {
    "auto": "Auto-generate  (ObjectName_YYYYMMDD_HHMMSS)",
    "api":  "Object API name  (e.g. Account)",
    "custom": "Custom (fixed name)",
}


@dataclass
class Template:
    name: str
    field_strategy: str    # "all" | "ask"
    include_deleted: bool
    output_format: str     # "csv" | "csv_labels" | "json" | "parquet" | "excel"
    filename_strategy: str # "auto" | "api" | "custom"
    custom_filename: str   # only used when filename_strategy == "custom"


def load_templates() -> list[Template]:
    if not TEMPLATES_FILE.exists():
        return []
    try:
        raw = json.loads(TEMPLATES_FILE.read_text(encoding="utf-8"))
        return [Template(**item) for item in raw]
    except Exception:
        return []


def save_templates(templates: list[Template]) -> None:
    TEMPLATES_FILE.write_text(
        json.dumps([asdict(t) for t in templates], indent=2),
        encoding="utf-8",
    )


def create_template_prompt() -> Optional[Template]:
    name = inquirer.text(message="Template name:").execute().strip()
    if not name:
        return None

    field_strategy = inquirer.select(
        message="Field selection:",
        choices=[
            {"name": FIELD_STRATEGY_LABELS["all"], "value": "all"},
            {"name": FIELD_STRATEGY_LABELS["ask"], "value": "ask"},
        ],
    ).execute()

    include_deleted = inquirer.confirm(
        message="Include deleted records?",
        default=False,
    ).execute()

    output_format = inquirer.select(
        message="Output format:",
        choices=[{"name": v, "value": k} for k, v in FORMAT_LABELS.items()],
    ).execute()

    filename_strategy = inquirer.select(
        message="Output filename:",
        choices=[{"name": v, "value": k} for k, v in FILENAME_STRATEGY_LABELS.items()],
    ).execute()

    custom_filename = ""
    if filename_strategy == "custom":
        custom_filename = inquirer.text(
            message="Custom filename (without extension):",
        ).execute().strip()

    return Template(
        name=name,
        field_strategy=field_strategy,
        include_deleted=include_deleted,
        output_format=output_format,
        filename_strategy=filename_strategy,
        custom_filename=custom_filename,
    )


def pick_template(templates: list[Template]) -> Optional[Template]:
    """Show template picker. Returns None if user picks 'no template'."""
    choices = [{"name": "No template — ask me everything", "value": None}] + [
        {"name": t.name, "value": t} for t in templates
    ]
    return inquirer.select(
        message="Use a template?",
        choices=choices,
    ).execute()
