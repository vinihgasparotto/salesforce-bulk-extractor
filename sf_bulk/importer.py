from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .auth import SalesforceSession, _raise_sf_error
from .display import print_error, print_warning, print_success
from .queue import ExtractJob
from .templates import Template

VALID_FORMATS = {"csv", "csv_labels", "json", "parquet", "excel"}
DEFAULT_IMPORT_FILE = Path("queue_import.yaml")


def _fetch_object_meta(session: SalesforceSession, object_name: str) -> dict | None:
    """Return {name, label, fields: [{name, label}]} or None if not found/queryable."""
    resp = session.get("/sobjects", timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)
    match = next(
        (o for o in resp.json().get("sobjects", [])
         if o["name"].lower() == object_name.lower() and o.get("queryable")),
        None,
    )
    return match


def _fetch_fields(session: SalesforceSession, object_name: str) -> dict[str, str]:
    """Return {api_name: label} for all fields on the object."""
    resp = session.get(f"/sobjects/{object_name}/describe", timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)
    return {f["name"]: f["label"] for f in resp.json().get("fields", [])}


def _resolve_fields(
    requested: Any,
    all_field_labels: dict[str, str],
    object_name: str,
) -> list[str]:
    """
    requested can be:
      - the string "all" (or omitted)
      - a list of field API names
    Returns the resolved list of valid field API names.
    """
    if requested is None or requested == "all":
        return list(all_field_labels.keys())

    if isinstance(requested, str):
        # Comma-separated inline: "Id, Name, Phone"
        requested = [f.strip() for f in requested.split(",")]

    valid = []
    all_lower = {k.lower(): k for k in all_field_labels}
    for name in requested:
        canonical = all_lower.get(name.strip().lower())
        if canonical:
            valid.append(canonical)
        else:
            print_warning(f"  Field '{name}' not found on {object_name} — skipped.")
    return valid


def _resolve_filename(raw: Any, object_name: str) -> str:
    if raw is None or raw == "api":
        return object_name
    if raw == "auto":
        return ""
    return str(raw)  # custom name


def import_jobs_from_file(
    session: SalesforceSession,
    file_path: Path,
    templates: list[Template] | None = None,
) -> list[ExtractJob]:
    """Parse a YAML import file and return a list of ExtractJob objects."""
    try:
        raw = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Could not parse YAML: {exc}")

    if not isinstance(raw, list):
        raise RuntimeError("Import file must be a YAML list of job entries.")

    template_map = {t.name: t for t in (templates or [])}
    jobs: list[ExtractJob] = []

    for i, entry in enumerate(raw, start=1):
        if not isinstance(entry, dict):
            print_warning(f"  Entry {i} is not a mapping — skipped.")
            continue

        object_name: str = str(entry.get("object", "")).strip()
        if not object_name:
            print_warning(f"  Entry {i} missing 'object' — skipped.")
            continue

        # Resolve template (if specified) and use it as defaults
        tmpl: Template | None = None
        tmpl_name = entry.get("template")
        if tmpl_name:
            tmpl = template_map.get(str(tmpl_name))
            if not tmpl:
                print_warning(f"  '{object_name}': template '{tmpl_name}' not found — ignoring.")

        # Validate object exists and is queryable
        obj_meta = _fetch_object_meta(session, object_name)
        if not obj_meta:
            print_error(f"  '{object_name}' not found or not queryable — skipped.")
            continue

        # Normalise to the correctly-cased API name from Salesforce
        object_name = obj_meta["name"]
        object_label = obj_meta["label"]

        all_field_labels = _fetch_fields(session, object_name)

        # Fields: explicit YAML > template field_strategy > default (all)
        if "fields" in entry:
            fields = _resolve_fields(entry["fields"], all_field_labels, object_name)
        elif tmpl and tmpl.field_strategy == "all":
            fields = list(all_field_labels.keys())
        elif tmpl and tmpl.field_strategy == "ask":
            # "ask" in a template means no preset — fall back to all when importing from file
            fields = list(all_field_labels.keys())
        else:
            fields = list(all_field_labels.keys())

        if not fields:
            print_error(f"  '{object_name}' has no valid fields after filtering — skipped.")
            continue

        # format: explicit YAML > template > "csv"
        if "format" in entry:
            output_format = str(entry["format"]).strip().lower()
        elif tmpl:
            output_format = tmpl.output_format
        else:
            output_format = "csv"

        if output_format not in VALID_FORMATS:
            print_warning(f"  '{object_name}': unknown format '{output_format}', defaulting to 'csv'.")
            output_format = "csv"

        # deleted: explicit YAML > template > False
        if "deleted" in entry:
            include_deleted = bool(entry["deleted"])
        elif tmpl:
            include_deleted = tmpl.include_deleted
        else:
            include_deleted = False

        # filename: explicit YAML > template > api name
        if "filename" in entry:
            output_filename = _resolve_filename(entry["filename"], object_name)
        elif tmpl:
            output_filename = _resolve_filename(
                tmpl.filename_strategy if tmpl.filename_strategy != "custom" else tmpl.custom_filename,
                object_name,
            )
        else:
            output_filename = object_name  # default: api name

        soql = f"SELECT {', '.join(fields)} FROM {object_name}"

        jobs.append(ExtractJob(
            object_name=object_name,
            object_label=object_label,
            fields=fields,
            field_labels=all_field_labels,
            include_deleted=include_deleted,
            output_format=output_format,
            soql=soql,
            output_filename=output_filename,
        ))
        print_success(f"  {object_label} ({object_name}) — {len(fields)} fields")

    return jobs
