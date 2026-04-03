from __future__ import annotations

from InquirerPy import inquirer
from InquirerPy.base.control import Choice

from .auth import SalesforceSession, _raise_sf_error

_SELECT_ALL_VALUE = "__SELECT_ALL__"


def pick_fields(
    session: SalesforceSession, object_name: str
) -> tuple[list[str], dict[str, str]]:
    resp = session.get(f"/sobjects/{object_name}/describe", timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)

    fields = resp.json().get("fields", [])
    fields.sort(key=lambda f: f["label"].lower())

    # Build label map for all fields (used by csv_labels output format)
    field_labels: dict[str, str] = {f["name"]: f["label"] for f in fields}

    # Default pre-selections
    default_names = {"Id", "Name"}

    select_all_choice = Choice(
        value=_SELECT_ALL_VALUE,
        name="★ Select All Fields",
        enabled=False,
    )

    field_choices = [
        Choice(
            value=f["name"],
            name=f"{f['label']} ({f['name']}) [{f['type']}]",
            enabled=f["name"] in default_names,
        )
        for f in fields
    ]

    all_choices = [select_all_choice] + field_choices

    while True:
        result: list[str] = inquirer.fuzzy(
            message=f"Select fields for {object_name}:",
            choices=all_choices,
            multiselect=True,
            long_instruction="Type to filter | Space to toggle | Enter to confirm | ★ to select all",
            max_height="60%",
        ).execute()

        if _SELECT_ALL_VALUE in result:
            # Return all field names
            return list(field_labels.keys()), field_labels

        if not result:
            print("  You must select at least one field. Please try again.")
            continue

        return result, field_labels
