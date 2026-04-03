from __future__ import annotations

from InquirerPy import inquirer
from InquirerPy.base.control import Choice

from .auth import SalesforceSession, _raise_sf_error


def _fetch_queryable(session: SalesforceSession) -> list[dict]:
    resp = session.get("/sobjects", timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)
    sobjects = resp.json().get("sobjects", [])
    queryable = [obj for obj in sobjects if obj.get("queryable")]
    queryable.sort(key=lambda o: o["label"].lower())
    return queryable


def pick_object(session: SalesforceSession) -> dict:
    queryable = _fetch_queryable(session)
    choices = [
        {"name": f"{obj['label']} ({obj['name']})", "value": obj}
        for obj in queryable
    ]
    selected = inquirer.fuzzy(
        message="Search and select an object:",
        choices=choices,
        long_instruction="Type to filter | Enter to select",
        max_height="40%",
    ).execute()
    return {"name": selected["name"], "label": selected["label"]}


def pick_objects(session: SalesforceSession) -> list[dict]:
    """Multi-select version — Tab to toggle, Enter to confirm."""
    queryable = _fetch_queryable(session)
    choices = [
        Choice(value=obj, name=f"{obj['label']} ({obj['name']})")
        for obj in queryable
    ]
    selected = inquirer.fuzzy(
        message="Search and select objects:",
        choices=choices,
        multiselect=True,
        keybindings={"toggle": [{"key": "tab"}]},
        long_instruction="Type to filter | Tab to toggle | Enter to confirm",
        max_height="40%",
    ).execute()
    return [{"name": obj["name"], "label": obj["label"]} for obj in selected]
