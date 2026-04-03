from __future__ import annotations

from InquirerPy import inquirer

from .auth import SalesforceSession, _raise_sf_error


def pick_object(session: SalesforceSession) -> dict:
    resp = session.get("/sobjects", timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)

    sobjects = resp.json().get("sobjects", [])
    queryable = [obj for obj in sobjects if obj.get("queryable")]
    queryable.sort(key=lambda o: o["label"].lower())

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
