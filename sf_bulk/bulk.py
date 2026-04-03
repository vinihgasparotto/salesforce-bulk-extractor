from __future__ import annotations

import time

from rich.console import Console

from .auth import SalesforceSession, _raise_sf_error
from .queue import ExtractJob


def submit_job(session: SalesforceSession, job: ExtractJob) -> str:
    operation = "queryAll" if job.include_deleted else "query"
    payload = {
        "operation": operation,
        "query": job.soql,
        "contentType": "CSV",
        "columnDelimiter": "COMMA",
        "lineEnding": "LF",
    }
    resp = session.post("/jobs/query", json=payload, timeout=30)
    if not resp.ok:
        _raise_sf_error(resp)
    return resp.json()["id"]


def abort_job(session: SalesforceSession, job_id: str) -> None:
    session.post(f"/jobs/query/{job_id}", json={"state": "Aborted"}, timeout=10)


def poll_job(session: SalesforceSession, job_id: str, console: Console) -> None:
    terminal_states = {"JobComplete", "Failed", "Aborted"}
    attempt = 0
    start = time.monotonic()

    with console.status("") as status:
        while True:
            resp = session.get(f"/jobs/query/{job_id}", timeout=30)
            if not resp.ok:
                _raise_sf_error(resp)

            data = resp.json()
            state = data.get("state", "Unknown")
            elapsed = int(time.monotonic() - start)
            status.update(f"[dim]Polling... state=[bold]{state}[/bold]  ({elapsed}s elapsed)[/dim]")

            if state in terminal_states:
                break

            interval = min(2 * (2 ** attempt), 30)
            time.sleep(interval)
            attempt += 1

    if state == "JobComplete":
        return

    error_msg = data.get("errorMessage") or state
    raise RuntimeError(f"Bulk job {job_id} ended with state '{state}': {error_msg}")
