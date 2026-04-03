from __future__ import annotations

import time
from datetime import timedelta

from InquirerPy import inquirer
from rich.table import Table

from sf_bulk.auth import get_session
from sf_bulk.browser import pick_object
from sf_bulk.bulk import abort_job, poll_job, submit_job
from sf_bulk.config import load_settings
from sf_bulk.display import console, print_error, print_header, print_success, print_warning
from sf_bulk.downloader import download_results
from sf_bulk.fields import get_all_fields, pick_fields
from sf_bulk.queue import FORMAT_LABELS, ExtractJob, Queue, load_queue, save_queue
from sf_bulk.importer import DEFAULT_IMPORT_FILE, import_jobs_from_file
from sf_bulk.templates import (
    Template,
    create_template_prompt,
    load_templates,
    pick_template,
    save_templates,
)

OUTPUT_FORMAT_CHOICES = [
    {"name": "CSV — Data Loader style (API names)", "value": "csv"},
    {"name": "CSV — field labels as headers", "value": "csv_labels"},
    {"name": "JSON", "value": "json"},
    {"name": "Parquet", "value": "parquet"},
    {"name": "Excel (.xlsx)", "value": "excel"},
]


def _build_soql(fields: list[str], object_name: str) -> str:
    return f"SELECT {', '.join(fields)} FROM {object_name}"


def _resolve_filename(filename_strategy: str, custom_filename: str, object_name: str) -> str:
    if filename_strategy == "api":
        return object_name
    if filename_strategy == "custom":
        return custom_filename
    return ""  # auto


def _add_to_queue(session, queue: Queue, templates: list[Template]) -> None:
    print_header("Add object to queue")

    obj = pick_object(session)

    # Template selection — only shown if templates exist
    template: Template | None = None
    if templates:
        template = pick_template(templates)

    if template:
        # ── Template path ────────────────────────────────────────────────────
        if template.field_strategy == "all":
            with console.status(f"[dim]Fetching all fields for {obj['name']}...[/dim]"):
                fields, field_labels = get_all_fields(session, obj["name"])
        else:
            print_header(f"Select fields for {obj['label']}")
            fields, field_labels = pick_fields(session, obj["name"])

        include_deleted = template.include_deleted
        output_format = template.output_format
        output_filename = _resolve_filename(
            template.filename_strategy, template.custom_filename, obj["name"]
        )

        fmt_label = FORMAT_LABELS.get(output_format, output_format)
        deleted_label = "deleted included" if include_deleted else "deleted excluded"
        console.print(
            f"  [dim]Template:[/dim] [bold]{template.name}[/bold]  "
            f"({len(fields)} fields · {fmt_label} · {deleted_label})"
        )
    else:
        # ── Manual path ──────────────────────────────────────────────────────
        print_header(f"Select fields for {obj['label']}")
        fields, field_labels = pick_fields(session, obj["name"])

        include_deleted = inquirer.confirm(
            message="Include deleted records?",
            default=False,
        ).execute()

        output_format = inquirer.select(
            message="Output format:",
            choices=OUTPUT_FORMAT_CHOICES,
        ).execute()

        filename_choice = inquirer.select(
            message="Output filename:",
            choices=[
                {"name": f"Auto-generate  ({obj['name']}_YYYYMMDD_HHMMSS)", "value": "auto"},
                {"name": f"Object API name  ({obj['name']})", "value": "api"},
                {"name": "Custom", "value": "custom"},
            ],
        ).execute()

        if filename_choice == "custom":
            custom = inquirer.text(
                message="Enter filename (without extension):",
            ).execute().strip()
        else:
            custom = ""

        output_filename = _resolve_filename(filename_choice, custom, obj["name"])

    soql = _build_soql(fields, obj["name"])

    print_header("Query preview")
    console.print(f"  [bold cyan]{soql}[/bold cyan]\n")
    if not inquirer.confirm(message="Add this query to the queue?", default=True).execute():
        print_warning("Cancelled — nothing added to queue.")
        return

    job = ExtractJob(
        object_name=obj["name"],
        object_label=obj["label"],
        fields=fields,
        field_labels=field_labels,
        include_deleted=include_deleted,
        output_format=output_format,
        soql=soql,
        output_filename=output_filename,
    )
    queue.add(job)
    save_queue(queue)

    fmt_label = FORMAT_LABELS.get(output_format, output_format)
    deleted_label = "deleted included" if include_deleted else "deleted excluded"
    print_success(
        f"Added: {obj['label']} — {len(fields)} field(s) — {fmt_label} — {deleted_label}"
    )


def _import_from_file(session, queue: Queue, templates) -> None:
    from pathlib import Path

    print_header("Import queue from file")
    raw_path = inquirer.text(
        message="Path to import file:",
        default=str(DEFAULT_IMPORT_FILE),
    ).execute().strip()

    file_path = Path(raw_path)
    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        return

    console.print(f"  Reading [bold]{file_path}[/bold]...\n")
    try:
        jobs = import_jobs_from_file(session, file_path, templates)
    except RuntimeError as exc:
        print_error(str(exc))
        return

    if not jobs:
        print_warning("No valid jobs found in file.")
        return

    action = inquirer.select(
        message=f"Found {len(jobs)} job(s) — how should they be added?",
        choices=[
            {"name": "Append to current queue", "value": "append"},
            {"name": "Replace current queue", "value": "replace"},
        ],
    ).execute()

    if action == "replace":
        queue.jobs.clear()

    for job in jobs:
        queue.add(job)
    save_queue(queue)
    print_success(f"{len(jobs)} job(s) added to queue.")


def _manage_templates(templates: list[Template]) -> None:
    while True:
        print_header("Manage Templates")
        choices = [{"name": "Create new template", "value": "create"}]
        if templates:
            choices.append({"name": "Delete a template", "value": "delete"})
        choices.append({"name": "Back", "value": "back"})

        action = inquirer.select(message="Choose action:", choices=choices).execute()

        if action == "create":
            t = create_template_prompt()
            if t:
                templates.append(t)
                save_templates(templates)
                print_success(f"Template '{t.name}' saved.")
            else:
                print_warning("No name entered — template not saved.")

        elif action == "delete":
            choice = inquirer.select(
                message="Select template to delete:",
                choices=[{"name": t.name, "value": t} for t in templates],
            ).execute()
            confirmed = inquirer.confirm(
                message=f"Delete '{choice.name}'?", default=False
            ).execute()
            if confirmed:
                templates.remove(choice)
                save_templates(templates)
                print_success(f"Template '{choice.name}' deleted.")

        elif action == "back":
            break


def _remove_from_queue(queue: Queue) -> None:
    print_header("Remove from queue")
    queue.display()
    index = inquirer.number(
        message="Enter job number to remove:",
        min_allowed=1,
        max_allowed=len(queue.jobs),
    ).execute()
    removed = queue.remove(int(index))
    save_queue(queue)
    print_success(f"Removed: {removed.object_label} ({removed.object_name})")


def _run_queue(session, queue: Queue, settings) -> None:
    print_header(f"Running queue ({len(queue.jobs)} job(s))")

    # Let user deselect any jobs they want to skip this run
    selected_jobs = inquirer.checkbox(
        message="Select jobs to run  (Tab to toggle, Enter to confirm):",
        choices=[
            {"name": f"{job.object_label} ({job.object_name})", "value": job, "enabled": True}
            for job in queue.jobs
        ],
        keybindings={"toggle": [{"key": "tab"}]},
    ).execute()

    if not selected_jobs:
        print_warning("No jobs selected — nothing to run.")
        return

    summary_rows = []
    jobs_snapshot = selected_jobs

    abort_run = False
    for i, job in enumerate(jobs_snapshot, start=1):
        console.print(f"\n  [{i}/{len(jobs_snapshot)}] [bold]{job.object_label}[/bold]")
        job_start = time.monotonic()
        job_id = None

        try:
            with console.status("[dim]Submitting job...[/dim]"):
                job_id = submit_job(session, job)
            print_success(f"Job submitted: {job_id}")

            poll_job(session, job_id, console)
            print_success("Job complete. Downloading results...")

            output_path, total_rows = download_results(
                session, job_id, job, settings.output_dir, console
            )

            elapsed = timedelta(seconds=int(time.monotonic() - job_start))
            print_success(f"Saved: {output_path}  ({total_rows:,} rows)")

            summary_rows.append(
                (job.object_name, total_rows, str(output_path), str(elapsed), None)
            )

        except RuntimeError as exc:
            elapsed = timedelta(seconds=int(time.monotonic() - job_start))
            print_error(str(exc))
            summary_rows.append((job.object_name, 0, "—", str(elapsed), str(exc)))

        except KeyboardInterrupt:
            console.print("\n")
            action = inquirer.select(
                message="Job interrupted — what would you like to do?",
                choices=[
                    {"name": "Skip this job and continue with the rest", "value": "skip"},
                    {"name": "Abort the entire run", "value": "abort"},
                ],
            ).execute()
            elapsed = timedelta(seconds=int(time.monotonic() - job_start))
            if job_id:
                with console.status("[dim]Cancelling Salesforce job...[/dim]"):
                    abort_job(session, job_id)
            summary_rows.append((job.object_name, 0, "—", str(elapsed), "Skipped by user"))
            if action == "abort":
                abort_run = True
                break

    _print_summary(summary_rows)

    # Remove successful jobs; keep failed + skipped jobs in the queue
    failed_jobs = {job for job, (_, _, _, _, err) in zip(jobs_snapshot, summary_rows) if err is not None}
    skipped_jobs = {job for job in queue.jobs if job not in selected_jobs}
    queue.jobs = [j for j in queue.jobs if j in failed_jobs or j in skipped_jobs]
    save_queue(queue)


def _print_summary(rows: list) -> None:
    print_header("Summary")
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Object")
    table.add_column("Rows", justify="right")
    table.add_column("File")
    table.add_column("Elapsed", justify="right")
    table.add_column("Status")

    for object_name, total_rows, path, elapsed, error in rows:
        status = "[red]FAILED[/red]" if error else "[green]OK[/green]"
        table.add_row(object_name, f"{total_rows:,}", path, elapsed, status)

    console.print(table)


def _main_menu_choices(queue: Queue) -> list:
    choices = [{"name": "Add object to queue", "value": "add"}]
    if not queue.is_empty():
        choices += [
            {"name": f"View queue  ({len(queue.jobs)} item(s))", "value": "view"},
            {"name": "Remove from queue", "value": "remove"},
            {"name": "Run queue", "value": "run"},
        ]
    choices.append({"name": "Import queue from file", "value": "import"})
    choices.append({"name": "Manage templates", "value": "templates"})
    choices.append({"name": "Quit", "value": "quit"})
    return choices


def main() -> None:
    settings = load_settings()

    with console.status("[dim]Authenticating to Salesforce...[/dim]"):
        try:
            session = get_session(settings)
        except RuntimeError as exc:
            print_error(f"Authentication failed: {exc}")
            return

    org_domain = session.instance_url.replace("https://", "").split(".")[0]
    print_success(f"Connected to {org_domain} ({session.instance_url})")

    queue = load_queue()
    templates = load_templates()

    while True:
        print_header("Main Menu")
        choice = inquirer.select(
            message="What would you like to do?",
            choices=_main_menu_choices(queue),
        ).execute()

        if choice == "add":
            try:
                _add_to_queue(session, queue, templates)
            except (RuntimeError, KeyboardInterrupt) as exc:
                if isinstance(exc, RuntimeError):
                    print_error(str(exc))

        elif choice == "view":
            print_header("Queue")
            queue.display()

        elif choice == "remove":
            _remove_from_queue(queue)

        elif choice == "run":
            _run_queue(session, queue, settings)

        elif choice == "import":
            try:
                _import_from_file(session, queue, templates)
            except (RuntimeError, KeyboardInterrupt) as exc:
                if isinstance(exc, RuntimeError):
                    print_error(str(exc))

        elif choice == "templates":
            _manage_templates(templates)

        elif choice == "quit":
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\nExiting.")
