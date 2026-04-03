from __future__ import annotations

import time
from datetime import timedelta

from InquirerPy import inquirer
from rich.table import Table

from sf_bulk.auth import get_session
from sf_bulk.browser import pick_object
from sf_bulk.bulk import poll_job, submit_job
from sf_bulk.config import load_settings
from sf_bulk.display import console, print_error, print_header, print_success, print_warning
from sf_bulk.downloader import download_results
from sf_bulk.fields import pick_fields
from sf_bulk.queue import FORMAT_LABELS, ExtractJob, Queue

OUTPUT_FORMAT_CHOICES = [
    {"name": "CSV — Data Loader style (API names)", "value": "csv"},
    {"name": "CSV — field labels as headers", "value": "csv_labels"},
    {"name": "JSON", "value": "json"},
    {"name": "Parquet", "value": "parquet"},
    {"name": "Excel (.xlsx)", "value": "excel"},
]


def _build_soql(fields: list[str], object_name: str) -> str:
    return f"SELECT {', '.join(fields)} FROM {object_name}"


def _add_to_queue(session, queue: Queue) -> None:
    print_header("Add object to queue")

    obj = pick_object(session)
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

    output_filename = inquirer.text(
        message="Output filename (leave blank to auto-generate):",
        instruction=f"e.g. my_accounts   →   my_accounts.csv",
    ).execute().strip()

    soql = _build_soql(fields, obj["name"])

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

    fmt_label = FORMAT_LABELS.get(output_format, output_format)
    deleted_label = "deleted included" if include_deleted else "deleted excluded"
    print_success(
        f"Added: {obj['label']} — {len(fields)} field(s) — {fmt_label} — {deleted_label}"
    )


def _remove_from_queue(queue: Queue) -> None:
    print_header("Remove from queue")
    queue.display()
    index = inquirer.number(
        message="Enter job number to remove:",
        min_allowed=1,
        max_allowed=len(queue.jobs),
    ).execute()
    removed = queue.remove(int(index))
    print_success(f"Removed: {removed.object_label} ({removed.object_name})")


def _run_queue(session, queue: Queue, settings) -> None:
    print_header(f"Running queue ({len(queue.jobs)} job(s))")

    summary_rows = []
    jobs_snapshot = list(queue.jobs)

    for i, job in enumerate(jobs_snapshot, start=1):
        console.print(f"\n  [{i}/{len(jobs_snapshot)}] [bold]{job.object_label}[/bold]")
        job_start = time.monotonic()

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

    _print_summary(summary_rows)

    # Remove only successful jobs from the queue
    failed_names = {row[0] for row in summary_rows if row[4] is not None}
    queue.jobs = [j for j in queue.jobs if j.object_name in failed_names]


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

    queue = Queue()

    while True:
        print_header("Main Menu")
        choice = inquirer.select(
            message="What would you like to do?",
            choices=_main_menu_choices(queue),
        ).execute()

        if choice == "add":
            try:
                _add_to_queue(session, queue)
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

        elif choice == "quit":
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\nExiting.")
