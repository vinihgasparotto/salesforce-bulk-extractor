from __future__ import annotations

from dataclasses import dataclass, field

from rich.table import Table

from .display import console

FORMAT_LABELS = {
    "csv": "CSV (API names)",
    "csv_labels": "CSV (labels)",
    "json": "JSON",
    "parquet": "Parquet",
    "excel": "Excel (.xlsx)",
}


@dataclass
class ExtractJob:
    object_name: str
    object_label: str
    fields: list[str]
    field_labels: dict[str, str]
    include_deleted: bool
    output_format: str
    soql: str
    output_filename: str = ""  # empty = auto-generate with timestamp


@dataclass
class Queue:
    jobs: list[ExtractJob] = field(default_factory=list)

    def add(self, job: ExtractJob) -> None:
        self.jobs.append(job)

    def remove(self, index: int) -> ExtractJob:
        if index < 1 or index > len(self.jobs):
            raise IndexError(f"No job at position {index}. Queue has {len(self.jobs)} item(s).")
        return self.jobs.pop(index - 1)

    def is_empty(self) -> bool:
        return len(self.jobs) == 0

    def display(self) -> None:
        if self.is_empty():
            console.print("  [dim]Queue is empty.[/dim]")
            return

        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        table.add_column("#", style="dim", width=3)
        table.add_column("Object", min_width=20)
        table.add_column("Fields", justify="right", width=8)
        table.add_column("Deleted", width=9)
        table.add_column("Format", width=16)
        table.add_column("Filename", min_width=16)

        for i, job in enumerate(self.jobs, start=1):
            table.add_row(
                str(i),
                f"{job.object_label} ({job.object_name})",
                str(len(job.fields)),
                "Yes" if job.include_deleted else "No",
                FORMAT_LABELS.get(job.output_format, job.output_format),
                job.output_filename or "[dim]auto[/dim]",
            )

        console.print(table)
