from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from pathlib import Path

from rich.console import Console

from .auth import SalesforceSession, _raise_sf_error
from .queue import ExtractJob

MAX_RECORDS_PER_PAGE = 50_000

FORMAT_EXT = {
    "csv": ".csv",
    "csv_labels": ".csv",
    "json": ".json",
    "parquet": ".parquet",
    "excel": ".xlsx",
}


def download_results(
    session: SalesforceSession,
    job_id: str,
    job: ExtractJob,
    output_dir: str,
    console: Console,
) -> tuple[Path, int]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ext = FORMAT_EXT.get(job.output_format, ".csv")
    if job.output_filename:
        stem = Path(job.output_filename).stem  # strip any extension the user typed
        output_path = Path(output_dir) / f"{stem}{ext}"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir) / f"{job.object_name}_{timestamp}{ext}"

    if job.output_format == "csv":
        total_rows = _download_csv(session, job_id, output_path, console)
    elif job.output_format == "csv_labels":
        total_rows = _download_csv_labels(session, job_id, job, output_path, console)
    else:
        rows = _fetch_all_rows(session, job_id, console)
        total_rows = len(rows)
        if job.output_format == "json":
            _write_json(rows, output_path)
        elif job.output_format == "parquet":
            _write_parquet(rows, output_path)
        elif job.output_format == "excel":
            _write_excel(rows, job.object_name, output_path)

    return output_path, total_rows


def _iter_pages(session: SalesforceSession, job_id: str):
    locator = None
    while True:
        params: dict = {"maxRecords": MAX_RECORDS_PER_PAGE}
        if locator:
            params["locator"] = locator

        resp = session.get(
            f"/jobs/query/{job_id}/results",
            params=params,
            timeout=120,
        )
        if not resp.ok:
            _raise_sf_error(resp)

        yield resp.text

        locator_header = resp.headers.get("Sforce-Locator", "null")
        if locator_header == "null":
            break
        locator = locator_header


def _download_csv(
    session: SalesforceSession,
    job_id: str,
    output_path: Path,
    console: Console,
) -> int:
    total_rows = 0
    first_page = True

    with console.status("[dim]Downloading...[/dim]") as status:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            for page_text in _iter_pages(session, job_id):
                lines = page_text.splitlines()
                if first_page:
                    f.write(page_text)
                    total_rows += max(0, len(lines) - 1)
                    first_page = False
                else:
                    body = "\n".join(lines[1:])
                    if body:
                        f.write("\n" + body)
                    total_rows += max(0, len(lines) - 1)
                status.update(f"[dim]Downloading... {total_rows:,} rows written[/dim]")

    return total_rows


def _download_csv_labels(
    session: SalesforceSession,
    job_id: str,
    job: ExtractJob,
    output_path: Path,
    console: Console,
) -> int:
    total_rows = 0
    header_written = False
    api_header: list[str] = []

    with console.status("[dim]Downloading...[/dim]") as status:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for page_text in _iter_pages(session, job_id):
                reader = csv.reader(io.StringIO(page_text))
                rows = list(reader)
                if not rows:
                    continue

                if not header_written:
                    api_header = rows[0]
                    label_header = [job.field_labels.get(col, col) for col in api_header]
                    writer.writerow(label_header)
                    header_written = True
                    data_rows = rows[1:]
                else:
                    data_rows = rows[1:]  # skip repeated header

                for row in data_rows:
                    writer.writerow(row)
                total_rows += len(data_rows)
                status.update(f"[dim]Downloading... {total_rows:,} rows written[/dim]")

    return total_rows


def _fetch_all_rows(
    session: SalesforceSession,
    job_id: str,
    console: Console,
) -> list[dict]:
    all_rows: list[dict] = []
    header: list[str] = []

    with console.status("[dim]Downloading...[/dim]") as status:
        for page_text in _iter_pages(session, job_id):
            reader = csv.reader(io.StringIO(page_text))
            rows = list(reader)
            if not rows:
                continue

            if not header:
                header = rows[0]
                data_rows = rows[1:]
            else:
                data_rows = rows[1:]

            all_rows.extend(dict(zip(header, row)) for row in data_rows)
            status.update(f"[dim]Downloading... {len(all_rows):,} rows[/dim]")

    return all_rows


def _write_json(rows: list[dict], output_path: Path) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def _write_parquet(rows: list[dict], output_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        table = pa.table({})
    else:
        columns: dict[str, list] = {key: [] for key in rows[0]}
        for row in rows:
            for key, val in row.items():
                columns[key].append(val)
        table = pa.table(columns)

    pq.write_table(table, str(output_path))


def _write_excel(rows: list[dict], sheet_name: str, output_path: Path) -> None:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]  # Excel sheet name limit

    if not rows:
        wb.save(output_path)
        return

    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, "") for h in headers])

    wb.save(output_path)
