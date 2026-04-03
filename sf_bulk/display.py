from rich.console import Console
from rich.rule import Rule

console = Console()


def print_error(msg: str) -> None:
    console.print(f"[bold red][ERROR][/bold red] {msg}")


def print_success(msg: str) -> None:
    console.print(f"[bold green][OK][/bold green] {msg}")


def print_warning(msg: str) -> None:
    console.print(f"[bold yellow][WARN][/bold yellow] {msg}")


def print_header(title: str) -> None:
    console.print(Rule(f"[bold]{title}[/bold]", style="dim"))
