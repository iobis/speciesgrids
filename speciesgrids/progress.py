"""Colored progress / stage logging helpers."""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Callable

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


console = Console(stderr=True)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging with Rich (colored) output."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True)],
        force=True,
    )


def progress_bar(*, transient: bool = False) -> Progress:
    """Standard Rich progress bar with ETA (for countable Python loops)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=transient,
    )


@contextmanager
def stage(description: str):
    """Log a timed build stage with colored start/finish lines."""
    logger = logging.getLogger("speciesgrids")
    logger.info("[cyan]→ %s[/cyan]", description)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - started
        logger.error("[red]✗ %s failed after %.1fs[/red]", description, elapsed)
        raise
    elapsed = time.perf_counter() - started
    logger.info("[green]✓ %s[/green] [dim](%.1fs)[/dim]", description, elapsed)


@contextmanager
def busy(description: str, *, detail: str | None = None):
    """Spinner + elapsed time for blocking work without a reliable counter (e.g. DuckDB COPY)."""
    logger = logging.getLogger("speciesgrids")
    if detail:
        logger.info("[dim]%s[/dim]", detail)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
        refresh_per_second=4,
    ) as progress:
        progress.add_task(description, total=None)
        yield


def run_busy(description: str, fn: Callable[[], None], *, detail: str | None = None) -> None:
    """Run ``fn`` while showing a spinner; log a heartbeat every 30s; re-raise worker errors."""
    logger = logging.getLogger("speciesgrids")
    error: list[BaseException] = []
    started = time.perf_counter()

    def _target():
        try:
            fn()
        except BaseException as exc:  # noqa: BLE001 — surface any failure to caller
            error.append(exc)

    thread = threading.Thread(target=_target, name="speciesgrids-busy", daemon=True)
    with busy(description, detail=detail):
        thread.start()
        next_heartbeat = 30.0
        while thread.is_alive():
            thread.join(timeout=0.25)
            elapsed = time.perf_counter() - started
            if elapsed >= next_heartbeat:
                logger.info(
                    "[dim]… still working: %s (%.0fs elapsed)[/dim]",
                    description,
                    elapsed,
                )
                next_heartbeat += 30.0
    if error:
        raise error[0]
