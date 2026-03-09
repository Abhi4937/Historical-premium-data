"""
Real-time terminal dashboard for Delta Exchange OHLC fetcher.

Shows per-account live status:
  - Current month being fetched
  - Contracts progress (done / total)
  - Errors and missing data count
  - Elapsed time per account
  - Next month in queue
  - Months completed so far

Overall stats:
  - Total months done / remaining
  - Start time + elapsed wall time
  - Estimated time to finish
  - Final summary report on completion
"""

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

LOG_PATH = Path(__file__).parent / "monitor.log"


class AccountState:
    """Tracks live state for one account worker."""

    def __init__(self, name: str):
        self.name            = name
        self.status          = "waiting"       # waiting | running | done | error
        self.current_month   = "-"
        self.next_month      = "-"
        self.contracts_done  = 0
        self.contracts_total = 0
        self.errors          = 0
        self.missing         = 0
        self.months_done     : list[str] = []
        self.month_start_ts  : Optional[float] = None
        self.month_times     : list[float] = []  # seconds per month
        self._lock           = threading.Lock()

    def start_month(self, year: int, month: int, next_ym: Optional[tuple] = None):
        with self._lock:
            self.status         = "running"
            self.current_month  = f"{year}-{month:02d}"
            self.contracts_done = 0
            self.contracts_total = 0
            self.month_start_ts = time.time()
            if next_ym:
                self.next_month = f"{next_ym[0]}-{next_ym[1]:02d}"

    def finish_month(self, year: int, month: int):
        with self._lock:
            elapsed = time.time() - (self.month_start_ts or time.time())
            self.month_times.append(elapsed)
            self.months_done.append(f"{year}-{month:02d}")
            self.status = "idle"
            self.current_month = "-"

    def update_progress(self, done: int, total: int):
        with self._lock:
            self.contracts_done  = done
            self.contracts_total = total

    def add_error(self):
        with self._lock:
            self.errors += 1

    def add_missing(self):
        with self._lock:
            self.missing += 1

    def set_done(self):
        with self._lock:
            self.status        = "done"
            self.current_month = "-"
            self.next_month    = "-"

    @property
    def avg_month_time(self) -> float:
        if not self.month_times:
            return 0.0
        return sum(self.month_times) / len(self.month_times)

    def elapsed_this_month(self) -> float:
        if self.month_start_ts:
            return time.time() - self.month_start_ts
        return 0.0


class Monitor:
    """
    Live terminal dashboard. Thread-safe.

    Usage:
        monitor = Monitor(accounts, months_list)
        monitor.start()
        # ... workers call monitor.state[name].start_month(...) etc.
        monitor.stop()
        monitor.print_summary()
    """

    def __init__(self, account_names: list[str], months_list: list[tuple]):
        self.state        = {name: AccountState(name) for name in account_names}
        self.months_list  = months_list
        self.total_months = len(months_list)
        self.start_time   = datetime.now(tz=timezone.utc)
        self.start_ts     = time.time()
        self._log         : deque = deque(maxlen=12)  # last 12 log lines for dashboard
        self._log_lock    = threading.Lock()
        self._live        : Optional[Live] = None
        self._stop_event  = threading.Event()
        self._thread      : Optional[threading.Thread] = None
        self.console      = Console()

        # File logger — writes every event + periodic snapshots to monitor.log
        self._file_logger = logging.getLogger("monitor.file")
        self._file_logger.setLevel(logging.DEBUG)
        self._file_logger.propagate = False
        if not self._file_logger.handlers:
            fh = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
            self._file_logger.addHandler(fh)

        self._file_logger.info("=" * 70)
        self._file_logger.info("SESSION START  accounts=%s  total_months=%d",
                               account_names, len(months_list))
        self._file_logger.info("Months queue: %s",
                               [f"{y}-{m:02d}" for y, m in months_list])

    # ── public log method (call from workers) ────────────────────────────────

    def log(self, message: str, level: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        colour = {"info": "white", "ok": "green", "warn": "yellow", "error": "red"}.get(level, "white")
        with self._log_lock:
            self._log.append(f"[{colour}][{ts}] {message}[/{colour}]")
        # Always write to file with level prefix
        self._file_logger.info("[%-5s] %s", level.upper(), message)

    # ── start / stop ──────────────────────────────────────────────────────────

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _run(self):
        last_snapshot = time.time()
        snapshot_interval = 60  # log full state snapshot every 60 seconds

        with Live(self._build_layout(), refresh_per_second=2,
                  console=self.console, screen=False) as live:
            self._live = live
            while not self._stop_event.is_set():
                live.update(self._build_layout())

                # Periodic snapshot to file every 60s
                if time.time() - last_snapshot >= snapshot_interval:
                    self._log_snapshot()
                    last_snapshot = time.time()

                time.sleep(0.5)
            live.update(self._build_layout())

    def _log_snapshot(self):
        """Write a full state snapshot to monitor.log every 60 seconds."""
        wall = time.time() - self.start_ts
        months_done = sum(len(s.months_done) for s in self.state.values())
        self._file_logger.info("-" * 70)
        self._file_logger.info("SNAPSHOT  elapsed=%s  months=%d/%d",
                               _fmt_seconds(wall), months_done, self.total_months)
        for s in self.state.values():
            self._file_logger.info(
                "  %-12s  status=%-8s  month=%-8s  contracts=%d/%d  "
                "errors=%d  missing=%d  next=%-8s  done=%s",
                s.name, s.status, s.current_month,
                s.contracts_done, s.contracts_total,
                s.errors, s.missing, s.next_month,
                ",".join(s.months_done) or "-",
            )
        self._file_logger.info("-" * 70)

    # ── layout builders ───────────────────────────────────────────────────────

    def _build_layout(self) -> Panel:
        wall_elapsed = time.time() - self.start_ts
        months_done  = sum(len(s.months_done) for s in self.state.values())
        months_left  = self.total_months - months_done

        # Estimate remaining time
        if months_done > 0:
            avg_sec_per_month = wall_elapsed / months_done
            num_workers       = len(self.state)
            est_remaining     = (months_left / num_workers) * avg_sec_per_month
        else:
            est_remaining = 0

        # ── header ────────────────────────────────────────────────────────────
        started_str   = self.start_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        elapsed_str   = _fmt_seconds(wall_elapsed)
        remaining_str = _fmt_seconds(est_remaining) if months_done > 0 else "calculating..."

        header = Table.grid(padding=(0, 2))
        header.add_column(justify="left")
        header.add_column(justify="left")
        header.add_column(justify="left")
        header.add_column(justify="left")
        header.add_row(
            f"[bold]Started:[/bold]  [cyan]{started_str}[/cyan]",
            f"[bold]Elapsed:[/bold]  [cyan]{elapsed_str}[/cyan]",
            f"[bold]Remaining:[/bold]  [yellow]{remaining_str}[/yellow]",
            f"[bold]Progress:[/bold]  [green]{months_done}[/green]/[white]{self.total_months}[/white] months",
        )

        # ── per-account table ─────────────────────────────────────────────────
        tbl = Table(
            show_header=True,
            header_style="bold magenta",
            border_style="grey50",
            expand=True,
        )
        tbl.add_column("Account",       style="bold cyan",  min_width=12)
        tbl.add_column("Status",        min_width=10)
        tbl.add_column("Current Month", min_width=12)
        tbl.add_column("Contracts",     min_width=14)
        tbl.add_column("Elapsed",       min_width=10)
        tbl.add_column("Errors",        min_width=8)
        tbl.add_column("Missing",       min_width=8)
        tbl.add_column("Next Month",    min_width=12)
        tbl.add_column("Done Months",   min_width=30)

        for acc_state in self.state.values():
            status_text, status_style = _status_display(acc_state.status)

            # contracts progress bar
            if acc_state.contracts_total > 0:
                pct  = acc_state.contracts_done / acc_state.contracts_total
                bars = int(pct * 10)
                bar  = f"[{'█' * bars}{'░' * (10 - bars)}] {acc_state.contracts_done}/{acc_state.contracts_total}"
            else:
                bar = "-"

            elapsed_month = _fmt_seconds(acc_state.elapsed_this_month()) \
                if acc_state.status == "running" else "-"

            done_months = ", ".join(acc_state.months_done[-4:])  # last 4
            if len(acc_state.months_done) > 4:
                done_months = f"...{done_months}"

            tbl.add_row(
                acc_state.name,
                Text(status_text, style=status_style),
                acc_state.current_month,
                bar,
                elapsed_month,
                str(acc_state.errors)  if acc_state.errors  > 0 else "[green]0[/green]",
                str(acc_state.missing) if acc_state.missing > 0 else "[green]0[/green]",
                acc_state.next_month,
                done_months or "-",
            )

        # ── queue preview ─────────────────────────────────────────────────────
        active = {s.current_month for s in self.state.values() if s.current_month != "-"}
        done   = {m for s in self.state.values() for m in s.months_done}
        pending = [
            f"{y}-{m:02d}" for y, m in self.months_list
            if f"{y}-{m:02d}" not in active and f"{y}-{m:02d}" not in done
        ]
        queue_str = "  ".join(f"[dim]{p}[/dim]" for p in pending[:8])
        if len(pending) > 8:
            queue_str += f"  [dim]...+{len(pending)-8} more[/dim]"

        # ── log panel ─────────────────────────────────────────────────────────
        with self._log_lock:
            log_lines = list(self._log)
        log_text = "\n".join(log_lines) if log_lines else "[dim]No events yet[/dim]"

        # ── assemble ──────────────────────────────────────────────────────────
        grid = Table.grid(padding=(0, 1))
        grid.add_column()
        grid.add_row(header)
        grid.add_row("")
        grid.add_row(tbl)
        grid.add_row(Panel(queue_str or "[dim]Queue empty[/dim]",
                           title="[bold]Pending Queue[/bold]", border_style="grey50"))
        grid.add_row(Panel(log_text, title="[bold]Recent Events[/bold]", border_style="grey50"))

        return Panel(grid, title="[bold green] Delta Exchange OHLC Fetcher [/bold green]",
                     border_style="green")

    # ── final summary ─────────────────────────────────────────────────────────

    def print_summary(self):
        wall = time.time() - self.start_ts
        total_errors  = sum(s.errors  for s in self.state.values())
        total_missing = sum(s.missing for s in self.state.values())
        months_done   = sum(len(s.months_done) for s in self.state.values())

        # Write full summary to monitor.log
        self._file_logger.info("=" * 70)
        self._file_logger.info("SESSION END")
        self._file_logger.info("  Total wall time   : %s", _fmt_seconds(wall))
        self._file_logger.info("  Months completed  : %d / %d", months_done, self.total_months)
        self._file_logger.info("  Total errors      : %d", total_errors)
        self._file_logger.info("  Missing data gaps : %d", total_missing)
        self._file_logger.info("  Started at        : %s", self.start_time.strftime("%Y-%m-%d %H:%M:%S UTC"))
        for s in self.state.values():
            self._file_logger.info(
                "  %-12s  months=%d  avg_per_month=%s  errors=%d  missing=%d  completed=%s",
                s.name, len(s.months_done), _fmt_seconds(s.avg_month_time),
                s.errors, s.missing, ",".join(s.months_done) or "-",
            )
        self._file_logger.info("=" * 70)

        tbl = Table(title="Final Summary", header_style="bold magenta", border_style="green")
        tbl.add_column("Metric",  style="bold")
        tbl.add_column("Value",   style="cyan")

        tbl.add_row("Total wall time",    _fmt_seconds(wall))
        tbl.add_row("Months completed",   f"{months_done} / {self.total_months}")
        tbl.add_row("Total errors",       f"[red]{total_errors}[/red]" if total_errors else "[green]0[/green]")
        tbl.add_row("Missing data gaps",  f"[yellow]{total_missing}[/yellow]" if total_missing else "[green]0[/green]")
        tbl.add_row("Started at",         self.start_time.strftime("%Y-%m-%d %H:%M:%S UTC"))

        self.console.print()
        self.console.print(tbl)

        # Per-account breakdown
        acc_tbl = Table(title="Per-Account Breakdown", header_style="bold magenta", border_style="grey50")
        acc_tbl.add_column("Account")
        acc_tbl.add_column("Months done")
        acc_tbl.add_column("Avg per month")
        acc_tbl.add_column("Errors")
        acc_tbl.add_column("Missing")

        for s in self.state.values():
            acc_tbl.add_row(
                s.name,
                str(len(s.months_done)),
                _fmt_seconds(s.avg_month_time),
                str(s.errors),
                str(s.missing),
            )
        self.console.print(acc_tbl)


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_seconds(secs: float) -> str:
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s   = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _status_display(status: str) -> tuple[str, str]:
    return {
        "waiting": ("⏳ Waiting",  "yellow"),
        "idle":    ("💤 Idle",     "dim"),
        "running": ("🔄 Running",  "green"),
        "done":    ("✅ Done",     "bold green"),
        "error":   ("❌ Error",    "red"),
    }.get(status, (status, "white"))
