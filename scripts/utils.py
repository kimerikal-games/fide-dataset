"""Shared helpers for the pipeline: dataset paths, month iteration, and CLI arguments."""

import argparse
import logging
from pathlib import Path
from typing import NamedTuple, Optional, Tuple

from constants import TIME_CONTROLS

logger = logging.getLogger(__name__)


def _initialize():
    """Build the registry of dataset directories and create them on disk."""
    dirs = {}

    if dirs:
        return dirs

    root = Path(__file__).parent.parent.resolve()

    dirs["root"] = root
    dirs["raw"] = root / "dataset" / "raw"
    dirs["monthly"] = root / "dataset" / "monthly"
    dirs["players"] = root / "dataset" / "players"
    dirs["ratings"] = root / "dataset" / "ratings"
    dirs["titles"] = root / "dataset" / "titles"

    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    return dirs


_PATH_REGISTRY = _initialize()


def get_path(key: str) -> Path:
    """Return the absolute path of a registered dataset directory."""
    return _PATH_REGISTRY[key]


class MonthRange:
    """A half-open range of (year, month) pairs from ``start`` up to ``end``."""

    def __init__(self, start: Tuple[int, int], end: Tuple[int, int]):
        """Store the inclusive start and exclusive end of the range."""
        self.start = start
        self.end = end

    def __iter__(self):
        """Yield each (year, month) pair from ``start`` up to but excluding ``end``."""
        year, month = self.start
        while (year, month) < self.end:
            yield year, month
            year, month = next_month(year, month)

    def __len__(self) -> int:
        """Return the number of months in the range, clamped at zero."""
        start_total = self.start[0] * 12 + (self.start[1] - 1)
        end_total = self.end[0] * 12 + (self.end[1] - 1)
        return max(end_total - start_total, 0)


def next_month(year: int, month: int) -> Tuple[int, int]:
    """Return the (year, month) pair that follows the given month."""
    month += 1
    if month > 12:
        month = 1
        year += 1
    return year, month


def iter_months(
    start: Tuple[int, int],
    end: Tuple[int, int],
):
    """Return an iterable over the half-open month range ``[start, end)``."""
    return MonthRange(start, end)


class YearMonth(NamedTuple):
    """A (year, month) pair identifying a single monthly rating list."""

    year: int
    month: int


def add_date_range_args(parser: argparse.ArgumentParser) -> None:
    """Register the single-month and batch date-range options on ``parser``."""
    parser.add_argument(
        "--month",
        type=int,
        help="Month for single-month mode (1-12)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year for single-month mode (e.g., 2023)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start year for batch mode (e.g., 2023)",
    )
    parser.add_argument(
        "--start-month",
        type=int,
        help="Start month for batch mode (1-12)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End year for batch mode (e.g., 2023)",
    )
    parser.add_argument(
        "--end-month",
        type=int,
        help="End month for batch mode (1-12)",
    )


def resolve_date_range(
    args: argparse.Namespace,
) -> Optional[Tuple[YearMonth, YearMonth]]:
    """Resolve CLI date arguments into a half-open (start, end) month range, or ``None`` if invalid."""
    single_args = (args.month, args.year)
    batch_args = (args.start_month, args.start_year, args.end_month, args.end_year)
    has_single = any(a is not None for a in single_args)
    has_batch = any(a is not None for a in batch_args)

    if has_single and has_batch:
        logger.error("Use either single-month (--month/--year) or batch (--start-*/--end-*) options, not both.")
        return None

    if has_batch:
        if not all(a is not None for a in batch_args):
            logger.error("For batch mode, provide all of --start-month, --start-year, --end-month, --end-year.")
            return None
        return (
            YearMonth(args.start_year, args.start_month),
            YearMonth(args.end_year, args.end_month),
        )

    if has_single:
        if not all(a is not None for a in single_args):
            logger.error("Provide both --month and --year for single-month mode.")
            return None
        start = YearMonth(args.year, args.month)
        return start, YearMonth(*next_month(args.year, args.month))

    logger.error("Provide either --month and --year for a single month, or --start-month/--start-year/--end-month/--end-year for a range.")
    return None


def add_time_control_arg(parser: argparse.ArgumentParser) -> None:
    """Register the ``--time-controls`` option on ``parser``."""
    parser.add_argument(
        "--time-controls",
        nargs="+",
        choices=TIME_CONTROLS,
        default=TIME_CONTROLS,
        help="Time controls to include (default: all)",
    )


def add_log_level_arg(parser: argparse.ArgumentParser) -> None:
    """Register the ``--log-level`` option on ``parser``."""
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )
