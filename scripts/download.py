"""Download monthly FIDE rating list archives from ratings.fide.com into ``dataset/raw``."""

import argparse
import logging
import sys
from pathlib import Path
from typing import List

import requests
from constants import MONTH_NAMES, URL_BASE
from utils import YearMonth, add_date_range_args, add_log_level_arg, add_time_control_arg, get_path, iter_months, resolve_date_range

logger = logging.getLogger(__name__)


def _main() -> int:
    """Parse CLI arguments and download the requested rating files."""
    parser = argparse.ArgumentParser(description="Download FIDE rating files.")

    add_date_range_args(parser)
    add_time_control_arg(parser)
    parser.add_argument(
        "--exist-action",
        choices=["skip", "overwrite"],
        default="skip",
        help="Action to take if file already exists (default: skip)",
    )
    add_log_level_arg(parser)

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    date_range = resolve_date_range(args)
    if date_range is None:
        return 1
    start, end = date_range

    with requests.Session() as session:
        download_rating_files(
            session=session,
            destination=get_path("raw"),
            time_controls=args.time_controls,
            start=start,
            end=end,
            overwrite=args.exist_action == "overwrite",
        )

    return 0


def download_rating_files(
    session: requests.Session,
    destination: Path,
    time_controls: List[str],
    start: YearMonth,
    end: YearMonth,
    overwrite: bool = False,
) -> None:
    """Download every requested time control and month, logging and skipping any that fail."""
    destination.mkdir(parents=True, exist_ok=True)
    for time_control in time_controls:
        for year, month in iter_months(start, end):
            try:
                download_rating_file(
                    session=session,
                    destination=destination,
                    time_control=time_control,
                    year=year,
                    month=month,
                    overwrite=overwrite,
                )
            except requests.RequestException as exc:
                logger.error(
                    "Failed to download %s %04d-%02d: %s",
                    time_control,
                    year,
                    month,
                    exc,
                )


def download_rating_file(
    session: requests.Session,
    destination: Path,
    time_control: str,
    year: int,
    month: int,
    overwrite: bool = False,
    timeout: float = 30.0,
) -> None:
    """Download a single time control and month, streaming it to disk via a temporary file."""
    dstfilename = f"{time_control}_{year:04d}-{month:02d}.xml.zip"
    zip_path = destination / dstfilename

    if zip_path.exists() and not overwrite:
        logger.info("Skipping %s (already exists).", dstfilename)
        return

    srcfilename = f"{time_control}_{MONTH_NAMES[month - 1]}{year % 100:d}frl_xml.zip"
    url = f"{URL_BASE}{srcfilename}"
    logger.info("Downloading %s from %s", dstfilename, url)

    tmp_path = zip_path.with_suffix(zip_path.suffix + ".part")
    try:
        with session.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as file_handle:
                for chunk in response.iter_content(chunk_size=65536):
                    file_handle.write(chunk)
        tmp_path.replace(zip_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.debug("Wrote %s", dstfilename)


if __name__ == "__main__":
    sys.exit(_main())
