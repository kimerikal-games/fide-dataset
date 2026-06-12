"""Parse the raw monthly XML archives into one tidy Parquet file per time control and month."""

import argparse
import logging
import os
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

import lxml.etree as ET
import pandas as pd
from utils import YearMonth, add_date_range_args, add_log_level_arg, add_time_control_arg, get_path, iter_months, resolve_date_range

logger = logging.getLogger(__name__)

INT_COLUMNS = frozenset({"fideid", "rating", "games", "k"})

RECOVERABLE_ERRORS = (zipfile.BadZipFile, ET.XMLSyntaxError, ValueError)


def _main() -> int:
    """Parse CLI arguments and process the requested raw files into Parquet."""
    parser = argparse.ArgumentParser(description=("Process FIDE rating files into Parquet format. Download the raw files first using download.py."))

    add_date_range_args(parser)
    add_time_control_arg(parser)
    parser.add_argument(
        "--missing-action",
        choices=["skip", "error"],
        default="error",
        help="Action to take if a raw file is missing (default: error)",
    )
    parser.add_argument(
        "--exist-action",
        choices=["skip", "overwrite"],
        default="skip",
        help="Action to take if the processed file already exists (default: skip)",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        help="Number of parallel worker processes (default: min(CPU count, 4)). Each worker holds a full month in memory, so raise this only if you have the RAM headroom -- the standard list is the heavy one.",
    )
    add_log_level_arg(parser)

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    date_range = resolve_date_range(args)
    if date_range is None:
        return 1
    start, end = date_range

    process_rating_files(
        source=get_path("raw"),
        destination=get_path("monthly"),
        time_controls=args.time_controls,
        start=start,
        end=end,
        missing_ok=args.missing_action == "skip",
        overwrite=args.exist_action == "overwrite",
        jobs=args.jobs,
        log_level=args.log_level,
    )

    return 0


def _init_worker(log_level: str) -> None:
    """Configure logging inside a freshly spawned worker process."""
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")


def process_rating_files(
    source: Path,
    destination: Path,
    time_controls: List[str],
    start: YearMonth,
    end: YearMonth,
    missing_ok: bool = False,
    overwrite: bool = False,
    jobs: Optional[int] = None,
    log_level: str = "INFO",
) -> None:
    """Process every requested file, dispatching to worker processes when ``jobs`` exceeds one."""
    destination.mkdir(parents=True, exist_ok=True)

    tasks = [(time_control, year, month) for time_control in time_controls for year, month in iter_months(start, end)]
    if not tasks:
        return

    if jobs is None:
        jobs = min(os.cpu_count() or 1, 4)
    jobs = max(1, min(jobs, len(tasks)))

    if jobs == 1:
        for time_control, year, month in tasks:
            try:
                process_rating_file(
                    source=source,
                    destination=destination,
                    time_control=time_control,
                    year=year,
                    month=month,
                    missing_ok=missing_ok,
                    overwrite=overwrite,
                )
            except RECOVERABLE_ERRORS as exc:
                logger.error("Failed to process %s %04d-%02d: %s", time_control, year, month, exc)
        return

    logger.info("Processing %d file(s) across %d worker(s)", len(tasks), jobs)
    with ProcessPoolExecutor(
        max_workers=jobs,
        initializer=_init_worker,
        initargs=(log_level,),
    ) as executor:
        futures = {
            executor.submit(
                process_rating_file,
                source=source,
                destination=destination,
                time_control=time_control,
                year=year,
                month=month,
                missing_ok=missing_ok,
                overwrite=overwrite,
            ): (time_control, year, month)
            for (time_control, year, month) in tasks
        }
        for future in as_completed(futures):
            time_control, year, month = futures[future]
            try:
                future.result()
            except RECOVERABLE_ERRORS as exc:
                logger.error("Failed to process %s %04d-%02d: %s", time_control, year, month, exc)
            except BaseException:
                executor.shutdown(cancel_futures=True)
                raise


def process_rating_file(
    source: Path,
    destination: Path,
    time_control: str,
    year: int,
    month: int,
    missing_ok: bool = False,
    overwrite: bool = False,
) -> None:
    """Parse one raw archive into a Parquet file, prefixing it with time control, year, and month."""
    filestem = f"{time_control}_{year:04d}-{month:02d}"
    source_path = source / f"{filestem}.xml.zip"
    dest_path = destination / f"{filestem}.parquet"

    if dest_path.exists() and not overwrite:
        logger.info("Skipping %s (already processed).", filestem)
        return

    if not source_path.exists():
        if missing_ok:
            logger.warning("Skipping %s (raw file missing).", filestem)
            return
        raise FileNotFoundError(f"Raw file not found: {source_path}")

    logger.info("Processing %s", filestem)
    logger.debug("Reading %s", source_path)
    df = parse_rating_file(source_path)
    df.insert(0, "time_control", time_control)
    df.insert(1, "year", year)
    df.insert(2, "month", month)

    logger.debug("Writing %s", dest_path)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    try:
        df.to_parquet(tmp_path, index=False, compression="zstd")
        tmp_path.replace(dest_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.debug("Finished %s", filestem)


def parse_rating_file(file: Path) -> pd.DataFrame:
    """Stream the XML player records out of a zipped rating list into a typed DataFrame."""
    with zipfile.ZipFile(file) as zf:
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if len(xml_names) != 1:
            raise ValueError(f"Expected exactly one XML member in {file}, found {xml_names}")
        with zf.open(xml_names[0]) as fh:
            rows = []
            for _, elem in ET.iterparse(fh, events=("end",), tag="player"):
                row = dict(elem.attrib)
                row.update({child.tag: child.text for child in elem})
                rows.append(row)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

    df = pd.DataFrame(rows)

    for col in df.columns:
        if col in INT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif col == "birthday":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
        else:
            df[col] = df[col].astype("string")

    return df


if __name__ == "__main__":
    sys.exit(_main())
