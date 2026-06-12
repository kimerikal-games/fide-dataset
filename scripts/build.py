"""Aggregate the monthly Parquet files into the published players, titles, and ratings datasets."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Protocol, Any

import polars as pl

from utils import add_log_level_arg, get_path

logger = logging.getLogger(__name__)


def _main() -> int:
    """Parse CLI arguments and build each requested target dataset."""
    parser = argparse.ArgumentParser(description="Build the dataset from the processed monthly Parquet files.")

    parser.add_argument(
        "--targets",
        nargs="+",
        choices=["players", "titles", "ratings", "all"],
        default=["all"],
        help="Which dataset to build (default: all)",
    )
    add_log_level_arg(parser)

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    if "all" in args.targets:
        targets = list(_BUILD_FUNCTIONS)
    else:
        targets = list(dict.fromkeys(args.targets))

    for target in targets:
        logger.info("Building %s dataset...", target)
        build_function = _BUILD_FUNCTIONS[target]
        build_function(
            source=get_path("monthly"),
            destination=get_path(target),
        )

    return 0


class _BuildFunction(Protocol):
    """Callable that reads the monthly files in ``source`` and writes one dataset to ``destination``."""

    def __call__(self, source: Path, destination: Path) -> None:
        """Build the dataset from ``source`` and write it under ``destination``."""
        ...


_BUILD_FUNCTIONS: dict[str, _BuildFunction] = {}


def register_build_function(name: str):
    """Return a decorator that registers a build function under ``name``."""

    def decorator(func: _BuildFunction) -> _BuildFunction:
        """Register ``func`` in the build registry and return it unchanged."""
        _BUILD_FUNCTIONS[name] = func
        return func

    return decorator


SCHEMA: dict[str, Any] = {
    "time_control": pl.String,
    "year": pl.Int64,
    "month": pl.Int64,
    "fideid": pl.Int64,
    "name": pl.String,
    "country": pl.String,
    "sex": pl.String,
    "title": pl.String,
    "w_title": pl.String,
    "o_title": pl.String,
    "rating": pl.Int64,
    "games": pl.Int64,
    "k": pl.Int64,
    "birthday": pl.Int64,
    "flag": pl.String,
    "foa_title": pl.String,
}


def _scan_monthly(source: Path) -> pl.LazyFrame:
    """Lazily scan every monthly Parquet file under ``source`` against the canonical schema."""
    return pl.scan_parquet(
        source / "*.parquet",
        schema=SCHEMA,
        missing_columns="insert",
    )


# fmt: off
@register_build_function("players")
def build_players(source: Path, destination: Path) -> None:
    """Write one row per player holding their most recent name, country, birthday, and sex."""
    players_lazy = (
        _scan_monthly(source)
        .select(["fideid", "name", "country", "birthday", "sex", "year", "month"])
        .with_columns(ym=pl.col("year") * 100 + pl.col("month"))
    )

    latest_ym = players_lazy.group_by("fideid").agg(pl.col("ym").max())

    players_lazy = (
        players_lazy
        .join(latest_ym, on=["fideid", "ym"], how="semi")
        .unique(subset="fideid", keep="any")
        .select(["fideid", "name", "country", "birthday", "sex"])
        .sort("fideid")
    )

    players_lazy.sink_parquet(destination / "players.parquet", compression="zstd", mkdir=True)


@register_build_function("titles")
def build_titles(source: Path, destination: Path) -> None:
    """Write one row per (player, month, title type, title), unnesting the comma-separated title columns."""
    titles_lazy = _scan_monthly(source).select(
        ["fideid", "year", "month", "title", "w_title", "o_title", "foa_title"]
    )

    title_cols = ["title", "w_title", "o_title", "foa_title"]

    def extract_titles(col: str) -> pl.LazyFrame:
        """Split one title column into individual rows, dropping empty and placeholder values."""
        return (
            titles_lazy
            .select([
                "fideid", "year", "month",
                pl.col(col)
                  .str.split(",")
                  .list.eval(pl.element().str.strip_chars())
                  .list.eval(pl.element().filter((pl.element() != "") & (pl.element() != "0")))
                  .alias("title"),
            ])
            .explode("title")
            .filter(pl.col("title").is_not_null())
            .with_columns(title_type=pl.lit(col))
            .select(["fideid", "year", "month", "title_type", "title"])
        )

    titles_lazy = (
        pl.concat(
            [extract_titles(col) for col in title_cols],
            how="vertical",
        )
        .unique(subset=["fideid", "year", "month", "title_type", "title"])
        .sort(["fideid", "year", "month", "title_type", "title"])
    )

    titles_lazy.sink_parquet(destination / "titles.parquet", compression="zstd", mkdir=True)


@register_build_function("ratings")
def build_ratings(source: Path, destination: Path) -> None:
    """Write the full rating time series of one row per player, time control, and month."""
    ratings_lazy = (
        _scan_monthly(source)
        .select(["fideid", "time_control", "year", "month", "rating", "games", "k"])
        .sort(["fideid", "time_control", "year", "month"])
    )

    ratings_lazy.sink_parquet(
        destination / "ratings.parquet",
        compression="zstd",
        mkdir=True,
    )
# fmt: on

if __name__ == "__main__":
    sys.exit(_main())
