# FIDE Ratings

A small pipeline that turns the monthly [FIDE](https://ratings.fide.com/) rating lists into tidy, analysis-ready [Parquet](https://parquet.apache.org/) datasets.

FIDE publishes its standard, rapid, and blitz rating lists once a month as zipped XML files.
This project downloads those archives, parses them into one Parquet file per month, and aggregates them into three clean tables covering players, titles, and the full rating history.

The bundled datasets span **February 2015 to April 2026**.

## Datasets

The pipeline produces three tables under `dataset/`:

Fast download links:
- [players.parquet](dataset/players/players.parquet)
- [ratings.parquet](dataset/ratings/ratings.parquet)
- [titles.parquet](dataset/titles/titles.parquet)

### `players/players.parquet`

One row per player, holding their most recent biographical details.

| Column     | Type   | Description                          |
| ---------- | ------ | ----------------------------------- |
| `fideid`   | int64  | FIDE player ID (unique key)         |
| `name`     | string | Player name                         |
| `country`  | string | Federation code (e.g. `ARG`, `USA`) |
| `birthday` | int64  | Year of birth (`0` if unknown)      |
| `sex`      | string | `M` or `F`                          |

### `ratings/ratings.parquet`

The full monthly rating time series, one row per player, time control, and month.

| Column         | Type   | Description                       |
| -------------- | ------ | --------------------------------- |
| `fideid`       | int64  | FIDE player ID                    |
| `time_control` | string | `standard`, `rapid`, or `blitz`   |
| `year`         | int64  | List year                         |
| `month`        | int64  | List month (1–12)                 |
| `rating`       | int64  | Rating for that month             |
| `games`        | int64  | Games played in the rating period |
| `k`            | int64  | K-factor                          |

### `titles/titles.parquet`

One row per player, month, and title, unnesting the comma-separated title columns FIDE publishes.

| Column       | Type   | Description                                                |
| ------------ | ------ | --------------------------------------------------------- |
| `fideid`     | int64  | FIDE player ID                                            |
| `year`       | int64  | List year                                                 |
| `month`      | int64  | List month (1–12)                                         |
| `title_type` | string | Source column: `title`, `w_title`, `o_title`, `foa_title` |
| `title`      | string | Title code (e.g. `GM`, `IM`, `FM`)                        |

## Installation

The project uses [uv](https://docs.astral.dev/uv/) for dependency management.

```bash
uv sync
```

## Usage

The pipeline has three stages. Run them in order, scoping each to the months you want with either single-month (`--month`/`--year`) or batch (`--start-*`/`--end-*`) arguments.

```bash
# 1. Download the raw monthly archives into dataset/raw/
uv run scripts/download.py --start-year 2015 --start-month 2 --end-year 2026 --end-month 5

# 2. Parse the archives into per-month Parquet files in dataset/monthly/
uv run scripts/process.py --start-year 2015 --start-month 2 --end-year 2026 --end-month 5

# 3. Aggregate the monthly files into the published datasets
uv run scripts/build.py
```

A single month can be fetched with the single-month form:

```bash
uv run scripts/download.py --year 2026 --month 4
```

Note that date ranges are **half-open**: the end month is exclusive. The examples above therefore cover February 2015 through April 2026.

## Pipeline layout

```
scripts/
  download.py   Download raw monthly XML archives from FIDE.
  process.py    Parse each archive into a tidy per-month Parquet file.
  build.py      Aggregate the monthly files into the published datasets.
  utils.py      Shared paths, month iteration, and CLI argument helpers.
  constants.py  Month names, time controls, and the download URL base.

dataset/
  raw/          Downloaded archives          (intermediate)
  monthly/      Per-month parsed Parquet      (intermediate)
  players/      Published players dataset
  ratings/      Published ratings dataset
  titles/       Published titles dataset
```

## Data source and license

Rating data is sourced from the official FIDE rating list downloads at <https://ratings.fide.com/>.
FIDE retains all rights to the underlying data; please review their terms before redistributing it.
This repository is not affiliated with FIDE.
It is an independent project to make FIDE rating data more accessible for analysis and research.
For official data and terms, please refer to FIDE's website.

The code in this repository is released under the MIT License (see `LICENSE`).
