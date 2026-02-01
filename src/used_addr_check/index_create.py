from pathlib import Path

import polars as pl
from loguru import logger


def _generate_index(haystack_file_path: Path, index_parquet_file_path: Path) -> None:
    """Convert the haystack_file_path into a searchable Parquet file.

    Args:
        haystack_file_path (Path): Path to the file to be indexed.
        index_parquet_file_path (Path): Path where the Parquet index file will be saved.

    Returns:
        Path of the created Parquet file.
    """

    logger.debug(f"Generating index at: {index_parquet_file_path}")

    pl.scan_csv(
        haystack_file_path,
        has_header=False,
        schema={"row": pl.String},
        separator="\n",
    ).sink_parquet(
        index_parquet_file_path,
        # Tested ztd:15 (high compression for smaller index size). Size difference from
        # default was very minimal (37GB vs 35GB). Much faster with default settings
        # (ztd:3 by default).
    )

    logger.debug(f"Index generated at: {index_parquet_file_path}")


def load_or_generate_index(
    haystack_file_path: Path,
    *,
    force_recreate: bool = False,
) -> pl.LazyFrame:
    """Attempts to load an index from a file, or generates one if it doesn't,
    or if `force_recreate` is enabled.

    Tries to load the index from a Parquet file first, then from a JSON file.

    If a file already exists, the `index_chunk_size` is ignored.
    """
    index_parquet_file_path = haystack_file_path.with_suffix(".parquet")

    if force_recreate or (not index_parquet_file_path.exists()):
        logger.info(f"Creating index for file: {haystack_file_path.name}")
        _generate_index(
            haystack_file_path,
            index_parquet_file_path=index_parquet_file_path,  # Output file.
        )

    index_length_rows = (
        pl.scan_parquet(index_parquet_file_path).select(pl.len()).collect().item()
    )
    logger.info(
        f"Index at {index_parquet_file_path.name} contains "
        f"{index_length_rows:,} rows, "
        f"{index_parquet_file_path.stat().st_size:,} bytes in disk."
    )

    return pl.scan_parquet(index_parquet_file_path)
