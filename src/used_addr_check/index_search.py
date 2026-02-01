import json
from pathlib import Path

import polars as pl
from loguru import logger

from used_addr_check.index_create import load_or_generate_index


def search_multiple_in_file(
    haystack_file_path: Path | str, needles: list[str] | str
) -> list[str]:
    """Searches for multiple needle strings in the file.

    Builds the index if not yet built.

    Returns the list of needles that were found in the file, sorted.

    Args:
        haystack_file_path (Path): The path to the file to search. Can be either
            .txt or .parquet.
        needles: The list of strings to search for in the file.

    Returns: A list of the needles that were found in the file.
    """
    if isinstance(needles, str):
        needles = [needles]

    haystack_file_path = Path(haystack_file_path)
    assert haystack_file_path.exists(), f"File not found: {haystack_file_path}"

    index_lazyframe = load_or_generate_index(haystack_file_path)

    # The parquet file is already sorted, and parquets include statistics (works very
    # similar to a B-Tree in practice), so is_in() is very efficient here.
    found_needles: list[str] = (
        index_lazyframe.select("row")
        .filter(pl.col("row").is_in(needles))
        .sort("row")
        .collect()
        .to_series()
        .to_list()
    )

    found_needles.sort()  # Ensure sorted.

    logger.info(f"Found {len(found_needles):,}/{len(needles):,} needles in the file")
    logger.info(f"Needles found: {json.dumps(sorted(found_needles))}")
    return found_needles


def search_in_file_with_index(haystack_file_path: Path, needle: str) -> bool:
    """Searches for a needle string in the file using a pre-built index.

    Builds the index if not yet built.

    Args:
        haystack_file_path: The path to the file to search.
        needle: The string to search for in the file.

    Returns: True if the `needle` string is found, False otherwise.
    """
    assert isinstance(haystack_file_path, Path)
    assert isinstance(needle, str)

    search_result = search_multiple_in_file(haystack_file_path, [needle])
    return len(search_result) > 0
