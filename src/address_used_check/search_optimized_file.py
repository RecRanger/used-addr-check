from typing import List
from pathlib import Path

from loguru import logger


def search_file(
    optimized_file_path: Path, search_queries: List[str] | str
) -> None:
    if isinstance(search_queries, str):
        search_queries = [search_queries]
    assert isinstance(
        search_queries, list
    ), f"Invalid search queries: {search_queries}"

    assert isinstance(
        optimized_file_path, Path
    ), f"Invalid file path: {optimized_file_path}"
    assert (
        optimized_file_path.exists()
    ), f"File not found: {optimized_file_path}"

    logger.info(f"Searching file: {optimized_file_path}")
    logger.info(f"Search queries: {search_queries}")

    raise NotImplementedError("search_file() not implemented yet")
