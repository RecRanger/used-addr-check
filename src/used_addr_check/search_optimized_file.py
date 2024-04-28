import bisect
from typing import List
from pathlib import Path

import numpy as np
from loguru import logger

from used_addr_check.optimized_file import OptimizedFilePreambleMetadata


def read_hashes_into_np_array(file_path: Path) -> np.ndarray:
    """
    Reads a file containing 8-byte hashes and returns a sorted array of ints.
    """
    logger.info(f"Reading hashes from file: {file_path}")

    metadata = OptimizedFilePreambleMetadata.from_file_preamble(file_path)
    logger.info(f"Loaded metadata: {metadata}")

    with open(file_path, "rb") as file:
        # Seek past the preamble to the start of the hashes
        file.seek(metadata.preamble_length_bytes)

        # Directly read the data into a numpy array
        np_array = np.fromfile(
            file, dtype=metadata.np_dtype, count=metadata.num_hashes
        )

    logger.info(f"Read the array into memory. Length: {len(np_array):,}")
    assert len(np_array) == metadata.num_hashes, (
        f"Expected {metadata.num_hashes} hashes, "
        f"but found {len(np_array)} in the file."
    )

    np_array.sort()
    logger.info(f"Sorted the array. Shape: {np_array.shape}")

    return np_array


def _do_bisect_search(haystack, needle: int) -> int | None:
    index = bisect.bisect_left(haystack, needle)
    if (index != len(haystack)) and (haystack[index] == needle):
        return index
    return None


def search_file(
    optimized_file_path: Path, search_queries: List[str] | str
) -> List[str]:
    """Searches for the given queries in the optimized file.
    Returns a list of queries that were found in the file.
    """
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

    metadata = OptimizedFilePreambleMetadata.from_file_preamble(
        optimized_file_path
    )

    haystack_np_array = read_hashes_into_np_array(optimized_file_path)

    found_queries: list[str] = []
    for query in search_queries:
        logger.info(f"Searching for query: {query}")

        # Compute the hash of the line
        hasher = metadata.hash_func()
        # NOTE: line contains the newline character at the end
        hasher.update(query.strip())
        # Retrieve the hash value as an integer
        query_hash_value = hasher.intdigest()

        search_result = _do_bisect_search(haystack_np_array, query_hash_value)
        if search_result:
            logger.info(f"Found: {query} (index={search_result:,})")
            found_queries.append(query)
        else:
            logger.info(f"Not found: {query}")

    return found_queries
