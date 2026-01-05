import bisect
from pathlib import Path
from typing import List
import json

from loguru import logger
from tqdm import tqdm

from used_addr_check.index_create import load_or_generate_index
from used_addr_check.index_types import IndexEntry
from used_addr_check.defaults import DEFAULT_INDEX_CHUNK_SIZE


def _binary_search_index(index: List[IndexEntry], needle: str) -> int:
    """
    Performs a binary search on the index to find the closest position for the
    needle string.

    Args:
    - index (List[IndexEntry]): The index list, as returned by `create_index()`
    - needle (str): The string to find in the index.

    Returns:
    - int: The index position (index into `index` list), to the left of where
        the needle could/would be located.
    """
    index_values = [entry.line_value for entry in index]
    idx = bisect.bisect_left(index_values, needle)

    # not sure what this condition is about
    if idx == len(index):
        return idx - 1

    if index[idx].line_value > needle:
        # common occurrence
        return idx - 1
    elif index[idx].line_value == needle:
        # rare occurrence - exact match on one of the index keys (e.g., 0)
        return idx
    elif index[idx].line_value < needle:
        # not sure if this can ever happen
        raise Exception("bisect.bisect_left failed to find the correct position")
    else:
        raise Exception("Unexpected condition")


def search_in_file_with_index(
    haystack_file_path: Path, needle: str, index: List[IndexEntry]
) -> bool:
    """
    Searches for a needle string in the file using a pre-built index to narrow
    down the search area.

    Args:
    - haystack_file_path (Path): The path to the file to search.
    - needle (str): The string to search for in the file.
    - index (List[Tuple[str, int, int]]): The index as built by `create_index`.

    Returns: True if the `needle` string is found, False otherwise.
    """
    assert isinstance(haystack_file_path, Path)
    assert isinstance(needle, str)
    assert isinstance(index, list)

    position = _binary_search_index(index, needle)
    # logger.debug(f"Search {needle}: Binary search position: {position}")
    # logger.debug(f"Search {needle}: {index[position]}")
    if position == len(index):
        # logger.debug(
        #     "Binary search position equals index length, returning False"
        # )
        return False

    # Find the bounds to search within the file
    start_offset = index[position].byte_offset
    end_offset = None
    if position + 1 < len(index):
        end_offset = index[position + 1].byte_offset

    with open(haystack_file_path, "r", encoding="ascii") as file:
        file.seek(start_offset)
        while True:
            if end_offset and file.tell() >= end_offset:
                break
            line = file.readline()
            if not line:
                break
            if line.strip() == needle:
                return True
    return False


def search_multiple_in_file(
    haystack_file_path: Path | str,
    needles: List[str] | str,
    index_chunk_size: int = DEFAULT_INDEX_CHUNK_SIZE,
) -> List[str]:
    """
    Searches for multiple needle strings in the file by pre-building an index
    and then searching within the file.

    Args:
    - haystack_file_path (Path): The path to the file to search.
    - needles (List[str]): The list of strings to search for in the file.

    Returns: A list of the needles that were found in the file.
    """
    if isinstance(needles, str):
        needles = [needles]

    haystack_file_path = Path(haystack_file_path)  # normalize to Path
    assert haystack_file_path.exists(), f"File not found: {haystack_file_path}"

    index = load_or_generate_index(haystack_file_path, index_chunk_size)

    # do the search
    found_needles = []
    for needle in tqdm(needles, desc="Searching needles", unit="needle"):
        if search_in_file_with_index(haystack_file_path, needle, index=index):
            found_needles.append(needle)
    logger.info(f"Found {len(found_needles):,}/{len(needles):,} needles in the file")
    logger.info(f"Needles found: {json.dumps(sorted(found_needles))}")
    return found_needles
