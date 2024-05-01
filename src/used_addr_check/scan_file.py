from pathlib import Path
from typing import List
import re

from loguru import logger

from used_addr_check.index_search import search_multiple_in_file


def extract_addresses_from_file(text_file_path: Path) -> List[str]:
    """
    Extracts bitcoin addresses from a file.

    Args:
    - text_file_path (Path): The path to the file to extract addresses from.

    Returns:
    - List[str]: A list of bitcoin addresses found in the file.
    """
    assert isinstance(text_file_path, Path)

    # TODO: implement chunk scanning for large files
    # TODO: add support for weird address formats (e.g., the ones at the
    # tail of the address list file)

    with text_file_path.open("r") as file:
        return re.findall(r"[13][a-km-zA-HJ-NP-Z1-9]{25,34}", file.read())


def scan_file_for_used_addresses(
    haystack_file_path: Path, needle_file_path: Path
):
    """
    Scans a file for bitcoin addresses, and see which one have been used.

    Args:
    - haystack_file_path (Path): The path to the file to scan.
    - needle_file_path (Path): The path to the file with the list of addresses
        to search for in the haystack file.
    """
    assert isinstance(haystack_file_path, Path)
    assert isinstance(needle_file_path, Path)

    needle_addresses = extract_addresses_from_file(needle_file_path)
    logger.info(
        f"Extracted {len(needle_addresses):,} addresses from the needle file"
    )
    matched_addresses = search_multiple_in_file(
        haystack_file_path, needle_addresses
    )
    logger.info(f"Found {len(matched_addresses):,} used addresses in the file")
