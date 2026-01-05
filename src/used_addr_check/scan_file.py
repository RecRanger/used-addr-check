from pathlib import Path
from typing import List, Literal
import re

from loguru import logger
from ripgrepy import Ripgrepy, RipGrepNotFound

from used_addr_check.index_search import search_multiple_in_file
from used_addr_check.defaults import DEFAULT_INDEX_CHUNK_SIZE

# BITCOIN_ADDR_REGEX = r"[13][a-km-zA-HJ-NP-Z1-9]{25,34}"

# Source: https://ihateregex.io/expr/bitcoin-address/
BITCOIN_ADDR_REGEX = r"\b((bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39})\b"


def _extract_addresses_from_file_python_re(text_file_path: Path) -> List[str]:
    """
    Extracts bitcoin addresses from a file using Python regex.

    Args:
    - text_file_path (Path): The path to the file to extract addresses from.

    Returns:
    - List[str]: A list of bitcoin addresses found in the file.
    """

    # TODO: could do this in chunks if we wanted, but using ripgrep instead

    logger.info("Using Python regex search")
    results = re.findall(BITCOIN_ADDR_REGEX, text_file_path.read_text(encoding="utf-8"))

    return [result[0] for result in results]


def _extract_addresses_from_file_ripgrep(text_file_path: Path) -> List[str]:
    """
    Extracts bitcoin addresses from a file using ripgrep.

    Args:
    - text_file_path (Path): The path to the file to extract addresses from.

    Returns:
    - List[str]: A list of bitcoin addresses found in the file.

    Raises: RipGrepNotFound: If ripgrep is not installed.
    """

    logger.info("Trying using ripgrep search")
    rg = Ripgrepy(
        regex_pattern=BITCOIN_ADDR_REGEX,
        path=str(text_file_path.absolute()),
    )
    results = (
        rg.only_matching()
        .json()
        .unrestricted()
        .unrestricted()
        .unrestricted()
        .run()
        .as_dict
    )

    matches = []
    for result in results:
        matches.extend(
            [sub_match["match"]["text"] for sub_match in result["data"]["submatches"]]
        )

    return matches


def extract_addresses_from_file(
    text_file_path: Path,
    enabled_searchers: List[Literal["ripgrep", "python_re"]] = [
        "ripgrep",
        "python_re",
    ],
) -> List[str]:
    """
    Extracts bitcoin addresses from a file, using either ripgrep or Python re.

    Args:
    - text_file_path (Path): The path to the file to extract addresses from.
    - enabled_searchers (List[Literal["ripgrep", "python_re"]]): The searchers
        to use to extract the addresses. Defaults to ["ripgrep", "python_re"].

    Returns:
    - List[str]: A list of bitcoin addresses found in the file.
    """
    assert isinstance(text_file_path, Path)
    assert set(enabled_searchers).issubset({"ripgrep", "python_re"})
    assert len(enabled_searchers) > 0
    assert len(enabled_searchers) == len(set(enabled_searchers)), (
        f"Duplicate searchers in enabled_searchers: {enabled_searchers}"
    )

    for searcher in enabled_searchers:
        if searcher == "ripgrep":
            try:
                return _extract_addresses_from_file_ripgrep(text_file_path)
            except RipGrepNotFound:
                logger.warning("ripgrep not found. Trying another searcher.")
                continue

        elif searcher == "python_re":
            return _extract_addresses_from_file_python_re(text_file_path)

        else:
            raise ValueError(f"Invalid searcher provided: {searcher}")

    raise Exception("This should never be reached. Address extraction has failed.")


def scan_file_for_used_addresses(
    haystack_file_path: Path,
    needle_file_path: Path,
    index_chunk_size: int = DEFAULT_INDEX_CHUNK_SIZE,
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
    assert haystack_file_path.exists(), f"File not found: {haystack_file_path}"
    assert needle_file_path.exists(), f"File not found: {needle_file_path}"

    needle_addresses = extract_addresses_from_file(needle_file_path)
    logger.info(
        f"Extracted {len(needle_addresses):,} addresses from the needle file "
        f" ({needle_file_path})"
    )

    # remove duplicates (get distinct addresses)
    count_before_distinct = len(needle_addresses)
    needle_addresses = list(set(needle_addresses))
    count_after_distinct = len(needle_addresses)
    addr_count_change = count_after_distinct - count_before_distinct  # neg
    if addr_count_change != 0:
        logger.info(
            "By removing duplicates, address count changed "
            f"from {count_before_distinct:,} to {count_after_distinct:,}"
            f" ({addr_count_change:,} addresses)."
        )

    matched_addresses = search_multiple_in_file(
        haystack_file_path,
        needles=needle_addresses,
        index_chunk_size=index_chunk_size,
    )
    logger.info(f"Found {len(matched_addresses):,} used addresses in the file")
