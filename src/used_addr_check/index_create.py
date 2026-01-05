from pathlib import Path
from typing import List

from loguru import logger
import orjson
import polars as pl
from tqdm import tqdm

from used_addr_check.index_types import IndexEntry
from used_addr_check.defaults import DEFAULT_INDEX_CHUNK_SIZE


def generate_index(
    haystack_file_path: Path, index_chunk_size: int = DEFAULT_INDEX_CHUNK_SIZE
) -> List[IndexEntry]:
    """
    Generates an index for a large sorted text file, storing every
    `index_chunk_size` lines.

    Args:
    - haystack_file_path (Path): Path to the file to be indexed.
    - index_chunk_size: The number of lines to store in each index entry.

    Returns:
    - List[IndexEntry]: A list of tuples containing
            line text, byte offset, and line number.
    """
    index: List[IndexEntry] = []
    haystack_file_size = haystack_file_path.stat().st_size
    with (
        tqdm(
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            total=haystack_file_size,
            desc="Indexing haystack file",
        ) as progress_bar,
        open(haystack_file_path, "rb") as file,
    ):
        offset = 0
        for line_number, line in enumerate(file):
            line_len = len(line)
            if line_number % index_chunk_size == 0:
                index.append(
                    IndexEntry(
                        line_value=line.strip().decode("utf-8"),
                        byte_offset=offset,
                        line_number=line_number,
                    )
                )

                progress_bar.n = offset
                progress_bar.refresh()

            offset += line_len
    return index


def store_index_json(index: List[IndexEntry], index_json_file_path: Path) -> None:
    """
    Stores the index in a file for later use.

    Args:
    - index (List[IndexEntry]): The index to store.
    - index_file_path (Path): The path to store the index.
    """

    with open(index_json_file_path, "wb") as file:
        file.write(orjson.dumps(index))


def load_index_json(index_json_file_path: Path) -> List[IndexEntry]:
    """
    Loads an index from a JSON file.

    Args:
    - index_json_file_path (Path): The path to the JSON file.

    Returns:
    - List[IndexEntry]: The loaded index.
    """
    with open(index_json_file_path, "rb") as file:
        raw_read: List[dict] = orjson.loads(file.read())
    return [IndexEntry(**val) for val in raw_read]


def store_index_parquet(index: List[IndexEntry], index_parquet_file_path: Path) -> None:
    """
    Stores the index in a parquet file for later use.

    Args:
    - index (List[IndexEntry]): The index to store.
    - index_file_path (Path): The path to store the index.
    """

    df = pl.DataFrame(index)
    df.write_parquet(index_parquet_file_path)


def load_index_parquet(index_parquet_file_path: Path) -> List[IndexEntry]:
    """
    Loads an index from a parquet file.

    Args:
    - index_parquet_file_path (Path): The path to the parquet file.

    Returns:
    - List[IndexEntry]: The loaded index.
    """
    df = pl.read_parquet(index_parquet_file_path)
    return [IndexEntry(**val) for val in df.to_dicts()]


def load_or_generate_index(
    haystack_file_path: Path,
    index_chunk_size: int = DEFAULT_INDEX_CHUNK_SIZE,
    force_recreate: bool = False,
) -> List[IndexEntry]:
    """Attempts to load an index from a file, or generates one if it doesn't,
    or if `force_recreate` is enabled.

    Tries to load the index from a Parquet file first, then from a JSON file.

    If a file already exists, the `index_chunk_size` is ignored.
    """
    index_json_file_path = haystack_file_path.with_suffix(".index.json")
    index_parquet_file_path = haystack_file_path.with_suffix(".index.parquet")

    if force_recreate or (
        not index_json_file_path.exists() and not index_parquet_file_path.exists()
    ):
        logger.info(f"Creating index for file: {haystack_file_path.name}")
        index = generate_index(haystack_file_path, index_chunk_size=index_chunk_size)
        logger.info(f"Index created with {len(index):,} entries")

        # store to main type (parquet)
        store_index_parquet(index, index_parquet_file_path)
        logger.info(
            f"Index stored in {index_parquet_file_path.name}, "
            f"size: {index_parquet_file_path.stat().st_size:,} bytes"
        )
        return index

    if index_parquet_file_path.exists():
        logger.info("Loading index from Parquet file")
        index = load_index_parquet(index_parquet_file_path)
        logger.info(f"Index loaded with {len(index):,} entries")
        return index

    if index_json_file_path.exists():
        logger.info("Loading index from JSON file")
        index = load_index_json(index_json_file_path)
        logger.info(f"Index loaded with {len(index):,} entries")
        return index

    raise Exception("This should be unreachable.")
