from pathlib import Path
import gzip
from typing import Literal

from loguru import logger
import numpy as np
from tqdm import tqdm
import orjson

from used_addr_check.optimized_file import (
    hash_name_to_size_and_func,
    OptimizedFilePreambleMetadata,
)


def ingest_raw_list_file(
    gzip_file_path: Path,
    output_path: Path,
    hash_algo: Literal["xxhash32", "xxhash64"] = "xxhash64",
    enable_tqdm: bool = True,
) -> None:
    logger.info(
        f"Ingesting file: {gzip_file_path} "
        f"to {output_path} with hash algorithm: {hash_algo}"
    )

    assert isinstance(gzip_file_path, Path)
    assert isinstance(output_path, Path)

    hash_size_bytes, hash_func = hash_name_to_size_and_func(hash_algo)

    metadata = OptimizedFilePreambleMetadata(
        hash_algo=hash_algo,
        hash_size_bytes=hash_size_bytes,
        endian="little",
        gzip_file_size_bytes=gzip_file_path.stat().st_size,
        is_sorted=True,
    )

    assert gzip_file_path.exists(), f"File not found: {gzip_file_path}"
    assert (
        gzip_file_path.suffix == ".gz"
    ), f"Invalid file extension: {gzip_file_path.suffix}"

    with (
        open(gzip_file_path, "rb") as gz_file_bin,
        gzip.GzipFile(fileobj=gz_file_bin, mode="rb") as gz_file,
        tqdm(
            desc="Ingesting file",
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            total=gzip_file_path.stat().st_size,
            disable=not enable_tqdm,
        ) as progress_bar,
    ):
        all_hashes: np.ndarray = np.array([], dtype=metadata.np_dtype)
        pending_hashes: list[int] = []
        # Iterate through each line in the gzipped file
        for line in gz_file:
            if len(line) < 5:
                logger.warning(f"Skipping short line: {line}")
                continue

            # Compute the hash of the line
            hasher = hash_func()
            # NOTE: line contains the newline character at the end
            hasher.update(line.strip())
            hash_value = hasher.intdigest()

            # Write the hash value to the output file as binary data
            pending_hashes.append(hash_value)
            if len(pending_hashes) >= 1000000:
                all_hashes = np.append(all_hashes, pending_hashes)
                pending_hashes = []

            # Update metadata
            metadata.num_hashes += 1
            metadata.total_bytes_read += len(line)

            # Update progress bar
            if enable_tqdm:
                if metadata.num_hashes % 1000000 == 0:
                    progress_bar.n = gz_file_bin.tell()
                    progress_bar.refresh()

        # Append the remaining hashes
        if len(pending_hashes) > 0:
            all_hashes = np.append(all_hashes, pending_hashes)

    # Closed the gzip file
    logger.info(
        "Finished ingesting file: "
        f"{metadata.num_hashes:,} hashes and "
        f"{metadata.total_bytes_read:,} bytes read"
    )
    logger.info(f"Writing metadata to preamble: {metadata}")

    # Sort the array
    logger.info(f"Sorting the array of {len(all_hashes):,} hashes.")
    all_hashes.sort()
    logger.info("Finished sorting the array.")

    # Check for duplicates (incantation)
    duplicate_count = np.count_nonzero(np.diff(all_hashes) == 0)
    if duplicate_count > 0:
        logger.warning(
            f"Found {duplicate_count} duplicate hashes in the array."
        )

    with open(output_path, "wb") as out_file:
        # at the end of the file, write the metadata to the preamble
        metadata_json: bytes = orjson.dumps(metadata)
        assert (
            len(metadata_json) <= metadata.preamble_length_bytes
        ), f"Metadata too large: {len(metadata_json)}"
        out_file.seek(0)
        out_file.write(metadata_json)
        out_file.write(
            bytes.fromhex("00")
            * (metadata.preamble_length_bytes - len(metadata_json))
        )
        assert (
            out_file.tell() == metadata.preamble_length_bytes
        ), f"Invalid preamble length: {out_file.tell()}"
        logger.info("Finished writing metadata to preamble.")

        # Write the hash value to the output file as binary data
        for array_slice in _yield_np_array_slices(all_hashes, 1000000):
            out_file.write(array_slice.tobytes())


def _yield_np_array_slices(array: np.ndarray, slice_size: int):
    for i in range(0, len(array), slice_size):
        yield array[i : i + slice_size]  # noqa: E203
