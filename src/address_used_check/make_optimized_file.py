from pathlib import Path
import gzip
from typing import Literal
from dataclasses import dataclass

from loguru import logger
from tqdm import tqdm
import xxhash
import orjson

OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES = 1024 * 16


@dataclass(kw_only=True)
class OptimizedFilePreambleMetadata:
    preamble_length_bytes: int = OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
    hash_algo: str
    hash_size_bytes: int
    endian: Literal["little", "big"]

    # stats (updated during ingest)
    num_hashes: int = 0
    total_bytes_read: int = 0
    # TODO: maybe store Search Tree stats here


def ingest_raw_list_file(
    gzip_file_path: Path,
    output_path: Path,
    hash_algo: Literal["xxhash32", "xxhash64"] = "xxhash64",
) -> None:
    logger.info(
        f"Ingesting file: {gzip_file_path} "
        f"to {output_path} with hash algorithm: {hash_algo}"
    )

    assert isinstance(gzip_file_path, Path)
    assert isinstance(output_path, Path)

    if hash_algo == "xxhash32":
        hash_func = xxhash.xxh32
        hash_size_bytes = 4
    elif hash_algo == "xxhash64":
        hash_func = xxhash.xxh64
        hash_size_bytes = 8
    else:
        raise ValueError(f"Invalid hash algorithm: {hash_algo}")

    metadata = OptimizedFilePreambleMetadata(
        hash_algo=hash_algo,
        hash_size_bytes=hash_size_bytes,
        endian="little",
    )

    assert gzip_file_path.exists(), f"File not found: {gzip_file_path}"
    assert (
        gzip_file_path.suffix == ".gz"
    ), f"Invalid file extension: {gzip_file_path.suffix}"

    estimated_total_addr = (
        gzip_file_path.stat().st_size * 3 // 35
    )  # assume 0.33 compression ratio, 35 bytes per line

    with open(output_path, "wb") as out_file:
        logger.info("Opened output file!")

        preamble_start_placeholder_msg = b"<<PREAMBLE_WILL_GO_HERE>>"
        preamble_end_placeholder_msg = b"<<PREAMBLE_WILL_END_HERE>>"
        preamble_middle_placeholder_msg = bytes.fromhex("00") * (
            OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
            - len(preamble_start_placeholder_msg)
            - len(preamble_end_placeholder_msg)
        )
        out_file.write(preamble_start_placeholder_msg)
        out_file.write(preamble_middle_placeholder_msg)
        out_file.write(preamble_end_placeholder_msg)

        preamble_len_sum = (
            len(preamble_start_placeholder_msg)
            + len(preamble_middle_placeholder_msg)
            + len(preamble_end_placeholder_msg)
        )
        assert (
            preamble_len_sum == OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
        ), f"Invalid preamble length: {preamble_len_sum}"
        assert (
            out_file.tell() == OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
        ), f"Invalid preamble length: {out_file.tell()}a"

        with gzip.open(gzip_file_path, "rb") as gz_file:
            # Iterate through each line in the gzipped file
            for line in tqdm(
                gz_file,
                unit="address",
                desc="Ingesting file",
                total=estimated_total_addr,
                unit_scale=True,
                unit_divisor=1024,
            ):
                # Note: line contains the newline character at the end

                # Compute the hash of the line
                hasher = hash_func()
                hasher.update(line.strip())
                # Retrieve the hash value as an integer
                hash_value = hasher.intdigest()
                # Write the hash value to the output file as binary data
                out_file.write(
                    hash_value.to_bytes(
                        metadata.hash_size_bytes, metadata.endian
                    )
                )

                # Update metadata
                metadata.num_hashes += 1
                metadata.total_bytes_read += len(line)

        logger.info(
            f"Finished ingesting file: {gzip_file_path} "
            f"with {metadata.num_hashes} hashes and "
            f"{metadata.total_bytes_read} bytes read"
        )
        logger.info(f"Writing metadata to preamble: {metadata}")

        # at the end of the file, write the metadata to the preamble
        metadata_json: bytes = orjson.dumps(metadata)
        assert (
            len(metadata_json) <= OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
        ), f"Metadata too large: {len(metadata_json)}"
        out_file.seek(0)
        out_file.write(metadata_json)
        out_file.write(
            bytes.fromhex("00")
            * (OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES - len(metadata_json))
        )
        assert (
            out_file.tell() == OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
        ), f"Invalid preamble length: {out_file.tell()}"
        logger.info("Finished writing metadata to preamble")
