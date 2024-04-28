from pathlib import Path
import gzip
from typing import Literal

from loguru import logger
from tqdm import tqdm
import xxhash
import orjson

from used_addr_check.optimized_file import OptimizedFilePreambleMetadata


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
        gzip_file_size_bytes=gzip_file_path.stat().st_size,
    )

    assert gzip_file_path.exists(), f"File not found: {gzip_file_path}"
    assert (
        gzip_file_path.suffix == ".gz"
    ), f"Invalid file extension: {gzip_file_path.suffix}"

    with open(output_path, "wb") as out_file:
        logger.info("Opened output file!")

        preamble_start_placeholder_msg = b"<<PREAMBLE_WILL_GO_HERE>>"
        preamble_end_placeholder_msg = b"<<PREAMBLE_WILL_END_HERE>>"
        preamble_middle_placeholder_msg = bytes.fromhex("00") * (
            metadata.preamble_length_bytes
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
            preamble_len_sum == metadata.preamble_length_bytes
        ), f"Invalid preamble length: {preamble_len_sum}"
        assert (
            out_file.tell() == metadata.preamble_length_bytes
        ), f"Invalid preamble length: {out_file.tell()}a"

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
            # Iterate through each line in the gzipped file
            for line in gz_file:
                if len(line) < 5:
                    logger.warning(f"Skipping short line: {line}")
                    continue

                # Compute the hash of the line
                hasher = hash_func()
                # NOTE: line contains the newline character at the end
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

                # Update progress bar
                if enable_tqdm:
                    progress_bar.n = gz_file_bin.tell()
                    if metadata.num_hashes % 1000000 == 0:
                        progress_bar.refresh()

        # gzip file is closed now. Update metadata.
        metadata.optimized_file_size_bytes = out_file.tell()

        logger.info(
            "Finished ingesting file: "
            f"{metadata.num_hashes:,} hashes and "
            f"{metadata.total_bytes_read:,} bytes read"
        )
        logger.info(f"Writing metadata to preamble: {metadata}")

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
