from dataclasses import dataclass
from typing import Callable, Literal
from pathlib import Path
import re

import numpy as np
import orjson
import xxhash

DEFAULT_OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES = 1024 * 16
DEFAULT_OPTIMIZED_FILE_HASH_ENDIAN = "little"


def hash_name_to_size_and_func(hash_name: str) -> tuple[int, Callable]:
    if hash_name == "xxhash32":
        return 4, xxhash.xxh32
    elif hash_name == "xxhash64":
        return 8, xxhash.xxh64
    else:
        raise ValueError(f"Unsupported hash algorithm: {hash_name}")


def hash_info_to_array_type_code(hash_size_bytes: int, endian: str) -> str:
    """Returns the array type code for the hash size,
    usable in array.array(...) constructor."""
    if hash_size_bytes == 4:
        size_code = "I"
    elif hash_size_bytes == 8:
        size_code = "Q"
    else:
        raise ValueError(f"Unsupported hash size: {hash_size_bytes}")

    if endian == "little":
        return f"<{size_code}"
    elif endian == "big":
        return f">{size_code}"
    else:
        raise ValueError(f"Unsupported endian: {endian}")


@dataclass(kw_only=True)
class OptimizedFilePreambleMetadata:
    preamble_length_bytes: int = DEFAULT_OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES
    hash_algo: str
    hash_size_bytes: int
    endian: Literal["little", "big"] = DEFAULT_OPTIMIZED_FILE_HASH_ENDIAN

    # stats (updated during ingest)
    num_hashes: int = 0
    total_bytes_read: int = 0
    gzip_file_size_bytes: int = 0
    optimized_file_size_bytes: int = 0
    # TODO: maybe store Search Tree stats here

    @property
    def array_type_code(self) -> str:
        # codes: https://docs.python.org/3/library/array.html
        if self.hash_size_bytes == 4:
            return "L"
        elif self.hash_size_bytes == 8:
            return "Q"
        else:
            raise ValueError(f"Unsupported hash size: {self.hash_size_bytes}")

    @property
    def np_dtype(self):
        if self.hash_size_bytes == 4:
            return np.uint32
        elif self.hash_size_bytes == 8:
            return np.uint64
        else:
            raise ValueError(f"Unsupported hash size: {self.hash_size_bytes}")

    @property
    def hash_func(self) -> Callable:
        hash_size_bytes, _hash_func = hash_name_to_size_and_func(
            self.hash_algo
        )
        assert hash_size_bytes == self.hash_size_bytes
        return _hash_func

    @classmethod
    def from_dict(cls, data: dict) -> "OptimizedFilePreambleMetadata":
        return cls(**data)

    @classmethod
    def from_json(
        cls, json_str: str | bytes | bytearray
    ) -> "OptimizedFilePreambleMetadata":
        return cls.from_dict(orjson.loads(json_str))

    @classmethod
    def from_file_preamble(
        cls, file_path: Path
    ) -> "OptimizedFilePreambleMetadata":

        # Read the JSON at the start of the file to find the
        # 'preamble_length_bytes' value, which we'll use to read the rest.
        with open(file_path, "rb") as fp:
            start_bytes = fp.read(200)
        start_bytes_str = start_bytes.decode("ascii", errors="replace")

        # Find the JSON string in the first 200 bytes
        match = re.search(
            r"\"preamble_length_bytes\"\s*[:]\s*(?P<val>\d+)", start_bytes_str
        )
        if not match:
            raise ValueError(
                "Could not find 'preamble_length_bytes' in the file."
            )

        # Extract the value and convert it to an integer
        preamble_length_bytes = int(match.group("val"))

        # Read the JSON at the start of the file
        with open(file_path, "rb") as fp:
            preamble_bytes = fp.read(preamble_length_bytes)
        preamble_bytes = preamble_bytes.strip(bytes.fromhex("00"))

        metadata = cls.from_json(preamble_bytes)
        assert (
            metadata.preamble_length_bytes == preamble_length_bytes
        ), "Regex search failed, or metadata is corrupt."
        return metadata
