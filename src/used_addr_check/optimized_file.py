from dataclasses import dataclass
from typing import Literal

DEFAULT_OPTIMIZED_FILE_PREAMBLE_LENGTH_BYTES = 1024 * 16
DEFAULT_OPTIMIZED_FILE_HASH_ENDIAN = "little"


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
