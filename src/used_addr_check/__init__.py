__VERSION__ = "0.1.6"
__AUTHOR__ = "RecRanger"

from .cli import main_cli
from .index_create import (
    generate_index,
    load_index_json,
    load_index_parquet,
    load_or_generate_index,
    store_index_json,
    store_index_parquet,
)
from .index_search import (
    search_in_file_with_index,
    search_multiple_in_file,  # <- main library function
)
from .index_types import IndexEntry

__all__ = [
    "IndexEntry",
    "generate_index",
    "load_index_json",
    "load_index_parquet",
    "load_or_generate_index",
    "main_cli",
    "search_in_file_with_index",
    "search_multiple_in_file",
    "store_index_json",
    "store_index_parquet",
]
