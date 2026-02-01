__VERSION__ = "0.2.0"
__AUTHOR__ = "RecRanger"

from .cli import main_cli
from .index_create import (
    load_or_generate_index,
)
from .index_search import (
    search_in_file_with_index,
    search_multiple_in_file,  # <- main library function
)

__all__ = [
    "load_or_generate_index",
    "main_cli",
    "search_in_file_with_index",
    "search_multiple_in_file",
]
